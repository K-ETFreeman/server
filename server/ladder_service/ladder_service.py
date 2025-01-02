"""
Manages interactions between players and matchmakers
"""
import asyncio
import json
import random
import re
import statistics
from collections import defaultdict
from typing import Awaitable, Callable, Optional, Iterable

import aiocron
import humanize
from sqlalchemy import and_, func, select, text, true

from server import metrics
from server.config import MAP_POOL_RATING_SELECTION_FUNCTIONS, config
from server.core import Service
from server.db import FAFDatabase
from server.db.models import (
    game_featuredMods,
    game_player_stats,
    game_stats,
    leaderboard,
    leaderboard_rating_journal,
    map_pool,
    map_pool_map_version,
    map_version,
    matchmaker_queue,
    matchmaker_queue_game,
    matchmaker_queue_map_pool
)
from server.decorators import with_logger
from server.exceptions import DisabledError
from server.game_service import GameService
from server.games import InitMode, LadderGame
from server.games.ladder_game import GameClosedError
from server.ladder_service.game_name import game_name
from server.ladder_service.violation_service import ViolationService
from server.matchmaker import (
    MapPool,
    MatchmakerQueueMapPool,
    MatchmakerQueue,
    OnMatchedCallback,
    Search
)
from server.metrics import MatchLaunch
from server.player_service import PlayerService
from server.players import Player, PlayerState
from server.types import GameLaunchOptions, Map, NeroxisGeneratedMap, MatchmakerQueueMapPoolVetoData


@with_logger
class LadderService(Service):
    """
    Service responsible for managing the automatches. Does matchmaking, updates
    statistics, and launches the games.
    """

    def __init__(
        self,
        database: FAFDatabase,
        game_service: GameService,
        player_service: PlayerService,
        violation_service: ViolationService,
    ):
        self._db = database
        self._informed_players: set[Player] = set()
        self.game_service = game_service
        self.player_service = player_service
        self.queues = {}
        self.pools_veto_data: list[MatchmakerQueueMapPoolVetoData] = []
        self.violation_service = violation_service

        self._searches: dict[Player, dict[str, Search]] = defaultdict(dict)
        self._allow_new_searches = True

    async def initialize(self) -> None:
        await self.update_data()
        self._update_cron = aiocron.crontab("*/10 * * * *", func=self.update_data)

    async def update_data(self) -> None:
        async with self._db.acquire() as conn:
            map_pool_maps = await self.fetch_map_pools(conn)
            db_queues = await self.fetch_matchmaker_queues(conn)

        for name, info in db_queues.items():
            if name not in self.queues:
                queue = MatchmakerQueue(
                    self.game_service,
                    self.on_match_found,
                    name=name,
                    queue_id=info["id"],
                    featured_mod=info["mod"],
                    rating_type=info["rating_type"],
                    team_size=info["team_size"],
                    params=info.get("params")
                )
                self.queues[name] = queue
                queue.initialize()
            else:
                queue = self.queues[name]
                queue.featured_mod = info["mod"]
                queue.rating_type = info["rating_type"]
                queue.team_size = info["team_size"]
                queue.rating_peak = await self.fetch_rating_peak(info["rating_type"])
            queue.map_pools.clear()
            for map_pool_id, min_rating, max_rating, veto_tokens_per_player, max_tokens_per_map, minimum_maps_after_veto in info["map_pools"]:
                map_pool_name, map_list = map_pool_maps[map_pool_id]
                if not map_list:
                    self._logger.warning(
                        "Map pool '%s' is empty! Some %s games will "
                        "likely fail to start!",
                        map_pool_name,
                        name
                    )
                queue.add_map_pool(
                    MatchmakerQueueMapPool(
                        MapPool(map_pool_id, map_pool_name, map_list),
                        min_rating,
                        max_rating,
                        veto_tokens_per_player,
                        max_tokens_per_map,
                        minimum_maps_after_veto
                    )
                )
        # Remove queues that don't exist anymore
        for queue_name in list(self.queues.keys()):
            if queue_name not in db_queues:
                self.queues[queue_name].shutdown()
                del self.queues[queue_name]
        prev_pools_veto_data = self.pools_veto_data
        new_pools_veto_data = self.get_pools_veto_data()
        if (new_pools_veto_data != prev_pools_veto_data):
            self.pools_veto_data = new_pools_veto_data
            for player in self.player_service.all_players:
                await player.update_vetoes(self.pools_veto_data)

    async def fetch_map_pools(self, conn) -> dict[int, tuple[str, list[Map]]]:
        result = await conn.execute(
            select(
                map_pool.c.id,
                map_pool.c.name,
                map_pool_map_version.c.id.label("map_pool_map_version_id"),
                map_pool_map_version.c.weight,
                map_pool_map_version.c.map_params,
                map_version.c.id.label("map_id"),
                map_version.c.filename,
                map_version.c.ranked,
            ).select_from(
                map_pool.outerjoin(map_pool_map_version)
                .outerjoin(map_version)
            )
        )
        map_pool_maps = {}
        for row in result:
            id_ = row.id
            name = row.name
            if id_ not in map_pool_maps:
                map_pool_maps[id_] = (name, list())
            _, map_list = map_pool_maps[id_]
            if row.map_id is not None:
                # Database filenames contain the maps/ prefix and .zip suffix.
                # This comes from the content server which hosts the files at
                # https://content.faforever.com/maps/name.zip
                folder_name = re.match(r"maps/(.+)\.zip", row.filename).group(1)
                map_list.append(
                    Map(
                        id=row.map_id,
                        map_pool_map_version_id=row.map_pool_map_version_id,
                        folder_name=folder_name,
                        ranked=row.ranked,
                        weight=row.weight,
                    )
                )
            elif row.map_params is not None:
                try:
                    params = json.loads(row.map_params)
                    map_type = params["type"]
                    if map_type == "neroxis":
                        map_list.append(
                            NeroxisGeneratedMap.of(params, row.weight, row.map_pool_map_version_id)
                        )
                    else:
                        self._logger.warning(
                            "Unsupported map type %s in pool %s",
                            map_type,
                            row.id
                        )

                except Exception:
                    self._logger.warning(
                        "Failed to load map in map pool %d. "
                        "Parameters are '%s'",
                        row.id,
                        row.map_params,
                        exc_info=True
                    )

        return map_pool_maps

    async def fetch_matchmaker_queues(self, conn):
        result = await conn.execute(
            select(
                matchmaker_queue.c.id,
                matchmaker_queue.c.technical_name,
                matchmaker_queue.c.team_size,
                matchmaker_queue.c.params,
                matchmaker_queue_map_pool.c.map_pool_id,
                matchmaker_queue_map_pool.c.min_rating,
                matchmaker_queue_map_pool.c.max_rating,
                matchmaker_queue_map_pool.c.veto_tokens_per_player,
                matchmaker_queue_map_pool.c.max_tokens_per_map,
                matchmaker_queue_map_pool.c.minimum_maps_after_veto,
                game_featuredMods.c.gamemod,
                leaderboard.c.technical_name.label("rating_type")
            )
            .select_from(
                matchmaker_queue
                .join(matchmaker_queue_map_pool)
                .join(game_featuredMods)
                .join(leaderboard)
            ).where(matchmaker_queue.c.enabled == true())
        )
        # So we don't log the same error multiple times when a queue has several
        # map pools
        errored = set()
        matchmaker_queues = defaultdict(lambda: defaultdict(list))
        for row in result:
            name = row.technical_name
            if name in errored:
                continue
            info = matchmaker_queues[name]
            try:
                info["id"] = row.id
                info["mod"] = row.gamemod
                info["rating_type"] = row.rating_type
                info["team_size"] = row.team_size
                info["params"] = json.loads(row.params) if row.params else None
                info["map_pools"].append((
                    row.map_pool_id,
                    row.min_rating,
                    row.max_rating,
                    row.veto_tokens_per_player,
                    row.max_tokens_per_map,
                    row.minimum_maps_after_veto
                ))
            except Exception:
                self._logger.warning(
                    "Unable to load queue '%s'!",
                    name,
                    exc_info=True
                )
                del matchmaker_queues[name]
                errored.add(name)
        return matchmaker_queues

    async def fetch_rating_peak(self, rating_type):
        async with self._db.acquire() as conn:
            result = await conn.execute(
                select(
                    leaderboard_rating_journal.c.rating_mean_before,
                    leaderboard_rating_journal.c.rating_deviation_before
                )
                .select_from(leaderboard_rating_journal.join(leaderboard))
                .where(leaderboard.c.technical_name == rating_type)
                .order_by(leaderboard_rating_journal.c.id.desc())
                .limit(1000)
            )
            rows = result.fetchall()
            rowcount = len(rows)

            rating_peak = 1000.0
            if rowcount > 0:
                rating_peak = statistics.mean(
                    row.rating_mean_before - 3 * row.rating_deviation_before for row in rows
                )
            metrics.leaderboard_rating_peak.labels(rating_type).set(rating_peak)

            if rowcount < 100:
                self._logger.warning(
                    "Could only fetch %s ratings for %s queue.",
                    rowcount,
                    rating_type
                )

            if rating_peak < 600 or rating_peak > 1200:
                self._logger.warning(
                    "Estimated rating peak for %s is %s. This could lead to issues with matchmaking.",
                    rating_type,
                    rating_peak
                )
            else:
                self._logger.info(
                    "Estimated rating peak for %s is %s.",
                    rating_type,
                    rating_peak
                )

            return rating_peak

    def start_search(
        self,
        players: list[Player],
        queue_name: str,
        on_matched: OnMatchedCallback = lambda _1, _2: None
    ):
        if not self._allow_new_searches:
            raise DisabledError()

        timeouts = self.violation_service.get_violations(players)
        if timeouts:
            self._logger.debug("timeouts: %s", timeouts)
            times = [
                {
                    "player": p.id,
                    "expires_at": violation.get_ban_expiration().isoformat()
                }
                for p, violation in timeouts.items()
            ]
            for player in players:
                player.write_message({
                    "command": "search_timeout",
                    "timeouts": times
                })
                # TODO: Do we need this or is `search_timeout` enough?
                player.write_message({
                    "command": "search_info",
                    "queue_name": queue_name,
                    "state": "stop"
                })
                # For compatibility with clients that don't understand
                # `search_timeout` only. This may be removed at any time.
                if len(times) == 1:
                    s = ""
                    are = "is"
                else:
                    s = "s"
                    are = "are"
                names = ", ".join(p.login for p in timeouts)
                max_time = humanize.naturaldelta(
                    max(
                        timeouts.values(),
                        key=lambda v: v.get_ban_expiration()
                    ).get_remaining()
                )
                player.write_message({
                    "command": "notice",
                    "style": "info",
                    "text": f"Player{s} {names} {are} timed out for {max_time}"
                })
            return
        # Cancel any existing searches that players have for this queue
        for player in players:
            if queue_name in self._searches[player]:
                self._cancel_search(player, queue_name)

        queue = self.queues[queue_name]
        search = Search(
            players,
            rating_type=queue.rating_type,
            on_matched=on_matched
        )

        for player in players:
            player.state = PlayerState.SEARCHING_LADDER

            self.write_rating_progress(player, queue.rating_type)

            player.write_message({
                "command": "search_info",
                "queue_name": queue_name,
                "state": "start"
            })

            self._searches[player][queue_name] = search

        self._logger.info("%s started searching for %s", search, queue_name)

        asyncio.create_task(queue.search(search))

    def cancel_search(
        self,
        initiator: Player,
        queue_name: Optional[str] = None
    ) -> None:
        if queue_name is None:
            queue_names = list(self._searches[initiator].keys())
        else:
            queue_names = [queue_name]

        for queue_name in queue_names:
            self._cancel_search(initiator, queue_name)

    def _cancel_search(self, initiator: Player, queue_name: str) -> None:
        """
        Cancel search for a specific player/queue.
        """
        cancelled_search = self._clear_search(initiator, queue_name)
        if cancelled_search is None:
            self._logger.debug(
                "Ignoring request to cancel a search that does not exist: "
                "%s, %s",
                initiator,
                queue_name
            )
            return
        cancelled_search.cancel()

        for player in cancelled_search.players:
            player.write_message({
                "command": "search_info",
                "queue_name": queue_name,
                "state": "stop"
            })
            if (
                not self._searches[player]
                and player.state == PlayerState.SEARCHING_LADDER
            ):
                player.state = PlayerState.IDLE
        self._logger.info(
            "%s stopped searching for %s", cancelled_search, queue_name
        )

    def _clear_search(
        self,
        initiator: Player,
        queue_name: str
    ) -> Optional[Search]:
        """
        Remove a search from the searches dictionary.

        Does NOT cancel the search.
        """
        search = self._searches[initiator].get(queue_name)

        if search is not None:
            for player in search.players:
                del self._searches[player][queue_name]

        return search

    def write_rating_progress(self, player: Player, rating_type: str) -> None:
        if player not in self._informed_players:
            self._informed_players.add(player)
            _, deviation = player.ratings[rating_type]

            if deviation > 490:
                player.write_message({
                    "command": "notice",
                    "style": "info",
                    "text": (
                        "<i>Welcome to the matchmaker</i><br><br><b>The "
                        "matchmaking system needs to calibrate your skill level; "
                        "your first few games may be more imbalanced as the "
                        "system attempts to learn your capability as a player."
                        "</b><br><b>"
                        "Afterwards, you'll be more reliably matched up with "
                        "people of your skill level: so don't worry if your "
                        "first few games are uneven. This will improve as you "
                        "play!</b>"
                    )
                })

    def on_match_found(
        self,
        s1: Search,
        s2: Search,
        queue: MatchmakerQueue
    ) -> None:
        """
        Callback for when a match is generated by a matchmaker queue.

        NOTE: This function is called while the matchmaker search lock is held,
        so it should only perform fast operations.
        """
        try:
            msg = {"command": "match_found", "queue_name": queue.name}

            for player in s1.players + s2.players:
                player.state = PlayerState.STARTING_AUTOMATCH
                player.write_message(msg)

                # Cancel any other searches
                queue_names = list(
                    name for name in self._searches[player].keys()
                    if name != queue.name
                )
                for queue_name in queue_names:
                    self._cancel_search(player, queue_name)

                self._clear_search(player, queue.name)

            asyncio.create_task(self.start_game(s1.players, s2.players, queue))
        except Exception:
            self._logger.exception(
                "Error processing match between searches %s, and %s",
                s1, s2
            )

    def start_game(
        self,
        team1: list[Player],
        team2: list[Player],
        queue: MatchmakerQueue
    ) -> Awaitable[None]:
        # We want assertion errors to trigger when the caller attempts to
        # create the async function, not when the function starts executing.
        assert len(team1) == len(team2)

        return self._start_game(team1, team2, queue)

    async def _start_game(
        self,
        team1: list[Player],
        team2: list[Player],
        queue: MatchmakerQueue
    ) -> None:
        self._logger.debug(
            "Starting %s game between %s and %s",
            queue.name,
            [p.login for p in team1],
            [p.login for p in team2]
        )
        game = None
        try:
            host = team1[0]
            all_players = team1 + team2
            all_guests = all_players[1:]

            played_map_ids = await self.get_game_history(
                all_players,
                queue.id,
                limit=config.LADDER_ANTI_REPETITION_LIMIT
            )

            def get_displayed_rating(player: Player) -> float:
                return player.ratings[queue.rating_type].displayed()

            ratings = (get_displayed_rating(player) for player in all_players)
            func = MAP_POOL_RATING_SELECTION_FUNCTIONS.get(
                config.MAP_POOL_RATING_SELECTION,
                statistics.mean
            )
            rating = func(ratings)

            pool = queue.get_map_pool_for_rating(rating)
            if not pool:
                raise RuntimeError(f"No map pool available for rating {rating}!")

            pool, *_, max_tokens_per_map, minimum_maps_after_veto = queue.map_pools[pool.id]

            vetoes_map = defaultdict(int)

            for m in pool.maps.values():
                for player in all_players:
                    vetoes_map[m.map_pool_map_version_id] += player.vetoes.get(m.map_pool_map_version_id, 0)

            if (max_tokens_per_map == 0):
                max_tokens_per_map = self.calculate_dynamic_tokens_per_map(minimum_maps_after_veto, vetoes_map.values())
                # this should never happen actually so i am not sure do we need this here or not
                if (max_tokens_per_map == 0):
                    self._logger.error("calculate_dynamic_tokens_per_map received impossible vetoes setup, all vetoes cancelled for a match")
                    vetoes_map = {}
                    max_tokens_per_map = 1
            game_map = pool.choose_map(played_map_ids, vetoes_map, max_tokens_per_map)

            game = self.game_service.create_game(
                game_class=LadderGame,
                game_mode=queue.featured_mod,
                host=host,
                name="Matchmaker Game",
                map=game_map,
                matchmaker_queue_id=queue.id,
                rating_type=queue.rating_type,
                max_players=len(all_players),
            )
            game.init_mode = InitMode.AUTO_LOBBY
            game.set_name_unchecked(game_name(team1, team2))

            team1 = sorted(team1, key=get_displayed_rating)
            team2 = sorted(team2, key=get_displayed_rating)

            # Shuffle the teams such that direct opponents remain the same
            zipped_teams = list(zip(team1, team2))
            random.shuffle(zipped_teams)

            for i, player in enumerate(
                player for pair in zipped_teams for player in pair
            ):
                # FA uses lua and lua arrays are 1-indexed
                slot = i + 1
                # 2 if even, 3 if odd
                team = (i % 2) + 2
                player.game = game

                # Set player options without triggering the logic for
                # determining that players have actually connected to the game.
                game._player_options[player.id]["Faction"] = player.faction.value
                game._player_options[player.id]["Team"] = team
                game._player_options[player.id]["StartSpot"] = slot
                game._player_options[player.id]["Army"] = slot
                game._player_options[player.id]["Color"] = slot

            game_options = queue.get_game_options()
            if game_options:
                game.game_options.update(game_options)

            self._logger.debug("Starting ladder game: %s", game)

            def make_game_options(player: Player) -> GameLaunchOptions:
                return GameLaunchOptions(
                    mapname=game_map.folder_name,
                    expected_players=len(all_players),
                    game_options=game_options,
                    team=game.get_player_option(player.id, "Team"),
                    faction=game.get_player_option(player.id, "Faction"),
                    map_position=game.get_player_option(player.id, "StartSpot")
                )

            await self.launch_match(game, host, all_guests, make_game_options)
            self._logger.debug("Ladder game launched successfully %s", game)
            metrics.matches.labels(queue.name, MatchLaunch.SUCCESSFUL).inc()
        except Exception as e:
            abandoning_players = []
            if isinstance(e, NotConnectedError):
                self._logger.info(
                    "Ladder game failed to start! %s setup timed out",
                    game
                )
                metrics.matches.labels(queue.name, MatchLaunch.TIMED_OUT).inc()
                abandoning_players = e.players
            elif isinstance(e, GameClosedError):
                self._logger.info(
                    "Ladder game %s failed to start! "
                    "Player %s closed their game instance",
                    game, e.player
                )
                metrics.matches.labels(queue.name, MatchLaunch.ABORTED_BY_PLAYER).inc()
                abandoning_players = [e.player]
            else:
                # All timeout errors should be transformed by the match starter.
                assert not isinstance(e, asyncio.TimeoutError)

                self._logger.exception("Ladder game failed to start %s", game)
                metrics.matches.labels(queue.name, MatchLaunch.ERRORED).inc()

            if game:
                await game.on_game_finish()

            game_id = game.id if game else None
            msg = {"command": "match_cancelled", "game_id": game_id}
            for player in all_players:
                player.write_message(msg)

            if abandoning_players:
                self._logger.info(
                    "Players failed to connect: %s",
                    abandoning_players
                )
                self.violation_service.register_violations(abandoning_players)

    async def launch_match(
        self,
        game: LadderGame,
        host: Player,
        guests: list[Player],
        make_game_options: Callable[[Player], GameLaunchOptions]
    ):
        # Launch the host
        if host.lobby_connection is None:
            raise NotConnectedError([host])

        host.lobby_connection.write_launch_game(
            game,
            is_host=True,
            options=make_game_options(host)
        )

        try:
            await game.wait_hosted(60)
        except asyncio.TimeoutError:
            raise NotConnectedError([host])
        finally:
            # TODO: Once the client supports `match_cancelled`, don't
            # send `launch_game` to the client if the host timed out. Until
            # then, failing to send `launch_game` will cause the client to
            # think it is searching for ladder, even though the server has
            # already removed it from the queue.

            # Launch the guests
            not_connected_guests = [
                player for player in guests
                if player.lobby_connection is None
            ]
            if not_connected_guests:
                raise NotConnectedError(not_connected_guests)

            for guest in guests:
                assert guest.lobby_connection is not None

                guest.lobby_connection.write_launch_game(
                    game,
                    is_host=False,
                    options=make_game_options(guest)
                )
        try:
            await game.wait_launched(60 + 10 * len(guests))
        except asyncio.TimeoutError:
            connected_players = game.get_connected_players()
            raise NotConnectedError([
                player for player in guests
                if player not in connected_players
            ])

    """
        function finds minimal max_tokens_per_map > 0 for given M (minimal_maps_after_veto) > 0 and [iterable] of veto tokens applied for each map in the bracket
        max_tokens_per_map - is the amount of veto_tokens required to fully ban a map
        minimal_maps_after_veto - minimal sum of map weights, anything lower is forbidden (for map diversity purpuses)

        lets rename max_tokens_per_map to T for simplicity
        then weight of map with V tokens applied is max((T - V) / T, 0)

        example: lets say we have A maps with 0 tokens applied, B Maps with 1 token, C Maps with 2 tokens
        the inequality to be true:
        A * max((T - 0) / T, 0) + B * max((T - 1) / T, 0) + C * max((T - 2) / T, 0) >= M

        max((T - 0) / T, 0) is always 1, so:
        B * max((T - 1) / T, 0) + C * max((T - 2) / T, 0) >= M - A

        due to max() function, it splits to 3:
        1) for 0 < T <= 1: 0 >= M - A which is the same as A >= M
        2) for 1 < T <= 2: B * (T - 1) / T >= M - A
        3) for T > 2: B * (T - 1) / T + C * (T - 2) / T >= M - A

        since we are looking for minimal T > 0, we should just check out cases from min T to max T

        in case 1): range T <= 1
          here we trying to find answer using only 0-tokens maps
          - if A >= M, then any value 0 < T <= 1 is valid answer, but to avoid division by zero errors, we return 1
            a bit contradicts with "minimal" in function definition, but since veto tokens applied is always an integer, result of 1 will give the same veto system result as 0.0001
          - if A < M, there is no answer in this case

        [1] for every next case: M > A is true (otherwise we would return answer in case 1)
        in case 2): range 1 < T <= 2
          here we trying to find answer using maps with 0-1 tokens
          B * (T - 1) / T >= M - A
          B * (T - 1) >= (M - A) * T
          BT - B - MT + AT >= 0
          (B - M + A)T >= B
          T >= B / (B + A - M)
          note 1: B + A is just amount of maps to which we try to shrink bracket (only maps with 0 and 1 tokens applied)
                  so we can say that denominator is "map balance", amount of map to which we try to shrink the bracket minus the minimal amount it should be,
                  so if map balance is negative, the answer should be obviously discarded
                  in case if map balance is 0 answer also should be discarded because B > 0 and division by zero happens
          note 2: since M is guaranteed to be > A due to [1], then A - M is negative, then B + A - M < B, so B / (B + A - M) > 1
          thus, since we're looking for MINIMAL answer, we should just replace > with = :
          T = B / (B + A - M)
          its always > 1 so we can forget lower border issue in 1 < T <= 2 above
          just calculating T, checking if its <= 2, if true - return T, if false - case 3

        [2] for every next case: B / (B + A - M) > 2 which is the same as B > 2B + 2A - 2M
        in case 3): range T > 2
            here we trying to find answer using maps with 0-2 tokens
            B * (T - 1) / T + C * (T - 2) / T >= M - A
            BT - B + CT - 2C >= (M - A) * T
            (B + C + A - M)T >= B + 2C
            T >= (B + 2C) / (B + C + A - M)
            note 1: now we can see pattern in nominator: its just sum of tokens applied to all maps to wich we currently try to shrink the bracket
                    its basically 0A + 1B in case 2, and 0A + 1B + 2C in case 3, you can return to A B C definitions above to be sure
            note 2: denominator is the same as in case 2, map balance. Again, A - M is negative, so B + C + A - M < B + C
            let prove that there is no need to worry about lower border, again:
            lets assume that (B + 2C) / (B + C + A - M) < 2
            then B + 2C < 2B + 2C + 2A - 2M
            then B < 2B + 2A - 2M
            which is contradicts with [2], so again, we can just replace >= with = :
            T = (B + 2C) / (B + C + A - M)
            just calulating it and return the value


        case X)
            if we had some number D of maps with X tokens, and in case 3 we received result more than X (and discarded it due to exceeding upper border), then we would say that
            [3] (B + 2C) / (B + C + A - M) > X which is the same as B + 2C > XB + XC + XA - XM
            and our X equation would have lower border of X ofcourse
            T = (B + 2C + XD) / (D + B + C + A - M)
            now lets prove that T > X:
            lets assume that T < X 
            (B + 2C + XD) / (D + B + C + A - M) < X
            B + 2C + XD < XD + XB + XC + XA - XM
            B + 2C < XB + XC + XA - XM
            which is contradicts with [3]
            so, we just proved that we should not worry about lower border in any case
            and just solve equations and not inequality for each case except case 1

        notices:
           1) in every case except case 1, nominator is just sum of tokens applied to all maps in equation
            and denominator is map balance
           2) for case 1 we always have tokens_sum equal to 0

        conclusion: whole solution process is
           1) sorting tokens applied in ascending order
           cycle:
             2) including next bunch of maps (with the same tokens applied value) to our group
             3) checking if tokens_sum == 0 
                then its case 1, and return 1 if maps_balance > 0
             4) otherwise
                solving equation for current group
                and checking the result vs upper border
                and upper border is equal to the amount of tokens applied to the map next to last map in our group, or infinity if there is no such one
    """
    def calculate_dynamic_tokens_per_map(self, M: float, tokens: Iterable[int]) -> float:
        sorted_tokens = sorted(tokens)
        # adding infinity as last upper border
        sorted_tokens.append(float('inf'))
        # t is the maximum amount of tokens applied to any single map in current group
        t = 0
        tokens_sum = 0
        for index, tokens in enumerate(sorted_tokens):
            # if at [index] our current group is ended
            if tokens > t:
                maps_balance = index - M
                # if our group is only 0-tokened maps
                if tokens_sum == 0 and maps_balance >= 0:
                    return 1
                if maps_balance > 0:
                    # solving the equation
                    candidate = tokens_sum / maps_balance
                    # checking it vs upper border
                    if candidate <= tokens:
                        return candidate
                t = tokens
            tokens_sum += tokens

        # return 0 never happens for correct tokens - M pairs
        return 0

    def get_pools_veto_data(self) -> list[MatchmakerQueueMapPoolVetoData]:
        result = []
        for queue in self.queues.values():
            for pool, *_, veto_tokens_per_player, max_tokens_per_map, minimum_maps_after_veto in queue.map_pools.values():
                if max_tokens_per_map == 0 and minimum_maps_after_veto >= len(pool.maps.values()) \
                   or max_tokens_per_map != 0 and queue.team_size * 2 * veto_tokens_per_player / max_tokens_per_map > len(pool.maps.values()) - minimum_maps_after_veto:
                    veto_tokens_per_player = 0
                    max_tokens_per_map = 1
                    minimum_maps_after_veto = 1
                    self._logger.error(f"Wrong vetoes setup detected for pool {pool.id} in queue {queue.id}")
                result.append(
                    MatchmakerQueueMapPoolVetoData(
                        map_pool_map_version_ids=[map.map_pool_map_version_id for map in pool.maps.values()],
                        veto_tokens_per_player=veto_tokens_per_player,
                        max_tokens_per_map=max_tokens_per_map,
                        minimum_maps_after_veto=minimum_maps_after_veto
                    )
                )
        return result

    async def get_game_history(
        self,
        players: list[Player],
        queue_id: int,
        limit: int = 3
    ) -> list[int]:
        async with self._db.acquire() as conn:
            result = []
            for player in players:
                query = select(
                    game_stats.c.mapId,
                ).select_from(
                    game_player_stats
                    .join(game_stats)
                    .join(matchmaker_queue_game)
                ).where(
                    and_(
                        game_player_stats.c.playerId == player.id,
                        game_stats.c.startTime >= func.DATE_SUB(
                            func.now(),
                            text("interval 1 day")
                        ),
                        matchmaker_queue_game.c.matchmaker_queue_id == queue_id
                    )
                ).order_by(
                    game_stats.c.startTime.desc(),
                    # Timestamps only have second resolution, so for this to
                    # work correctly in the unit tests we also need id
                    game_stats.c.id.desc()
                ).limit(limit)

                result.extend([
                    row.mapId for row in await conn.execute(query)
                ])
        return result

    def on_connection_lost(self, conn: "LobbyConnection") -> None:
        if not conn.player:
            return

        player = conn.player
        self.cancel_search(player)
        del self._searches[player]
        if player in self._informed_players:
            self._informed_players.remove(player)

    async def graceful_shutdown(self):
        self._allow_new_searches = False

        for queue in self.queues.values():
            queue.shutdown()

        for player, searches in self._searches.items():
            for queue_name in list(searches.keys()):
                self._cancel_search(player, queue_name)


class NotConnectedError(asyncio.TimeoutError):
    def __init__(self, players: list[Player]):
        self.players = players
