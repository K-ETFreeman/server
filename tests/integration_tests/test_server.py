import asyncio
import contextlib
import re

import pytest
from sqlalchemy import and_, select

from server.config import config
from server.db.models import avatars, avatars_list, ban, friends_and_foes
from server.protocol import DisconnectedError
from tests.utils import fast_forward

from .conftest import (
    connect_and_sign_in,
    connect_client,
    connect_mq_consumer,
    get_session,
    perform_login,
    read_until,
    read_until_command
)
from .test_game import (
    host_game,
    join_game,
    open_fa,
    send_player_options,
    setup_game_1v1
)


@fast_forward(10)
async def test_server_proxy_mode(lobby_server_proxy, proxy_server, caplog):
    with caplog.at_level("TRACE"):
        _, _, proto = await connect_and_sign_in(
            ("test", "test_password"),
            lobby_server_proxy,
            address=proxy_server.sockets[0].getsockname()
        )
        await read_until_command(proto, "game_info", timeout=5)

    matches = [
        re.search(
            r"Client connected from \d+\.\d+\.\d+\.\d+:\d+ via proxy \d+\.\d+\.\d+\.\d+:\d+",
            message
        )
        for message in caplog.messages
        if "Client connected from" in message
    ]
    assert matches and matches[0]


async def test_server_proxy_mode_direct(lobby_server_proxy, caplog):
    with caplog.at_level("TRACE"):
        proto = await connect_client(lobby_server_proxy)
        with pytest.raises(DisconnectedError):
            await get_session(proto)

    assert "this may indicate a misconfiguration" in caplog.text


@fast_forward(10)
async def test_login_timeout(lobby_server, monkeypatch, caplog):
    monkeypatch.setattr(config, "LOGIN_TIMEOUT", 5)

    proto = await connect_client(lobby_server)

    await asyncio.sleep(5)

    with pytest.raises(DisconnectedError), caplog.at_level("TRACE"):
        await proto.read_message()

    assert "Client took too long to log in" in caplog.text


@fast_forward(10)
async def test_disconnect_before_login_timeout(
    lobby_server,
    monkeypatch,
    caplog
):
    monkeypatch.setattr(config, "LOGIN_TIMEOUT", 5)

    proto = await connect_client(lobby_server)

    await asyncio.sleep(1)

    with pytest.raises(DisconnectedError), caplog.at_level("TRACE"):
        await proto.close()
        await asyncio.sleep(4)
        await proto.read_message()

    assert "Client disconnected" in caplog.text
    assert "Client took too long to log in" not in caplog.text


async def test_server_deprecated_client(lobby_server):
    proto = await connect_client(lobby_server)

    await proto.send_message({
        "command": "ask_session",
        "user_agent": "faf-client",
        "version": "0.0.0"
    })
    msg = await proto.read_message()

    assert msg["command"] == "notice"

    proto = await connect_client(lobby_server)
    await proto.send_message({"command": "ask_session", "version": "0.0.0"})
    msg = await proto.read_message()

    assert msg["command"] == "notice"


async def test_very_long_message(lobby_server, caplog):
    _, _, proto = await connect_and_sign_in(("test", "test_password"), lobby_server)

    DATA_SIZE = 2 ** 20
    # We don't want to capture the TRACE of our massive message
    with caplog.at_level("DEBUG"):
        await proto.send_message({
            "command": "invalid",
            "data": "B" * DATA_SIZE}
        )
        await read_until_command(proto, "invalid")


async def test_old_client_error(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )

    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "InitiateTest",
        "target": "connectivity"
    })
    msg = await proto.read_message()
    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Your client version is no longer supported. Please update to the newest version: https://faforever.com"
    }

    error_msg = {
        "command": "notice",
        "style": "error",
        "text": "Cannot join game. Please update your client to the newest version."
    }
    await proto.send_message({"command": "game_host"})
    msg = await proto.read_message()
    assert msg == error_msg

    await proto.send_message({"command": "game_join"})
    msg = await proto.read_message()
    assert msg == error_msg

    await proto.send_message({"command": "game_matchmaking", "state": "start"})
    msg = await proto.read_message()
    assert msg == error_msg


@fast_forward(50)
async def test_ping_message(lobby_server):
    _, _, proto = await connect_and_sign_in(("test", "test_password"), lobby_server)

    # We should receive the message every 45 seconds
    await read_until_command(proto, "ping", timeout=46)


@fast_forward(10)
async def test_graceful_shutdown(
    lobby_instance,
    lobby_server,
    tmp_user,
):
    _, _, proto = await connect_and_sign_in(
        await tmp_user("Player"),
        lobby_server
    )
    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "matchmaker_info"
    })
    msg = await read_until_command(proto, "matchmaker_info", timeout=5)
    assert msg["queues"]

    await proto.send_message({
        "command": "game_matchmaking",
        "state": "start",
        "queue_name": "ladder1v1"
    })
    await read_until_command(proto, "search_info", state="start", timeout=5)

    await lobby_instance.graceful_shutdown()

    # First we get the notice message
    msg = await read_until_command(proto, "notice", timeout=5)
    assert "The server will be shutting down" in msg["text"]

    # Then the queues are shut down
    await read_until_command(proto, "search_info", state="stop", timeout=5)

    # Matchmaker info should not include any queues
    await proto.send_message({
        "command": "matchmaker_info"
    })
    msg = await read_until_command(proto, "matchmaker_info", timeout=5)
    assert msg == {
        "command": "matchmaker_info",
        "queues": []
    }

    # Hosting custom games should be disabled
    await proto.send_message({
        "command": "game_host",
        "visibility": "public",
    })
    msg = await read_until_command(proto, "disabled", timeout=5)
    assert msg == {
        "command": "disabled",
        "request": "game_host"
    }

    # Joining matchmaker queues should be disabled
    await proto.send_message({
        "command": "game_host",
        "visibility": "public",
    })
    msg = await read_until_command(proto, "disabled", timeout=5)
    assert msg == {
        "command": "disabled",
        "request": "game_host"
    }


@fast_forward(10)
async def test_graceful_shutdown_kick(
    lobby_instance,
    lobby_server,
    tmp_user,
    monkeypatch
):
    monkeypatch.setattr(config, "SHUTDOWN_KICK_IDLE_PLAYERS", True)

    async def test_connected(proto) -> bool:
        try:
            await proto.send_message({"command": "ping"})
            await read_until_command(proto, "pong", timeout=5)
            return True
        except (DisconnectedError, ConnectionError, asyncio.TimeoutError):
            return False

    _, _, idle_proto = await connect_and_sign_in(
        await tmp_user("Idle"),
        lobby_server
    )
    _, _, host_proto = await connect_and_sign_in(
        await tmp_user("Host"),
        lobby_server
    )
    player1_id, _, player1_proto = await connect_and_sign_in(
        await tmp_user("Player"),
        lobby_server
    )
    player2_id, _, player2_proto = await connect_and_sign_in(
        await tmp_user("Player"),
        lobby_server
    )
    protos = (
        idle_proto,
        host_proto,
        player1_proto,
        player2_proto
    )
    for proto in protos:
        await read_until_command(proto, "game_info")

    # Host is in lobby, not playing a game
    await host_game(host_proto, visibility="public")

    # Player1 and Player2 are playing a game
    await setup_game_1v1(
        player1_proto,
        player1_id,
        player2_proto,
        player2_id
    )

    await lobby_instance.graceful_shutdown()

    # Check that everyone is notified
    for proto in protos:
        msg = await read_until_command(proto, "notice")
        assert "The server will be shutting down" in msg["text"]

    # Players in lobby should be told to close their games
    await read_until_command(host_proto, "notice", style="kill")

    # Idle players are kicked every 1 second
    await asyncio.sleep(1)

    assert await test_connected(idle_proto) is False
    assert await test_connected(host_proto) is False

    assert await test_connected(player1_proto) is True
    assert await test_connected(player2_proto) is True


@fast_forward(60)
async def test_drain(
    lobby_instance,
    lobby_server,
    tmp_user,
    monkeypatch,
    caplog,
):
    monkeypatch.setattr(config, "SHUTDOWN_GRACE_PERIOD", 10)

    player1_id, _, player1_proto = await connect_and_sign_in(
        await tmp_user("Player"),
        lobby_server
    )
    player2_id, _, player2_proto = await connect_and_sign_in(
        await tmp_user("Player"),
        lobby_server
    )
    await setup_game_1v1(
        player1_proto,
        player1_id,
        player2_proto,
        player2_id
    )

    await lobby_instance.drain()

    assert "Graceful shutdown period ended! 1 games are still live!" in caplog.messages


@fast_forward(15)
async def test_player_info_broadcast(lobby_server):
    p1 = await connect_client(lobby_server)
    p2 = await connect_client(lobby_server)

    await perform_login(p1, ("test", "test_password"))
    await perform_login(p2, ("Rhiza", "puff_the_magic_dragon"))

    await read_until(
        p2,
        lambda m: (
            m["command"] == "player_info"
            and any(map(lambda d: d["login"] == "test", m["players"]))
        )
    )

    await p1.close()
    await read_until(
        p2,
        lambda m: (
            m["command"] == "player_info"
            and any(map(
                lambda d: d["login"] == "test" and d.get("state") == "offline",
                m["players"]
            ))
        )
    )


@pytest.mark.rabbitmq
@fast_forward(5)
async def test_player_info_broadcast_to_rabbitmq(lobby_server, channel):
    mq_proto = await connect_mq_consumer(
        lobby_server,
        channel,
        "broadcast.playerInfo.update"
    )
    mq_proto_all = await connect_mq_consumer(
        lobby_server,
        channel,
        "broadcast.*.update"
    )

    _, _, proto = await connect_and_sign_in(
        ("test", "test_password"), lobby_server
    )
    await read_until_command(proto, "player_info")

    await read_until(
        mq_proto, lambda m: "player_info" in m.values()
        and any(map(lambda d: d["login"] == "test", m["players"]))
    )
    await read_until(
        mq_proto_all, lambda m: "player_info" in m.values()
        and any(map(lambda d: d["login"] == "test", m["players"]))
    )


@fast_forward(5)
async def test_info_broadcast_authenticated(lobby_server):
    proto1 = await connect_client(lobby_server)
    proto2 = await connect_client(lobby_server)
    proto3 = await connect_client(lobby_server)

    await perform_login(proto1, ("test", "test_password"))
    await perform_login(proto2, ("Rhiza", "puff_the_magic_dragon"))
    await proto1.send_message({
        "command": "game_matchmaking",
        "state": "start",
        "mod": "ladder1v1",
        "faction": "uef"
    })
    # Will timeout if the message is never received
    await read_until_command(proto2, "matchmaker_info")
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(proto3.read_message(), 0.2)
        # Unauthenticated connections should not receive the message
        assert False


@fast_forward(5)
async def test_game_info_not_sent_to_foes(lobby_server):
    _, _, proto1 = await connect_and_sign_in(
        ("test", "test_password"), lobby_server
    )
    await read_until_command(proto1, "game_info")

    await host_game(proto1, title="No Foes Allowed", visibility="public")
    msg = await read_until_command(proto1, "game_info")

    assert msg["featured_mod"] == "faf"
    assert msg["title"] == "No Foes Allowed"
    assert msg["visibility"] == "public"

    _, _, proto2 = await connect_and_sign_in(
        ("foed_by_test", "foe"), lobby_server
    )
    # Check game info sent during login
    msg2 = await read_until_command(proto2, "game_info")
    assert msg2["games"] == []

    # Trigger a game_info message
    await proto1.send_message({
        "target": "game",
        "command": "ClearSlot",
        "args": [2]
    })

    await read_until_command(proto1, "game_info")
    # Foe should not see the update
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto2, "game_info", timeout=1)


@fast_forward(10)
async def test_game_info_sent_to_friends(lobby_server):
    # test is the friend of friends
    _, _, proto1 = await connect_and_sign_in(
        ("friends", "friends"), lobby_server
    )
    await read_until_command(proto1, "game_info")

    await host_game(proto1, title="Friends Only", visibility="friends")

    # Host should see their own game
    msg = await read_until_command(proto1, "game_info", state="open")
    assert msg["featured_mod"] == "faf"
    assert msg["title"] == "Friends Only"
    assert msg["visibility"] == "friends"

    _, _, proto2 = await connect_and_sign_in(
        ("test", "test_password"), lobby_server
    )
    _, _, proto3 = await connect_and_sign_in(
        ("Rhiza", "puff_the_magic_dragon"), lobby_server
    )
    # Check game info sent during login
    msg2 = await read_until_command(proto2, "game_info")
    msg3 = await read_until_command(proto3, "game_info")

    # The hosts friend should see the game
    assert msg2["games"]
    assert msg2["games"][0] == msg

    # However, the other person should not
    assert msg3["games"] == []

    # Trigger a game_info message
    await proto1.send_message({
        "target": "game",
        "command": "ClearSlot",
        "args": [2]
    })

    # The hosts friend should see the update
    await read_until_command(proto2, "game_info")

    # However, the other person should not
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto3, "game_info", timeout=1)


@pytest.mark.parametrize("limit", (
    (None, 1000),
    (1500, 1700),
    (1500, None),
))
@fast_forward(5)
async def test_game_info_not_broadcast_out_of_rating_range(lobby_server, limit):
    # Rhiza has displayed rating of 1462
    _, _, proto1 = await connect_and_sign_in(
        ("test", "test_password"), lobby_server
    )
    _, _, proto2 = await connect_and_sign_in(
        ("Rhiza", "puff_the_magic_dragon"), lobby_server
    )
    await read_until_command(proto1, "game_info")
    await read_until_command(proto2, "game_info")

    await proto1.send_message({
        "command": "game_host",
        "title": "No noobs!",
        "mod": "faf",
        "visibility": "public",
        "rating_min": limit[0],
        "rating_max": limit[1],
        "enforce_rating_range": True
    })

    msg = await read_until_command(proto1, "game_info")

    assert msg["featured_mod"] == "faf"
    assert msg["title"] == "No noobs!"
    assert msg["visibility"] == "public"

    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto2, "game_info", timeout=1)


@fast_forward(10)
async def test_game_info_broadcast_to_players_in_lobby(lobby_server):
    # test is the friend of friends
    friends_id, _, proto1 = await connect_and_sign_in(
        ("friends", "friends"), lobby_server
    )
    test_id, _, proto2 = await connect_and_sign_in(
        ("test", "test_password"), lobby_server
    )
    await read_until_command(proto1, "game_info")
    await read_until_command(proto2, "game_info")

    await proto1.send_message({
        "command": "game_host",
        "title": "Friends Only",
        "mod": "faf",
        "visibility": "friends"
    })

    # The host and his friend should see the game
    await read_until_command(proto1, "game_info", teams={})
    await read_until_command(proto2, "game_info", teams={})
    # The host joins which changes the lobby state
    await open_fa(proto1)
    await send_player_options(
        proto1,
        [friends_id, "Army", 1],
        [friends_id, "Team", 1],
    )
    msg = await read_until_command(proto1, "game_info", teams={"1": ["friends"]})
    msg2 = await read_until_command(proto2, "game_info", teams={"1": ["friends"]})

    assert msg == msg2
    assert msg["featured_mod"] == "faf"
    assert msg["title"] == "Friends Only"
    assert msg["visibility"] == "friends"
    assert msg["state"] == "open"

    game_id = msg["uid"]
    await join_game(proto2, game_id)

    await read_until_command(proto1, "game_info", teams={"1": ["friends"]})
    await read_until_command(proto2, "game_info", teams={"1": ["friends"]})
    await send_player_options(proto1, [test_id, "Army", 1], [test_id, "Team", 1])
    await read_until_command(proto1, "game_info", teams={"1": ["friends", "test"]})
    await read_until_command(proto2, "game_info", teams={"1": ["friends", "test"]})

    # Now we unfriend the person in the lobby
    await proto1.send_message({
        "command": "social_remove",
        "friend": test_id
    })
    # And change some game options to trigger a new update message
    await proto1.send_message({
        "target": "game",
        "command": "GameOption",
        "args": ["Title", "New Title"]
    })

    # The host and the other player in the lobby should see the game even
    # though they are not friends anymore
    msg = await read_until_command(proto1, "game_info", timeout=5)
    msg2 = await read_until_command(proto2, "game_info", timeout=5)

    assert msg == msg2
    assert msg["featured_mod"] == "faf"
    assert msg["title"] == "New Title"
    assert msg["visibility"] == "friends"


@pytest.mark.rabbitmq
@fast_forward(10)
async def test_info_broadcast_to_rabbitmq(lobby_server, channel):
    mq_proto_all = await connect_mq_consumer(
        lobby_server,
        channel,
        "broadcast.*.update"
    )

    _, _, proto = await connect_and_sign_in(
        ("test", "test_password"), lobby_server
    )
    await read_until_command(proto, "game_info", timeout=10)
    # matchmaker_info is broadcast whenever the timer pops
    await read_until_command(mq_proto_all, "matchmaker_info")

    # Check that game_info is broadcast when a new game is hosted
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(mq_proto_all, "game_info", timeout=3)

    await host_game(proto)
    await read_until_command(mq_proto_all, "game_info")


@pytest.mark.parametrize("user", [
    ("test", "test_password"),
    ("ban_revoked", "ban_revoked"),
    ("ban_expired", "ban_expired"),
    ("No_UID", "his_pw")
])
@fast_forward(10)
async def test_game_host_authenticated(lobby_server, user):
    _, _, proto = await connect_and_sign_in(user, lobby_server)
    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "game_host",
        "title": "My Game",
        "mod": "faf",
        "visibility": "public",
    })

    msg = await read_until_command(proto, "game_launch", timeout=10)

    assert msg["mod"] == "faf"
    assert "args" in msg
    assert isinstance(msg["uid"], int)


@fast_forward(10)
async def test_game_host_missing_fields(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )

    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "game_host",
        "mod": "",
        "visibility": "public",
        "title": "",
    })

    msg = await read_until_command(proto, "game_info", timeout=10)

    assert msg["title"] == "test's game"
    assert msg["game_type"] == "custom"
    assert msg["mapname"] == "scmp_007"
    assert msg["map_file_path"] == "maps/scmp_007.zip"
    assert msg["featured_mod"] == "faf"


@fast_forward(10)
async def test_game_host_name_only_spaces(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )

    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "game_host",
        "mod": "",
        "visibility": "public",
        "title": "    ",
    })

    msg = await read_until_command(proto, "notice", timeout=10)

    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Title must not be empty.",
    }


@fast_forward(10)
async def test_game_host_name_non_ascii(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )

    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "game_host",
        "mod": "",
        "visibility": "public",
        "title": "ÇÒÖL GÃMÊ"
    })

    msg = await read_until_command(proto, "notice", timeout=10)

    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Title must contain only ascii characters."
    }


@fast_forward(30)
async def test_game_host_then_queue(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )
    await read_until_command(proto, "game_info")

    # Send game_host but don't send GameState: Idle, then immediately queue
    await proto.send_message({
        "command": "game_host",
        "title": "My Game",
        "mod": "faf",
        "visibility": "public",
    })
    await proto.send_message({
        "command": "game_matchmaking",
        "state": "start",
        "faction": "uef"
    })

    msg = await read_until_command(proto, "notice", timeout=10)
    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Can't join a queue while test is in state STARTING_GAME",
    }


@fast_forward(10)
async def test_play_game_while_queueing(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )
    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "game_matchmaking",
        "state": "start",
        "faction": "uef"
    })

    await proto.send_message({"command": "game_host"})
    msg = await read_until_command(proto, "notice")
    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Can't host a game while in state SEARCHING_LADDER"
    }

    await proto.send_message({"command": "game_join"})
    msg = await read_until_command(proto, "notice")
    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Can't join a game while in state SEARCHING_LADDER"
    }

    await proto.send_message({"command": "restore_game_session"})
    msg = await read_until_command(proto, "notice")
    assert msg == {
        "command": "notice",
        "style": "error",
        "text": "Can't reconnect to a game while in state SEARCHING_LADDER"
    }


@fast_forward(10)
async def test_restore_game_session_no_game(lobby_server):
    player_id, session, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )
    await read_until_command(proto, "game_info")

    await proto.send_message({"command": "restore_game_session", "game_id": 42})

    msg = await read_until_command(proto, "notice")
    assert msg == {
        "command": "notice",
        "style": "info",
        "text": "The game you were connected to no longer exists"
    }


@pytest.mark.parametrize("command", ["game_host", "game_join"])
async def test_server_ban_prevents_hosting(lobby_server, database, command):
    """
    Players who are banned while they are online, should immediately be
    prevented from joining or hosting games until their ban expires.
    """
    player_id, _, proto = await connect_and_sign_in(
        ("banme", "banme"), lobby_server
    )
    # User successfully logs in
    await read_until_command(proto, "game_info")

    async with database.acquire() as conn:
        await conn.execute(
            ban.insert().values(
                player_id=player_id,
                author_id=player_id,
                reason="Test live ban",
                expires_at=None,
                level="GLOBAL"
            )
        )

    await proto.send_message({"command": command})

    msg = await proto.read_message()
    assert msg == {
        "command": "notice",
        "style": "error",
        "text": (
            "You are banned from FAF forever. <br>Reason: <br>Test live ban<br>"
            "<br><i>If you would like to appeal this ban, please send an email "
            "to: moderation@faforever.com</i>"
        )
    }


@fast_forward(5)
async def test_coop_list(lobby_server):
    _, _, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )

    await read_until_command(proto, "game_info")

    await proto.send_message({"command": "coop_list"})

    msg = await read_until_command(proto, "coop_info")
    assert "name" in msg
    assert "description" in msg
    assert "filename" in msg


async def test_ice_servers_empty(lobby_server):
    _, _, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )

    await read_until_command(proto, "game_info")

    await proto.send_message({"command": "ice_servers"})

    msg = await read_until_command(proto, "ice_servers")

    # By default the server config should not have any ice servers
    assert msg == {
        "command": "ice_servers",
        "ice_servers": []
    }


async def get_player_selected_avatars(conn, player_id):
    return await conn.execute(
        select(avatars.c.id, avatars_list.c.url)
        .select_from(avatars_list.join(avatars))
        .where(
            and_(
                avatars.c.idUser == player_id,
                avatars.c.selected == 1,
            )
        )
    )


@fast_forward(30)
async def test_avatar_list_empty(lobby_server):
    _, _, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )
    await read_until_command(proto, "game_info")

    await proto.send_message({
        "command": "avatar", "action": "list_avatar"
    })
    msg = await read_until_command(proto, "avatar")

    assert msg == {
        "command": "avatar",
        "avatarlist": []
    }


@fast_forward(30)
async def test_avatar_select(lobby_server, database):
    # This user has multiple avatars in the test data
    player_id, _, proto = await connect_and_sign_in(
        ("player_service1", "player_service1"),
        lobby_server
    )
    await read_until_command(proto, "game_info")
    # Skip any latent player broadcasts
    with contextlib.suppress(asyncio.TimeoutError):
        await read_until_command(proto, "player_info", timeout=5)

    await proto.send_message({
        "command": "avatar", "action": "list_avatar"
    })

    msg = await read_until_command(proto, "avatar")
    avatar_list = msg["avatarlist"]

    for avatar in avatar_list:
        await proto.send_message({
            "command": "avatar",
            "action": "select",
            "avatar": avatar["url"]
        })
        msg = await read_until_command(proto, "player_info")
        assert msg["players"][0]["avatar"] == avatar

    async with database.acquire() as conn:
        result = await get_player_selected_avatars(conn, player_id)
        assert result.rowcount == 1
        row = result.fetchone()
        assert row.url == avatar["url"]

    await proto.send_message({
        "command": "avatar",
        "action": "select",
        "avatar": "BOGUS!"
    })
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto, "player_info", timeout=10)

    async with database.acquire() as conn:
        result = await get_player_selected_avatars(conn, player_id)
        assert result.rowcount == 1
        row = result.fetchone()
        assert row.url == avatar["url"]


@fast_forward(30)
async def test_avatar_select_not_owned(lobby_server, database):
    # This user has no avatars
    player_id, _, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server
    )
    await read_until_command(proto, "game_info")
    # Skip any latent player broadcasts
    with contextlib.suppress(asyncio.TimeoutError):
        await read_until_command(proto, "player_info", timeout=5)

    await proto.send_message({
        "command": "avatar",
        "action": "select",
        "avatar": "https://content.faforever.com/faf/avatars/UEF.png"
    })
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto, "player_info", timeout=10)

    async with database.acquire() as conn:
        result = await get_player_selected_avatars(conn, player_id)
        assert result.rowcount == 0


@fast_forward(30)
async def test_social_add_friend(lobby_server, database):
    subject_id = 10
    player_id, _, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server,
    )
    await read_until_command(proto, "game_info")

    async with database.acquire() as conn:
        result = await conn.execute(
            select(friends_and_foes)
            .where(
                and_(
                    friends_and_foes.c.user_id == player_id,
                    friends_and_foes.c.subject_id == subject_id,
                )
            )
        )
        row = result.fetchone()
        assert row is None

    # Other player doesn't even need to be online
    await proto.send_message({
        "command": "social_add",
        "friend": subject_id,
    })
    await asyncio.sleep(5)

    async with database.acquire() as conn:
        result = await conn.execute(
            select(friends_and_foes)
            .where(
                and_(
                    friends_and_foes.c.user_id == player_id,
                    friends_and_foes.c.subject_id == subject_id,
                )
            )
        )
        row = result.fetchone()

        assert row.subject_id == subject_id
        assert row.status == "FRIEND"


@fast_forward(30)
async def test_social_add_foe(lobby_server, database):
    subject_id = 10
    player_id, _, proto = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server,
    )
    await read_until_command(proto, "game_info")

    async with database.acquire() as conn:
        result = await conn.execute(
            select(friends_and_foes)
            .where(
                and_(
                    friends_and_foes.c.user_id == player_id,
                    friends_and_foes.c.subject_id == subject_id,
                )
            )
        )
        row = result.fetchone()
        assert row is None

    # Other player doesn't even need to be online
    await proto.send_message({
        "command": "social_add",
        "foe": subject_id,
    })
    await asyncio.sleep(5)

    async with database.acquire() as conn:
        result = await conn.execute(
            select(friends_and_foes)
            .where(
                and_(
                    friends_and_foes.c.user_id == player_id,
                    friends_and_foes.c.subject_id == subject_id,
                )
            )
        )
        row = result.fetchone()

        assert row.subject_id == subject_id
        assert row.status == "FOE"


@fast_forward(30)
async def test_social_add_friend_while_hosting(lobby_server):
    _, _, proto1 = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server,
    )
    rhiza_id, _, proto2 = await connect_and_sign_in(
        ("Rhiza", "puff_the_magic_dragon"),
        lobby_server,
    )
    await read_until_command(proto1, "game_info")
    await read_until_command(proto2, "game_info")

    game_id = await host_game(proto1, visibility="friends")
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto2, "game_info", timeout=10, uid=game_id)

    await proto1.send_message({
        "command": "social_add",
        "friend": rhiza_id,
    })

    await read_until_command(
        proto2,
        "game_info",
        timeout=10,
        uid=game_id,
        state="open",
    )


@fast_forward(30)
async def test_social_add_foe_while_hosting(lobby_server):
    _, _, proto1 = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server,
    )
    rhiza_id, _, proto2 = await connect_and_sign_in(
        ("Rhiza", "puff_the_magic_dragon"),
        lobby_server,
    )
    await read_until_command(proto1, "game_info")
    await read_until_command(proto2, "game_info")

    game_id = await host_game(proto1)
    await read_until_command(proto2, "game_info", timeout=10, uid=game_id)

    await proto1.send_message({
        "command": "social_add",
        "foe": rhiza_id,
    })

    await read_until_command(
        proto2,
        "game_info",
        timeout=10,
        uid=game_id,
        state="closed",
    )


@fast_forward(30)
async def test_social_remove_friend_while_hosting(lobby_server):
    _, _, proto1 = await connect_and_sign_in(
        ("friends", "friends"),
        lobby_server,
    )
    test_id, _, proto2 = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server,
    )
    await read_until_command(proto1, "game_info")
    await read_until_command(proto2, "game_info")

    game_id = await host_game(proto1, visibility="friends")
    await read_until_command(proto2, "game_info", timeout=5, uid=game_id)

    await proto1.send_message({
        "command": "social_remove",
        "friend": test_id,
    })

    await read_until_command(
        proto2,
        "game_info",
        timeout=10,
        uid=game_id,
        state="closed",
    )


@fast_forward(30)
async def test_social_remove_foe_while_hosting(lobby_server):
    _, _, proto1 = await connect_and_sign_in(
        ("test", "test_password"),
        lobby_server,
    )
    rhiza_id, _, proto2 = await connect_and_sign_in(
        ("foed_by_test", "foe"),
        lobby_server,
    )
    await read_until_command(proto1, "game_info")
    await read_until_command(proto2, "game_info")

    game_id = await host_game(proto1)
    with pytest.raises(asyncio.TimeoutError):
        await read_until_command(proto2, "game_info", timeout=5, uid=game_id)

    await proto1.send_message({
        "command": "social_remove",
        "foe": rhiza_id,
    })

    await read_until_command(
        proto2,
        "game_info",
        timeout=10,
        uid=game_id,
        state="open",
    )
