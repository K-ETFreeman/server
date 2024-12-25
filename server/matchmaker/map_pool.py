import random
from collections import Counter
from typing import Iterable

from ..decorators import with_logger
from ..types import Map, MapPoolMap


@with_logger
class MapPool(object):
    def __init__(
        self,
        map_pool_id: int,
        name: str,
        maps: Iterable[MapPoolMap] = ()
    ):
        self.id = map_pool_id
        self.name = name
        self.set_maps(maps)

    def set_maps(self, maps: Iterable[MapPoolMap]) -> None:
        self.maps = {map_.id: map_ for map_ in maps}

    def choose_map(self, played_map_ids: Iterable[int]=(), vetoesMap=None, max_tokens_per_map=1) -> Map:
        if vetoesMap is None:
            vetoesMap = {}
        """
        Select a random map using veto system weights. 
        The maps which are least played from played_map_ids 
        and not vetoed by any player are getting x2 weight multiplier.
        """
        if not self.maps:
            self._logger.critical(
                "Trying to choose a map from an empty map pool: %s", self.name
            )
            raise RuntimeError(f"Map pool {self.name} not set!")

        # Make sure the counter has every available map
        counter = Counter(self.maps.keys())
        counter.update(id_ for id_ in played_map_ids if id_ in self.maps)

        least_common = counter.most_common()[::-1]
        least_count = 1
        for id_, count in least_common:
            if isinstance(self.maps[id_], Map):
                least_count = count
                break

        # Trim off the maps with higher play counts
        for i, (_, count) in enumerate(least_common):
            if count > least_count:
                least_common = least_common[:i]
                break


        least_common_ids = {id_ for id_, _ in least_common}     
        
        # Multiply weight by 2 if map is least common and not vetoed by anyone
        mapList = list((map.map_pool_map_version_id, map, 2 if (map.id in least_common_ids) and (vetoesMap.get(map.map_pool_map_version_id,0) == 0) else 1) for id, map in self.maps.items())
        
        weights = [max(0, (1 - vetoesMap.get(id, 0) / max_tokens_per_map) * map.weight * least_common_multiplier) for id, map, least_common_multiplier in mapList]
        return random.choices(mapList, weights=weights, k=1)[0][1]

    def __repr__(self) -> str:
        return f"MapPool({self.id}, {self.name}, {list(self.maps.values())})"
