"""
General type definitions
"""

import base64
import random
import re
from typing import Any, NamedTuple, Optional, Protocol


class Address(NamedTuple):
    """A peer IP address"""

    host: str
    port: int

    @classmethod
    def from_string(cls, address: str) -> "Address":
        host, port = address.rsplit(":", 1)
        return cls(host, int(port))


class GameLaunchOptions(NamedTuple):
    """Additional options used to configure the FA lobby"""

    mapname: Optional[str] = None
    team: Optional[int] = None
    faction: Optional[int] = None
    expected_players: Optional[int] = None
    map_position: Optional[int] = None
    game_options: Optional[dict[str, Any]] = None


class MapPoolMap(Protocol):
    id: int
    weight: int

    def get_map(self) -> "Map": ...


class Map(NamedTuple):
    id: Optional[int]
    map_pool_map_version_id: Optional[int]
    folder_name: str
    ranked: bool = False
    # Map pool only
    weight: int = 1

    @property
    def file_path(self):
        """A representation of the map name as it looks in the database"""
        return f"maps/{self.folder_name}.zip"

    @property
    def scenario_file(self):
        return f"/maps/{self.folder_name}/{self.folder_name}_scenario.lua"

    def get_map(self) -> "Map":
        return self


class NeroxisGeneratedMap(NamedTuple):
    id: int
    map_pool_map_version_id: Optional[int]
    version: str
    spawns: int
    map_size_pixels: int
    weight: int = 1

    _NAME_PATTERN = re.compile(
        "neroxis_map_generator_([0-9.]+)_([a-z2-7]+)_([a-z2-7]+)"
    )

    @classmethod
    def is_neroxis_map(cls, folder_name: str) -> bool:
        """Check if mapname is map generator"""
        return cls._NAME_PATTERN.fullmatch(folder_name) is not None

    @classmethod
    def of(cls, params: dict, weight: int = 1):
        """Create a NeroxisGeneratedMap from params dict"""
        assert params["type"] == "neroxis"

        map_size_pixels = int(params["size"])

        if map_size_pixels <= 0:
            raise Exception("Map size is zero or negative")

        if map_size_pixels % 64 != 0:
            raise Exception("Map size is not a multiple of 64")

        spawns = int(params["spawns"])
        if spawns % 2 != 0:
            raise Exception("spawns is not a multiple of 2")

        version = params["version"]
        return cls(
            cls._get_id(version, spawns, map_size_pixels),
            version,
            spawns,
            map_size_pixels,
            weight,
        )

    @staticmethod
    def _get_id(version: str, spawns: int, map_size_pixels: int) -> int:
        return -int.from_bytes(
            bytes(
                f"{version}_{spawns}_{map_size_pixels}",
                encoding="ascii"
            ),
            "big"
        )

    def get_map(self) -> Map:
        """
        Generate a map name based on the version and parameters. If invalid
        parameters are specified hand back None
        """
        seed_bytes = random.getrandbits(64).to_bytes(8, "big")
        seed_str = base64.b32encode(seed_bytes).decode("ascii").replace("=", "").lower()

        size_byte = (self.map_size_pixels // 64).to_bytes(1, "big")
        spawn_byte = self.spawns.to_bytes(1, "big")
        option_bytes = spawn_byte + size_byte
        option_str = base64.b32encode(option_bytes).decode("ascii").replace("=", "").lower()

        folder_name = f"neroxis_map_generator_{self.version}_{seed_str}_{option_str}"

        return Map(
            id=self.id,
            folder_name=folder_name,
            ranked=True,
            weight=self.weight,
        )


# Default for map argument in game/game_service. Games are only created without
# the map argument in unit tests.
MAP_DEFAULT = Map(
    id=None,
    map_pool_map_version_id=None,
    folder_name="scmp_007",
    ranked=False,
)
