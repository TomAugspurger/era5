import stactools.core

from stactools.era5.stac import create_collection, create_item

__all__ = ["create_collection", "create_item"]

stactools.core.use_fsspec()


def register_plugin(registry):
    from stactools.era5 import commands

    registry.register_subcommand(commands.create_era5_command)


__version__ = "0.1.0"
