import json
import logging

import click

from stactools.era5 import stac

logger = logging.getLogger(__name__)


def create_era5_command(cli):
    """Creates the stactools-era5 command line utility."""

    @cli.group(
        "era5",
        short_help=("Commands for working with stactools-era5"),
    )
    def era5():
        pass

    @era5.command(
        "create-collection",
        short_help="Creates a STAC collection",
    )
    @click.argument("root_path")
    @click.argument("protocol")
    @click.argument("destination")
    @click.option(
        "--extra-field",
        default=None,
        help="Key-value pairs to include in extra-fields",
        multiple=True,
    )
    @click.option(
        "--storage-option",
        default=None,
        help="Storage options",
        multiple=True,
    )
 
    def create_collection_command(root_path, protocol, destination: str, extra_field, storage_option):
        """Creates a STAC Collection

        Args:
            destination (str): An HREF for the Collection JSON
        """
        extra_fields = dict(k.split("=", 1) for k in extra_field)
        storage_options = dict(k.split("=", 1) for k in storage_option)

        collection = stac.create_collection(
            root_path,
            protocol,
            storage_options,
            extra_fields,
        )

        with open(destination, "w") as f:
            json.dump(collection.to_dict(), f, indent=2)

        return None

    @era5.command("create-item", short_help="Create a STAC item")
    @click.argument("source")
    @click.argument("destination")
    def create_item_command(source: str, destination: str):
        """Creates a STAC Item

        Args:
            source (str): HREF of the Asset associated with the Item
            destination (str): An HREF for the STAC Collection
        """
        item = stac.create_item(source)

        item.save_object(dest_href=destination)

        return None

    return era5
