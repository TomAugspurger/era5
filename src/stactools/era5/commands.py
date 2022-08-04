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
    @click.option(
        "--root-path",
        default=None,
        help="Storage options",
        multiple=True,
    )
    @click.option(
        "--kind",
        default=None,
        help="Storage options",
        multiple=True,
    )
    @click.option("--protocol", default="abfs")
    @click.option("--destination", default="-")
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
    def create_collection_command(
        root_path, kind, destination: str, protocol, extra_field, storage_option
    ):
        """Creates a STAC Collection

        Args:
            destination (str): An HREF for the Collection JSON
        """
        extra_fields = dict(k.split("=", 1) for k in extra_field)
        storage_options = dict(k.split("=", 1) for k in storage_option)

        collection = stac.create_collection(
            root_path,
            kind,
            protocol,
            storage_options,
            extra_fields,
        )

        with open(destination, "w") as f:
            json.dump(collection.to_dict(), f, indent=2)

        return None

    @era5.command("create-pc-item", short_help="Create a STAC item.")
    @click.option("--path")
    @click.option("--kind")
    @click.option("--protocol")
    @click.option("--account-name")
    @click.option("--destination", type=click.File("wt"))
    def create_pc_item(path, kind, protocol, account_name, destination):
        import planetary_computer

        container_name = path.split("/")[0]
        credential = planetary_computer.sas.get_token(
            account_name, container_name
        ).token
        item = stac.create_item(
            path,
            kind=kind,
            protocol=protocol,
            storage_options={"account_name": account_name, "credential": credential},
        )
        json.dump(item.to_dict(), destination, indent=2)

    return era5
