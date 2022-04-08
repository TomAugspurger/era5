from __future__ import annotations

import os
import json
import pathlib
import pystac

import adlfs
import azure.storage.blob
import planetary_computer
import dask
import dask.distributed

from stactools.era5 import stac


def create_collection():
    extra_fields = {
        "msft:storage_account": "cpdataeuwest",
        "msft:container": "era5",
        "msft:short_description": (
            "A comprehensive reanalysis, which assimilates as many observations "
            "as possible in the upper air and near surface."
        ),
    }

    token = planetary_computer.sas.get_token("cpdataeuwest", "era5").token
    protocol = "abfs"
    root = "era5/ERA5/1979/01"
    storage_options = dict(account_name="cpdataeuwest", credential=token)

    collection = stac.create_collection(
        root, protocol, storage_options, extra_fields=extra_fields
    )
    collection.providers.append(
        pystac.Provider(
            "Microsoft",
            roles=[pystac.ProviderRole.HOST],
            url="https://planetarycomputer.microsoft.com/",
        )
    )
    pathlib.Path(f"{collection.id}.json").write_text(
        json.dumps(collection.to_dict(), indent=2)
    )

    return collection


if __name__ == "__main__":
    create_collection()
