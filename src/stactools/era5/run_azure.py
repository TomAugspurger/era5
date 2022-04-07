from __future__ import annotations

import os
import json
import pathlib

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
    return collection


def create_items(stac_credential=None):
    # import tqdm.notebook
    asset_credential = planetary_computer.sas.get_token("cpdataeuwest", "era5").token
    stac_credential = stac_credential or os.environ["AZURE_STAC_CREDENTIAL"]

    storage_options = dict(account_name="cpdataeuwest", credential=asset_credential)
    fs = adlfs.AzureBlobFileSystem(**storage_options)
    # TODO: support a "since"-style argument
    roots = fs.glob("era5/ERA5/*/*")
    ditems = []
    for root in roots:
        paths = fs.ls(root)
        for kind, store_paths in stac.group_paths(paths):
            ditems.append(
                dask.delayed(stac.create_item)(
                    kind, store_paths, "abfs", storage_options
                )
            )

    with dask.distributed.Client() as client:
        print(client.dashboard_link)
        items = dask.compute(*ditems)

    item_dir = pathlib.Path("items")
    item_dir.mkdir(exist_ok=True)
    for item in items:
        (item_dir / f"{item.id}.json").write_text(json.dumps(item.to_dict()))

    cc = azure.storage.blob.ContainerClient(
        "https://cpdataeuwest.blob.core.windows.net",
        "era5-stac",
        credential=stac_credential,
    )
    content_settings = azure.storage.blob.ContentSettings(media_type="application/json")
    for item in items:
        cc.upload_blob(
            f"items/{item.id}.json",
            json.dumps(item.to_dict()).encode(),
            content_settings=content_settings,
        )
    return items
