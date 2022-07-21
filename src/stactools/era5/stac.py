from __future__ import annotations

import copy
import datetime
import pathlib
from typing import Any

import fsspec
import pystac
import xarray as xr
import xstac

from stactools.era5.constants import ITEM_ASSETS

FC_VARIABLES = [
    "air_temperature_at_2_metres_1hour_Maximum",
    "air_temperature_at_2_metres_1hour_Minimum",
    "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",
    "precipitation_amount_1hour_Accumulation",
]
AN_VARIABLES = [
    "surface_air_pressure",
    "sea_surface_temperature",
    "eastward_wind_at_10_metres",
    "air_temperature_at_2_metres",
    "eastward_wind_at_100_metres",
    "northward_wind_at_10_metres",
    "northward_wind_at_100_metres",
    "air_pressure_at_mean_sea_level",
    "dew_point_temperature_at_2_metres",
]
KINDS = ["an", "fc"]


def create_collection(
    root_paths: tuple[str],
    kinds: tuple[str],
    protocol: str,
    storage_options: dict[str, Any] | None = None,
    extra_fields: dict[str, Any] = None,
) -> pystac.Collection:
    """
    Create a collection from a representative `fc` and `an` paths.
    """
    storage_options = storage_options or {}
    extra_fields = extra_fields or {}

    items = [
        create_item(root_path, kind, protocol, storage_options=storage_options)
        for root_path, kind in zip(root_paths, kinds)
    ]

    collection_datacube = create_collection_datacube(items)
    # Done with I/O

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[[-180.0, -90.0, 180.0, 90.0]]),
        temporal=pystac.TemporalExtent(
            intervals=[
                [datetime.datetime(1959, 1, 1), None],
            ]
        ),
    )
    keywords = [
        "ERA5",
        "ECMWF",
        "Precipitation",
        "Temperature",
        "Reanalysis",
        "Weather",
    ]
    providers = [
        pystac.Provider(
            "ECMWF",
            roles=[pystac.ProviderRole.PRODUCER, pystac.ProviderRole.LICENSOR],
            url="https://www.ecmwf.int/",
        ),
        pystac.Provider(
            "Microsoft",
            roles=[pystac.ProviderRole.PROCESSOR, pystac.ProviderRole.HOST],
            url="https://planetarycomputer.microsoft.com",
        ),
    ]
    extra_fields.update(collection_datacube)
    collection_id = "era5"

    r = pystac.Collection(
        collection_id,
        description="{{ collection.description }}",
        extent=extent,
        keywords=keywords,
        extra_fields=extra_fields,
        providers=providers,
        title="ERA5",
        license="proprietary",
    )
    r.add_links(
        [
            pystac.Link(
                rel=pystac.RelType.LICENSE,
                target="https://apps.ecmwf.int/datasets/licences/copernicus/",
                media_type="application/pdf",
                title="License to Use Copernicus Products",
            ),
            pystac.Link(
                rel="describedby",
                target="https://confluence.ecmwf.int/display/CKB/ERA5",
                media_type="text/html",
                title="Project homepage",
            ),
            pystac.Link(
                rel="describedby",
                target="https://confluence.ecmwf.int/display/CKB/How+to+acknowledge+and+cite+a+Climate+Data+Store+%28CDS%29+catalogue+entry+and+the+data+published+as+part+of+it",  # noqa: 501
                media_type="text/html",
                title="How to cite",
            ),
        ]
    )
    r.add_asset(
        "thumbnail",
        pystac.Asset(
            "https://ai4edatasetspublicassets.blob.core.windows.net/assets/pc_thumbnails/era5-thumbnail.png",  # noqa: E501
            title="Thumbnail",
            media_type=pystac.MediaType.PNG,
        ),
    )

    # Summaries
    r.summaries.maxcount = 50
    summaries = {
        "era5:kind": ["fc", "an"],
    }
    for k, v in summaries.items():
        r.summaries.add(k, v)

    # Item assets
    item_assets = pystac.extensions.item_assets.ItemAssetsExtension.ext(
        r, add_if_missing=True
    )
    item_assets.item_assets = {
        k: pystac.extensions.item_assets.AssetDefinition(v)
        for k, v in ITEM_ASSETS.items()
    }

    # Datacube
    r.stac_extensions.append(
        "https://stac-extensions.github.io/datacube/v2.0.0/schema.json"
    )
    r.set_self_href("collection.json")

    r.validate()
    r.remove_links(pystac.RelType.SELF)
    r.remove_links(pystac.RelType.ROOT)

    return r


def create_item(
    path: str,
    kind: str,
    protocol: str,
    storage_options: dict[str, Any] | None = None,
) -> pystac.Item:
    """
    Create an ERA5 item from a Zarr group.

    Parameters
    ----------
    path : str
        The "root" of an item, like ERA5/1979/01/
    kind: str
        One of "an" or "fc". This function will list all files under the root
        'path' and filter down to just those for kind 'kind'.
    """
    storage_options = storage_options or {}
    fs = fsspec.filesystem(protocol, **storage_options)
    store = fs.get_mapper(path)
    ds = xr.open_dataset(store, engine="zarr", consolidated=True)
    properties = {"era5:kind": kind, "start_datetime": None, "end_datetime": None}

    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [180.0, -90.0],
                [180.0, 90.0],
                [-180.0, 90.0],
                [-180.0, -90.0],
                [180.0, -90.0],
            ]
        ],
    }
    bbox = [-180.0, -90.0, 180.0, 90.0]
    item_id = f"era5-{kind}"

    template = pystac.Item(
        item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=None,
        properties=properties,
    )
    item = xstac.xarray_to_stac(
        ds,
        template,
        temporal_dimension="time",
        x_dimension="lon",
        y_dimension="lat",
        reference_system="epsg:4326",
    )

    asset_extra_fields = {
        "xarray:open_kwargs": {
            "engine": "zarr",
            "chunks": {},
            "consolidated": True,
            "storage_options": {"account_name": fs.account_name},
        }
    }
    title = f"Zarr store for '{kind}' variables."
    item.add_asset(
        "data",
        pystac.Asset(
            f"{protocol}://{path}",
            title=title,
            media_type="application/vnd+zarr",
            roles=["data"],
            extra_fields=asset_extra_fields,
        ),
    )
    return item


def create_collection_datacube(items: list[pystac.Item]) -> dict[str, Any]:
    # generate from 2 items

    collection_datacube: dict[str, dict[str, Any]] = {"cube:variables": {}}
    for item in items:
        collection_datacube["cube:dimensions"] = copy.deepcopy(
            item.properties["cube:dimensions"]
        )
        collection_datacube["cube:dimensions"]["time"]["extent"] = [
            "1959-01-01T00:00:00Z",
            None,
        ]
        # varies by month, so we set it to null
        collection_datacube["cube:variables"].update(
            copy.deepcopy(item.properties["cube:variables"])
        )

        for _, v in collection_datacube["cube:variables"].items():
            # variable length months
            v["shape"] = [None] + v["shape"][1:]

    return collection_datacube


def collection_key(path: str) -> bool:
    fc_vars = {
        "air_temperature_at_2_metres_1hour_Maximum",
        "air_temperature_at_2_metres_1hour_Minimum",
        "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",
        "precipitation_amount_1hour_Accumulation",
    }
    p = pathlib.Path(path)
    return p.stem in fc_vars
