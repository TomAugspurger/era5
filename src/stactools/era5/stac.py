from __future__ import annotations

from typing import Any

import pathlib
import itertools
import copy
import datetime
import json
import pathlib

import fsspec
import pystac
import xarray as xr
import xstac


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


def create_collection(
    root_path: str,
    protocol: str,
    storage_options: dict[str, Any] | None = None,
    extra_fields: dict[str, Any] = None,
) -> pystac.Collection:
    """
    Create a collection from a representative `fc` and `an` paths.
    """
    storage_options = storage_options or {}
    extra_fields = extra_fields or {}

    fs = fsspec.filesystem(protocol, **storage_options)
    all_store_paths = fs.ls(root_path)

    items = [
        create_item(kind, store_paths, protocol, storage_options)
        for kind, store_paths in group_paths(all_store_paths)
    ]

    collection_datacube = create_collection_datacube(items)
    # Done with I/O

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent(
            intervals=[
                datetime.datetime(1979, 1, 1),
                None,
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
            "Planet OS",
            roles=[pystac.ProviderRole.PROCESSOR],
            url="https://planetos.com/",
        ),
    ]
    extra_fields.update(collection_datacube)
    collection_id = "era5-pds"

    r = pystac.Collection(
        collection_id,
        description="{{ collection.description }}",
        extent=extent,
        keywords=keywords,
        extra_fields=extra_fields,
        providers=providers,
        title="ERA5 - PDS",
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
                target="https://confluence.ecmwf.int/display/CKB/How+to+acknowledge+and+cite+a+Climate+Data+Store+%28CDS%29+catalogue+entry+and+the+data+published+as+part+of+it",  # noqa
                media_type="text/html",
                title="How to cite",
            ),
        ]
    )
    r.add_asset(
        "thumbnail",
        pystac.Asset(
            "https://datastore.copernicus-climate.eu/c3s/published-forms-v2/c3sprod_clone/reanalysis-era5-pressure-levels/overview.jpg",
            title="Thumbnail",
            media_type=pystac.MediaType.JPEG,
        ),
    )

    # Summaries
    r.summaries.maxcount = 50
    summaries = {
        "era5:kind": ["fc", "an"],
    }
    for k, v in summaries.items():
        r.summaries.add(k, v)

    r.stac_extensions.append(
        "https://stac-extensions.github.io/datacube/v2.0.0/schema.json"
    )
    r.set_self_href("collection.json")

    r.validate()
    r.remove_links(pystac.RelType.SELF)
    r.remove_links(pystac.RelType.ROOT)

    return r


def create_item(
    kind: str,
    store_paths: list[str],
    protocol: str,
    storage_options: dict[str, Any] | None = None,
) -> pystac.Item:
    """
    Create an ERA5 item from a list of paths, all of the same "kind".

    This item has one asset per path in ``store_paths``.
    """
    storage_options = storage_options or {}
    fs = fsspec.filesystem(protocol, **storage_options)
    dss = [
        xr.open_dataset(fs.get_mapper(store), engine="zarr", consolidated=True)
        for store in store_paths
    ]
    ds = xr.combine_by_coords(dss, join="exact")
    properties = {"start_datetime": None, "end_datetime": None, "era5:kind": kind}
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
    bbox = [-180, -90, 180, 90]
    item_id = name_item(store_paths[0].rsplit("/", 1)[0], kind)

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
    for store in store_paths:
        p = pathlib.Path(store)
        v = ds[p.stem]
        item.add_asset(
            p.stem,
            pystac.Asset(
                f"{fs.protocol}://{store}",
                title=v.attrs["long_name"],
                media_type="application/vnd+zarr",
                roles=["data"],
                extra_fields=asset_extra_fields,
            ),
        )

    return item


def name_item(root: str, kind: str) -> str:
    *_, year, month = root.rstrip("/").split("/")
    return "-".join(["era5-pds", year, month, kind])


def create_collection_datacube(items: list[pystac.Item, pystac.Item]) -> dict[str, Any]:
    # generate from 2 items

    collection_datacube = {"cube:variables": {}}
    for item in items:
        collection_datacube["cube:dimensions"] = copy.deepcopy(
            item.properties["cube:dimensions"]
        )
        collection_datacube["cube:dimensions"]["time"]["extent"] = [
            "1970-01-01T00:00:00Z",
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


def group_paths(paths: list[str]):
    paths = sorted(paths, key=collection_key)
    for k, v in itertools.groupby(paths, key=collection_key):
        v = list(v)
        k2 = "fc" if k else "an"
        yield k2, v


def collection_key(path: str) -> bool:
    fc_vars = {
        "air_temperature_at_2_metres_1hour_Maximum",
        "air_temperature_at_2_metres_1hour_Minimum",
        "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",
        "precipitation_amount_1hour_Accumulation",
    }
    p = pathlib.Path(path)
    return p.stem in fc_vars

