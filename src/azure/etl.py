"""
ETL from the Climate Data Store to Azure
"""
from __future__ import annotations

import argparse
import enum
import functools
import logging
import os
import subprocess
import sys
import tempfile
import time
from typing import Any

import fsspec
import numpy as np
import pandas as pd
import rich.logging
import urllib3
import xarray as xr
import zarr

import azure.storage.blob

logger = logging.getLogger(__name__)

FIRST_PERIOD = pd.Period("1959-01-01", freq="M")
FIRST_LAST_DATETIME = pd.Timestamp("1958-12-31")


class Kind(str, enum.Enum):
    forecast = "forecast"
    analysis = "analysis"


FC_VARIABLES = [
    "total_precipitation",
    "maximum_2m_temperature_since_previous_post_processing",
    "minimum_2m_temperature_since_previous_post_processing",
    "surface_solar_radiation_downwards",
]

AN_VARIABLES = [
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_dewpoint_temperature",
    "2m_temperature",
    "mean_sea_level_pressure",
    "sea_surface_temperature",
    "surface_pressure",
]
KINDS_TO_VARIABLES = {Kind.forecast.value: FC_VARIABLES, Kind.analysis: AN_VARIABLES}

DS_ATTRS = {"institution": "ECMWF", "source": "Reanalysis", "title": "ERA5 forecasts"}

ATTRS = {
    "precipitation_amount_1hour_Accumulation": {
        "long_name": "Total precipitation",
        "nameCDM": "Total_precipitation_1hour_Accumulation",
        "nameECMWF": "Total precipitation",
        "product_type": "forecast",
        "shortNameECMWF": "tp",
        "standard_name": "precipitation_amount",
        "units": "m",
    },
    "air_temperature_at_2_metres_1hour_Maximum": {
        "long_name": "Maximum temperature at 2 metres since previous post-processing",
        "nameCDM": "Maximum_temperature_at_2_metres_since_previous_post-processing_surface_1_Hour_2",  # noqa: E501
        "nameECMWF": "Maximum temperature at 2 metres since previous post-processing",
        "product_type": "forecast",
        "shortNameECMWF": "mx2t",
        "standard_name": "air_temperature",
        "units": "K",
    },
    "air_temperature_at_2_metres_1hour_Minimum": {
        "long_name": "Minimum temperature at 2 metres since previous post-processing",
        "nameCDM": "Minimum_temperature_at_2_metres_since_previous_post-processing_surface_1_Hour_2",  # noqa: E501
        "nameECMWF": "Minimum temperature at 2 metres since previous post-processing",
        "product_type": "forecast",
        "shortNameECMWF": "mn2t",
        "standard_name": "air_temperature",
        "units": "K",
    },
    "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation": {
        "long_name": "Surface solar radiation downwards",
        "nameCDM": "Surface_solar_radiation_downwards_surface_1_Hour_Accumulation",
        "nameECMWF": "Surface solar radiation downwards",
        "product_type": "forecast",
        "shortNameECMWF": "ssrd",
        "standard_name": "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air",
        "units": "J m**-2",
    },
    "surface_air_pressure": {
        "long_name": "Surface pressure",
        "nameCDM": "Surface_pressure_surface",
        "nameECMWF": "Surface pressure",
        "product_type": "analysis",
        "shortNameECMWF": "sp",
        "standard_name": "surface_air_pressure",
        "units": "Pa",
    },
    "sea_surface_temperature": {
        "long_name": "Sea surface temperature",
        "nameCDM": "Sea_surface_temperature_surface",
        "nameECMWF": "Sea surface temperature",
        "product_type": "analysis",
        "shortNameECMWF": "sst",
        "standard_name": "sea_surface_temperature",
        "units": "K",
    },
    "eastward_wind_at_10_metres": {
        "long_name": "10 metre U wind component",
        "nameCDM": "10_metre_U_wind_component_surface",
        "nameECMWF": "10 metre U wind component",
        "product_type": "analysis",
        "shortNameECMWF": "10u",
        "standard_name": "eastward_wind",
        "units": "m s**-1",
    },
    "air_temperature_at_2_metres": {
        "long_name": "2 metre temperature",
        "nameCDM": "2_metre_temperature_surface",
        "nameECMWF": "2 metre temperature",
        "product_type": "analysis",
        "shortNameECMWF": "2t",
        "standard_name": "air_temperature",
        "units": "K",
    },
    "eastward_wind_at_100_metres": {
        "long_name": "100 metre U wind component",
        "nameCDM": "100_metre_U_wind_component_surface",
        "nameECMWF": "100 metre U wind component",
        "product_type": "analysis",
        "shortNameECMWF": "100u",
        "standard_name": "eastward_wind",
        "units": "m s**-1",
    },
    "northward_wind_at_10_metres": {
        "long_name": "10 metre V wind component",
        "nameCDM": "10_metre_V_wind_component_surface",
        "nameECMWF": "10 metre V wind component",
        "product_type": "analysis",
        "shortNameECMWF": "10v",
        "standard_name": "northward_wind",
        "units": "m s**-1",
    },
    "northward_wind_at_100_metres": {
        "long_name": "100 metre V wind component",
        "nameCDM": "100_metre_V_wind_component_surface",
        "nameECMWF": "100 metre V wind component",
        "product_type": "analysis",
        "shortNameECMWF": "100v",
        "standard_name": "northward_wind",
        "units": "m s**-1",
    },
    "dew_point_temperature_at_2_metres": {
        "long_name": "2 metre dewpoint temperature",
        "nameCDM": "2_metre_dewpoint_temperature_surface",
        "nameECMWF": "2 metre dewpoint temperature",
        "product_type": "analysis",
        "shortNameECMWF": "2d",
        "standard_name": "dew_point_temperature",
        "units": "K",
    },
    "air_pressure_at_mean_sea_level": {
        "long_name": "Mean sea level pressure",
        "nameCDM": "Mean_sea_level_pressure_surface",
        "nameECMWF": "Mean sea level pressure",
        "product_type": "analysis",
        "shortNameECMWF": "msl",
        "standard_name": "air_pressure_at_mean_sea_level",
        "units": "Pa",
    },
    "lon": {
        "long_name": "longitude",
        "standard_name": "longitude",
        "units": "degrees_east",
    },
    "lat": {
        "long_name": "latitude",
        "standard_name": "latitude",
        "units": "degrees_north",
    },
    "time": {"standard_name": "time"},
}

NAMES = {
    "sp": "surface_air_pressure",
    "tp": "precipitation_amount_1hour_Accumulation",
    "longitude": "lon",
    "latitude": "lat",
    "time": "time",
    "ssrd": "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",  # noqa: E501
    "mx2t": "air_temperature_at_2_metres_1hour_Maximum",
    "mn2t": "air_temperature_at_2_metres_1hour_Minimum",
    "v10": "northward_wind_at_10_metres",
    "v100": "northward_wind_at_100_metres",
    "u10": "eastward_wind_at_10_metres",
    "u100": "eastward_wind_at_100_metres",
    "t2m": "air_temperature_at_2_metres",
    "sst": "sea_surface_temperature",
    "d2m": "dew_point_temperature_at_2_metres",
    "msl": "air_pressure_at_mean_sea_level",
}

# These are "forecast" variables, which need a `time1_bnds` data variable and `nv` coordinate.
HAS_TIME_BOUNDS = {
    "precipitation_amount_1hour_Accumulation",
    "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",
    "air_temperature_at_2_metres_1hour_Maximum",
    "air_temperature_at_2_metres_1hour_Minimum",
}

filenames_to_keys = {
    "100m_u_component_of_wind": "eastward_wind_at_100_metres",
    "100m_v_component_of_wind": "northward_wind_at_100_metres",
    "10m_u_component_of_wind": "eastward_wind_at_10_metres",
    "10m_v_component_of_wind": "northward_wind_at_10_metres",
    "2m_dewpoint_temperature": "dew_point_temperature_at_2_metres",
    "2m_temperature": "air_temperature_at_2_metres",
    "total_precipitation": "precipitation_amount_1hour_Accumulation",
    "maximum_2m_temperature_since_previous_post_processing": "air_temperature_at_2_metres_1hour_Maximum",  # noqa: E501
    "minimum_2m_temperature_since_previous_post_processing": "air_temperature_at_2_metres_1hour_Minimum",  # noqa: E501
    "sea_surface_temperature": "sea_surface_temperature",
    "surface_pressure": "surface_air_pressure",
    "surface_solar_radiation_downwards": "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",  # noqa: E501
    "mean_sea_level_pressure": "air_pressure_at_mean_sea_level",
}


def build_query(variable: str, period: pd.Period):
    assert period.freq == "M"

    params = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": variable,
        "year": str(period.year),
        "month": f"{period.month:0>2}",
        "day": [f"{i + 1:0>2}" for i in range(period.days_in_month)],
        "time": [f"{i:02d}:00" for i in range(24)],
    }
    return params


def transform(cds_ds: xr.Dataset) -> xr.Dataset:
    """
    Transform an xarray Dataset to match what's provided by era5-pds.
    """
    # rename
    variables = set(cds_ds.variables)
    name_dict = {k: v for k, v in NAMES.items() if k in variables}
    result = cds_ds.rename(name_dict)

    # ds attrs
    result.attrs.update(DS_ATTRS)  # type: ignore
    history = result.attrs.pop("history", None)
    logger.debug("Dropping history %s", history)

    # variable attrs
    for var in result.variables:
        result[var].attrs.update(ATTRS[var])  # type: ignore

    if set(result.data_vars) & HAS_TIME_BOUNDS:
        logger.debug("Adding time1_bounds for %s", variables)
        offset = np.array(10800000000000, dtype="timedelta64[ns]")  # 1 Day
        start = result.time.data - offset

        time1_bounds = xr.DataArray(
            np.stack((start, result.time.data), axis=1),
            dims=("time", "nv"),
            coords={"time": result.time},
        )
        result["time1_bounds"] = time1_bounds

    keys = ["lat", "lon", "time"] + list(result.data_vars)
    result = result[keys]

    return result


def retry(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        e = RuntimeError
        for i in range(1, 11):
            try:
                return func(*args, **kwargs)
            except ConnectionResetError:
                logger.exception("Connection reset error on try %d/10", i)
            time.sleep(5)
        else:
            logger.warning("Failed 10 times. Re-raising")
            raise e

    return wrapper


@retry
def fetch_one(
    variable: str, period: pd.Period, cds_api_key: str, basedir: str | None = None
) -> xr.Dataset:
    import cdsapi

    cds = cdsapi.Client(url="https://cds.climate.copernicus.eu/api/v2", key=cds_api_key)
    urllib3.disable_warnings()

    params = build_query(variable, period)
    filename = f"{variable}.nc"
    if basedir:
        filename = os.path.join(basedir, filename)

    cds.retrieve("reanalysis-era5-single-levels", params, filename)
    return xr.open_dataset(filename, chunks={"time": 24})


def do_one(
    kind: str,
    period: pd.Period,
    cds_api_key: str,
    output_protocol: str,
    output_path: str,
    output_storage_options: dict[str, Any],
) -> None:
    """
    Copy and convert for one month - kind.
    """
    variables = KINDS_TO_VARIABLES[kind]
    td = tempfile.TemporaryDirectory()
    N = len(variables)
    datasets = []

    with td:
        for i, variable in enumerate(variables, 1):
            logger.info(
                "Downloading period - variable: %s - %s [%d / %d]",
                period,
                variable,
                i,
                N,
            )
            datasets.append(fetch_one(variable, period, cds_api_key, basedir=td.name))

        logger.info("Transforming variables")
        transformed = [transform(ds) for ds in datasets]
        logger.info("Transformed variables")

        logger.info("Combining variables")
        ds = xr.combine_by_coords(transformed, join="exact")
        logger.info("Combined variables")

        store = fsspec.filesystem(output_protocol, **output_storage_options).get_mapper(
            output_path
        )
        kwargs: dict[str, Any] = {"consolidated": True}
        if period != FIRST_PERIOD:
            kwargs["mode"] = "a"
            kwargs["append_dim"] = "time"

        logger.info(
            "Writing output to %s://%s", output_protocol, output_path, extra=kwargs
        )

        # TODO: validate that index is expected
        ds.to_zarr(store, **kwargs)

    logger.info("Wrote output to %s://%s", output_protocol, output_path)


def determine_next_period(
    output_protocol, output_path, output_storage_options
) -> tuple[pd.Timestamp, pd.Period]:
    """
    Determine the next period to fetch, based on what's present in storage.
    """
    store = fsspec.filesystem(output_protocol, **output_storage_options).get_mapper(
        output_path
    )
    if not store.fs.exists(os.path.join(output_path, ".zmetadata")):
        # initial write to this
        return FIRST_LAST_DATETIME, FIRST_PERIOD

    last = xr.open_dataset(store, engine="zarr").time[-1]
    dt = pd.to_datetime(last.data)
    hour = pd.Timedelta(hours=1)
    if dt.hour != 23:
        raise ValueError(f"Last hour written '{dt.hour}' is not 23! Check on the data.")

    period = pd.Period(dt + hour, freq="M")
    return dt, period


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["forecast", "analysis"])
    parser.add_argument(
        "--credential", type=str, default=os.environ.get("ETL_CREDENTIAL")
    )
    parser.add_argument("--output-protocol", type=str, default="abfs")
    parser.add_argument("--output-path", type=str, default=None)
    # parser.add_argument("--output-storage_options", type=str)
    parser.add_argument("--cds-api-key", default=os.environ.get("ETL_CDS_API_KEY"))
    parser.add_argument("--start-period", default=None)
    parser.add_argument("--end-period", default=None)

    return parser.parse_args(args)


def prepare():
    try:
        import cdsapi  # noqa: F401
    except ImportError:
        subprocess.call([sys.executable, "-m", "pip", "install", "cdsapi"])


def main(args=None):
    args = parse_args(args)
    logger.setLevel(logging.INFO)
    logger.addHandler(rich.logging.RichHandler())

    credential = args.credential
    kind = args.kind
    output_protocol = args.output_protocol
    output_path = args.output_path
    cds_api_key = args.cds_api_key
    start_period = args.start_period
    end_period = args.end_period
    # output_storage_options = args.output_storage_options

    # assert kind in

    output_storage_options = {"account_name": "cpdataeuwest", "credential": credential}

    if output_path is None:
        output_path = f"era5/{kind}.zarr"

    last, next_period = determine_next_period(
        output_protocol=output_protocol,
        output_path=output_path,
        output_storage_options=output_storage_options,
    )

    if start_period is None:
        start_period = next_period
    else:
        start_period = pd.Period(start_period, freq="M")

    if (start_period - pd.Period(last, freq="M")) != pd.offsets.MonthEnd(1):
        raise ValueError(
            f"start_period={start_period} is not consecutive with the last timestamp={last}"
        )

    if end_period is None:
        end_period = pd.Period(pd.Timestamp.now() - pd.offsets.MonthBegin(2), freq="M")
    else:
        end_period = pd.Period(end_period, freq="M")

    periods = pd.period_range(start_period, end_period, freq="M")
    N = len(periods)

    logger.info("Preparing")
    prepare()

    logger.info("Beginning extract for kind=%s - periods=%s", kind, periods)

    for i, period in enumerate(periods, 1):
        logger.info("Starting %s - %s [%d/%d]", kind, period, i, N)
        do_one(
            kind,
            period,
            cds_api_key=cds_api_key,
            output_protocol=output_protocol,
            output_path=output_path,
            output_storage_options=output_storage_options,
        )
        logger.info("Finished %s - %s [%d/%d]", kind, period, i, N)

    if len(periods):
        logger.info("Starting compact 'time' dimension")
        prefix = output_path
        if output_protocol == "abfs":
            # strip the container name
            container_name, prefix = output_path.split("/", 1)
            account_name = output_storage_options["account_name"]

            cc = azure.storage.blob.ContainerClient(
                f"https://{account_name}.blob.core.windows.net",
                container_name,
                credential=credential,
            )
            compact(prefix, cc)
            logger.info("Finished compact 'time' dimension")


def compact(prefix: str, cc: azure.storage.blob.ContainerClient):
    """
    Compact the `time` dimension into a single chunk.
    """
    # using azure.storage.blob rather than adlfs / fsspec to
    # avoid any caching issues.
    store = zarr.ABSStore(prefix=prefix, client=cc)
    ds = xr.open_dataset(store, engine="zarr", chunks={})

    time = ds["time"]
    time.encoding["chunks"] = len(
        time,
    )
    time.encoding["preferred_chunks"] = {"time": len(time)}
    ds["time"].encoding

    new_store = zarr.MemoryStore()
    ds[["time"]].to_zarr(new_store)

    for k, v in new_store.items():
        if k.startswith("time"):
            name = f"{prefix}/{k}"
            print(k, "->", name)
            cc.upload_blob(name, v, overwrite=True)

    zarr.consolidate_metadata(store)

    # Remove any stale fragments
    KEEP = {".zarray", ".zattrs", "0"}

    for blob in cc.list_blobs(name_starts_with="forecast.zarr/time/"):
        if blob.name.split("/")[-1] not in KEEP:
            logger.info("Deleting %s", blob.name)
            cc.delete_blob(blob)


if __name__ == "__main__":
    main()
