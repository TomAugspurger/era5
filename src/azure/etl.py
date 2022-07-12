import pandas as pd
import numpy as np
import cdsapi
import urllib3
import xarray as xr

import logging

logger = logging.getLogger(__name__)


FC_VARIABLES = [
    "large_scale_precipitation",
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
    "mean_sea_level_pressure"
    "sea_surface_temperature",
    "surface_pressure",
]


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
        "nameCDM": "Maximum_temperature_at_2_metres_since_previous_post-processing_surface_1_Hour_2",
        "nameECMWF": "Maximum temperature at 2 metres since previous post-processing",
        "product_type": "forecast",
        "shortNameECMWF": "mx2t",
        "standard_name": "air_temperature",
        "units": "K",
    },
    "air_temperature_at_2_metres_1hour_Minimum": {
        "long_name": "Minimum temperature at 2 metres since previous post-processing",
        "nameCDM": "Minimum_temperature_at_2_metres_since_previous_post-processing_surface_1_Hour_2",
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
    "air_pressure_at_mean_sea_level": {
        "long_name": "Mean sea level pressure",
        "nameCDM": "Mean_sea_level_pressure_surface",
        "nameECMWF": "Mean sea level pressure",
        "product_type": "analysis",
        "shortNameECMWF": "msl",
        "standard_name": "air_pressure_at_mean_sea_level",
        "units": "Pa",
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
    "air_pressure_at_mean_sea_level": {'long_name': 'Mean sea level pressure',
 'nameCDM': 'Mean_sea_level_pressure_surface',
 'nameECMWF': 'Mean sea level pressure',
 'product_type': 'analysis',
 'shortNameECMWF': 'msl',
 'standard_name': 'air_pressure_at_mean_sea_level',
 'units': 'Pa'},
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
    "lsp": "precipitation_amount_1hour_Accumulation",
    "longitude": "lon",
    "latitude": "lat",
    "time": "time",
    "ssrd": "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",
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
    # "surface_pressure": "surface_air_pressure",
}


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
    "large_scale_precipitation": "precipitation_amount_1hour_Accumulation",
    "maximum_2m_temperature_since_previous_post_processing": "air_temperature_at_2_metres_1hour_Maximum",
    "minimum_2m_temperature_since_previous_post_processing": "air_temperature_at_2_metres_1hour_Minimum",
    "sea_surface_temperature": "sea_surface_temperature",
    "surface_pressure": "surface_air_pressure",
    "surface_solar_radiation_downwards": "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation",
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
    Transform an xarray Dataset to match what's provided by era5-pds
    """
    # rename
    variables = set(cds_ds.variables)
    name_dict = {k: v for k, v in NAMES.items() if k in variables}
    result = cds_ds.rename(name_dict)

    # ds attrs
    result.attrs.update(DS_ATTRS)
    result.attrs.pop("history", None)
    # variable attrs
    for var in result.variables:
        result[var].attrs.update(ATTRS[var])

    if set(result.data_vars) & HAS_TIME_BOUNDS:
        # 1 day
        offset = np.array(10800000000000, dtype="timedelta64[ns]")
        start = result.time.data - offset
        # nv = xr.DataArray([0, 1], dims=("nv",))
        # nv = xr.DataArray([0, 1], name="nv", dims="nv")

        time1_bounds = xr.DataArray(
            np.stack((start, result.time.data), axis=1),
            dims=("time", "nv"),
            coords={"time": result.time},
        )
        result["time1_bounds"] = time1_bounds

    keys = ["lat", "lon", "time"] + list(result.data_vars)
    result = result[keys]

    return result


def fetch_one(variable: str, period: pd.Period, cds_api_key: str) -> xr.Dataset:
    cds = cdsapi.Client(url="https://cds.climate.copernicus.eu/api/v2", key=cds_api_key)
    urllib3.disable_warnings()

    params = build_query(variable, period)
    # TODO: tempfile, lifetime, etc.
    filename = f"{variable}.nc"

    cds.retrieve("reanalysis-era5-single-levels", params, filename)
    return xr.open_dataset(filename)
