import planetary_computer.sas
import pytest

from stactools.era5 import stac


@pytest.mark.parametrize("kind", ["forecast", "analysis"])
def test_create_item(kind):
    result = stac.create_item(
        f"era5/{kind}.zarr",
        kind=kind,
        protocol="abfs",
        storage_options={
            "account_name": "cpdataeuwest",
            "credential": planetary_computer.sas.get_token(
                "cpdataeuwest", "era5"
            ).token,
        },
    )

    assert result.assets["data"].href == f"abfs://era5/{kind}.zarr"
    assert result.assets["data"].extra_fields["xarray:open_kwargs"] == {
        "engine": "zarr",
        "chunks": {},
        "consolidated": True,
        "storage_options": {"account_name": "cpdataeuwest"},
    }
    assert result.id == f"era5-{kind}"
