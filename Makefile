examples/era5.json:
	stac era5 create-collection \
		--root-path=era5/forecast.zarr --root-path=era5/analysis.zarr --kind=forecast --kind=analysis \
		--destination=$@ \
		--storage-option "account_name=cpdataeuwest" \
		--storage-option "credential=${SAS_TOKEN}" \
		--extra-field "msft:storage_account=cpdataeuwest" \
		--extra-field "msft:container=era5" \
		--extra-field "msft:short_description=A comprehensive reanalysis, which assimilates as many observations as possible in the upper air and near surface."
