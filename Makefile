examples/era5-pds.json:
	stac era5 create-collection era5/ERA5/1979/01 abfs $@ \
		--storage-option "account_name=cpdataeuwest" \
		--storage-option "credential=${SAS_TOKEN}" \
		--extra-field "msft:storage_account=cpdataeuwest" \
		--extra-field "msft:container=era5" \
		--extra-field "msft:short_description=A comprehensive reanalysis, which assimilates as many observations as possible in the upper air and near surface."