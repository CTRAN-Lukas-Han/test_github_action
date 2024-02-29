from dagster import define_asset_job, AssetSelection

PSU_extract_job = define_asset_job(name="PSU_extract",selection=AssetSelection.groups("PSU_Extract"))
