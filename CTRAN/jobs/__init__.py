from dagster import op, job, Config, OpExecutionContext,MetadataValue, define_asset_job,AssetSelection
import pandas as pd
from .PSU_extract import *

class FileConfig(Config):
    filename: str
    filepath: str

@op
def process_file(context: OpExecutionContext, config: FileConfig):
    context.log.info(config.filename)
    df = pd.read_csv(config.filepath, skiprows=1)
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df


log_file_job = define_asset_job(name="log_file_job",selection=AssetSelection.groups("Fuel_Tickets"))
