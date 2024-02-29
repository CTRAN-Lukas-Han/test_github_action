from dagster import asset, AssetExecutionContext, MetadataValue, DailyPartitionsDefinition
from CTRAN.resources import ODBCResource
import polars as pl
import os

base_dir = os.getenv("BI_DIRECTORY_BASE")

@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-09-01"))
def pull_timeblocks_eam(context: AssetExecutionContext, EAM: ODBCResource) -> pl.DataFrame:
    partition_date_str=context.partition_key
    with open("./CTRAN/queries/EAM_Time_Blocks.sql","r") as f:
        query = f.read()

    conn = EAM.connect()
    df = pl.read_database(query,conn,execute_options={"parameters": [partition_date_str] })
    conn.close()
    df = df.with_columns( 
        pl.col('In Date Time').cast(pl.Datetime).dt.replace_time_zone("America/Los_Angeles", ambiguous='latest'),
        pl.col('Out Date Time').cast(pl.Datetime).dt.replace_time_zone("America/Los_Angeles", ambiguous='latest'),
    )
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown())
    })
    return df

@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-09-01"))
def write_csv_blocks(context: AssetExecutionContext, pull_timeblocks_eam: pl.DataFrame) -> None:
    partition_date_str=context.partition_key
    f = os.path.join(base_dir,"DWCTRAN/PAY/PAY_Maint Timekeeping/Data Files",f'{partition_date_str}.csv')
    pull_timeblocks_eam.write_csv(f,datetime_format="%Y-%m-%dT%H:%M:%S%:z")
    return None
