from dagster import asset, AssetExecutionContext, MetadataValue, get_dagster_logger, Config, AssetCheckResult, Definitions, asset, asset_check, DailyPartitionsDefinition
from CTRAN.resources import SFTPResource, INITResource, ODBCResource, OracleResource
import pandas as pd
import pandera as pa
from typing import Dict
import os
import pathlib
from datetime import date, datetime, timedelta

base_dir = os.getenv("BI_DIRECTORY_BASE")
psu_asset_partition = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(group_name="PSU_Extract", partitions_def=psu_asset_partition)
def init_psu_extract(context: AssetExecutionContext, INIT2: OracleResource) -> pd.DataFrame:
    with open("./CTRAN/queries/PSUTransitPortalDataFactored.sql","r") as f:
        query = f.read()

    context.log.info(query)
    partition_date_str=context.partition_key
    partition_date_str_offset = (datetime.strptime(partition_date_str,"%Y-%m-%d").date() - timedelta(days=5)).strftime("%Y-%m-%d") #offset by five days to allow init data to come in
    conn = INIT2.connect()
    df = pd.read_sql(query,conn,params={"QUERY_DATE":partition_date_str_offset})
    conn.close()
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df

@asset(group_name="PSU_Extract")
def fin_vehicle_extract(context: AssetExecutionContext, DWCTRAN: ODBCResource) -> pd.DataFrame:
    with open("./CTRAN/queries/FIN_VEHICLES.sql","r") as f:
        query = f.read()

    context.log.info(query)
    conn = DWCTRAN.connect()
    df=pd.read_sql(query, conn)
    conn.close()
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df

@asset(group_name="PSU_Extract",partitions_def=psu_asset_partition)
def compose_psu_csv(context: AssetExecutionContext, fin_vehicle_extract: pd.DataFrame, init_psu_extract: pd.DataFrame) -> os.PathLike:
    partition_date_str=context.partition_key
    output_path=os.path.join(base_dir,"DWCTRAN/BDA/BDA_PSU_EXTRACT/PSU_TransitPortal_"+partition_date_str+".csv")
    df = init_psu_extract.merge(fin_vehicle_extract,how="left",left_on="VEHICLE_NUMBER",right_on="VEHICLE")

    # check column type and chage them to int
    columns_to_check = ['STOP_TIME', 'ARRIVE_TIME', 'TRIP_ID', 'SEATED_CAPACITY']

    for column in columns_to_check:
        if df[column].dtype != 'int64':
            df[column] = df[column].astype('Int64')
            print(f"Changed data type of {column} to {df[column].dtype}")

    df[['SERVICE_DATE','VEHICLE_NUMBER','LEAVE_TIME','TRAIN','ROUTE_NUMBER','DIRECTIONAL','SERVICE_KEY','TRIP_NUMBER','STOP_TIME',
        'ARRIVE_TIME','DWELL','LOCATION_ID','DOOR','LIFT','ONS','OFFS','ESTIMATED_LOAD','MAXIMUM_SPEED','TRAIN_MILEAGE','PATTERN_DISTANCE',
        'LOCATION_DISTANCE','X_COORDINATE','Y_COORDINATE','DATA_SOURCE','SCHEDULE_STATUS','TRIP_ID','SEATED_CAPACITY','TIME_PERIOD']].to_csv(output_path, index=False)
    return pathlib.Path(output_path)

@asset(group_name="PSU_Extract",partitions_def=psu_asset_partition)
def psu_upload_file(context: AssetExecutionContext, PSUSFTP: SFTPResource, compose_psu_csv: os.PathLike) -> None:
    context.log.info(PSUSFTP.listdir())
    context.log.info(compose_psu_csv)
    stat=PSUSFTP.uploadFile(compose_psu_csv, "./sftp/"+os.path.basename(compose_psu_csv))
    context.add_output_metadata(metadata={"fileStat":print(stat)})
    return None

"""
@asset_check(asset=compose_psu_csv)
def validate_psu_csv_schema(context: AssetExecutionContext,compose_psu_csv: Dict[str,os.PathLike]):
    context.log.info(compose_psu_csv.values())
    psu_csv=pd.read_csv(compose_psu_csv, parse_dates=['SERVICE_DATE'],dtype={'STOP_TIME':'Int64', 'ARRIVE_TIME':'Int64', 'TRIP_ID':'Int64', 'SEATED_CAPACITY':'Int64', 'TIME_PERIOD':str, 'DIRECTIONAL':str,'SERVICE_KEY':str})
    context.log.info(psu_csv.dtypes.to_dict())
    expected_schema = pa.DataFrameSchema({
        'SERVICE_DATE': pa.Column('datetime64[ns]'),
        'VEHICLE_NUMBER':pa.Column('int64'),
        'LEAVE_TIME':pa.Column('int64'),
        'TRAIN':pa.Column('int64'),
        'ROUTE_NUMBER':pa.Column('int64'),
        'DIRECTIONAL':pa.Column('str'),
        'SERVICE_KEY':pa.Column('str'),
        'TRIP_NUMBER':pa.Column('int64'),
        'STOP_TIME':pa.Column('Int64', nullable=True),
        'ARRIVE_TIME':pa.Column('Int64'),
        'DWELL':pa.Column('int64'),
        'LOCATION_ID':pa.Column('int64'),
        'DOOR':pa.Column('int64'),
        'LIFT':pa.Column('int64'),
        'ONS':pa.Column('int64'),
        'OFFS':pa.Column('int64'),
        'ESTIMATED_LOAD':pa.Column('int64'),
        'MAXIMUM_SPEED':pa.Column('float64', nullable=True),
        'TRAIN_MILEAGE':pa.Column('float64'),
        'PATTERN_DISTANCE':pa.Column('float64'),
        'LOCATION_DISTANCE':pa.Column('float64', nullable=True),
        'X_COORDINATE':pa.Column('float64', nullable=True),
        'Y_COORDINATE':pa.Column('float64', nullable=True),
        'DATA_SOURCE':pa.Column('int64'),
        'SCHEDULE_STATUS':pa.Column('int64'),
        'TRIP_ID':pa.Column('Int64'),
        'SEATED_CAPACITY':pa.Column('Int64', nullable=True),
        'TIME_PERIOD':pa.Column('str')},
        index = pa.Index(int)
     )
    return AssetCheckResult(passed=type(expected_schema.validate(psu_csv)) == pd.DataFrame)
"""
