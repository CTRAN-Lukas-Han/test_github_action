from dagster import asset, AssetExecutionContext, MetadataValue, get_dagster_logger, Config, AssetCheckResult, Definitions, asset, asset_check
from CTRAN.resources import SFTPResource, INITResource, MSSQLResource
import pandas as pd
import pandera as pa
import os
import pathlib
from datetime import date

base_dir = os.getenv("BI_DIRECTORY_BASE")

@asset(group_name="PSU_Extract")
def init_psu_extract(context: AssetExecutionContext, INIT: INITResource) -> pd.DataFrame:
    with open("./CTRAN/queries/PSUTransitPortalDataFactored.sql","r") as f:
        query = f.read()

    context.log.info(query)
    rows = INIT.query(query)
    header_row = ['SERVICE_DATE', 'VEHICLE_NUMBER', 'LEAVE_TIME', 'TRAIN', 'ROUTE_NUMBER', 'DIRECTIONAL', 'SERVICE_KEY', 
                  'TRIP_NUMBER', 'STOP_TIME', 'ARRIVE_TIME', 'DWELL', 'LOCATION_ID', 'DOOR', 'LIFT', 'ONS', 'OFFS', 'ESTIMATED_LOAD', 
                  'MAXIMUM_SPEED', 'TRAIN_MILEAGE', 'PATTERN_DISTANCE', 'LOCATION_DISTANCE', 'X_COORDINATE', 
                  'Y_COORDINATE', 'DATA_SOURCE', 'SCHEDULE_STATUS', 'TRIP_ID', 'TIME_PERIOD']
    df = pd.DataFrame(rows, columns=header_row)
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df

@asset(group_name="PSU_Extract")
def fin_vehicle_extract(context: AssetExecutionContext, DWCTRAN: MSSQLResource) -> pd.DataFrame:
    with open("./CTRAN/queries/FIN_VEHICLES.sql","r") as f:
        query = f.read()

    context.log.info(query)
    rows = DWCTRAN.query(query)
    header_row = ['VEHICLE',
	'ASSET_ID',
	'VIN',
	'FLEET',
	'DESCRIPTION',
	'MODEL_YEAR',
	'PURCHASE_YEAR',
	'[LENGTH]',
	'LICENSE_PLATE',
	'ACQUISITION_COST',
	'SERVICETYPE_NTD',
	'SEATED_CAPACITY',
	'EFFECTIVE_FROM',
	'EFFECTIVE_TO']
    df = pd.DataFrame(rows, columns=header_row)
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df

@asset(group_name="PSU_Extract")
def compose_psu_csv(context: AssetExecutionContext, fin_vehicle_extract: pd.DataFrame, init_psu_extract: pd.DataFrame) -> os.PathLike:
    output_path="/media/windowsshare/Business Intelligence/DWCTRAN/BDA/BDA_PSU_EXTRACT/PSU_TransitPortal_"+date.today().isoformat()+".csv"
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

@asset(group_name="PSU_Extract")
def psu_upload_file(context: AssetExecutionContext, PSUSFTP: SFTPResource, compose_psu_csv: os.PathLike) -> None:
    context.log.info(PSUSFTP.listdir())
    context.log.info(compose_psu_csv)
    stat=PSUSFTP.uploadFile(compose_psu_csv, "./sftp/"+os.path.basename(compose_psu_csv))
    context.add_output_metadata(metadata={"fileStat":print(stat)})
    return None

'''
@asset_check(asset=compose_psu_csv)
def validate_psu_csv_schema(context: AssetExecutionContext,compose_psu_csv: os.PathLike):
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
'''

