from dagster import asset, get_dagster_logger, MetadataValue, AssetExecutionContext, Config  #MaterializeResult
from CTRAN.resources import mSETResource,MSSQLResource #,INITResource, MSSQLResource
import pandas as pd
import os
from CTRAN.utils import transformEAMxml,transformCell
from datetime import datetime
import numpy as np

class FileConfig(Config):
    filename: str
    filepath: str

@asset
def health_check_asset(context: AssetExecutionContext) -> None:
    context.log.info("Your materialization is working.")

@asset(group_name="Fuel_Tickets")
def EAM_BEB_list(context:AssetExecutionContext, EAM: MSSQLResource) -> pd.DataFrame:
    data = EAM.query("SELECT distinct EQ_equip_no FROM eq_fueltype WHERE fuel_type = 'E'")
    df = pd.DataFrame(data, columns=['Equipment ID'])
    df.astype({'Equipment ID':'str'})
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df


@asset(group_name="Fuel_Tickets")
def EAM_vehicle_list(context:AssetExecutionContext, EAM: MSSQLResource) -> pd.DataFrame:
    data = EAM.query("select EQ_equip_no,last_meter_1_reading from eq_main where CLASS_class_meter = 'MILES' and asset_type = 'ASSET'")
    df = pd.DataFrame(data, columns=['Equipment ID','Meter 1'])
    df.astype({'Equipment ID':'str','Meter 1':int})
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df


@asset(group_name="Fuel_Tickets")
def EAM_diesel_tank_info(context:AssetExecutionContext, EAM: MSSQLResource) -> pd.DataFrame:
    data = EAM.query("select TANK_tank_no ,qty_on_hand , X_datetime_update from FUE_MAIN fm2 where TANK_tank_no in ('4', '5', '6')")
    df = pd.DataFrame(data, columns=['Tank ID','Quantity', 'X_datetime_update'])
    df.astype({'Tank ID':'str','Quantity':int, 'X_datetime_update':'datetime64[ns]'})
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df
'''
@asset(group_name="Fuel_Tickets")
def gas_vehicles(context:AssetExecutionContext):
    gas_vecs_fp = "/media/windowsshare/Business Intelligence/Departments/Maintenance/Data Uploads/Fuel Tickets/Gas Vehicles.csv"
    df = pd.read_csv(gas_vecs_fp)
    df = df[['ID']]
    df = df.astype({'ID': 'str'})
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df
'''

@asset(group_name="Fuel_Tickets")
def prep_ticket_files(
    context: AssetExecutionContext, 
    EAM_vehicle_list: pd.DataFrame, 
    EAM_diesel_tank_info: pd.DataFrame,
    EAM_BEB_list: pd.DataFrame, 
    config: FileConfig
    ) -> pd.DataFrame:

    context.log.info(config.filename)
    df = pd.read_csv(config.filepath, skiprows=1)

    df['op'] = "[i]"
    df['fuel type'] = np.where( df['RIH'] == 103,'gas','diesel' )
    df['tank'] = np.where( df['RIH'] == 103,'7','5' )
    df['key'] = 'test'
    df['Date'] = df['Date'].apply(lambda x: x.replace(" 0:", " 12:"))
    context.log.info(df.head(20))
    df['Date']=pd.to_datetime(df['Date'], format="%m/%d/%Y %I:%M %p")
    df=df.astype({'op':'str','Vehicle Number':'str','Odometer':'str','tank':'str','Total Fuel':'str','Date':'str'})
    df=df[df['Total Fuel']!= '0.0']


    context.log.info(df.head())
    context.log.info(EAM_vehicle_list.head())

    df=df.merge(EAM_vehicle_list, left_on=['Vehicle Number'],right_on=['Equipment ID'], how='left')
    df = df[df['Vehicle Number'] != '0']
    df['Odometer'] = np.where( (df['Vehicle Number'].isin(EAM_BEB_list['Equipment ID']) ),df['Meter 1'],df['Odometer'] )
    df['Odometer'] = np.where(df['Vehicle Number'] == '9999', ['' for x in df['Odometer']] , df['Odometer'])
    df['Vehicle Number'] = np.where(((df['Vehicle Number'] == '9999') & (df['fuel type'] == 'gas')), '3' , df['Vehicle Number'])
    df['Vehicle Number'] = np.where(((df['Vehicle Number'] == '9999') & (df['fuel type'] == 'diesel')), '2' , df['Vehicle Number'])
    df = df[['op','key','Vehicle Number','Odometer','tank','Total Fuel','Date']]

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df

@asset(group_name="Fuel_Tickets")
def tranform_xml(context: AssetExecutionContext, prep_ticket_files: pd.DataFrame) -> str:
    df = prep_ticket_files.map(lambda x: transformCell(x))
    df['rows']=df.apply('\n'.join, axis=1)
    df['rows']=df['rows'].map(lambda x: '<Row>\n{}\n</Row>'.format(x))
    xml = transformEAMxml(df['rows'])
    filename=str(int(datetime.utcnow().timestamp())) + '.xml'
    fp = os.path.join(os.getenv("BI_DIRECTORY_BASE"),'Departments/Maintenance/Data Uploads/Fuel Tickets/Uploads','upload_' + filename)
    with open(fp,'w') as f:
        f.write(xml)
        f.close()
    context.add_output_metadata(
        metadata={
            "output_file": filename
        }
    )
    return fp
