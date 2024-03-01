from dagster import asset, AssetExecutionContext, MetadataValue, get_dagster_logger, Config 
from CTRAN.resources import SFTPResource, INITResource, MSSQLResource
import pandas as pd
import os
import pathlib
from datetime import date
from datetime import datetime, timedelta
import pytz
import csv

@asset(group_name = "C_VAN_Dashboard")
def my_asset(INIT: INITResource):
    query_statement = f"""
        select *
        from nom_line
    """
    df = pd.DataFrame(INIT.query(query_statement))

    print(df)
    
    file_path = "/media/windowsshare/Business Intelligence/Dagster_Test/Code Location 2/test/test.csv"
    df.to_csv(file_path, index=False)

@asset(group_name = "C_VAN_Dashboard")
def test_2(Trapeze: MSSQLResource):
    query_statement = f"""
        select *
        from Schedules s 
        where SchId = 7423
    """
    query_body, query_header = Trapeze.query(query_statement)
    header = [i[0] for i in query_header]
    df = pd.DataFrame(query_body, columns = header)

    print(df)

    file_path = "/media/windowsshare/Business Intelligence/Dagster_Test/Code Location 2/test/test_2.csv"
    df.to_csv(file_path, index=False) 

@asset(group_name = "C_VAN_Dashboard")
def test_3(DWCTRAN: MSSQLResource):
    query_statement = f"""
        SELECT TOP (1000) [VEHICLE_CODE]
            ,[ODOMETER]
            ,[ACT_TIME]
            ,[NOM_TIME]
            ,[LINE_CODE]
            ,[COURSE_CODE]
        FROM [test].[dbo].[INIT_MOBILE_EV_TRIP]
        where VEHICLE_CODE = 'C2174'
    """
    df = pd.DataFrame(DWCTRAN.query(query_statement))

    print(df)

    file_path = "/media/windowsshare/Business Intelligence/Dagster_Test/Code Location 2/test/test_3.csv"
    df.to_csv(file_path, index=False)


@asset(group_name = "C_VAN_Dashboard")
def test_4(EAM: MSSQLResource):
    query_statement = f"""
        select *
        FROM RPT_EMPLOYEES
        where [Employee ID] = '3187'
    """
    df = pd.DataFrame(EAM.query(query_statement))

    print(df)

    file_path = "/media/windowsshare/Business Intelligence/Dagster_Test/Code Location 2/test/test_4.csv"
    df.to_csv(file_path, index=False)