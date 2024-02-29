from dagster import Definitions, load_assets_from_modules, EnvVar
from CTRAN.resources import INITResource, MSSQLResource, mSETResource, StubSFTP, SFTPResource, ODBCResource, OracleResource
import CTRAN.assets
from CTRAN.jobs import *
from CTRAN.sensors import fuel_upload_jobs
from CTRAN.schedules import *
from os import getenv

all_assets = load_assets_from_modules([assets])

resources={
    "local": {
        "INIT": INITResource(
            username = EnvVar("INIT_USERNAME"),
            password = EnvVar("INIT_PASSWORD"),
            connection_string='//INIT-CTRAN-ORC:1521/MOBILE.INIT-CTRAN-ORC'
        ),
        "INIT2": OracleResource(
            username = EnvVar("INIT_USERNAME"),
            password = EnvVar("INIT_PASSWORD"),
            connection_string='//INIT-CTRAN-ORC:1521/MOBILE.INIT-CTRAN-ORC'
        ),
        "DWCTRAN": ODBCResource(
            driver="{FreeTDS}",
            username=EnvVar("WINDOWS_SERVICE_USERNAME"),
            password=EnvVar("WINDOWS_SERVICE_PASSWORD"),
            server="AOM-C-REPORT01\ReportStaging",
            database="test",
            port=58805
        ),
        "EAM": ODBCResource(
            driver="{ODBC Driver 18 for SQL Server}",
            username=EnvVar("EAM_USERNAME"),
            password=EnvVar("EAM_PASSWORD"),
            server="CT-V-TRAP-EAMDB",
            database="EAMProd22"
        ),
        "Trapeze": MSSQLResource(
            username=EnvVar("WINDOWS_SERVICE_USERNAME"),
            password=EnvVar("WINDOWS_SERVICE_PASSWORD"),
            host="VM-DB-TP15",
            database="mmsdata1"
        ),
        "mSET": mSETResource(
            username=EnvVar("MSET_SERVICE_USERNAME"),
            password=EnvVar("MSET_SERVICE_PASSWORD")
        ),
        "PSUSFTP": StubSFTP(),
    },
    "production": {
        "INIT": INITResource(
            username = EnvVar("INIT_USERNAME"),
            password = EnvVar("INIT_PASSWORD"),
            connection_string='//INIT-CTRAN-ORC:1521/MOBILE.INIT-CTRAN-ORC'
        ),
        "INIT2": OracleResource(
            username = EnvVar("INIT_USERNAME"),
            password = EnvVar("INIT_PASSWORD"),
            connection_string='//INIT-CTRAN-ORC:1521/MOBILE.INIT-CTRAN-ORC'
        ),
        "DWCTRAN": ODBCResource(
            driver="{FreeTDS}",
            username=EnvVar("WINDOWS_SERVICE_USERNAME"),
            password=EnvVar("WINDOWS_SERVICE_PASSWORD"),
            server="AOM-C-REPORT01\ReportStaging",
            database="test",
            port=58805
        ),
        "EAM": ODBCResource(
            driver="{ODBC Driver 18 for SQL Server}",
            username=EnvVar("EAM_USERNAME"),
            password=EnvVar("EAM_PASSWORD"),
            server="CT-V-TRAP-EAMDB",
            database="EAMProd22"
        ),
        "Trapeze": MSSQLResource(
            username=EnvVar("WINDOWS_SERVICE_USERNAME"),
            password=EnvVar("WINDOWS_SERVICE_PASSWORD"),
            host="VM-DB-TP15",
            database="mmsdata1"
        ),
        "mSET": mSETResource(
            username=EnvVar("MSET_SERVICE_USERNAME"),
            password=EnvVar("MSET_SERVICE_PASSWORD")
        ),
        "PSUSFTP": SFTPResource(
            user=EnvVar("PSU_SSHUSER"),
            host=EnvVar("PSU_SSHHOST"),
            keypass=EnvVar("PSU_SSHKEYPASSWORD"),
            keyfile=EnvVar("PSU_SSHKEYFILE"),
        ),
    },
}

deployment_name=getenv("DAGSTER_DEPLOYMENT")

defs = Definitions(
    assets=all_assets,
#    asset_checks=[CTRAN.assets.validate_psu_csv_schema], 
    resources=resources[deployment_name],
    sensors=[fuel_upload_jobs],
    jobs=[log_file_job,PSU_extract_job],
    schedules=[psu_schedule]
)
