from dagster import Definitions, load_assets_from_modules, EnvVar
from CTRAN.resources import INITResource, MSSQLResource, mSETResource, SFTPResource
import CTRAN.assets
from CTRAN.jobs import *
from CTRAN.sensors import fuel_upload_jobs
from CTRAN.schedules import *

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    #asset_checks=[CTRAN.assets.validate_psu_csv_schema],
    resources={
        "INIT": INITResource(
            username = EnvVar("INIT_USERNAME"), 
            password = EnvVar("INIT_PASSWORD"), 
            connection_string='//INIT-CTRAN-ORC.c-tran.org:1521/MOBILE.INIT-CTRAN-ORC'  # '//INIT-CTRAN-ORC:1521/MOBILE.INIT-CTRAN-ORC'
        ),
        "DWCTRAN": MSSQLResource(
            username=EnvVar("WINDOWS_SERVICE_USERNAME"),
            password=EnvVar("WINDOWS_SERVICE_PASSWORD"),
            host="10.111.20.27\ReportStaging",  # "AOM-C-REPORT01\ReportStaging"
            database="test"
        ),
        "EAM": MSSQLResource(
            username=EnvVar("EAM_USERNAME"),
            password=EnvVar("EAM_PASSWORD"),
            host="10.111.20.150",  # "CT-V-TRAP-EAMDB"
            database="EAMProd22"
        ),
        "Trapeze": MSSQLResource(
            username=EnvVar("WINDOWS_SERVICE_USERNAME"),
            password=EnvVar("WINDOWS_SERVICE_PASSWORD"),
            host="10.111.20.73",  # "VM-DB-TP15"
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
    sensors=[fuel_upload_jobs],
    jobs=[log_file_job,PSU_extract_job],
    schedules=[psu_schedule]
)
