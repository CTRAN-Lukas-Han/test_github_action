import os
from dagster import sensor, RunRequest, RunConfig, EnvVar
from CTRAN.jobs import log_file_job,FileConfig

base_dir=os.getenv("BI_DIRECTORY_BASE")

@sensor(job=log_file_job)
def fuel_upload_jobs():
    FUEL_DATA_DIR=os.path.join(base_dir, "Power Platform Test")
    for filename in os.listdir(FUEL_DATA_DIR):
        filepath = os.path.join(FUEL_DATA_DIR,filename)
        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                run_config=RunConfig(
                    ops={"prep_ticket_files": FileConfig(filename=filename, filepath=filepath)}
                )
            )
            
