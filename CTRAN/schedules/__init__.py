from dagster import ScheduleDefinition,define_asset_job
from CTRAN.jobs import PSU_extract_job

psu_schedule = ScheduleDefinition(job=PSU_extract_job, cron_schedule="8 6 * * *", execution_timezone="America/Los_Angeles")
