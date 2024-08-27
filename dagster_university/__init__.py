from dagster import Definitions, load_assets_from_modules

from .assets import metrics, trips, zones, trips_by_week, requests
from .resources import database_resource
from .jobs import trip_update_job, weekly_update_job, adhoc_request_job
from .schedules import trip_update_schedule, weekly_update_schedule
from .sensors import adhoc_request_sensor


trip_assets = load_assets_from_modules([trips])
zone_aaets = load_assets_from_modules([zones])
metric_assets = load_assets_from_modules([metrics])
metric_assets += load_assets_from_modules([trips_by_week])
request_assets = load_assets_from_modules([requests])

all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]
all_sensors = [adhoc_request_sensor]


defs = Definitions(
    assets=[*trip_assets, *metric_assets, *zone_aaets, *request_assets],
    resources={
        "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)
