from dagster import Definitions, load_assets_from_modules

from .assets import metrics, trips, zones, trips_by_week
from .resources import database_resource

trip_assets = load_assets_from_modules([trips])
zone_aaets = load_assets_from_modules([zones])
metric_assets = load_assets_from_modules([metrics])
metric_assets += load_assets_from_modules([trips_by_week])

defs = Definitions(
    assets=[*trip_assets, *metric_assets, *zone_aaets],
    resources={
        "database": database_resource,
    }
)
