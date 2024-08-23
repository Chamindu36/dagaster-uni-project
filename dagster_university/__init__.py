# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import metrics, trips, zones

trip_assets = load_assets_from_modules([trips])
zone_aaets = load_assets_from_modules([zones])
metric_assets = load_assets_from_modules([metrics])

defs = Definitions(
    assets=[*trip_assets, *metric_assets, *zone_aaets],
)
