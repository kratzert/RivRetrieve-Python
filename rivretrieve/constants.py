"""Module for global constants."""

# Index.
GAUGE_ID = "gauge_id"
TIME_INDEX = "time"

# Attributes.
ALTITUDE = "altitude"
AREA = "area"
COUNTRY = "country"
LATITUDE = "latitude"
LONGITUDE = "longitude"
RIVER = "river"
SOURCE = "source"
STATION_NAME = "station_name"

# General list of variables.
DISCHARGE = "discharge"
STAGE = "stage"
_WATER_TEMPERATURE = "water-temperature"
_CATCHMENT_PRECIPITATION = "catchment-precipitation"

# List of temporal resolutions.
DAILY = "daily"
INSTANTANEOUS = "instantaneous"

# List of temporal aggregrations.
_MEAN = "mean"
_MIN = "min"
_MAX = "max"
_SUM = "sum"

# ------------------------------ Supported set of variables ----------------------------------------

# Discharge.
DISCHARGE_DAILY_MEAN = f"{DISCHARGE}_{DAILY}_{_MEAN}"
DISCHARGE_DAILY_MAX = f"{DISCHARGE}_{DAILY}_{_MAX}"
DISCHARGE_DAILY_MIN = f"{DISCHARGE}_{DAILY}_{_MIN}"
DISCHARGE_INSTANT = f"{DISCHARGE}_{INSTANTANEOUS}"

# Stage.
STAGE_DAILY_MEAN = f"{STAGE}_{DAILY}_{_MEAN}"
STAGE_DAILY_MAX = f"{STAGE}_{DAILY}_{_MAX}"
STAGE_DAILY_MIN = f"{STAGE}_{DAILY}_{_MIN}"
STAGE_INSTANT = f"{STAGE}_{INSTANTANEOUS}"

# Water temperature.
WATER_TEMPERATURE_DAILY_MEAN = f"{_WATER_TEMPERATURE}_{DAILY}_{_MEAN}"

# Precipitation
CATCHMENT_PRECIPITATION_DAILY_SUM = f"{_CATCHMENT_PRECIPITATION}_{DAILY}_{_SUM}"
