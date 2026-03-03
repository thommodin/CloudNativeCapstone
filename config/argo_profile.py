import pyarrow

SCHEMA = pyarrow.schema(
    [
        pyarrow.field("N_PROF", pyarrow.int64()),
        pyarrow.field("N_LEVELS", pyarrow.int64()),
        pyarrow.field("DATA_TYPE", pyarrow.string()),
        pyarrow.field("FORMAT_VERSION", pyarrow.string()),
        pyarrow.field("HANDBOOK_VERSION", pyarrow.string()),
        pyarrow.field("REFERENCE_DATE_TIME", pyarrow.string()),
        pyarrow.field("DATE_CREATION", pyarrow.string()),
        pyarrow.field("DATE_UPDATE", pyarrow.string()),
        pyarrow.field("PLATFORM_NUMBER", pyarrow.string()),
        pyarrow.field("PROJECT_NAME", pyarrow.string()),
        pyarrow.field("PI_NAME", pyarrow.string()),
        pyarrow.field("CYCLE_NUMBER", pyarrow.float64()),
        pyarrow.field("DIRECTION", pyarrow.string()),
        pyarrow.field("DATA_CENTRE", pyarrow.string()),
        pyarrow.field("DC_REFERENCE", pyarrow.string()),
        pyarrow.field("DATA_STATE_INDICATOR", pyarrow.string()),
        pyarrow.field("DATA_MODE", pyarrow.string()),
        pyarrow.field("PLATFORM_TYPE", pyarrow.string()),
        pyarrow.field("FLOAT_SERIAL_NO", pyarrow.string()),
        pyarrow.field("FIRMWARE_VERSION", pyarrow.string()),
        pyarrow.field("WMO_INST_TYPE", pyarrow.string()),
        pyarrow.field("JULD", pyarrow.timestamp("ns")),
        pyarrow.field("JULD_QC", pyarrow.string()),
        pyarrow.field("JULD_LOCATION", pyarrow.timestamp("ns")),
        pyarrow.field("LATITUDE", pyarrow.float64()),
        pyarrow.field("LONGITUDE", pyarrow.float64()),
        pyarrow.field("POSITION_QC", pyarrow.string()),
        pyarrow.field("POSITIONING_SYSTEM", pyarrow.string()),
        pyarrow.field("PROFILE_PRES_QC", pyarrow.string()),
        pyarrow.field("PROFILE_TEMP_QC", pyarrow.string()),
        pyarrow.field("PROFILE_PSAL_QC", pyarrow.string()),
        pyarrow.field("VERTICAL_SAMPLING_SCHEME", pyarrow.string()),
        pyarrow.field("CONFIG_MISSION_NUMBER", pyarrow.float64()),
        pyarrow.field("PRES", pyarrow.float32()),
        pyarrow.field("PRES_QC", pyarrow.string()),
        pyarrow.field("PRES_ADJUSTED", pyarrow.float32()),
        pyarrow.field("PRES_ADJUSTED_QC", pyarrow.string()),
        pyarrow.field("PRES_ADJUSTED_ERROR", pyarrow.float32()),
        pyarrow.field("TEMP", pyarrow.float32()),
        pyarrow.field("TEMP_QC", pyarrow.string()),
        pyarrow.field("TEMP_ADJUSTED", pyarrow.float32()),
        pyarrow.field("TEMP_ADJUSTED_QC", pyarrow.string()),
        pyarrow.field("TEMP_ADJUSTED_ERROR", pyarrow.float32()),
        pyarrow.field("PSAL", pyarrow.float32()),
        pyarrow.field("PSAL_QC", pyarrow.string()),
        pyarrow.field("PSAL_ADJUSTED", pyarrow.float32()),
        pyarrow.field("PSAL_ADJUSTED_QC", pyarrow.string()),
        pyarrow.field("PSAL_ADJUSTED_ERROR", pyarrow.float32()),
    ]
)

REQUIRED_VARIABLES: set[str] = {"PRES", "TEMP", "PSAL", "JULD", "LATITUDE", "LONGITUDE"}

REQUIRED_DIMS: set[str] = {"N_PROF", "N_LEVELS"}

DROP_DIMS: set[str] = {"N_PARAM", "N_HISTORY", "N_CALIB"}
