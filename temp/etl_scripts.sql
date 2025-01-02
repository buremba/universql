CREATE OR REPLACE FILE FORMAT csv_with_arrays
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

------ RUNS ON SNOWFLAKE COMPUTE --------
-- CREATE OR REPLACE ICEBERG OBJECTS

CREATE SCHEMA if not exists iceberg_db.iot;

CREATE OR REPLACE FILE FORMAT csv_with_arrays
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE ICEBERG TABLE iceberg_db.iot.dim_devices (
    device_id VARCHAR NOT NULL,
    device_name VARCHAR,
    device_type VARCHAR,
    manufacturer VARCHAR,
    model_number VARCHAR,
    firmware_version VARCHAR,
    installation_date DATE,
    location_id VARCHAR,
    location_name VARCHAR,
    facility_zone VARCHAR,
    is_active BOOLEAN,
    expected_lifetime_months INT,
    maintenance_interval_days INT,
    last_maintenance_date DATE,
    next_scheduled_maintenance DATE,
    created_at TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (device_id)
    )
external_volume = iceberg_external_volume
catalog = 'SNOWFLAKE'
BASE_LOCATION = 's3://melchi-snowflake-iceberg/dim_devices';

CREATE OR REPLACE ICEBERG TABLE iceberg_db.iot.fact_device_readings (
    reading_id VARCHAR NOT NULL,
    device_id VARCHAR NOT NULL,
    reading_timestamp TIMESTAMP_NTZ,
    temperature FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    voltage FLOAT,
    "current" FLOAT,
    power_consumption FLOAT,
    operational_status VARCHAR,
    error_code VARCHAR,
    reading_quality_score FLOAT,
    is_anomaly BOOLEAN,
    batch_id VARCHAR,
    created_at TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (reading_id),
    FOREIGN KEY (device_id) REFERENCES dim_devices(device_id)
    )
external_volume = iceberg_external_volume
catalog = 'SNOWFLAKE'
BASE_LOCATION = 's3://melchi-snowflake-iceberg/fact_device_readings';

CREATE OR REPLACE ICEBERG TABLE iceberg_db.iot.fact_maintenance_logs (
    maintenance_id VARCHAR NOT NULL,
    device_id VARCHAR NOT NULL,
    maintenance_type VARCHAR,
    maintenance_date TIMESTAMP_NTZ,
    technician_id VARCHAR,
    technician_name VARCHAR,
    work_order_number VARCHAR,
    maintenance_duration_minutes INT,
    parts_replaced ARRAY(VARCHAR),
    maintenance_cost DECIMAL(12,2),
    maintenance_notes VARCHAR,
    next_maintenance_date DATE,
    maintenance_status VARCHAR,
    quality_check_passed BOOLEAN,
    created_at TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (maintenance_id),
    FOREIGN KEY (device_id) REFERENCES dim_devices(device_id)
)
external_volume = iceberg_external_volume
catalog = 'SNOWFLAKE'
BASE_LOCATION = 's3://melchi-snowflake-iceberg/fact_maintenance_logs';

-- agg device health
CREATE OR REPLACE ICEBERG TABLE iceberg_db.iot.agg_device_health (
    device_id VARCHAR NOT NULL,
    date_key DATE NOT NULL,
    avg_temperature FLOAT,
    avg_pressure FLOAT,
    avg_humidity FLOAT,
    avg_power_consumption FLOAT,
    max_temperature FLOAT,
    max_pressure FLOAT,
    total_errors INT,
    unique_error_codes INT,
    operational_time_minutes INT,
    downtime_minutes INT,
    anomaly_count INT,
    health_score FLOAT,
    maintenance_due_in_days INT,
    created_at TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (device_id, date_key),
    FOREIGN KEY (device_id) REFERENCES dim_devices(device_id)
)
external_volume = iceberg_external_volume
catalog = 'SNOWFLAKE'
BASE_LOCATION = 's3://melchi-snowflake-iceberg/agg_device_health';

-- CREATE OR REPLACE TEMP TABLES

--- RUNS ON LOCAL COMPUTE ---
-- Create temporary staging tables for raw data
CREATE OR REPLACE TEMPORARY TABLE stg_device_metadata (
    device_id VARCHAR,
    device_name VARCHAR,
    device_type VARCHAR,
    manufacturer VARCHAR,
    model_number VARCHAR,
    firmware_version VARCHAR,
    installation_date DATE,
    location_id VARCHAR,
    location_name VARCHAR,
    facility_zone VARCHAR,
    is_active BOOLEAN,
    expected_lifetime_months INT,
    maintenance_interval_days INT,
    last_maintenance_date DATE
);

CREATE OR REPLACE TEMPORARY TABLE stg_telemetry_readings (
    reading_id VARCHAR,
    device_id VARCHAR,
    reading_timestamp TIMESTAMP_NTZ,
    temperature FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    voltage FLOAT,
    "current" FLOAT,
    power_consumption FLOAT,
    operational_status VARCHAR,
    error_code VARCHAR,
    reading_quality_score FLOAT,
    is_anomaly BOOLEAN,
    batch_id VARCHAR
);

CREATE OR REPLACE TEMPORARY TABLE stg_maintenance_logs (
    maintenance_id VARCHAR,
    device_id VARCHAR,
    maintenance_type VARCHAR,
    maintenance_date TIMESTAMP_NTZ,
    technician_id VARCHAR,
    technician_name VARCHAR,
    work_order_number VARCHAR,
    maintenance_duration_minutes INT,
    parts_replaced ARRAY,
    maintenance_cost DECIMAL(12,2),
    maintenance_notes VARCHAR,
    next_maintenance_date DATE,
    maintenance_status VARCHAR,
    quality_check_passed BOOLEAN
);

-- COPY INTO TEMP TABLES

COPY INTO stg_device_metadata
FROM @iceberg_db.public.landing_stage/initial_objects/device_metadata.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO stg_telemetry_readings
FROM @iceberg_db.public.landing_stage/initial_objects/telemetry_data.jsonl
FILE_FORMAT = (TYPE = JSON)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO stg_maintenance_logs
FROM @iceberg_db.public.landing_stage/initial_objects/maintenance_logs.csv
FILE_FORMAT = csv_with_arrays;

-- TRANSFORM DATA IN ICEBERG TABLES BASED ON TEMP TABLES

UPDATE iceberg_db.iot.dim_devices target
SET 
    device_name = source.device_name,
    device_type = source.device_type,
    manufacturer = source.manufacturer,
    model_number = source.model_number,
    firmware_version = source.firmware_version,
    installation_date = source.installation_date,
    location_id = source.location_id,
    location_name = source.location_name,
    facility_zone = source.facility_zone,
    is_active = source.is_active,
    expected_lifetime_months = source.expected_lifetime_months,
    maintenance_interval_days = source.maintenance_interval_days,
    last_maintenance_date = source.last_maintenance_date,
    next_scheduled_maintenance = source.next_scheduled_maintenance,
    updated_at = source.updated_at
FROM (
    SELECT 
        device_id,
        device_name,
        device_type,
        manufacturer,
        model_number,
        firmware_version,
        installation_date,
        location_id,
        location_name,
        facility_zone,
        is_active,
        expected_lifetime_months,
        maintenance_interval_days,
        last_maintenance_date,
        DATEADD('days', maintenance_interval_days, last_maintenance_date) as next_scheduled_maintenance,
        CURRENT_TIMESTAMP() as updated_at
    FROM stg_device_metadata
) source
WHERE target.device_id = source.device_id;

-- INSERT PORTION runs on snowflake; SELECT PORTION runs on local
INSERT INTO iceberg_db.iot.dim_devices (
    device_id, device_name, device_type, manufacturer, model_number,
    firmware_version, installation_date, location_id, location_name,
    facility_zone, is_active, expected_lifetime_months,
    maintenance_interval_days, last_maintenance_date,
    next_scheduled_maintenance, created_at, updated_at
)
SELECT 
    source.device_id,
    source.device_name,
    source.device_type,
    source.manufacturer,
    source.model_number,
    source.firmware_version,
    source.installation_date,
    source.location_id,
    source.location_name,
    source.facility_zone,
    source.is_active,
    source.expected_lifetime_months,
    source.maintenance_interval_days,
    source.last_maintenance_date,
    DATEADD('days', maintenance_interval_days, last_maintenance_date) as next_scheduled_maintenance,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM stg_device_metadata source
WHERE device_id NOT IN (SELECT device_id FROM iceberg_db.iot.dim_devices);

-- INSERT PORTION runs on Snowflake; SELECT runs on local
-- Transform and load fact_maintenance_logs
INSERT INTO iceberg_db.iot.fact_maintenance_logs (
    maintenance_id, device_id, maintenance_type, maintenance_date,
    technician_id, technician_name, work_order_number,
    maintenance_duration_minutes, parts_replaced, maintenance_cost,
    maintenance_notes, next_maintenance_date, maintenance_status,
    quality_check_passed, created_at
)
SELECT 
    maintenance_id,
    device_id,
    maintenance_type,
    maintenance_date,
    technician_id,
    technician_name,
    work_order_number,
    maintenance_duration_minutes,
    parts_replaced::array(varchar),
    maintenance_cost,
    maintenance_notes,
    next_maintenance_date,
    maintenance_status,
    quality_check_passed,
    CURRENT_TIMESTAMP() as created_at
FROM stg_maintenance_logs
WHERE maintenance_id NOT IN (SELECT maintenance_id FROM fact_maintenance_logs);

-- INSERT PORTION runs on snowflake; EVERYTHING ELSE runs on duckdb
-- Calculate and load aggregated device health metrics
INSERT INTO agg_device_health (
    device_id,
    date_key,
    avg_temperature,
    avg_pressure,
    avg_humidity,
    avg_power_consumption,
    max_temperature,
    max_pressure,
    total_errors,
    unique_error_codes,
    operational_time_minutes,
    downtime_minutes,
    anomaly_count,
    health_score,
    maintenance_due_in_days,
    created_at
)
WITH daily_metrics AS (
    SELECT 
        device_id,
        DATE(reading_timestamp) as date_key,
        AVG(temperature) as avg_temperature,
        AVG(pressure) as avg_pressure,
        AVG(humidity) as avg_humidity,
        AVG(power_consumption) as avg_power_consumption,
        MAX(temperature) as max_temperature,
        MAX(pressure) as max_pressure,
        COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) as total_errors,
        COUNT(DISTINCT error_code) as unique_error_codes,
        COUNT(CASE WHEN operational_status = 'OPERATIONAL' THEN 1 END) * 60 as operational_time_minutes,
        COUNT(CASE WHEN operational_status != 'OPERATIONAL' THEN 1 END) * 60 as downtime_minutes,
        COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count
    FROM stg_telemetry_readings
    GROUP BY device_id, DATE(reading_timestamp)
),
device_maintenance AS (
    SELECT 
        d.device_id,
        d.next_scheduled_maintenance,
        DATEDIFF('day', CURRENT_DATE(), d.next_scheduled_maintenance) as days_until_maintenance
    FROM dim_devices d
)
SELECT 
    m.device_id,
    m.date_key,
    m.avg_temperature,
    m.avg_pressure,
    m.avg_humidity,
    m.avg_power_consumption,
    m.max_temperature,
    m.max_pressure,
    m.total_errors,
    m.unique_error_codes,
    m.operational_time_minutes,
    m.downtime_minutes,
    m.anomaly_count,
    -- Calculate health score (example formula)
    100 - (
        (m.total_errors * 5) + 
        (m.anomaly_count * 2) + 
        (CASE WHEN m.max_temperature > 85 THEN 10 ELSE 0 END) +
        (m.downtime_minutes / 144)  -- Penalize based on downtime (10% per day of downtime)
    ) as health_score,
    dm.days_until_maintenance,
    CURRENT_TIMESTAMP() as created_at
FROM daily_metrics m
JOIN device_maintenance dm ON m.device_id = dm.device_id
WHERE NOT EXISTS (
    SELECT 1 FROM agg_device_health a 
    WHERE a.device_id = m.device_id AND a.date_key = m.date_key
);

DROP TABLE IF EXISTS stg_device_metadata;
DROP TABLE IF EXISTS stg_telemetry_readings;
DROP TABLE IF EXISTS stg_maintenance_logs;