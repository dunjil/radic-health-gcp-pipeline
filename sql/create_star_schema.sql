-- Dataset creation (optional if already exists)
CREATE SCHEMA IF NOT EXISTS `radichealth_analytics.healthcare_dataset`;

-- Dimension Tables

CREATE TABLE IF NOT EXISTS `radichealth_analytics.healthcare_dataset.dim_patient` (
  patient_id STRING PRIMARY KEY,
  gender STRING,
  birth_date DATE,
  ethnicity STRING,
  is_deceased BOOL,
  deceased_date DATE
);

CREATE TABLE IF NOT EXISTS `radichealth_analytics.healthcare_dataset.dim_provider` (
  provider_id STRING PRIMARY KEY,
  first_name STRING,
  last_name STRING,
  specialty STRING,
  npi STRING,
  is_current BOOL
);

CREATE TABLE IF NOT EXISTS `radichealth_analytics.healthcare_dataset.dim_facility` (
  facility_id STRING PRIMARY KEY,
  name STRING,
  location STRING,
  bed_count INT64
);

CREATE TABLE IF NOT EXISTS `radichealth_analytics.healthcare_dataset.dim_diagnosis` (
  diagnosis_code STRING PRIMARY KEY,
  description STRING,
  category STRING,
  is_current BOOL
);

CREATE TABLE IF NOT EXISTS `radichealth_analytics.healthcare_dataset.dim_date` (
  date_key INT64 PRIMARY KEY,  -- e.g., 20240508
  date_value DATE,
  day INT64,
  month INT64,
  year INT64,
  day_of_week STRING,
  week_of_year INT64,
  is_weekend BOOL
);

-- Fact Table

CREATE TABLE IF NOT EXISTS `radichealth_analytics.healthcare_dataset.fact_encounter` (
  encounter_id STRING PRIMARY KEY,
  patient_id STRING,
  provider_id STRING,
  facility_id STRING,
  diagnosis_code STRING,
  admission_date DATE,
  discharge_date DATE,
  date_key INT64, -- FK to dim_date
  length_of_stay INT64,
  total_charges FLOAT64,
  payments_received FLOAT64,
  insurance_type STRING,
  referral_provider_id STRING,

  -- Foreign Keys (documented only, since BigQuery doesn’t enforce constraints)
  -- FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id),
  -- FOREIGN KEY (provider_id) REFERENCES dim_provider(provider_id),
  -- FOREIGN KEY (facility_id) REFERENCES dim_facility(facility_id),
  -- FOREIGN KEY (diagnosis_code) REFERENCES dim_diagnosis(diagnosis_code),
  -- FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  -- FOREIGN KEY (referral_provider_id) REFERENCES dim_provider(provider_id)
);
