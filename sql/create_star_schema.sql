-- Dataset creation (optional if already exists)
CREATE SCHEMA IF NOT EXISTS `radic-healthcare.healthcare_dataset`;

-- Dimension Tables
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.dim_patient` (
  patient_id STRING,
  gender STRING,
  birth_date DATE,
  ethnicity STRING,
  is_deceased BOOL,
  deceased_date DATE
);

CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.dim_provider` (
  provider_id STRING,
  first_name STRING,
  last_name STRING,
  specialty STRING,
  npi STRING,
  is_current BOOL
);

CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.dim_facility` (
  facility_id STRING,
  name STRING,
  location STRING,
  bed_count INT64
);

CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.dim_diagnosis` (
  diagnosis_code STRING,
  description STRING,
  category STRING,
  is_current BOOL
);

CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.dim_date` (
  date_key INT64,  -- e.g., 20240508
  date_value DATE,
  day INT64,
  month INT64,
  year INT64,
  day_of_week STRING,
  week_of_year INT64,
  is_weekend BOOL
);

-- Fact Table
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.fact_encounter` (
  encounter_id STRING,
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
  referral_provider_id STRING
);