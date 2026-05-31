-- Databricks notebook source
drop table if exists clinical.metadata.tables;
drop table if exists clinical.metadata.table_parameters;
drop table if exists clinical.metadata.table_watermarks;
drop table if exists clinical.metadata.pipeline_runs

-- COMMAND ----------

-- DBTITLE 1,Create Tables
-- =====================================================
-- CATALOG & SCHEMA
-- =====================================================

CREATE CATALOG IF NOT EXISTS clinical;

CREATE SCHEMA IF NOT EXISTS clinical.metadata;


-- =====================================================
-- 1️⃣ metadata.tables
-- Static registry of logical tables
-- =====================================================

CREATE TABLE IF NOT EXISTS clinical.metadata.tables (
    table_id            INT,
    table_name          STRING,

    source_system       STRING,        -- CTMS / EDC / SHIPMENT / LIMS / BIOBANK / FILE / API
    source_schema       STRING,
    source_table        STRING,
    source_path         STRING,

    target_layer        STRING,        -- silver / gold

    bronze_schema       STRING,
    silver_schema       STRING,
    gold_schema         STRING,

    active_flag         BOOLEAN,
    load_order          INT,
    created_at          TIMESTAMP
)
USING DELTA;


-- =====================================================
-- 2️⃣ metadata.table_parameters
-- Processing configuration (load type, PK, watermark)
-- =====================================================

CREATE TABLE IF NOT EXISTS clinical.metadata.table_parameters (
    table_id            INT,
    parameter_name      STRING,        -- load_type / primary_key / watermark_column
    parameter_value     STRING,
    created_at          TIMESTAMP
)
USING DELTA;


-- =====================================================
-- 3️⃣ metadata.table_watermarks
-- Stores last successful watermark per table
-- =====================================================

CREATE TABLE IF NOT EXISTS clinical.metadata.table_watermarks (
    table_id                INT,
    last_watermark_value    STRING,
    last_updated_at         TIMESTAMP,
    last_run_id             BIGINT
)
USING DELTA
PARTITIONED BY (table_id);


-- =====================================================
-- 4️⃣ metadata.pipeline_runs
-- Execution audit table
-- =====================================================

CREATE TABLE IF NOT EXISTS clinical.metadata.pipeline_runs (
    run_id              BIGINT,
    table_id            INT,
    layer               STRING,        -- Bronze / Silver / Gold
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              STRING,
    number_of_records   BIGINT,
    error_message       STRING
)
USING DELTA
PARTITIONED BY (table_id);



-- COMMAND ----------

-- =====================================================
-- INSERT INTO metadata.tables
-- =====================================================

-- COMMAND ----------

-- CTMS TABLES (FILES)

INSERT INTO clinical.metadata.tables VALUES

(1,'study_master','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/study_master/','bronze',
'bronze','silver',NULL,TRUE,1,current_timestamp()),

(2,'site_master','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/site_master/','bronze',
'bronze','silver',NULL,TRUE,3,current_timestamp()),

(3,'visit_schedule','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/visit_schedule/','bronze',
'bronze','silver',NULL,TRUE,6,current_timestamp()),

(4,'sample_plan','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/sample_plan/','bronze',
'bronze','silver',NULL,TRUE,9,current_timestamp());

-- COMMAND ----------

-- =====================================================
-- MISSING CTMS TABLES (FILES)
-- =====================================================

INSERT INTO clinical.metadata.tables VALUES

-- CONSENT
(33,'consent_master','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/consent_master/',
'bronze','bronze','silver',NULL,TRUE,13,current_timestamp()),

-- INVESTIGATOR
(34,'investigator_master','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/investigator_master/',
'bronze','bronze','silver',NULL,TRUE,5,current_timestamp()),

-- PROTOCOL VERSION
(35,'protocol_version','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/protocol_version/',
'bronze','bronze','silver',NULL,TRUE,2,current_timestamp()),

-- SAMPLE TYPE MASTER
(36,'sample_type_master_ctms','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/sample_type_master_ctms/',
'bronze','bronze','silver',NULL,TRUE,8,current_timestamp()),

-- STUDY ENROLLMENT PLAN
(37,'study_enrollment_plan','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/study_enrollment_plan/',
'bronze','bronze','silver',NULL,TRUE,12,current_timestamp()),

-- STUDY SITE MAP
(38,'study_site_map','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/study_site_map/',
'bronze','bronze','silver',NULL,TRUE,4,current_timestamp()),

-- TEST PLAN
(39,'test_plan','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/test_plan/',
'bronze','bronze','silver',NULL,TRUE,10,current_timestamp()),

-- TREATMENT ARM
(40,'treatment_arm','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/treatment_arm/',
'bronze','bronze','silver',NULL,TRUE,11,current_timestamp()),

-- VISIT ACTIVITY PLAN
(41,'visit_activity_plan','blob',NULL,NULL,
'/Volumes/clinical/source/ctms_volume/visit_activity_plan/',
'bronze','bronze','silver',NULL,TRUE,7,current_timestamp());

-- COMMAND ----------


-- EDC TABLES (DB)
INSERT INTO clinical.metadata.tables VALUES
(5,'subject','sqlserver','clinical_data','subject',NULL,'bronze','bronze','silver',NULL,TRUE,5,current_timestamp()),
(6,'visit','sqlserver','clinical_data','visit',NULL,'bronze','bronze','silver',NULL,TRUE,6,current_timestamp()),
(7,'form_instance','sqlserver','clinical_data','form_instance',NULL,'bronze','bronze','silver',NULL,TRUE,7,current_timestamp()),
(8,'field_data','sqlserver','clinical_data','field_data',NULL,'bronze','bronze','silver',NULL,TRUE,7,current_timestamp()),
(9,'lab_data','sqlserver','clinical_data','lab_data',NULL,'bronze','bronze','silver',NULL,TRUE,7,current_timestamp());

-- COMMAND ----------


-- METADATA  (DB)
INSERT INTO clinical.metadata.tables VALUES
(10,'form_definition','sqlserver','clinical_metadata','form_definition',NULL,'bronze','bronze','silver',NULL,TRUE,5,current_timestamp()),
(11,'field_definition','sqlserver','clinical_metadata','field_definition',NULL,'bronze','bronze','silver',NULL,TRUE,6,current_timestamp());

-- COMMAND ----------

-- AUDIT  (DB)
INSERT INTO clinical.metadata.tables VALUES
(12,'audit_trail','sqlserver','audit','audit_trail',NULL,'bronze','bronze','silver',NULL,TRUE,5,current_timestamp());

-- COMMAND ----------

-- SHIPMENT (SAP DB)
INSERT INTO clinical.metadata.tables VALUES
(13,'carrier_master','sqlserver','sap_logistics','carrier_master',NULL,'bronze','bronze','silver',NULL,TRUE,8,current_timestamp()),
(14,'shipment_header','sqlserver','sap_logistics','shipment_header',NULL,'bronze','bronze','silver',NULL,TRUE,9,current_timestamp()),
(15,'shipment_item','sqlserver','sap_logistics','shipment_item',NULL,'bronze','bronze','silver',NULL,TRUE,10,current_timestamp()),
(16,'shipment_status','sqlserver','sap_logistics','shipment_status',NULL,'bronze','bronze','silver',NULL,TRUE,10,current_timestamp()),
(17,'delivery_header','sqlserver','sap_logistics','delivery_header',NULL,'bronze','bronze','silver',NULL,TRUE,10,current_timestamp()),
(18,'delivery_item','sqlserver','sap_logistics','delivery_item',NULL,'bronze','bronze','silver',NULL,TRUE,11,current_timestamp());

-- COMMAND ----------

-- LIMS TABLES
INSERT INTO clinical.metadata.tables VALUES
(19,'lims_sample','sqlserver','lims','lims_sample',NULL,'bronze','bronze','silver',NULL,TRUE,11,current_timestamp()),
(20,'accession','sqlserver','lims','accession',NULL,'bronze','bronze','silver',NULL,TRUE,12,current_timestamp()),
(21,'analysis','sqlserver','lims','analysis',NULL,'bronze','bronze','silver',NULL,TRUE,13,current_timestamp()),
(22,'result','sqlserver','lims','result',NULL,'bronze','bronze','silver',NULL,TRUE,14,current_timestamp()),
(23,'result_audit','sqlserver','lims','result_audit',NULL,'bronze','bronze','silver',NULL,TRUE,15,current_timestamp()),
(24,'lims_sample_location','sqlserver','lims','lims_sample_location',NULL,'bronze','bronze','silver',NULL,TRUE,12,current_timestamp());

-- COMMAND ----------

-- BIOBANK TABLES
INSERT INTO clinical.metadata.tables VALUES
(25,'sample','sqlserver','biobank','sample',NULL,'bronze','bronze','silver',NULL,TRUE,19,current_timestamp()),
(26,'freezer','sqlserver','biobank','freezer',NULL,'bronze','bronze','silver',NULL,TRUE,15,current_timestamp()),
(27,'rack','sqlserver','biobank','rack',NULL,'bronze','bronze','silver',NULL,TRUE,16,current_timestamp()),
(28,'storage_box','sqlserver','biobank','storage_box',NULL,'bronze','bronze','silver',NULL,TRUE,17,current_timestamp()),
(29,'storage_position','sqlserver','biobank','storage_position',NULL,'bronze','bronze','silver',NULL,TRUE,18,current_timestamp()),
(30,'sample_location','sqlserver','biobank','sample_location',NULL,'bronze','bronze','silver',NULL,TRUE,20,current_timestamp()),
(31,'storage_history','sqlserver','biobank','storage_history',NULL,'bronze','bronze','silver',NULL,TRUE,21,current_timestamp()),
(32,'freezer_temperature','sqlserver','biobank','freezer_temperature',NULL,'bronze','bronze','silver',NULL,TRUE,16,current_timestamp());


-- COMMAND ----------

-- metadata.table_parameters (CRITICAL PART)

-- =====================================================
-- metadata.table_parameters
-- ENTERPRISE CLINICAL RESEARCH CONFIGURATION
-- =====================================================

INSERT INTO clinical.metadata.table_parameters VALUES

-- =====================================================
-- 1. CTMS FILE TABLES (FULL LOAD)
-- =====================================================

-- study_master
(1,'load_type','FULL',current_timestamp()),
(1,'primary_key','study_id',current_timestamp()),

-- site_master
(2,'load_type','FULL',current_timestamp()),
(2,'primary_key','site_id',current_timestamp()),

-- visit_schedule
(3,'load_type','FULL',current_timestamp()),
(3,'primary_key','visit_code',current_timestamp()),

-- sample_plan
(4,'load_type','FULL',current_timestamp()),
(4,'primary_key','sample_plan_id',current_timestamp()),

-- consent_master
(33,'load_type','FULL',current_timestamp()),
(33,'primary_key','consent_id',current_timestamp()),

-- investigator_master
(34,'load_type','FULL',current_timestamp()),
(34,'primary_key','investigator_id',current_timestamp()),

-- protocol_version
(35,'load_type','FULL',current_timestamp()),
(35,'primary_key','protocol_version_id',current_timestamp()),

-- sample_type_master_ctms
(36,'load_type','FULL',current_timestamp()),
(36,'primary_key','sample_type_id',current_timestamp()),

-- study_enrollment_plan
(37,'load_type','FULL',current_timestamp()),
(37,'primary_key','enrollment_plan_id',current_timestamp()),

-- study_site_map
(38,'load_type','FULL',current_timestamp()),
(38,'primary_key','study_site_map_id',current_timestamp()),

-- test_plan
(39,'load_type','FULL',current_timestamp()),
(39,'primary_key','test_plan_id',current_timestamp()),

-- treatment_arm
(40,'load_type','FULL',current_timestamp()),
(40,'primary_key','treatment_arm_id',current_timestamp()),

-- visit_activity_plan
(41,'load_type','FULL',current_timestamp()),
(41,'primary_key','visit_activity_plan_id',current_timestamp()),



-- =====================================================
-- 2. EDC TABLES
-- =====================================================

-- subject
(5,'load_type','MERGE',current_timestamp()),
(5,'primary_key','subject_id',current_timestamp()),
(5,'watermark_column','updated_at',current_timestamp()),

-- visit
(6,'load_type','MERGE',current_timestamp()),
(6,'primary_key','visit_id',current_timestamp()),
(6,'watermark_column','created_at',current_timestamp()),

-- form_instance
(7,'load_type','MERGE',current_timestamp()),
(7,'primary_key','form_instance_id',current_timestamp()),
(7,'watermark_column','updated_at',current_timestamp()),

-- field_data
(8,'load_type','MERGE',current_timestamp()),
(8,'primary_key','field_data_id',current_timestamp()),
(8,'watermark_column','updated_at',current_timestamp()),

-- lab_data
(9,'load_type','MERGE',current_timestamp()),
(9,'primary_key','lab_record_id',current_timestamp()),
(9,'watermark_column','updated_date',current_timestamp()),



-- =====================================================
-- 3. CLINICAL METADATA TABLES
-- =====================================================

-- form_definition
(10,'load_type','MERGE',current_timestamp()),
(10,'primary_key','form_id',current_timestamp()),
(10,'watermark_column','updated_at',current_timestamp()),

-- field_definition
(11,'load_type','MERGE',current_timestamp()),
(11,'primary_key','field_id',current_timestamp()),
(11,'watermark_column','updated_at',current_timestamp()),



-- =====================================================
-- 4. AUDIT TABLES
-- =====================================================

-- audit_trail
(12,'load_type','APPEND',current_timestamp()),
(12,'primary_key','audit_id',current_timestamp()),
(12,'watermark_column','change_datetime',current_timestamp()),



-- =====================================================
-- 5. SAP LOGISTICS TABLES
-- =====================================================

-- carrier_master
(13,'load_type','MERGE',current_timestamp()),
(13,'primary_key','carrier_id',current_timestamp()),
(13,'watermark_column','updated_date',current_timestamp()),

-- shipment_header
(14,'load_type','MERGE',current_timestamp()),
(14,'primary_key','shipment_id',current_timestamp()),
(14,'watermark_column','updated_date',current_timestamp()),

-- shipment_item
(15,'load_type','MERGE',current_timestamp()),
(15,'primary_key','shipment_id,item_id',current_timestamp()),
(15,'watermark_column','updated_date',current_timestamp()),

-- shipment_status
(16,'load_type','APPEND',current_timestamp()),
(16,'primary_key','status_id',current_timestamp()),
(16,'watermark_column','status_datetime',current_timestamp()),

-- delivery_header
(17,'load_type','MERGE',current_timestamp()),
(17,'primary_key','delivery_id',current_timestamp()),
(17,'watermark_column','updated_date',current_timestamp()),

-- delivery_item
(18,'load_type','MERGE',current_timestamp()),
(18,'primary_key','delivery_id,item_id',current_timestamp()),
(18,'watermark_column','updated_date',current_timestamp()),



-- =====================================================
-- 6. LIMS TABLES
-- =====================================================

-- lims_sample
(19,'load_type','MERGE',current_timestamp()),
(19,'primary_key','sample_id',current_timestamp()),
(19,'watermark_column','updated_date',current_timestamp()),

-- accession
(20,'load_type','MERGE',current_timestamp()),
(20,'primary_key','accession_id',current_timestamp()),
(20,'watermark_column','updated_date',current_timestamp()),

-- analysis
(21,'load_type','MERGE',current_timestamp()),
(21,'primary_key','analysis_id',current_timestamp()),
(21,'watermark_column','updated_date',current_timestamp()),

-- result
(22,'load_type','MERGE',current_timestamp()),
(22,'primary_key','result_id',current_timestamp()),
(22,'watermark_column','updated_date',current_timestamp()),

-- result_audit
(23,'load_type','APPEND',current_timestamp()),
(23,'primary_key','audit_id',current_timestamp()),
(23,'watermark_column','change_datetime',current_timestamp()),

-- lims_sample_location
(24,'load_type','APPEND',current_timestamp()),
(24,'primary_key','location_event_id',current_timestamp()),
(24,'watermark_column','event_datetime',current_timestamp()),



-- =====================================================
-- 7. BIOBANK TABLES
-- =====================================================

-- biobank_sample
(25,'load_type','MERGE',current_timestamp()),
(25,'primary_key','sample_id',current_timestamp()),
(25,'watermark_column','updated_date',current_timestamp()),

-- freezer
(26,'load_type','MERGE',current_timestamp()),
(26,'primary_key','freezer_id',current_timestamp()),
(26,'watermark_column','created_date',current_timestamp()),

-- rack
(27,'load_type','MERGE',current_timestamp()),
(27,'primary_key','rack_id',current_timestamp()),
(27,'watermark_column','created_date',current_timestamp()),

-- storage_box
(28,'load_type','MERGE',current_timestamp()),
(28,'primary_key','box_id',current_timestamp()),
(28,'watermark_column','created_date',current_timestamp()),

-- storage_position
(29,'load_type','MERGE',current_timestamp()),
(29,'primary_key','position_id',current_timestamp()),
(29,'watermark_column','created_date',current_timestamp()),

-- biobank_sample_location
(30,'load_type','MERGE',current_timestamp()),
(30,'primary_key','sample_id',current_timestamp()),
(30,'watermark_column','last_updated',current_timestamp()),

-- storage_history
(31,'load_type','APPEND',current_timestamp()),
(31,'primary_key','history_id',current_timestamp()),
(31,'watermark_column','event_datetime',current_timestamp()),

-- freezer_temperature
(32,'load_type','APPEND',current_timestamp()),
(32,'primary_key','temperature_id',current_timestamp()),
(32,'watermark_column','recorded_datetime',current_timestamp());


-- COMMAND ----------

-- WATERMARK INITIALIZATION

-- =====================================================
-- WATERMARK INITIALIZATION
-- clinical.metadata.table_watermarks
-- =====================================================
-- Initialize ONLY incremental tables
-- FULL load tables DO NOT require watermark initialization
-- =====================================================

INSERT INTO clinical.metadata.table_watermarks VALUES

-- =====================================================
-- 1. EDC TABLES
-- =====================================================

-- subject
(5,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- visit
(6,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- form_instance
(7,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- field_data
(8,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- lab_data
(9,'1900-01-01 00:00:00',current_timestamp(),NULL),



-- =====================================================
-- 2. CLINICAL METADATA TABLES
-- =====================================================

-- form_definition
(10,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- field_definition
(11,'1900-01-01 00:00:00',current_timestamp(),NULL),



-- =====================================================
-- 3. AUDIT TABLES
-- =====================================================

-- audit_trail
(12,'1900-01-01 00:00:00',current_timestamp(),NULL),



-- =====================================================
-- 4. SAP LOGISTICS TABLES
-- =====================================================

-- carrier_master
(13,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- shipment_header
(14,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- shipment_item
(15,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- shipment_status
(16,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- delivery_header
(17,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- delivery_item
(18,'1900-01-01 00:00:00',current_timestamp(),NULL),



-- =====================================================
-- 5. LIMS TABLES
-- =====================================================

-- lims_sample
(19,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- accession
(20,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- analysis
(21,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- result
(22,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- result_audit
(23,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- lims_sample_location
(24,'1900-01-01 00:00:00',current_timestamp(),NULL),



-- =====================================================
-- 6. BIOBANK TABLES
-- =====================================================

-- biobank_sample
(25,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- freezer
(26,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- rack
(27,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- storage_box
(28,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- storage_position
(29,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- biobank_sample_location
(30,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- storage_history
(31,'1900-01-01 00:00:00',current_timestamp(),NULL),

-- freezer_temperature
(32,'1900-01-01 00:00:00',current_timestamp(),NULL);

-- COMMAND ----------

-- GOLD TABLES (CLINICAL KPIs)
-- =====================================================
-- GOLD TABLES (CLINICAL KPI / ANALYTICS LAYER)
-- =====================================================
-- NOTE:
-- Existing IDs 20,21,22,23 are already used by LIMS tables
-- so using new IDs starting from 100
-- =====================================================

INSERT INTO clinical.metadata.tables VALUES

-- =====================================================
-- SAMPLE TRACKING SUMMARY
-- End-to-end sample movement tracking
-- =====================================================
(100,'sample_tracking_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,100,current_timestamp()),

-- =====================================================
-- LAB PERFORMANCE
-- Lab SLA / turnaround / quality metrics
-- =====================================================
(101,'lab_performance','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,101,current_timestamp()),

-- =====================================================
-- SHIPMENT SLA SUMMARY
-- Shipment delay / delivery KPI analytics
-- =====================================================
(102,'shipment_sla_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,102,current_timestamp()),

-- =====================================================
-- STUDY ENROLLMENT SUMMARY
-- Enrollment KPI aggregation
-- =====================================================
(103,'study_enrollment_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,103,current_timestamp()),

-- =====================================================
-- ADVERSE EVENT SUMMARY
-- Safety analytics
-- =====================================================
(104,'adverse_event_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,104,current_timestamp()),

-- =====================================================
-- SITE PERFORMANCE SUMMARY
-- Site-wise operational metrics
-- =====================================================
(105,'site_performance_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,105,current_timestamp()),

-- =====================================================
-- FREEZER UTILIZATION SUMMARY
-- Biobank utilization analytics
-- =====================================================
(106,'freezer_utilization_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,106,current_timestamp()),

-- =====================================================
-- SAMPLE REJECTION SUMMARY
-- Rejected / invalid sample analytics
-- =====================================================
(107,'sample_rejection_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,107,current_timestamp()),

-- =====================================================
-- LAB ABNORMAL RESULT SUMMARY
-- Critical/abnormal test trends
-- =====================================================
(108,'lab_abnormal_result_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,108,current_timestamp()),

-- =====================================================
-- SHIPMENT TEMPERATURE EXCURSION SUMMARY
-- Cold-chain compliance monitoring
-- =====================================================
(109,'shipment_temperature_excursion_summary','SILVER',NULL,NULL,NULL,'gold',NULL,'silver','gold',TRUE,109,current_timestamp())
;


-- COMMAND ----------


-- We implemented a metadata-driven pipeline framework where ingestion, transformation, and loading are dynamically controlled using metadata tables, 
-- enabling scalable and reusable data pipelines across CTMS, EDC, LIMS, shipment, and biobank systems.
