-- Databricks notebook source
select * from clinical.metadata.tables  --- 51 rows

-- COMMAND ----------

select * from clinical.metadata.table_parameters   --- 110 rows

-- COMMAND ----------

select * from clinical.metadata.pipeline_runs order by start_time desc

-- COMMAND ----------

select * from clinical.metadata.table_parameters where table_id = 4

-- COMMAND ----------

select t.table_name, table_parameters.parameter_value from clinical.metadata.tables t join clinical.metadata.table_parameters where t.table_id = table_parameters.table_id and table_parameters.parameter_name='load_type'

-- COMMAND ----------

select * from clinical.metadata.table_watermarks  -- 28 rows

-- COMMAND ----------

select * from clinical.metadata.schemas;
