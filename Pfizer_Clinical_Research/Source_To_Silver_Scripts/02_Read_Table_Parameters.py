# Databricks notebook source
# ==========================================
# 1️⃣ Create Widget
# ==========================================

default_metadata = "{'table_id': '5', 'table_name': 'subject', 'source_system': 'sqlserver', 'source_schema': 'clinical_data', 'source_table': 'subject', 'source_path': '', 'target_layer': 'bronze', 'bronze_schema': 'bronze', 'silver_schema': 'silver', 'gold_schema': '', 'active_flag': 'True', 'load_order': '5', 'created_at': '2026-05-16 15:24:18.818564'}"
dbutils.widgets.text("table_metadata", default_metadata)
table_metadata_str = dbutils.widgets.get("table_metadata")

print(f"Table Metadata Passed: {table_metadata_str}")

if not table_metadata_str:
    raise ValueError("table_metadata widget cannot be empty")

import ast
table_metadata = ast.literal_eval(table_metadata_str)

table_id = table_metadata.get("table_id")
if not table_id:
    raise ValueError("table_id not found in table_metadata")

table_id = int(table_id)

# COMMAND ----------

# ==========================================
# 2️⃣ Read Table Parameters
# ==========================================

params_df = (
    spark.table("clinical.metadata.table_parameters")
         .filter(f"table_id = {table_id}")
)
display(params_df)

# COMMAND ----------

# ==========================================
# 3️⃣ Convert to Single JSON Object
# ==========================================

rows = params_df.select("parameter_name", "parameter_value").collect()

parameters_dict = {
    row.parameter_name: row.parameter_value
    for row in rows
}

print("Parameters JSON Object:")
print(parameters_dict)

# COMMAND ----------

# ==========================================
# 4️⃣ Set Databricks Task Value
# ==========================================

dbutils.jobs.taskValues.set(
    key="table_parameters",
    value=parameters_dict
)

print("Task value 'table_parameters' has been set.")
