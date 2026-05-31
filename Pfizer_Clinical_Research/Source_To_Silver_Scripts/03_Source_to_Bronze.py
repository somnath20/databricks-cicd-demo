# Databricks notebook source
# =====================================================
# 1️⃣ Widgets
# =====================================================

import json
from pyspark.sql.functions import current_timestamp

start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

# =====================================================
# WIDGETS - CTMS FILE INGESTION
# Example: STUDY MASTER
# =====================================================

dbutils.widgets.text(
    "table_metadata",
    "{'table_id': '1', "
    "'table_name': 'study_master', "
    "'source_system': 'blob', "
    "'source_schema': '', "
    "'source_table': '', "
    "'source_path': '/Volumes/clinical/source/volume/ctms/study_master/', "
    "'target_layer': 'bronze', "
    "'bronze_schema': 'bronze', "
    "'silver_schema': 'silver', "
    "'active_flag': 'True', "
    "'load_order': '1', "
    "'created_at': '2026-05-10 20:30:00'}"
)

dbutils.widgets.text(
    "table_parameters",
    "{'load_type': 'FULL', "
    "'primary_key': 'study_id'}"
)

dbutils.widgets.text(
    "run_id",
    "472519629468311"
)
run_id=dbutils.widgets.get("run_id")

# Parse JSON safely
table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))

print("Table Metadata:", table_metadata)
print("Table Parameters:", table_parameters)
print(f"Run ID: {run_id}")

# COMMAND ----------

# =====================================================
# 2️⃣ Extract Variables
# =====================================================

table_id = int(table_metadata["table_id"])
table_name = table_metadata["table_name"]
source_system = table_metadata["source_system"].lower()
source_schema = table_metadata["source_schema"]
source_table = table_metadata["source_table"]
source_path = table_metadata["source_path"]
bronze_schema = table_metadata["bronze_schema"]

load_type = table_parameters.get("load_type")
watermark_column = table_parameters.get("watermark_column")

bronze_table_fqn = f"clinical.{bronze_schema}.{table_name}"

print(f"Target Bronze Table: {bronze_table_fqn}")

# COMMAND ----------

# DBTITLE 1,Metadata Entry
# =====================================================
# Make and entry to audit table
# =====================================================
entry_exists = spark.sql(f"""
    SELECT 1
    FROM clinical.metadata.pipeline_runs
    WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE clinical.metadata.pipeline_runs
        SET
            layer = 'Silver',
            start_time = TIMESTAMP('{start_time}'),
            end_time = NULL,
            status = 'INPROGRESS',
            number_of_records = NULL,
            error_message = NULL
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO clinical.metadata.pipeline_runs
        VALUES (
            {run_id},
            {table_id},
            'Silver',
            TIMESTAMP('{start_time}'),
            NULL,  -- end time
            'INPROGRESS',
            NULL, --number of records
            NULL -- error message
        )
    """)

# COMMAND ----------

# =====================================================
# 3️⃣ Get Last Watermark (For Filtering Only)
# =====================================================

last_watermark = None

if load_type in ["APPEND", "MERGE"] and watermark_column:
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM clinical.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

print("Last Watermark:", last_watermark)

# COMMAND ----------

if source_system == "blob":
    schema_df = spark.sql(f"""
    SELECT schema_json
    FROM clinical.metadata.schemas
    WHERE table_name = '{table_name}'
    AND active_flag = true
    ORDER BY schema_version DESC
    LIMIT 1
    """)
    schema_json = schema_df.collect()[0]["schema_json"]
    print(f"Schema JSON: {schema_json}")


# COMMAND ----------

import json
from pyspark.sql.types import *

if source_system == "blob":
    schema_definition = json.loads(schema_json)
    fields = []

    for col in schema_definition:
        datatype = col["type"]

        if datatype == "string":
            spark_type = StringType()

        elif datatype == "date":
            spark_type = DateType()

        elif datatype == "timestamp":
            spark_type = TimestampType()

        elif datatype == "integer":
            spark_type = IntegerType()

        else:
            spark_type = StringType()

        fields.append(
            StructField(col["name"], spark_type, True)
        )

    dynamic_schema = StructType(fields)
    print(f"Dynamic Schema: {dynamic_schema}")

# COMMAND ----------

# =====================================================
# 4️⃣ Read Source
# =====================================================

from pyspark.sql.functions import *

try:

    # =================================================
    # SQL SERVER SOURCE
    # =================================================

    if source_system == "sqlserver":

        # 🔐 Read connection JSON from secret
        secret_json = dbutils.secrets.get(
            scope="clinical-scope",
            key="sqlserver-connection-json"
        )

        config = json.loads(secret_json)

        jdbc_url = (
            f"jdbc:sqlserver://{config['host']}:{config['port']};"
            f"database={config['database']}"
        )

        jdbc_properties = {
            "user": config["user"],
            "password": config["password"],
            "driver": config["driver"]
        }

        # =============================================
        # Dynamic Incremental Query
        # =============================================

        if load_type in ["APPEND", "MERGE"] and last_watermark:

            query = f"""
            (
                SELECT *
                FROM {source_schema}.{source_table}
                WHERE {watermark_column} > '{last_watermark}'
            ) AS src
            """

        else:

            query = f"""
            (
                SELECT *
                FROM {source_schema}.{source_table}
            ) AS src
            """

        # =============================================
        # Read Source Table
        # =============================================

        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=jdbc_properties
        )

    # =================================================
    # FILE / BLOB SOURCE
    # =================================================

    elif source_system == "blob":

        source_df = (
            spark.readStream
            .format("cloudFiles")

            .option(
                "cloudFiles.format",
                "csv"
            )

            .option(
                "cloudFiles.schemaLocation",
                f"/Volumes/clinical/source/ctms_volume/_schema/{table_name}"
            )

            .option(
                "rescuedDataColumn",
                "_rescued_data"
            )

            .option(
                "header",
                "true"
            )

            .schema(dynamic_schema)

            .load(source_path)
        )

    else:

        raise ValueError(
            f"Unsupported source_system: {source_system}"
        )

    # =================================================
    # 5️⃣ Add Audit Columns
    # =================================================

    source_df = source_df.withColumn(
        "insert_timestamp",
        current_timestamp()
    )

    # =================================================
    # Add Source File Name
    # =================================================

    if source_system == "blob":

        source_df = source_df.withColumn(
            "source_file_name",
            col("_metadata.file_path")
        )

    else:

        source_df = source_df.withColumn(
            "source_file_name",
            lit(None).cast("string")
        )

    # =================================================
    # 6️⃣ Write to Bronze
    # =================================================

    if source_system == "blob":

        query = (
            source_df.writeStream

            .format("delta")

            # =========================================
            # Checkpoint Location
            # =========================================

            .option(
                "checkpointLocation",
                f"/Volumes/clinical/source/ctms_volume/_checkpoints/{table_name}"
            )

            # =========================================
            # Append Only Bronze
            # =========================================

            .outputMode("append")

            # =========================================
            # Process Available Files
            # =========================================

            .trigger(availableNow=True)

            # =========================================
            # Write to Bronze Table
            # =========================================

            .toTable(bronze_table_fqn)
        )

        # =============================================
        # Wait for Stream Completion
        # =============================================

        query.awaitTermination()

        records_read = spark.table(
            bronze_table_fqn
        ).count()

    else:

        (
            source_df.write

            .format("delta")

            .mode("append")

            .saveAsTable(bronze_table_fqn)
        )

        records_read = source_df.count()

    # =================================================
    # SUCCESS LOGGING
    # =================================================

    print("====================================")
    print("Source → Bronze Load Completed")
    print(f"Table      : {table_name}")
    print(f"Load Type  : {load_type}")
    print(f"Records    : {records_read}")
    print("====================================")

except Exception as e:

    # =================================================
    # FAILURE LOGGING
    # =================================================

    end_time = spark.sql(
        "SELECT current_timestamp()"
    ).collect()[0][0]

    error_message = str(e).replace("'", "")

    spark.sql(f"""
        UPDATE clinical.metadata.pipeline_runs
        SET
            end_time = TIMESTAMP('{end_time}'),
            status = 'FAILED',
            error_message = '{error_message}'
        WHERE table_id = {table_id}
        AND run_id = {run_id}
    """)

    print("====================================")
    print("PIPELINE FAILED")
    print(f"Table : {table_name}")
    print(f"Error : {error_message}")
    print("====================================")

    raise

# COMMAND ----------


#(com.databricks.sql.cloudfiles.errors.CloudFilesException) [CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE] Cannot infer schema when the input path `/Volumes/clinical/source/landing_volume/ctms/study_master/` is empty. Please try to start the stream when there are files in the input path, or specify the schema. SQLSTATE: 42000

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/clinical/source/ctms_volume/study_master/"
    )
)

# COMMAND ----------

# WHY AUTOLOADER FAILS

# Because you still have:

# .load(source_path)

# WITHOUT explicit schema.

# So Autoloader tries:

# schema inference

# BUT:

# no files exist to infer schema from

# Hence error.
