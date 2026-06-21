# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC Reads raw finance ticks from Kafka (Confluent/Event Hubs-compatible endpoint
# MAGIC or your self-hosted Kafka exposed to Databricks via VNet peering) and lands
# MAGIC them as an **append-only Delta table** in ADLS, completely untouched
# MAGIC (the "store everything, exactly as received" bronze principle).
# MAGIC
# MAGIC Widgets let this notebook be parameterized when called from an
# MAGIC Azure Data Factory pipeline or a Databricks Job.

# COMMAND ----------

dbutils.widgets.text("kafka_broker", "<your-kafka-broker>:9092", "Kafka Broker")
dbutils.widgets.text("storage_account", "<your-storage-account>", "ADLS Storage Account")
dbutils.widgets.text("container", "bronze", "ADLS Container")
dbutils.widgets.text("checkpoint_path", "/checkpoints/bronze/finance_ticks", "Checkpoint Path (DBFS)")

kafka_broker = dbutils.widgets.get("kafka_broker")
storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/finance_ticks"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount / auth note
# MAGIC In production, auth to ADLS is via a **Databricks secret-scope-backed
# MAGIC service principal** (OAuth) configured at the cluster level, e.g.:
# MAGIC ```
# MAGIC spark.conf.set("fs.azure.account.auth.type.<account>.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net",
# MAGIC                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.<account>.dfs.core.windows.net",
# MAGIC                dbutils.secrets.get("finance-kv", "sp-client-id"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.<account>.dfs.core.windows.net",
# MAGIC                dbutils.secrets.get("finance-kv", "sp-client-secret"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.<account>.dfs.core.windows.net",
# MAGIC                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
# MAGIC ```
# MAGIC Left out of this notebook so credentials never end up hardcoded in source control.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/finance-pipeline")  # adjust to your repo path in Databricks Repos
from common.schema import TICK_SPARK_SCHEMA, KAFKA_TOPIC_RAW
from pyspark.sql.functions import col, from_json, current_timestamp

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", KAFKA_TOPIC_RAW)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze_df = (
    raw.selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp", "partition", "offset")
    .select(
        from_json(col("json_value"), TICK_SPARK_SCHEMA).alias("data"),
        "kafka_timestamp",
        "partition",
        "offset",
    )
    .select("data.*", "kafka_timestamp", "partition", "offset")
    .withColumn("_ingested_at", current_timestamp())
)

# COMMAND ----------

(
    bronze_df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .partitionBy("symbol")
    .trigger(processingTime="30 seconds")
    .start(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Register as a table for SQL access / downstream notebooks:
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS finance.bronze_ticks
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@<account>.dfs.core.windows.net/finance_ticks'
# MAGIC ```
