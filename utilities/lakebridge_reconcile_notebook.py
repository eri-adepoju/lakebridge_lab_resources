# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebridge Reconcile Notebook
# MAGIC
# MAGIC This notebook demonstrates how to use Lakebridge's reconcile functionality to compare data between source and target systems.
# MAGIC
# MAGIC ## Overview
# MAGIC - **Source Schema**: tpch
# MAGIC - **Target Catalog**: lakebridge_lab_resources  
# MAGIC - **Target Schema**: tpch
# MAGIC - **Tables**: customer (with aggregate reconciliation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation
# MAGIC
# MAGIC Install the required Lakebridge packages

# COMMAND ----------

# MAGIC %pip install databricks-labs-lakebridge
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports
# MAGIC
# MAGIC Import all necessary modules for reconciliation

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.lakebridge import __version__
from databricks.labs.lakebridge.config import (
    DatabaseConfig,
    ReconcileConfig,
    ReconcileMetadataConfig,
    TableRecon
)
from databricks.labs.lakebridge.reconcile.recon_config import (
    Table,
    ColumnMapping,
    ColumnThresholds,
    Transformation,
    JdbcReaderOptions,
    Aggregate,
    Filters
)
from databricks.labs.lakebridge.reconcile.execute import (
    recon,
    reconcile_aggregates
)
from databricks.labs.lakebridge.reconcile.exception import ReconciliationException

print("All required modules imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup
# MAGIC
# MAGIC Configure the reconciliation properties and table specifications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile Configuration
# MAGIC
# MAGIC Set up the main reconciliation configuration

# COMMAND ----------

reconcile_config = ReconcileConfig(
    data_source="databricks",  # Since both source and target are Databricks
    report_type="all",          # Generate schema, row, and data reports
    secret_scope="remorph_databricks",  # Secret scope for credentials
    database_config=DatabaseConfig(
        source_schema="tpch",
        target_catalog="lakebridge_lab_resources",
        target_schema="tpch"
    ),
    metadata_config=ReconcileMetadataConfig(
        catalog="lakebridge_lab_resources",
        schema="reconcile"
    )
)

print("Reconcile configuration created successfully")
print(f"Data source: {reconcile_config.data_source}")
print(f"Report type: {reconcile_config.report_type}")
print(f"Source schema: {reconcile_config.database_config.source_schema}")
print(f"Target catalog: {reconcile_config.database_config.target_catalog}")
print(f"Target schema: {reconcile_config.database_config.target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Reconciliation Configuration
# MAGIC
# MAGIC Configure the specific tables to be reconciled with aggregate functions

# COMMAND ----------

table_recon = TableRecon(
    source_schema="tpch",
    target_catalog="lakebridge_lab_resources",
    target_schema="tpch",
    tables=[
        Table(
            source_name="customer",
            target_name="customer",
            join_columns=["c_custkey"],
            aggregates=[
                Aggregate(
                    agg_columns=["c_custkey"],
                    type="MIN",
                    group_by_columns=["c_nationkey"]
                ),
                Aggregate(
                    agg_columns=["c_custkey"],
                    type="MAX"
                )
            ]
        )
    ]
)

print("Table reconciliation configuration created successfully")
print(f"Number of tables configured: {len(table_recon.tables)}")
for table in table_recon.tables:
    print(f"Table: {table.source_name} -> {table.target_name}")
    print(f"Join columns: {table.join_columns}")
    print(f"Number of aggregates: {len(table.aggregates)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Reconciliation
# MAGIC
# MAGIC Run the reconciliation process using the configured settings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize Workspace Client
# MAGIC
# MAGIC Set up the workspace client for reconciliation

# COMMAND ----------

# Initialize the workspace client
ws = WorkspaceClient(product="lakebridge", product_version=__version__)

print(f"Lakebridge version: {__version__}")
print("Workspace client initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Reconciliation
# MAGIC
# MAGIC Execute the reconciliation process and handle any exceptions

# COMMAND ----------

try:
    print("Starting reconciliation process...")
    result = recon(
        ws=ws,
        spark=spark,  # notebook spark session
        table_recon=table_recon,
        reconcile_config=reconcile_config
    )
    
    print("✅ Reconciliation completed successfully!")
    print(f"Reconciliation ID: {result.recon_id}")
    print(f"Result details: {result}")
    
except ReconciliationException as e:
    recon_id = e.reconcile_output.recon_id
    print(f"❌ Reconciliation failed with ID: {recon_id}")
    print(f"Error details: {e}")
    
except Exception as e:
    print(f"❌ Unexpected error occurred: {str(e)}")
    print(f"Error type: {type(e).__name__}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results and Next Steps
# MAGIC
# MAGIC ### What Happens Next?
# MAGIC
# MAGIC 1. **Metadata Storage**: All reconciliation metadata is stored in the configured metadata catalog/schema
# MAGIC 2. **Dashboard Access**: Access the LAKEBRIDGE_Reconciliation_Metrics AI/BI Dashboard using the reconciliation ID for detailed reports
# MAGIC 3. **Report Types Generated**: 
# MAGIC    - Schema comparison
# MAGIC    - Row-level reconciliation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregates-Reconcile Utility
# MAGIC
# MAGIC Run aggregate reconciliation separately using the `reconcile_aggregates` function.
# MAGIC This is useful when you want to focus specifically on aggregate comparisons.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Aggregates Reconciliation
# MAGIC
# MAGIC Execute aggregate reconciliation independently

# COMMAND ----------

try:
    print("Starting aggregates reconciliation process...")
    
    # Run aggregate reconciliation
    aggregate_result = reconcile_aggregates(
        ws=ws,
        spark=spark,
        table_recon=table_recon,
        reconcile_config=reconcile_config
    )
    
    print("✅ Aggregates reconciliation completed successfully!")
    print(f"Aggregate reconciliation ID: {aggregate_result.recon_id}")
    print(f"Result details: {aggregate_result}")
    
    # Display aggregate-specific information
    if hasattr(aggregate_result, 'aggregate_results'):
        print("\n📊 Aggregate Results Summary:")
        for table_name, agg_results in aggregate_result.aggregate_results.items():
            print(f"  Table: {table_name}")
            for agg_name, agg_value in agg_results.items():
                print(f"    {agg_name}: {agg_value}")
    
except ReconciliationException as e:
    recon_id = e.reconcile_output.recon_id if hasattr(e, 'reconcile_output') else 'Unknown'
    print(f"❌ Aggregates reconciliation failed with ID: {recon_id}")
    print(f"Error details: {e}")
    
except Exception as e:
    print(f"❌ Unexpected error in aggregates reconciliation: {str(e)}")
    print(f"Error type: {type(e).__name__}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results and Next Steps
# MAGIC
# MAGIC ### What Happens Next?
# MAGIC
# MAGIC 1. **Metadata Storage**: All reconciliation metadata is stored in the configured metadata catalog/schema
# MAGIC 2. **Dashboard Access**: Access the LAKEBRIDGE_Reconciliation_Metrics AI/BI Dashboard using the reconciliation ID for detailed reports
# MAGIC 3. **Report Types Generated**: 
# MAGIC    - Data-level reconciliation with aggregates