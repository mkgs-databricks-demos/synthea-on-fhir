# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Retrieve widget values
dbutils.widgets.text("catalog_use", "ncqai")
dbutils.widgets.text("clinical_mart_schema_use", "clinical_mart")

catalog_use = dbutils.widgets.get("catalog_use")
clinical_mart_schema_use = dbutils.widgets.get("clinical_mart_schema_use")

print(f"""
catalog_use:               {catalog_use}
clinical_mart_schema_use:  {clinical_mart_schema_use}
""")

# COMMAND ----------

# DBTITLE 1,Set catalog and schema context
# MAGIC %sql
# MAGIC DECLARE OR REPLACE VARIABLE catalog_use STRING DEFAULT :catalog_use;
# MAGIC DECLARE OR REPLACE VARIABLE clinical_mart_schema_use STRING DEFAULT :clinical_mart_schema_use;
# MAGIC SELECT catalog_use, clinical_mart_schema_use;

# COMMAND ----------

# DBTITLE 1,Find and process YAML metric view definitions
import os
from pathlib import Path

# Define the path to the metric views folder
metric_views_path = Path(os.path.abspath("../../fixtures/metric_views"))

# Find all .metric_view.yml files
yml_files = sorted(metric_views_path.glob("*.metric_view.yml"))

print(f"Found {len(yml_files)} metric view YAML file(s):")
for yml_file in yml_files:
    print(f"  - {yml_file.name}")

# COMMAND ----------

# DBTITLE 1,Create or replace metric views
# Loop through each YAML file and create metric views
results = []
for yml_file in yml_files:
    print(f"\nProcessing: {yml_file.name}")

    # Read the YAML file contents as-is
    with open(yml_file, "r") as f:
        yaml_content = f.read()

    # Substitute catalog and clinical_mart_schema placeholders
    yaml_content = yaml_content.format(
        catalog=catalog_use,
        clinical_mart_schema=clinical_mart_schema_use,
    )

    # Extract metric view name (remove .metric_view.yml extension)
    view_name = yml_file.stem.replace(".metric_view", "")

    # Build the fully qualified metric view name
    full_view_name = f"{catalog_use}.{clinical_mart_schema_use}.{view_name}"

    print(f"  Creating/replacing metric view: {full_view_name}")

    # Build the CREATE OR REPLACE VIEW with YAML definition
    create_sql = f"""CREATE OR REPLACE VIEW {full_view_name}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_content}
$$"""

    # Execute the SQL
    try:
        spark.sql(create_sql)
        print(f"  Successfully created/replaced {full_view_name}")
        results.append((view_name, "SUCCESS"))
    except Exception as e:
        print(f"  FAILED: {e}")
        results.append((view_name, f"FAILED: {e}"))

print(f"\n{'=' * 60}")
print(f"Metric view registration complete!")
print(f"  Success: {sum(1 for _, s in results if s == 'SUCCESS')}")
print(f"  Failed:  {sum(1 for _, s in results if s != 'SUCCESS')}")

# COMMAND ----------

# DBTITLE 1,Verify registered metric views
# MAGIC %sql
# MAGIC -- Verify the metric views exist in the clinical mart schema
# MAGIC SHOW VIEWS IN IDENTIFIER(:catalog_use || '.' || :clinical_mart_schema_use)
# MAGIC   LIKE 'mv_*';

# COMMAND ----------

# DBTITLE 1,Validate metric views with MEASURE() queries
import yaml

validation_results = []

for yml_file in yml_files:
    view_name = yml_file.stem.replace(".metric_view", "")
    full_view_name = f"{catalog_use}.{clinical_mart_schema_use}.{view_name}"

    with open(yml_file, "r") as f:
        spec = yaml.safe_load(f)

    measures = [m["name"] for m in spec.get("measures", [])]
    if not measures:
        validation_results.append((view_name, "SKIPPED", "No measures defined"))
        continue

    measure_cols = ", ".join(f"MEASURE(`{m}`) AS `{m}`" for m in measures)
    query = f"SELECT {measure_cols} FROM `{catalog_use}`.`{clinical_mart_schema_use}`.`{view_name}`"

    try:
        row = spark.sql(query).first()
        non_null = sum(1 for m in measures if row[m] is not None)
        validation_results.append((view_name, "PASS", f"{non_null}/{len(measures)} measures returned non-null values"))
    except Exception as e:
        validation_results.append((view_name, "FAIL", str(e)[:120]))

# Summary
print(f"{'View':<35} {'Status':<8} {'Detail'}")
print("-" * 90)
for name, status, detail in validation_results:
    print(f"{name:<35} {status:<8} {detail}")

failed = [r for r in validation_results if r[1] == "FAIL"]
assert not failed, f"{len(failed)} metric view(s) failed validation"