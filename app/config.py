from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
WAREHOUSE_DIR = ROOT_DIR / "warehouse"
BRONZE_DIR = WAREHOUSE_DIR / "bronze"
SILVER_DIR = WAREHOUSE_DIR / "silver"
GOLD_DIR = WAREHOUSE_DIR / "gold"
SPARK_JOB_DIR = ROOT_DIR / "spark_job"
DBT_PROJECT_DIR = ROOT_DIR / "dbt"
DEPLOYMENT_DIR = ROOT_DIR / "deployment"

PIPELINE_FRESHNESS_SLA_MINUTES = {
    "bronze_orders": 5,
    "silver_orders": 10,
    "silver_latest_order_state": 10,
    "gold_customer_order_metrics": 15,
}
PIPELINE_FRESHNESS_SLA_DAYS = {
    "gold_daily_region_sales": 0,
}
