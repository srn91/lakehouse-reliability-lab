from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
WAREHOUSE_DIR = ROOT_DIR / "warehouse"
BRONZE_DIR = WAREHOUSE_DIR / "bronze"
SILVER_DIR = WAREHOUSE_DIR / "silver"
GOLD_DIR = WAREHOUSE_DIR / "gold"

