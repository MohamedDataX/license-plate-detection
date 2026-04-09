"""Config and parameters"""
from pathlib import Path

# path to raw dataset
DATASET = "../license-plate-detection-dataset-10125-images"
# target image resolution
IMG_SIZE = 256

# setup output directory
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Spark cluster settings
SPARK_CONFIG = {
    "appName": "LicensePlate-SSD-Preprocessing",
    "master": "local[*]",
    "driver_memory": "4g"
}

# Dataset split mapping
SPLITS = {"train": 0, "valid": 1, "test": 2}