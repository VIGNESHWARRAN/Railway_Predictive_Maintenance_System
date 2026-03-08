#!/usr/bin/env python3
"""
download_dataset.py
Downloads MetroPT-3 dataset from Kaggle into the dataset/ folder.
Requires: pip install kaggle  +  ~/.kaggle/kaggle.json API key
"""

import os
import subprocess
import sys
from pathlib import Path

DATASET_DIR = Path(__file__).parent.parent / "dataset"
KAGGLE_SLUG = "anshtanwar/metro-train-dataset"

def download():
    DATASET_DIR.mkdir(exist_ok=True)

    # Check kaggle CLI
    try:
        subprocess.run(["kaggle", "--version"], check=True, capture_output=True)
    except FileNotFoundError:
        print("❌ kaggle CLI not found. Install with: pip install kaggle")
        print("   Then place your API key at ~/.kaggle/kaggle.json")
        sys.exit(1)

    print(f"⬇️  Downloading MetroPT-3 dataset to {DATASET_DIR}...")
    subprocess.run([
        "kaggle", "datasets", "download",
        KAGGLE_SLUG,
        "--path", str(DATASET_DIR),
        "--unzip"
    ], check=True)

    # Verify
    csv_files = list(DATASET_DIR.glob("*.csv"))
    if csv_files:
        print(f"✅ Dataset ready: {[f.name for f in csv_files]}")
    else:
        print("⚠️  No CSV files found. Check download output above.")

if __name__ == "__main__":
    download()
