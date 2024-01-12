from pathlib import Path
import os

PARENT_DIR = Path("__file__").parent.resolve().parent
MODEL_DIR = PARENT_DIR / "model"
DATA_DIR = PARENT_DIR / "data"
RAW_DATA_DIR = PARENT_DIR / "data" / "raw"
TRANSFORMED_DATA_DIR = PARENT_DIR / "data" / "transformed"
FEATURES_DATA_DIR = PARENT_DIR / "data" / "features"

if not Path(MODEL_DIR).exists():
    os.mkdir(MODEL_DIR)

if not Path(DATA_DIR).exists():
    os.mkdir(DATA_DIR)

if not Path(RAW_DATA_DIR).exists():
    os.mkdir(RAW_DATA_DIR)

if not Path(TRANSFORMED_DATA_DIR).exists():
    os.mkdir(TRANSFORMED_DATA_DIR)
    
if not Path(FEATURES_DATA_DIR).exists():
    os.mkdir(FEATURES_DATA_DIR)