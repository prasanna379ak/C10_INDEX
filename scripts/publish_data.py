from pathlib import Path
import shutil

# =========================
# Project root
# =========================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
PUBLIC_DATA_DIR = BASE_DIR / "public" / "data"

SOURCE_FILE = DATA_DIR / "index_values.csv"
TARGET_FILE = PUBLIC_DATA_DIR / "index_values.csv"

# =========================
# Publish logic
# =========================

def publish():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError("index_values.csv not found in data/")

    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)

    shutil.copy2(SOURCE_FILE, TARGET_FILE)

    print("Published index_values.csv to public/data/")

if __name__ == "__main__":
    publish()
