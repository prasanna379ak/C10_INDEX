import pandas as pd
import requests
from datetime import datetime, timezone
import json
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_FILE = BASE_DIR / "config" / "index_config.yaml"

INDEX_FILE = DATA_DIR / "index_values.csv"
CONSTITUENTS_FILE = DATA_DIR / "constituents_current.csv"
STATE_FILE = DATA_DIR / "index_state.json"

with open(CONFIG_FILE, "r") as f:
    cfg = yaml.safe_load(f)

COINGECKO_URL = cfg["data_source"]["coingecko_url"]

def load_state():
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def fetch_market_caps(coin_ids):
    params = {
        "vs_currency": cfg["data_source"]["vs_currency"],
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": len(coin_ids),
        "page": 1,
        "sparkline": False
    }
    r = requests.get(COINGECKO_URL, params=params, timeout=30)
    r.raise_for_status()
    return {c["id"]: c["market_cap"] for c in r.json()}

def calculate_index():
    cons = pd.read_csv(CONSTITUENTS_FILE)
    state = load_state()
    base_value = state["base_index_value"]

    caps = fetch_market_caps(cons["coin_id"].tolist())
    cons["current_market_cap"] = cons["coin_id"].map(caps)

    if cons["current_market_cap"].isnull().any():
        raise RuntimeError("Missing market cap data")

    if round(cons["weight"].sum(), 6) != 1.0:
        raise RuntimeError("Weights do not sum to 1")

    cons["cap_ratio"] = cons["current_market_cap"] / cons["market_cap_at_rebalance"]
    index_value = base_value * (cons["weight"] * cons["cap_ratio"]).sum()

    row = pd.DataFrame([{
        "timestamp": datetime.now(timezone.utc),
        "index_value": round(index_value, 6)
    }])

    if INDEX_FILE.exists():
        row.to_csv(INDEX_FILE, mode="a", header=False, index=False)
    else:
        row.to_csv(INDEX_FILE, index=False)

    print(f"Index updated: {index_value:.2f}")
