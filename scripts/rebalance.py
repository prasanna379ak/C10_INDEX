import pandas as pd
import requests
from datetime import datetime, timezone
from pathlib import Path
import json
import yaml

# =========================
# Project root
# =========================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_FILE = BASE_DIR / "config" / "index_config.yaml"

CURRENT_FILE = DATA_DIR / "constituents_current.csv"
HISTORY_DIR = DATA_DIR / "constituents_history"
STATE_FILE = DATA_DIR / "index_state.json"
INDEX_FILE = DATA_DIR / "index_values.csv"

HISTORY_DIR.mkdir(exist_ok=True)

# =========================
# Load config
# =========================

with open(CONFIG_FILE, "r") as f:
    cfg = yaml.safe_load(f)

BTC_ID = cfg["assets"]["forced_btc_id"]
EXCLUDED_IDS = set(cfg["exclusions"]["ids"])
TOTAL_ASSETS = cfg["assets"]["total"]

WEIGHTS = (
    [cfg["weights"]["btc"], cfg["weights"]["second"]] +
    [cfg["weights"]["others"]] * (TOTAL_ASSETS - 2)
)

COINGECKO_URL = cfg["data_source"]["coingecko_url"]
PARAMS = {
    "vs_currency": cfg["data_source"]["vs_currency"],
    "order": "market_cap_desc",
    "per_page": cfg["data_source"]["fetch_limit"],
    "page": 1,
    "sparkline": False
}

# =========================
# Helpers
# =========================

def is_excluded(coin):
    return coin["id"] in EXCLUDED_IDS


def safe_load_last_index_value(default):
    """
    Safely load the last valid index_value from index_values.csv.
    Ignores malformed rows.
    """
    if not INDEX_FILE.exists():
        return default

    try:
        df = pd.read_csv(
            INDEX_FILE,
            on_bad_lines="skip"  # 👈 critical fix
        )
        if df.empty:
            return default
        return float(df["index_value"].iloc[-1])
    except Exception:
        return default


# =========================
# Rebalance
# =========================

def run_rebalance():
    r = requests.get(COINGECKO_URL, params=PARAMS, timeout=30)
    r.raise_for_status()
    data = r.json()

    coins = []

    # Force BTC
    for c in data:
        if c["id"] == BTC_ID:
            coins.append({
                "coin_id": c["id"],
                "symbol": c["symbol"],
                "market_cap_at_rebalance": c["market_cap"]
            })
            break
    else:
        raise RuntimeError("BTC not found — rebalance aborted")

    # Fill remaining slots
    for c in data:
        if c["id"] == BTC_ID:
            continue
        if is_excluded(c):
            continue

        coins.append({
            "coin_id": c["id"],
            "symbol": c["symbol"],
            "market_cap_at_rebalance": c["market_cap"]
        })

        if len(coins) == TOTAL_ASSETS:
            break

    if len(coins) < TOTAL_ASSETS:
        raise RuntimeError("Not enough eligible assets")

    df = pd.DataFrame(coins)
    df["rank"] = range(1, TOTAL_ASSETS + 1)
    df["weight"] = WEIGHTS
    df["rebalance_timestamp"] = datetime.now(timezone.utc)

    # Save snapshot + current
    ts = df["rebalance_timestamp"].iloc[0].strftime("%Y_%m_%d")
    snapshot_file = HISTORY_DIR / f"constituents_{ts}.csv"

    df.to_csv(snapshot_file, index=False)
    df.to_csv(CURRENT_FILE, index=False)

    # Update index state (robust)
    base_value = safe_load_last_index_value(cfg["index"]["base_value"])

    state = {
        "base_index_value": base_value,
        "last_rebalance": df["rebalance_timestamp"].iloc[0].isoformat()
    }

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

    print("Rebalance completed successfully")
    print(f"Snapshot saved: {snapshot_file}")
    print(f"Base index value set to: {base_value}")


if __name__ == "__main__":
    run_rebalance()
