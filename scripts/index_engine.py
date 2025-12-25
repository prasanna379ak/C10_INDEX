from build_index import calculate_index
from rebalance import run_rebalance
from datetime import datetime, timedelta
import pandas as pd
import os

REBALANCE_DAYS = 90
CONSTITUENTS_FILE = "data/constituents_current.csv"

def rebalance_due():
    if not os.path.exists(CONSTITUENTS_FILE):
        return True

    df = pd.read_csv(CONSTITUENTS_FILE)
    last = pd.to_datetime(df["rebalance_timestamp"].iloc[0])
    return datetime.now(last.tzinfo) - last >= timedelta(days=REBALANCE_DAYS)

def run_index():
    if rebalance_due():
        print("Rebalance due")
        run_rebalance()
    else:
        print("No rebalance needed")

    calculate_index()

if __name__ == "__main__":
    run_index()
