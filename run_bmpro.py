#!/usr/bin/env python3
"""
Runner script to pull Bitcoin Magazine Pro metrics into Sheet10.

Columns:
  A = Date
  B = BTC Price (USD) from Yahoo Finance
  C onwards = BMPro metrics

Usage:
  cd ~/Downloads
  
  # INSERT NEW ROWS (for dates not in sheet):
  python3 run_bmpro.py --days 1825           # 5 years, append at bottom
  python3 run_bmpro.py --days 3 --top        # 3 days, insert at top
  
  # UPDATE EXISTING ROWS (fill in missing metrics):
  python3 run_bmpro.py --days 2 --update     # Update last 2 days
  python3 run_bmpro.py --from 2025-12-01 --to 2025-12-02 --update
"""

import io
import os
import sys
import datetime as dt
import argparse
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import gspread
import yfinance as yf
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BM_BASE = "https://api.bitcoinmagazinepro.com"
SHEET_NAME = "Sheet10"  # Target sheet

# No delay between metric API calls - fetch as fast as possible
# Delay is applied when writing to Google Sheets instead

# Google Sheets write batch size and delay
SHEETS_BATCH_SIZE = 30  # Write 30 rows at a time
SHEETS_BATCH_DELAY = 3  # 3 seconds between sheet write batches

# All metrics in the exact order from Sheet5 (columns C onwards)
# Matches your sheet headers exactly - NO "Bitcoin Investor Tool" at start
METRIC_SLUGS = [
    "200wma-heatmap",          # 200 Week Moving Average Heatmap
    "stock-to-flow",           # Stock-to-Flow Model
    "fear-and-greed",          # Fear And Greed Index
    "pi-cycle-top",            # Pi Cycle Top Indicator
    "golden-ratio",            # The Golden Ratio Multiplier
    "profitable-days",         # Bitcoin Profitable Days
    "rainbow-indicator",       # Bitcoin Rainbow Price Chart Indicator
    "pi-cycle-top-bottom",     # Pi Cycle Top & Bottom Indicator
    "pi-cycle-top-prediction", # Pi Cycle Top Prediction
    "power-law",               # Power Law
    "price-forecast-tools",    # Price Forecast Tools
    "mvrv-zscore",             # MVRV Z-Score
    "rhodl-ratio",             # RHODL Ratio
    "nupl",                    # Net Unrealized Profit/Loss (NUPL)
    "reserve-risk",            # Reserve Risk
    "active-address-sentiment",# AASI (Active Address Sentiment Indicator)
    "advanced-nvt-signal",     # Advanced NVT Signal
    "realized-price",          # Realized Price
    "vdd-multiple",            # Value Days Destroyed (VDD) Multiple
    "cvdd",                    # CVDD
    "top-cap",                 # Top Cap
    "delta-top",               # Delta Top
    "balanced-price",          # Balanced Price
    "terminal-price",          # Terminal Price
    "lth-realized-price",      # Long-Term Holder Realized Price
    "sth-realized-price",      # Short-Term Holder Realized Price
    "addresses-in-profit",     # Percent Addresses in Profit
    "addresses-in-loss",       # Percent Addresses in Loss
    "sopr",                    # Spent Output Profit Ratio (SOPR)
    "short-term-holder-mvrv",  # Short Term Holder MVRV
    "short-term-holder-mvrv-z-score",  # Short Term Holder MVRV Z-Score
    "long-term-holder-mvrv",   # Long Term Holder MVRV
    "long-term-holder-mvrv-z-score",   # Long Term Holder MVRV Z-Score
    "mvrv-zscore-2yr",         # MVRV Z-Score 2YR Rolling
    "everything-indicator",    # Everything Indicator
    "hodl-waves",              # HODL Waves
    "hodl-1y",                 # 1+ Year HODL Wave
    "hodl-5y",                 # 5+ Years HODL Wave
    "hodl-10y",                # 10+ Years HODL Wave
    "rcap-hodl-waves",         # Realized Cap HODL Waves
    "whale-watching",          # Whale Shadows (aka Revived Supply)
    "bdd",                     # Coin Days Destroyed
    "bdd-supply-adjusted",     # Supply Adjusted Coin Days Destroyed
    "circulating-supply",      # Circulating Supply
    "active-addresses",        # Bitcoin Active Addresses
    "wallets-001",             # Addresses with Balance > 0.01 BTC
    "wallets-01",              # Addresses with Balance > 0.1 BTC
    "wallets-1",               # Addresses with Balance > 1 BTC
    "wallets-10",              # Addresses with Balance > 10 BTC
    "wallets-100",             # Addresses with Balance > 100 BTC
    "wallets-1000",            # Addresses with Balance > 1,000 BTC
    "wallets-10000",           # Addresses with Balance > 10,000 BTC
    "wallets-1-usd",           # Addresses with Balance > $1
    "wallets-10-usd",          # Addresses with Balance > $10
    "wallets-100-usd",         # Addresses with Balance > $100
    "wallets-1k-usd",          # Addresses with Balance > $1k
    "wallets-10k-usd",         # Addresses with Balance > $10k
    "wallets-100k-usd",        # Addresses with Balance > $100k
    "wallets-1m-usd",          # Addresses with Balance > $1m
    "non-zero-addresses",      # Addresses with Non Zero Balance
    "new-addresses",           # Number of New Addresses
    "lightning-capacity",      # Bitcoin Lightning Capacity
    "lightning-nodes",         # Bitcoin Lightning Nodes
    "puell-multiple",          # The Puell Multiple
    "hashrate-ribbons",        # Hash Ribbons Indicator
    "miner-difficulty",        # Bitcoin Miner Difficulty
    "miner-revenue-total",     # Miner Revenue (Total)
    "miner-revenue-block-rewards",  # Miner Revenue (Block Rewards)
    "miner-revenue-fees",      # Miner Revenue (Fees)
    "miner-revenue-fees-pct",  # Miner Revenue (Fees vs Rewards)
    "block-height",            # Block Height
    "blocks-mined",            # Blocks Mined
    "hashprice",               # Hashprice
    "hashprice-volatility",    # Hashprice Volatility
    "fr-average",              # Bitcoin Funding Rates
    "crosby-ratio",            # Bitcoin Crosby Ratio
    "active-address-growth-trend",  # Active Address Growth Trend
    "bitcoin-volatility",      # Bitcoin Volatility
    "sth-vs-lth-supply",       # Long and Short-Term Holders
    "bitcoin-seasonality",     # Bitcoin Seasonality
    "high-yield-credit-vs-btc",# High Yield Credit: Bitcoin Cycles
    "pmi-vs-btc",              # Manufacturing PMI: BTC Cycles
    "m2-vs-btc-yoy",           # US M2 Money vs Bitcoin YoY
    "m1-vs-btc-yoy",           # US M1 Money vs Bitcoin YoY
    "m2-vs-btc",               # US M2 Money vs Bitcoin
    "m1-vs-btc",               # US M1 Money vs Bitcoin
    "m2-global-vs-btc",        # Global M2 vs BTC
    "m2-global-vs-btc-yoy",    # Global M2 vs BTC YoY
    "fed-balance-sheet",       # Fed Balance Sheet vs BTC
    "fed-target-rate",         # Fed Funds Target Range v BTC
    "fed-target-rate-abs-change",   # Fed Rate: YoY Change (Abs)
    "fed-target-rate-pct-change",   # Fed Rate: YoY Change (%)
    "us-interest-vs-btc",      # US Interest Payments vs BTC
    "btc-vs-us-debt-gdp-ratio",# US Debt/GDP Ratio vs BTC
    "financial-stress-index-vs-btc",  # Financial Stress Index vs BTC
    "bull-market-comparison",  # Bull Market Comparison
    "financial-stress-index-vs-btc",  # Financial Stress Index vs BTC (duplicate in your list)
    "bull-market-comparison",  # Bull Market Comparison (duplicate in your list)
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


class BMProAPI:
    """Simple API client for Bitcoin Magazine Pro - no delays between calls."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
    
    def fetch_metric(self, slug: str, from_date: str = None, to_date: str = None) -> pd.DataFrame:
        """Fetch a metric from Bitcoin Magazine Pro API (no rate limiting delay)."""
        
        url = f"{BM_BASE}/metrics/{slug}"
        params = {"key": self.api_key}
        
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date
        
        try:
            r = self.session.get(url, params=params, timeout=60)
            r.raise_for_status()
            
            # Fix escaped newlines in response
            text = r.text.replace('\\n', '\n')
            # Remove leading/trailing quotes if present
            if text.startswith('"'):
                text = text[1:]
            if text.endswith('"'):
                text = text[:-1]
            
            # Parse CSV response with index_col=0 (unnamed first column)
            df = pd.read_csv(io.StringIO(text), index_col=0)
            
            # Normalize date column name
            for col in df.columns:
                if col.lower() == "date":
                    df = df.rename(columns={col: "date"})
                    break
            
            return df
            
        except requests.HTTPError as e:
            if e.response and e.response.status_code == 429:
                print(f"\n⚠️  Rate limited on {slug}!", file=sys.stderr)
                return pd.DataFrame()
            else:
                print(f"HTTP Error fetching {slug}: {e}", file=sys.stderr)
                return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching {slug}: {e}", file=sys.stderr)
            return pd.DataFrame()


def fetch_btc_prices(start_date: dt.date, end_date: dt.date) -> Dict[str, float]:
    """
    Fetch daily BTC prices from Yahoo Finance for the given date range.
    Returns a dict: date_str (YYYY-MM-DD) -> price
    """
    print("Fetching BTC prices from Yahoo Finance...", end=" ", flush=True)
    
    try:
        btc = yf.Ticker("BTC-USD")
        # Add buffer days to ensure we get all dates
        start_str = (start_date - dt.timedelta(days=5)).strftime("%Y-%m-%d")
        end_str = (end_date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        
        df = btc.history(start=start_str, end=end_str)
        
        prices = {}
        for idx, row in df.iterrows():
            date_str = idx.strftime("%Y-%m-%d")
            prices[date_str] = round(row["Close"], 2)
        
        print(f"✓ Got {len(prices)} days")
        return prices
    except Exception as e:
        print(f"✗ Error: {e}")
        return {}


def get_gspread_client():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS missing or not found")
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(creds)


def format_value(value: Any) -> str:
    """Format a value for the sheet."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, float):
        if abs(value) >= 1:
            return f"{value:.2f}"
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def fetch_all_metrics_for_period(api: BMProAPI, start_date: dt.date, end_date: dt.date) -> Dict[str, Dict[str, Any]]:
    """Fetch all metrics for a single period (max 365 days) and organize by date."""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    print(f"\n{'='*60}")
    print(f"Fetching BMPro metrics: {start_str} to {end_str}")
    print(f"{'='*60}")
    
    # Dict: date_str -> {slug: value}
    data_by_date: Dict[str, Dict[str, Any]] = {}
    
    for i, slug in enumerate(METRIC_SLUGS):
        print(f"[{i+1}/{len(METRIC_SLUGS)}] Fetching {slug}...", end=" ", flush=True)
        df = api.fetch_metric(slug, from_date=start_str, to_date=end_str)
        
        if df is None or df.empty:
            print("empty")
            continue
        
        # Find the value column (last column that's not date or price)
        value_col = None
        for col in reversed(df.columns.tolist()):
            if col.lower() not in ["date", "price", ""]:
                value_col = col
                break
        
        if value_col is None:
            value_col = df.columns[-1] if len(df.columns) > 0 else None
        
        if value_col is None:
            print("no value column")
            continue
        
        count = 0
        for _, row in df.iterrows():
            date_str = str(row.get("date", ""))[:10]
            if not date_str:
                continue
            
            if date_str not in data_by_date:
                data_by_date[date_str] = {}
            
            data_by_date[date_str][slug] = row.get(value_col)
            count += 1
        
        print(f"{count} rows")
    
    return data_by_date


def update_sheet(data_by_date: Dict[str, Dict[str, Any]], btc_prices: Dict[str, float], 
                 sheet_name: str = SHEET_NAME, insert_at_top: bool = False):
    """
    Update sheet with the fetched data.
    Column A = Date, Column B = BTC Price, Column C onwards = metrics
    """
    
    ssid = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not ssid:
        raise SystemExit("GOOGLE_SHEETS_SPREADSHEET_ID missing")
    
    gc = get_gspread_client()
    sh = gc.open_by_key(ssid)
    
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=3000, cols=100)
        # Add headers
        headers = ["Date", "BTC Price (USD)"] + [
            "200 Week Moving Average Heatmap", "Stock-to-Flow Model", "Fear And Greed Index",
            "Pi Cycle Top Indicator", "The Golden Ratio Multiplier", "Bitcoin Profitable Days",
            "Bitcoin Rainbow Price Chart Indicator", "Pi Cycle Top & Bottom Indicator",
            "Pi Cycle Top Prediction", "Power Law", "Price Forecast Tools", "MVRV Z-Score",
            "RHODL Ratio", "Net Unrealized Profit/Loss (NUPL)", "Reserve Risk",
            "AASI (Active Address Sentiment Indicator)", "Advanced NVT Signal", "Realized Price",
            "Value Days Destroyed (VDD) Multiple", "CVDD", "Top Cap", "Delta Top", "Balanced Price",
            "Terminal Price", "Long-Term Holder Realized Price", "Short-Term Holder Realized Price",
            "Percent Addresses in Profit", "Percent Addresses in Loss", "Spent Output Profit Ratio (SOPR)",
            "Short Term Holder MVRV", "Short Term Holder MVRV Z-Score", "Long Term Holder MVRV",
            "Long Term Holder MVRV Z-Score", "MVRV Z-Score 2YR Rolling", "Everything Indicator",
            "HODL Waves", "1+ Year HODL Wave", "5+ Years HODL Wave", "10+ Years HODL Wave",
            "Realized Cap HODL Waves", "Whale Shadows (aka Revived Supply)", "Coin Days Destroyed",
            "Supply Adjusted Coin Days Destroyed", "Circulating Supply", "Bitcoin Active Addresses",
            "Addresses with Balance > 0.01 BTC", "Addresses with Balance > 0.1 BTC",
            "Addresses with Balance > 1 BTC", "Addresses with Balance > 10 BTC",
            "Addresses with Balance > 100 BTC", "Addresses with Balance > 1,000 BTC",
            "Addresses with Balance > 10,000 BTC", "Addresses with Balance > $1",
            "Addresses with Balance > $10", "Addresses with Balance > $100",
            "Addresses with Balance > $1k", "Addresses with Balance > $10k",
            "Addresses with Balance > $100k", "Addresses with Balance > $1m",
            "Addresses with Non Zero Balance", "Number of New Addresses",
            "Bitcoin Lightning Capacity", "Bitcoin Lightning Nodes", "The Puell Multiple",
            "Hash Ribbons Indicator", "Bitcoin Miner Difficulty", "Miner Revenue (Total)",
            "Miner Revenue (Block Rewards)", "Miner Revenue (Fees)", "Miner Revenue (Fees vs Rewards)",
            "Block Height", "Blocks Mined", "Hashprice", "Hashprice Volatility",
            "Bitcoin Funding Rates", "Bitcoin Crosby Ratio", "Active Address Growth Trend",
            "Bitcoin Volatility", "Long and Short-Term Holders", "Bitcoin Seasonality",
            "High Yield Credit: Bitcoin Cycles", "Manufacturing PMI: BTC Cycles",
            "US M2 Money vs Bitcoin YoY", "US M1 Money vs Bitcoin YoY", "US M2 Money vs Bitcoin",
            "US M1 Money vs Bitcoin", "Global M2 vs BTC", "Global M2 vs BTC YoY",
            "Fed Balance Sheet vs BTC", "Fed Funds Target Range v BTC", "Fed Rate: YoY Change (Abs)",
            "Fed Rate: YoY Change (%)", "US Interest Payments vs BTC", "US Debt/GDP Ratio vs BTC",
            "Financial Stress Index vs BTC", "Bull Market Comparison",
            "Financial Stress Index vs BTC", "Bull Market Comparison"
        ]
        ws.append_row(headers, value_input_option="USER_ENTERED")
        print(f"Created new sheet '{sheet_name}' with headers")
    
    # Get existing data
    existing_values = ws.get_all_values()
    existing_dates = set()
    
    if len(existing_values) > 1:
        for row in existing_values[1:]:
            if row and row[0]:
                existing_dates.add(row[0].strip())
    
    # Combine all dates from both BMPro data and BTC prices
    all_dates = set(data_by_date.keys()) | set(btc_prices.keys())
    
    # Sort dates based on mode
    if insert_at_top:
        sorted_dates = sorted(all_dates, reverse=True)
    else:
        sorted_dates = sorted(all_dates)
    
    # Build rows to insert (only new dates)
    rows_to_insert = []
    for date_str in sorted_dates:
        if date_str in existing_dates:
            continue
        
        metrics = data_by_date.get(date_str, {})
        btc_price = btc_prices.get(date_str, "")
        
        # Build row: [Date, BTC Price, metric1, metric2, ...]
        row = [date_str, format_value(btc_price) if btc_price else ""]
        
        for slug in METRIC_SLUGS:
            value = metrics.get(slug)
            row.append(format_value(value))
        
        rows_to_insert.append(row)
    
    if not rows_to_insert:
        print(f"No new dates to insert into {sheet_name}")
        return {"inserted": 0, "dates": []}
    
    # Write in small batches with delays
    total_inserted = 0
    total_batches = (len(rows_to_insert) + SHEETS_BATCH_SIZE - 1) // SHEETS_BATCH_SIZE
    
    print(f"\nWriting {len(rows_to_insert)} rows in {total_batches} batches of {SHEETS_BATCH_SIZE}...")
    
    if insert_at_top:
        for i in range(0, len(rows_to_insert), SHEETS_BATCH_SIZE):
            batch = rows_to_insert[i:i + SHEETS_BATCH_SIZE]
            batch_num = i // SHEETS_BATCH_SIZE + 1
            print(f"  [{batch_num}/{total_batches}] Inserting {len(batch)} rows at top...", end=" ", flush=True)
            
            try:
                ws.insert_rows(batch, row=2, value_input_option="USER_ENTERED")
                total_inserted += len(batch)
                print(f"✓ (total: {total_inserted})")
            except Exception as e:
                print(f"✗ Error: {e}")
                print("    Waiting 60s before retry...")
                time.sleep(60)
                try:
                    ws.insert_rows(batch, row=2, value_input_option="USER_ENTERED")
                    total_inserted += len(batch)
                    print(f"    Retry ✓")
                except Exception as e2:
                    print(f"    Retry failed: {e2}")
                    break
            
            if i + SHEETS_BATCH_SIZE < len(rows_to_insert):
                time.sleep(SHEETS_BATCH_DELAY)
    else:
        for i in range(0, len(rows_to_insert), SHEETS_BATCH_SIZE):
            batch = rows_to_insert[i:i + SHEETS_BATCH_SIZE]
            batch_num = i // SHEETS_BATCH_SIZE + 1
            print(f"  [{batch_num}/{total_batches}] Appending {len(batch)} rows...", end=" ", flush=True)
            
            try:
                ws.append_rows(batch, value_input_option="USER_ENTERED")
                total_inserted += len(batch)
                print(f"✓ (total: {total_inserted})")
            except Exception as e:
                print(f"✗ Error: {e}")
                print("    Waiting 60s before retry...")
                time.sleep(60)
                try:
                    ws.append_rows(batch, value_input_option="USER_ENTERED")
                    total_inserted += len(batch)
                    print(f"    Retry ✓")
                except Exception as e2:
                    print(f"    Retry failed: {e2}")
                    break
            
            if i + SHEETS_BATCH_SIZE < len(rows_to_insert):
                time.sleep(SHEETS_BATCH_DELAY)
    
    return {
        "inserted": total_inserted,
        "dates": [r[0] for r in rows_to_insert[:total_inserted]]
    }


def update_existing_rows(data_by_date: Dict[str, Dict[str, Any]], btc_prices: Dict[str, float],
                         sheet_name: str = SHEET_NAME):
    """
    Update existing rows with missing metric data.
    Finds rows by date and fills in empty cells.
    """
    
    ssid = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not ssid:
        raise SystemExit("GOOGLE_SHEETS_SPREADSHEET_ID missing")
    
    gc = get_gspread_client()
    sh = gc.open_by_key(ssid)
    ws = sh.worksheet(sheet_name)
    
    print(f"Reading existing data from {sheet_name}...")
    all_values = ws.get_all_values()
    
    if len(all_values) < 2:
        print("No data rows found")
        return {"updated": 0}
    
    headers = all_values[0]
    
    # Build a map of date -> row number
    date_to_row = {}
    for i, row in enumerate(all_values[1:], start=2):  # 1-indexed, skip header
        if row and row[0]:
            date_to_row[row[0].strip()] = i
    
    # Find rows that need updating
    updates = []  # List of {'range': 'C2', 'values': [[value]]}
    dates_to_update = set(data_by_date.keys()) | set(btc_prices.keys())
    
    rows_updated = 0
    for date_str in dates_to_update:
        if date_str not in date_to_row:
            continue
        
        row_num = date_to_row[date_str]
        existing_row = all_values[row_num - 1]  # 0-indexed for list access
        
        # Extend row if needed
        while len(existing_row) < len(headers):
            existing_row.append("")
        
        row_has_updates = False
        
        # Update BTC price (column B) if empty
        if date_str in btc_prices and (len(existing_row) < 2 or not existing_row[1]):
            updates.append({
                'range': f'B{row_num}',
                'values': [[format_value(btc_prices[date_str])]]
            })
            row_has_updates = True
        
        # Update metrics (columns C onwards) if empty
        if date_str in data_by_date:
            metrics = data_by_date[date_str]
            for col_idx, slug in enumerate(METRIC_SLUGS, start=2):  # Start at column C (index 2)
                col_letter = chr(65 + col_idx) if col_idx < 26 else chr(64 + col_idx // 26) + chr(65 + col_idx % 26)
                
                # Check if cell is empty and we have data
                cell_empty = col_idx >= len(existing_row) or not existing_row[col_idx]
                has_data = slug in metrics and metrics[slug] is not None
                
                if cell_empty and has_data:
                    updates.append({
                        'range': f'{col_letter}{row_num}',
                        'values': [[format_value(metrics[slug])]]
                    })
                    row_has_updates = True
        
        if row_has_updates:
            rows_updated += 1
    
    if not updates:
        print("No cells to update")
        return {"updated": 0}
    
    print(f"Updating {len(updates)} cells across {rows_updated} rows...")
    
    # Batch update in chunks to avoid API limits
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        try:
            ws.batch_update(batch, value_input_option="USER_ENTERED")
            print(f"  Updated {min(i + batch_size, len(updates))}/{len(updates)} cells")
        except Exception as e:
            print(f"  Error updating batch: {e}")
            time.sleep(60)
            try:
                ws.batch_update(batch, value_input_option="USER_ENTERED")
            except Exception as e2:
                print(f"  Retry failed: {e2}")
        
        if i + batch_size < len(updates):
            time.sleep(SHEETS_BATCH_DELAY)
    
    print(f"✓ Updated {rows_updated} rows")
    return {"updated": rows_updated}


def main():
    parser = argparse.ArgumentParser(description="Pull Bitcoin Magazine Pro metrics to Sheet10")
    parser.add_argument("--from", dest="start", help="Start date YYYY-MM-DD", default=None)
    parser.add_argument("--to", dest="end", help="End date YYYY-MM-DD", default=None)
    parser.add_argument("--days", type=int, default=None, help="Number of recent days")
    parser.add_argument("--sheet", type=str, default=SHEET_NAME, help=f"Target sheet name (default: {SHEET_NAME})")
    parser.add_argument("--top", action="store_true", help="Insert new rows at top (row 2) instead of appending at bottom")
    parser.add_argument("--update", action="store_true", help="Update existing rows with missing data instead of inserting new rows")
    args = parser.parse_args()
    
    api_key = os.environ.get("BM_PRO_API_KEY")
    if not api_key:
        print("BM_PRO_API_KEY not set!", file=sys.stderr)
        sys.exit(1)
    
    today = dt.date.today()
    
    if args.start and args.end:
        start_date = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
    elif args.days:
        end_date = today
        start_date = end_date - dt.timedelta(days=args.days - 1)
    else:
        start_date = end_date = today
    
    total_days = (end_date - start_date).days + 1
    print(f"Total date range: {start_date} to {end_date} ({total_days} days)")
    print(f"Target sheet: {args.sheet}")
    print(f"Google Sheets: write {SHEETS_BATCH_SIZE} rows, wait {SHEETS_BATCH_DELAY}s between batches")
    
    api = BMProAPI(api_key)
    
    # Fetch BTC prices for entire date range upfront (Yahoo Finance is fast)
    print(f"\n{'='*60}")
    btc_prices = fetch_btc_prices(start_date, end_date)
    
    # Split into 365-day batches
    batch_size_days = 365
    current_start = start_date
    batch_num = 0
    total_batches = (total_days + batch_size_days - 1) // batch_size_days
    total_inserted = 0
    result = {"inserted": 0, "dates": []}
    
    while current_start <= end_date:
        batch_num += 1
        current_end = min(current_start + dt.timedelta(days=batch_size_days - 1), end_date)
        
        print(f"\n{'#'*60}")
        print(f"# API BATCH {batch_num}/{total_batches}: {current_start} to {current_end}")
        print(f"{'#'*60}")
        
        # Fetch BMPro metrics for this batch (no delays between API calls)
        batch_data = fetch_all_metrics_for_period(api, current_start, current_end)
        
        print(f"\n✓ Fetched {len(batch_data)} dates from BMPro API")
        
        # Filter BTC prices for this batch's date range
        batch_btc_prices = {
            d: p for d, p in btc_prices.items()
            if current_start.strftime("%Y-%m-%d") <= d <= current_end.strftime("%Y-%m-%d")
        }
        
        # Write this batch to Google Sheets
        if batch_data or batch_btc_prices:
            if args.update:
                # Update existing rows with missing data
                print(f"\nUpdating existing rows in {args.sheet}...")
                result = update_existing_rows(batch_data, batch_btc_prices, args.sheet)
                total_inserted += result.get('updated', 0)
                print(f"✓ Updated {result.get('updated', 0)} rows")
            else:
                # Insert new rows
                mode = "TOP (row 2)" if args.top else "BOTTOM (append)"
                print(f"\nWriting to {args.sheet} at {mode}...")
                result = update_sheet(batch_data, batch_btc_prices, args.sheet, insert_at_top=args.top)
                total_inserted += result['inserted']
                print(f"✓ Wrote {result['inserted']} rows to sheet")
        
        # Move to next batch
        current_start = current_end + dt.timedelta(days=1)
        
        # Wait for user input before next API batch (if more batches remaining)
        if current_start <= end_date:
            print(f"\n{'='*60}")
            print(f"BATCH {batch_num} COMPLETE!")
            print(f"Total rows written so far: {total_inserted}")
            print(f"Remaining batches: {total_batches - batch_num}")
            print(f"{'='*60}")
            print(f"\n⏳ Press ENTER when ready to fetch next 365-day batch...")
            print(f"   (or Ctrl+C to stop)")
            try:
                input()
            except KeyboardInterrupt:
                print("\n\nStopped by user.")
                break
    
    print(f"\n{'='*60}")
    action = "updated" if args.update else "inserted"
    print(f"ALL DONE! Total rows {action}: {total_inserted}")
    print(f"{'='*60}")
    print(result)


if __name__ == "__main__":
    main()
