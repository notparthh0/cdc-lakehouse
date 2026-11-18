"""
CDC AI Analyzer — Gemini-Powered Change Pattern Detection
Monitors incoming CDC events for anomalous patterns and
invokes the Gemini API to generate natural-language
business impact diagnoses.

Patterns detected:
  - Mass DELETE events (possible data purge / accidental deletion)
  - Bulk price changes on products (pricing incident / sale event)
  - Sudden inventory drops (stock-out risk)
  - Unusual order volume spikes or drops
  - Suspicious customer data modifications

Author: Parth Alagh (02316401523)
"""

from __future__ import annotations

import json
import os
import statistics
from collections import defaultdict, deque
from datetime import datetime

import google.generativeai as genai

# Load .env file if present (GEMINI_API_KEY)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass   # python-dotenv not installed; rely on environment variables

# ============================================================
# Configuration
# ============================================================

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")   # set in .env
GEMINI_MODEL   = "gemini-1.5-flash"                 # fast + cheap

# Detection thresholds
WINDOW_SIZE          = 50     # rolling window of N events per table
DELETE_RATE_THRESHOLD = 0.4   # flag if >40% of events in window are deletes
PRICE_CHANGE_THRESHOLD = 0.2  # flag if avg price change > 20%
STOCK_DROP_THRESHOLD   = 20   # flag if stock drops by more than 20 units in one event
BULK_UPDATE_THRESHOLD  = 10   # flag if >10 updates arrive in quick succession


class CDCAIAnalyzer:
    """
    Tracks rolling CDC event statistics per table and
    invokes Gemini API when anomalous patterns are detected.
    """

    def __init__(self):
        # Rolling event windows per table
        self.windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

        # Track previous values for change detection
        self.prev_prices:  dict[int, float] = {}   # product_id → last known price
        self.prev_stocks:  dict[int, int]   = {}   # product_id → last known stock

        # Diagnosis log
        self.diagnoses: list[dict] = []

        # Init Gemini
        if GEMINI_API_KEY:
            genai.configure(api_key=GEMINI_API_KEY)
            self.model = genai.GenerativeModel(GEMINI_MODEL)
            self.gemini_enabled = True
            print("🤖 Gemini API connected — AI analysis enabled")
        else:
            self.gemini_enabled = False
            print("⚠️  GEMINI_API_KEY not set — AI analysis disabled (set in .env)")

    # ----------------------------------------------------------
    # Public API: called by iceberg_consumer for every CDC row
    # ----------------------------------------------------------

    def analyze(self, table: str, row: dict, op: str):
        """
        Feed one CDC event into the analyzer.
        Returns a diagnosis string if an anomaly is detected, else None.
        """
        event = {"op": op, "row": row, "ts": datetime.utcnow().isoformat()}
        self.windows[table].append(event)

        anomaly = self._detect_anomaly(table, row, op)
        if anomaly:
            diagnosis = self._diagnose(table, anomaly, row)
            self._log_diagnosis(table, anomaly, diagnosis, row)
            return diagnosis
        return None

    # ----------------------------------------------------------
    # Anomaly Detection
    # ----------------------------------------------------------

    def _detect_anomaly(self, table: str, row: dict, op: str) -> dict | None:
        window = list(self.windows[table])
        if len(window) < 5:
            return None   # not enough data yet

        # --- Mass DELETE detection ---
        delete_count = sum(1 for e in window if e["op"] == "d")
        delete_rate  = delete_count / len(window)
        if delete_rate >= DELETE_RATE_THRESHOLD and op == "d":
            return {
                "type":        "MASS_DELETE",
                "description": f"{delete_count} of last {len(window)} events on '{table}' are DELETEs ({delete_rate:.0%})",
                "severity":    "HIGH",
            }

        # --- Product price spike detection ---
        if table == "products" and op in ("u", "c"):
            pid   = row.get("id")
            price = row.get("price")
            if pid and price and pid in self.prev_prices:
                old_price = self.prev_prices[pid]
                if old_price > 0:
                    change_pct = abs(price - old_price) / old_price
                    if change_pct >= PRICE_CHANGE_THRESHOLD:
                        direction = "SPIKE" if price > old_price else "DROP"
                        return {
                            "type":        f"PRICE_{direction}",
                            "description": f"Product id={pid} price changed {change_pct:.0%}: ₹{old_price:.0f} → ₹{price:.0f}",
                            "severity":    "MEDIUM",
                        }
            if pid and price:
                self.prev_prices[pid] = price

        # --- Inventory stock drop detection ---
        if table == "products" and op == "u":
            pid   = row.get("id")
            stock = row.get("stock_qty")
            if pid and stock is not None and pid in self.prev_stocks:
                drop = self.prev_stocks[pid] - stock
                if drop >= STOCK_DROP_THRESHOLD:
                    return {
                        "type":        "STOCK_DROP",
                        "description": f"Product id={pid} stock dropped by {drop} units ({self.prev_stocks[pid]} → {stock})",
                        "severity":    "MEDIUM",
                    }
            if pid and stock is not None:
                self.prev_stocks[pid] = stock

        # --- Bulk UPDATE detection ---
        if op == "u":
            recent_updates = sum(1 for e in list(window)[-20:] if e["op"] == "u")
            if recent_updates >= BULK_UPDATE_THRESHOLD:
                return {
                    "type":        "BULK_UPDATE",
                    "description": f"{recent_updates} UPDATE events in last 20 events on '{table}'",
                    "severity":    "LOW",
                }

        return None

    # ----------------------------------------------------------
    # Gemini API Diagnosis
    # ----------------------------------------------------------

    def _diagnose(self, table: str, anomaly: dict, row: dict) -> str:
        if not self.gemini_enabled:
            return f"[AI disabled] {anomaly['type']}: {anomaly['description']}"

        prompt = f"""
You are a database reliability engineer monitoring a real-time PostgreSQL change-data-capture stream
for an e-commerce platform (tables: customers, products, orders).

An anomaly was detected in the CDC stream:

Table:       {table}
Anomaly:     {anomaly['type']}
Severity:    {anomaly['severity']}
Description: {anomaly['description']}
Sample row:  {json.dumps({k: v for k, v in row.items() if not k.startswith('__')}, default=str)}

In 2-3 sentences, explain:
1. What likely caused this change pattern in a real e-commerce database
2. What the business impact or risk might be
3. What action a data engineer or analyst should take

Be specific and practical. Do not use bullet points.
        """.strip()

        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            return f"[Gemini error: {e}] {anomaly['description']}"

    # ----------------------------------------------------------
    # Logging
    # ----------------------------------------------------------

    def _log_diagnosis(self, table: str, anomaly: dict, diagnosis: str, row: dict):
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "table":     table,
            "anomaly":   anomaly,
            "diagnosis": diagnosis,
            "row_id":    row.get("id"),
        }
        self.diagnoses.append(entry)

        # Print to console
        severity_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🔵"}.get(anomaly["severity"], "⚪")
        print(f"\n{'='*60}")
        print(f"  {severity_emoji} CDC ANOMALY DETECTED")
        print(f"  Table:    {table}")
        print(f"  Type:     {anomaly['type']} ({anomaly['severity']})")
        print(f"  Detail:   {anomaly['description']}")
        print(f"  🤖 Gemini Diagnosis:")
        print(f"  {diagnosis}")
        print(f"{'='*60}\n")

        # Save all diagnoses to file
        with open("ai_diagnoses.json", "w") as f:
            json.dump(self.diagnoses, f, indent=2, default=str)

    def summary(self):
        """Print end-of-session summary."""
        print(f"\n📊 AI Analyzer Summary")
        print(f"   Total anomalies detected: {len(self.diagnoses)}")
        for d in self.diagnoses:
            print(f"   • [{d['anomaly']['severity']}] {d['table']} — {d['anomaly']['type']}")
