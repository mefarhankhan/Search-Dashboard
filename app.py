import os
import json
import time
import threading
import requests
import gspread
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)
CORS(app)

# ==============================
# 🔐 Google API Setup
# ==============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

sheet = client.open("BOOK QUERIES").worksheet("All orders")

# ==============================
# 🔴 REDASH CONFIG
# ==============================
REDASH_API_KEY = os.environ.get("REDASH_API_KEY")
REDASH_QUERY_ID = "19923"
REDASH_BASE_URL = "https://data.testbook.com"

# ==============================
# ✂️ HELPERS
# ==============================
def last10(val):
    v = str(val).strip().replace(" ", "").replace("+", "")
    return v[-10:] if len(v) >= 10 else v

def get_short_product(name):
    return name[:100] if name else ""   # ✅ Increased limit

# ==============================
# 🚀 SHEET CACHE
# ==============================
cache = {}
last_updated = 0
CACHE_TTL = 1800  # 30 min

def refresh_cache():
    global cache, last_updated
    try:
        records = sheet.get_all_records()
        new_cache = {}

        print(f"📊 Total rows fetched: {len(records)}")

        for data in records:
            # ✅ Normalize keys
            normalized = {k.strip().lower(): v for k, v in data.items()}

            mobile_raw = str(
                normalized.get("customer mobile") or
                normalized.get("mobile") or
                ""
            ).strip()

            email = str(
                normalized.get("customer email") or
                normalized.get("email") or
                ""
            ).strip().lower()

            order_id = str(
                normalized.get("order id") or
                ""
            ).strip().replace(" ", "")

            m10 = last10(mobile_raw)

            if m10:
                new_cache.setdefault(m10, []).append(data)

            if email:
                new_cache.setdefault(email, []).append(data)

            if order_id:
                new_cache.setdefault(order_id, []).append(data)

        cache = new_cache
        last_updated = time.time()

        print(f"✅ Sheet cache loaded: {len(cache)} users")

    except Exception as e:
        print("❌ Cache error:", str(e))

def refresh_cache_async():
    threading.Thread(target=refresh_cache, daemon=True).start()

def get_cached_data():
    if time.time() - last_updated > CACHE_TTL:
        refresh_cache_async()
    return cache

# ==============================
# 🔴 REDASH CACHE
# ==============================
redash_cache = []
redash_last_updated = 0
REDASH_CACHE_TTL = 300  # 5 min

def get_redash_data():
    global redash_cache, redash_last_updated

    if time.time() - redash_last_updated < REDASH_CACHE_TTL:
        return redash_cache

    try:
        url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results.json"
        headers = {"Authorization": f"Key {REDASH_API_KEY}"}

        res = requests.get(url, headers=headers, timeout=15)

        data = res.json()
        rows = data.get("query_result", {}).get("data", {}).get("rows", [])

        redash_cache = rows
        redash_last_updated = time.time()

        print(f"🔴 Redash cache: {len(rows)} rows")
        return rows

    except Exception as e:
        print("❌ Redash error:", str(e))
        return []

# ==============================
# 🏠 HOME
# ==============================
@app.route("/")
def home():
    return render_template("dashboard.html")

# ==============================
# 🔍 ORDER SEARCH
# ==============================
@app.route("/search", methods=["POST"])
def search():
    data = request.get_json(silent=True) or {}
    query_raw = data.get("query", "")

    if not query_raw:
        return jsonify({"status": "Invalid query"})

    query_mobile = last10(query_raw)
    query_email = str(query_raw).strip().lower()
    query_order = str(query_raw).strip().replace(" ", "")

    data_cache = get_cached_data()

    rows = (
        data_cache.get(query_mobile)
        or data_cache.get(query_email)
        or data_cache.get(query_order)
    )

    if rows:
        orders = []

        for row in rows:
            normalized = {k.strip().lower(): v for k, v in row.items()}

            awb = str(
                normalized.get("awb code") or
                normalized.get("awb") or
                ""
            ).strip()

            status = str(normalized.get("status") or "").strip()

            rto_reason = str(
                normalized.get("latest ndr reason") or
                ""
            ).strip()

            is_rto = "rto" in status.lower()

            orders.append({
                "awb": awb or None,
                "status": status or "Pending",
                "courier": normalized.get("courier company", "") or "Not Assigned",
                "product": get_short_product(normalized.get("product name", "")),
                "created_at": normalized.get("shiprocket created at", "") or "NA",
                "edd": normalized.get("edd") or "NA",
                "tracking_link": f"https://shiprocket.co/tracking/{awb}" if awb else None,

                # ✅ RTO Reason
                "rto_reason": rto_reason if is_rto and rto_reason else None
            })

        return jsonify({
            "count": len(orders),
            "orders": list(reversed(orders))
        })

    return jsonify({"status": "Not Found"})

# ==============================
# 🔍 BOOK SEARCH (REDASH)
# ==============================
@app.route("/book-search", methods=["POST"])
def book_search():
    try:
        data = request.get_json()
        query = data.get("query", "").lower()

        rows = get_redash_data()
        results = []

        for row in rows:
            book_name = str(row.get("pName") or "").lower()

            if query in book_name:
                results.append({
                    "name": row.get("pName"),
                    "edd": row.get("estimated_delivery") or row.get("EDD") or "Not Available"
                })

        return jsonify({"books": results})

    except Exception as e:
        return jsonify({"error": str(e)})

# ==============================
# 🚀 START
# ==============================
refresh_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
