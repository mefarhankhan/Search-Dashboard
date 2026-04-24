import os
import json
import time
import threading
import requests
import gspread
from flask import Flask, request, jsonify
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
    return name[:40] if name else ""

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

        for data in records:
            mobile_raw = str(data.get("Customer Mobile", "")).strip()
            email = str(data.get("Customer Email", "")).strip().lower()
            order_id = str(data.get("Order ID", "")).strip()  # ✅ Order ID added

            m10 = last10(mobile_raw)

            if m10:
                new_cache.setdefault(m10, []).append(data)

            if email:
                new_cache.setdefault(email, []).append(data)

            if order_id:
                new_cache.setdefault(order_id, []).append(data)  # ✅ Cache by Order ID

        cache = new_cache
        last_updated = time.time()

        print(f"✅ Sheet cache: {len(cache)} users")

    except Exception as e:
        print("❌ Cache error:", str(e))

def refresh_cache_async():
    threading.Thread(target=refresh_cache, daemon=True).start()

def get_cached_data():
    if time.time() - last_updated > CACHE_TTL:
        refresh_cache_async()
    return cache

# ==============================
# ⚡ REDASH CACHE
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

        if res.status_code != 200:
            print("❌ Redash failed")
            return []

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
# 🔍 REDASH SEARCH (UPDATED)
# ==============================
def check_redash(query):
    rows = get_redash_data()
    q_mobile = last10(query)
    q_order = str(query).strip()

    for row in rows:
        mobile = last10(row.get("mobile", ""))
        order_id = str(row.get("order_id") or row.get("orderId") or "").strip()

        if q_mobile == mobile or q_order == order_id:  # ✅ Order ID match added
            status = str(
                row.get("shippingStatus") or 
                row.get("shipping_status") or 
                row.get("status") or 
                ""
            ).strip()

            product = str(row.get("pName") or "Not Available").strip()

            return {
                "status": status if status else "Not Available",
                "product": product
            }

    return None

# ==============================
# 🏠 ROUTES
# ==============================
@app.route("/")
def home():
    return "API running 🚀"

@app.route("/search", methods=["POST"])
def search():
    try:
        data = request.get_json(silent=True) or {}
        query_raw = data.get("query", "")

        if not query_raw:
            return jsonify({"status": "Invalid query"})

        query_mobile = last10(query_raw)
        query_email = str(query_raw).strip().lower()
        query_order = str(query_raw).strip()

        data_cache = get_cached_data()

        # ======================
        # ✅ SHEET SEARCH
        # ======================
        rows = (
            data_cache.get(query_mobile)
            or data_cache.get(query_email)
            or data_cache.get(query_order)   # ✅ Order ID search
        )

        if rows:
            orders = []

            for row in rows:
                awb_raw = str(row.get("AWB Code") or row.get("AWB") or "").strip()
                awb = awb_raw if awb_raw else None

                orders.append({
                    "awb": awb,
                    "status": row.get("Status", "").strip() or "Pending",
                    "courier": row.get("Courier Company", "").strip() or "Not Assigned",
                    "product": get_short_product(row.get("Product Name", "")) or "Not Available",
                    "created_at": row.get("Shiprocket Created At", "").strip() or "Not Available",
                    "edd": str(row.get("EDD") or "").strip() or "Not Available",
                    "tracking_link": f"https://shiprocket.co/tracking/{awb}" if awb else None
                })

            return jsonify({
                "count": len(orders),
                "orders": list(reversed(orders))
            })

        # ======================
        # 🔥 REDASH FALLBACK
        # ======================
        result = check_redash(query_raw)

        if result:
            return jsonify({
                "count": 1,
                "orders": [{
                    "awb": None,
                    "status": result["status"],
                    "courier": "Not Available",
                    "product": result["product"],
                    "created_at": "Not Available",
                    "edd": "Not Available",
                    "tracking_link": None
                }]
            })

        return jsonify({"status": "Not Found"})

    except Exception as e:
        print("❌ ERROR:", str(e))
        return jsonify({"status": "Error", "message": str(e)})

# ==============================
# 🔄 MANUAL REFRESH
# ==============================
@app.route("/refresh")
def manual_refresh():
    refresh_cache_async()
    return "Sheet cache refresh started 🚀"

# ==============================
# 🚀 STARTUP
# ==============================
refresh_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
