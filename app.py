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
# ⚡ HELPERS
# ==============================
def last10(val):
    v = str(val).replace(" ", "").replace("+", "")
    return v[-10:]

# ==============================
# 🚀 ULTRA FAST CACHE
# ==============================
mobile_cache = {}
email_cache = {}
order_cache = {}

last_updated = 0
CACHE_TTL = 3600  # 1 hour

lock = threading.Lock()

def refresh_cache():
    global mobile_cache, email_cache, order_cache, last_updated

    try:
        records = sheet.get_all_records()
        print(f"📊 Rows fetched: {len(records)}")

        m_cache, e_cache, o_cache = {}, {}, {}

        for row in records:
            # normalize once
            data = {k.strip().lower(): v for k, v in row.items()}

            mobile = last10(data.get("customer mobile", ""))
            email = str(data.get("customer email", "")).strip().lower()
            order_id = str(data.get("order id", "")).replace(" ", "")

            # pre-build response object (🔥 no processing later)
            awb = str(data.get("awb code") or data.get("awb") or "").strip()
            status = str(data.get("status") or "").strip()
            rto_reason = str(data.get("latest ndr reason") or "").strip()

            order_obj = {
                "awb": awb or None,
                "status": status or "Pending",
                "courier": data.get("courier company") or "Not Assigned",
                "product": (data.get("product name") or "")[:100],
                "created_at": data.get("shiprocket created at") or "NA",
                "edd": data.get("edd") or "NA",
                "tracking_link": f"https://shiprocket.co/tracking/{awb}" if awb else None,
                "rto_reason": rto_reason if "rto" in status.lower() else None
            }

            if mobile:
                m_cache.setdefault(mobile, []).append(order_obj)
            if email:
                e_cache.setdefault(email, []).append(order_obj)
            if order_id:
                o_cache.setdefault(order_id, []).append(order_obj)

        # atomic swap (thread safe)
        with lock:
            mobile_cache = m_cache
            email_cache = e_cache
            order_cache = o_cache
            last_updated = time.time()

        print(f"✅ Cache ready: {len(m_cache)} mobiles")

    except Exception as e:
        print("❌ Cache error:", str(e))


def refresh_cache_async():
    threading.Thread(target=refresh_cache, daemon=True).start()


def get_data():
    if time.time() - last_updated > CACHE_TTL:
        refresh_cache_async()

    return mobile_cache, email_cache, order_cache

# ==============================
# 🔴 REDASH CACHE (LIGHT)
# ==============================
redash_cache = []
redash_last_updated = 0
REDASH_CACHE_TTL = 600  # 10 min

def get_redash_data():
    global redash_cache, redash_last_updated

    if time.time() - redash_last_updated < REDASH_CACHE_TTL:
        return redash_cache

    try:
        url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results.json"
        headers = {"Authorization": f"Key {REDASH_API_KEY}"}

        res = requests.get(url, headers=headers, timeout=10)
        rows = res.json().get("query_result", {}).get("data", {}).get("rows", [])

        redash_cache = rows
        redash_last_updated = time.time()

        print(f"🔴 Redash rows: {len(rows)}")
        return rows

    except Exception as e:
        print("❌ Redash error:", str(e))
        return redash_cache

# ==============================
# 🏠 HOME
# ==============================
@app.route("/")
def home():
    return render_template("dashboard.html")

# ==============================
# 🔍 SEARCH (ULTRA FAST ⚡)
# ==============================
@app.route("/search", methods=["POST"])
def search():
    data = request.get_json(silent=True) or {}
    query = str(data.get("query", "")).strip()

    if not query:
        return jsonify({"status": "Invalid query"})

    q_mobile = last10(query)
    q_email = query.lower()
    q_order = query.replace(" ", "")

    m_cache, e_cache, o_cache = get_data()

    rows = (
        m_cache.get(q_mobile)
        or e_cache.get(q_email)
        or o_cache.get(q_order)
    )

    if rows:
        return jsonify({
            "count": len(rows),
            "orders": list(reversed(rows))
        })

    return jsonify({"status": "Not Found"})

# ==============================
# 🔍 BOOK SEARCH
# ==============================
@app.route("/book-search", methods=["POST"])
def book_search():
    data = request.get_json() or {}
    query = data.get("query", "").lower()

    rows = get_redash_data()
    results = []

    for row in rows:
        name = str(row.get("pName") or "").lower()
        if query in name:
            results.append({
                "name": row.get("pName"),
                "edd": row.get("estimated_delivery") or "NA"
            })

    return jsonify({"books": results})

# ==============================
# 🚀 START
# ==============================
refresh_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
