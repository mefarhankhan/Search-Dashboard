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
# 🔐 GOOGLE SHEET SETUP
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
REDASH_API_KEY_1 = os.environ.get("REDASH_API_KEY_1")
REDASH_API_KEY_2 = os.environ.get("REDASH_API_KEY_2")

REDASH_BASE_URL = "https://data.testbook.com"

REDASH_QUERY_ID = "19923"
BOOK_SEARCH_QUERY_ID = "17893"

# ==============================
# ⚡ HELPERS
# ==============================
def last10(val):
    v = str(val).strip().replace(" ", "").replace("+", "")
    return v[-10:] if v else ""

def normalize_id(val):
    """
    Handles Mongo _id formats:
    - string id
    - {"$oid": "..."}
    """
    if isinstance(val, dict):
        return val.get("$oid", "")
    return str(val).strip().replace(" ", "")

# ==============================
# 🚀 SHEET CACHE
# ==============================
mobile_cache = {}
email_cache = {}
order_cache = {}

last_updated = 0
CACHE_TTL = 3600

lock = threading.Lock()

def refresh_cache():
    global mobile_cache, email_cache, order_cache, last_updated

    try:
        records = sheet.get_all_records()
        print(f"📊 Rows fetched: {len(records)}")

        m_cache, e_cache, o_cache = {}, {}, {}

        for row in records:
            try:
                if not isinstance(row, dict):
                    continue

                data = {}
                for k, v in row.items():
                    data[str(k).strip().lower()] = str(v).strip() if v else ""

                mobile = last10(data.get("customer mobile", ""))
                email = data.get("customer email", "").lower()
                order_id = data.get("order id", "").replace(" ", "")

                awb = data.get("awb code") or data.get("awb") or ""
                status = data.get("status", "")
                rto_reason = data.get("latest ndr reason", "")

                order_obj = {
                    "awb": awb or None,
                    "status": status or "Pending",
                    "courier": data.get("courier company") or "Not Assigned",
                    "product": data.get("product name") or "",
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

            except Exception as e:
                print("⚠️ Skipping row:", str(e))

        with lock:
            mobile_cache = m_cache
            email_cache = e_cache
            order_cache = o_cache
            last_updated = time.time()

        print(f"✅ Cache ready | M:{len(m_cache)} E:{len(e_cache)} O:{len(o_cache)}")

    except Exception as e:
        print("❌ Cache error:", str(e))


def refresh_cache_async():
    threading.Thread(target=refresh_cache, daemon=True).start()


def get_data():
    if time.time() - last_updated > CACHE_TTL:
        refresh_cache_async()
    return mobile_cache, email_cache, order_cache

# ==============================
# 🔴 REDASH FALLBACK (ORDER FIXED)
# ==============================
def check_redash_order(query):
    try:
        url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results.json?max_age=0"
        headers = {"Authorization": f"Key {REDASH_API_KEY_1}"}

        res = requests.get(url, headers=headers, timeout=10)
        rows = res.json().get("query_result", {}).get("data", {}).get("rows", [])

        q = last10(query)

        for row in rows:

            # 🔥 FIX: handle both string + Mongo ObjectId format
            raw_id = normalize_id(row.get("_id", ""))

            mobile = last10(row.get("mobile", ""))

            if q == mobile or query == raw_id:
                return {
                    "awb": None,
                    "status": row.get("shippingStatus") or "Preorder",
                    "courier": "Not Available",
                    "product": row.get("pName") or "Not Available",
                    "created_at": "Not Available",
                    "edd": row.get("estimated_delivery") or "Not Available",
                    "tracking_link": None,
                    "rto_reason": None
                }

        return None

    except Exception as e:
        print("❌ Redash fallback error:", str(e))
        return None

# ==============================
# 🔴 BOOK CACHE
# ==============================
book_cache = []
book_last_updated = 0
BOOK_CACHE_TTL = 600

def get_book_redash_data():
    global book_cache, book_last_updated

    if time.time() - book_last_updated < BOOK_CACHE_TTL:
        return book_cache

    try:
        url = f"{REDASH_BASE_URL}/api/queries/{BOOK_SEARCH_QUERY_ID}/results.json"
        headers = {"Authorization": f"Key {REDASH_API_KEY_2}"}

        res = requests.get(url, headers=headers, timeout=10)
        rows = res.json().get("query_result", {}).get("data", {}).get("rows", [])

        book_cache = rows
        book_last_updated = time.time()

        print(f"📚 Book rows: {len(rows)}")
        return rows

    except Exception as e:
        print("❌ Book Redash error:", str(e))
        return book_cache

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

    # 🔴 REDASH fallback
    redash_result = check_redash_order(query)

    if redash_result:
        return jsonify({
            "count": 1,
            "orders": [redash_result]
        })

    return jsonify({"status": "Not Found"})

# ==============================
# 🔍 BOOK SEARCH
# ==============================
@app.route("/book-search", methods=["POST"])
def book_search():
    data = request.get_json() or {}
    query = data.get("query", "").lower().strip()

    if not query:
        return jsonify({"books": []})

    rows = get_book_redash_data()
    results = []

    for row in rows:
        name = str(row.get("BookTitleEnglish") or "").lower()

        if query in name:
            results.append({
                "name": row.get("BookTitleEnglish"),
                "expected_dispatch_time": row.get("estimatedDeliveryTime") or "Not Available"
            })

    return jsonify({"books": results})

# ==============================
# 🚀 START
# ==============================
refresh_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
