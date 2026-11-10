import os
import json
import time
import threading
import requests
import gspread
import re
import unicodedata
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(
    __name__,
    static_folder="Static",
    static_url_path="/static"
)
CORS(app)

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


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
# 🔴 REDASH CONFIG (UNCHANGED)
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
    if isinstance(val, dict):
        return val.get("$oid", "")
    return str(val).strip().replace(" ", "")

# 🔥 NEW: Strong order ID normalize
def normalize_order_id(val):
    return str(val).strip().lower().replace(" ", "")

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

                # normalize keys
                data = {}
                for k, v in row.items():
                    data[str(k).strip().lower()] = str(v).strip() if v else ""

                mobile = last10(data.get("customer mobile", ""))
                email = data.get("customer email", "").lower()

                # 🔥 FIXED ORDER ID HANDLING
                order_id_raw = data.get("order id", "")
                order_id = normalize_order_id(order_id_raw)

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

    "customer_name": data.get("customer name") or "",
    "customer_email": data.get("customer email") or "",
    "customer_mobile": data.get("customer mobile") or "",

    "address_line1": data.get("address line 1") or "",
    "address_line2": data.get("address line 2") or "",
    "city": data.get("address city") or "",
    "state": data.get("address state") or "",
    "pincode": data.get("address pincode") or "",

    "tracking_link": f"https://shiprocket.co/tracking/{awb}" if awb else None,
    "rto_reason": rto_reason if "rto" in status.lower() else None
}

                if mobile:
                    m_cache.setdefault(mobile, []).append(order_obj)
                if email:
                    e_cache.setdefault(email, []).append(order_obj)

                # ✅ store order id
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
# 🔴 REDASH FALLBACK (UNCHANGED)
# ==============================
def check_redash_order(query):
    try:
        url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results.json?max_age=0"
        headers = {"Authorization": f"Key {REDASH_API_KEY_1}"}

        res = requests.get(url, headers=headers, timeout=10)
        rows = res.json().get("query_result", {}).get("data", {}).get("rows", [])

        q = last10(query)

        for row in rows:
            raw_id = normalize_id(row.get("_id", ""))
            mobile = last10(row.get("mobile", ""))

            if q == mobile or query == raw_id:
                return {
                    "customer_name":row.get("student") or "Not Available",
                    "customer_mobile":row.get("mobile") or "Not Available",
                    "customer_email":row.get("email") or "Not Available",
                    "address_line1":row.get("fullAddress") or "Not Available",
                    "awb": None,
                    "status": row.get("shippingStatus") or "Preorder",
                    "courier": "Not Available",
                    "product": row.get("shippingPname") or "Not Available",
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
# BOOK SEARCH DATA
# ==============================
book_cache = None
book_last_updated = 0
BOOK_CACHE_TTL = 600


def normalize_book_text(value):
    """Ignore letter case and differences in whitespace."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip().casefold()


def normalize_book_key(value):
    """Allow spaces, underscores and case differences in column names."""
    return "".join(
        character
        for character in str(value).casefold()
        if character.isalnum()
    )


def get_book_redash_data():
    global book_cache, book_last_updated

    if (
        book_cache is not None
        and time.time() - book_last_updated < BOOK_CACHE_TTL
    ):
        return book_cache

    if not REDASH_API_KEY_2:
        raise RuntimeError("REDASH_API_KEY_2 is not configured")

    url = (
        f"{REDASH_BASE_URL}/api/queries/"
        f"{BOOK_SEARCH_QUERY_ID}/results.json"
    )

    response = requests.get(
        url,
        headers={
            "Authorization": f"Key {REDASH_API_KEY_2}"
        },
        timeout=15
    )

    # Authentication and server errors must not become empty results.
    response.raise_for_status()

    payload = response.json()

    try:
        rows = payload["query_result"]["data"]["rows"]
    except (KeyError, TypeError):
        raise ValueError(
            "Redash did not return completed query results"
        ) from None

    if not isinstance(rows, list):
        raise ValueError("Redash returned an invalid book list")

    books = []

    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Redash returned an invalid book row")

        fields = {
            normalize_book_key(key): value
            for key, value in row.items()
        }

        if "booktitleenglish" not in fields:
            raise ValueError(
                "BookTitleEnglish column is missing from Redash results"
            )

        title = str(fields.get("booktitleenglish") or "").strip()

        if not title:
            continue

        dispatch = fields.get("estimateddeliverytime")

        books.append({
            "name": title,
            "expected_dispatch_time": (
                str(dispatch).strip()
                if dispatch is not None and str(dispatch).strip()
                else "Not Available"
            )
        })

    # Cache only successfully validated data.
    book_cache = books
    book_last_updated = time.time()

    app.logger.info("Loaded %s books from Redash", len(books))

    return books
    
# ==============================
# 🏠 HOME
# ==============================
@app.route("/")
def home():
    return render_template("dashboard.html")

# ==============================
# 🔍 ORDER SEARCH (FIXED)
# ==============================
@app.route("/search", methods=["POST"])
def search():
    data = request.get_json(silent=True) or {}
    query = str(data.get("query", "")).strip()

    if not query:
        return jsonify({"status": "Invalid query"})

    q_mobile = last10(query)
    q_email = query.lower()
    q_order = normalize_order_id(query)

    m_cache, e_cache, o_cache = get_data()

    rows = (
        m_cache.get(q_mobile)
        or e_cache.get(q_email)
        or o_cache.get(q_order)
    )

    # 🔥 PARTIAL MATCH (IMPORTANT)
    if not rows:
        for key, value in o_cache.items():
            if q_order in key:
                rows = value
                break

    if rows:
        return jsonify({
            "count": len(rows),
            "orders": list(reversed(rows))
        })

    # fallback unchanged
    redash_result = check_redash_order(query)

    if redash_result:
        return jsonify({
            "count": 1,
            "orders": [redash_result]
        })

    return jsonify({"status": "Not Found"})

@app.route("/book-search", methods=["POST"])
def book_search():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({
            "error": "Send a JSON object containing a query"
        }), 400

    raw_query = data.get("query", "")

    if not isinstance(raw_query, str):
        return jsonify({
            "error": "Book name must be text"
        }), 400

    query = normalize_book_text(raw_query)

    if not query:
        return jsonify({"books": []})

    try:
        books = get_book_redash_data()
    except Exception:
        app.logger.exception("Book search data loading failed")

        return jsonify({
            "error": (
                "Unable to load book details. Please try again later."
            )
        }), 502

    results = [
        book
        for book in books
        if query in normalize_book_text(book["name"])
    ]

    return jsonify({
        "books": results
    })



# ==============================
# 🚀 START
# ==============================
threading.Thread(
    target=refresh_cache,
    daemon=True
).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
