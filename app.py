from flask import Flask, render_template, request, jsonify
import hashlib
import pandas as pd
from datetime import datetime
import sqlite3
import os

app = Flask(__name__)

# ==============================
# GLOBAL CONFIG
# ==============================

INITIAL_BUDGET = 1000000
SYSTEM_STATUS = "ACTIVE"
DB_FILE = "fraud_system.db"
LEDGER_FILE = "ledger.txt"
REGISTRY_FILE = "jan_dhan_registry_advanced.xlsx"
SCHEME_AMOUNT_MAP = {
    "Health_Scheme": 5000.0,
    "Education_Scheme": 10000.0,
    "Agriculture_Scheme": 15000.0,
    "Housing_Scheme": 20000.0,
}
AMOUNT_TOLERANCE = 0.01


# ==============================
# DATABASE HELPERS
# ==============================

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            citizen_hash TEXT NOT NULL,
            scheme TEXT NOT NULL,
            amount REAL NOT NULL,
            previous_hash TEXT NOT NULL,
            current_hash TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS citizens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            citizen_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            account_status TEXT NOT NULL,
            aadhaar_linked INTEGER NOT NULL,
            scheme_eligibility TEXT NOT NULL,
            scheme_amount REAL NOT NULL,
            claim_count INTEGER NOT NULL,
            last_claim_date TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()
    backfill_ledger_from_file()
    backfill_citizens_from_excel()


def backfill_ledger_from_file():
    if not os.path.exists(LEDGER_FILE):
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    with open(LEDGER_FILE, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    for line in lines:
        parts = line.split("|")
        if len(parts) != 6:
            continue
        timestamp, citizen_hash, scheme, amount_str, previous_hash, current_hash = parts
        cursor.execute("SELECT 1 FROM ledger_entries WHERE current_hash = ?", (current_hash,))
        if cursor.fetchone():
            continue
        try:
            amount_value = float(amount_str)
        except ValueError:
            amount_value = 0.0
        cursor.execute(
            """
            INSERT INTO ledger_entries (timestamp, citizen_hash, scheme, amount, previous_hash, current_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (timestamp, citizen_hash, scheme, amount_value, previous_hash, current_hash)
        )

    conn.commit()
    conn.close()


def backfill_citizens_from_excel():
    if not os.path.exists(REGISTRY_FILE):
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        df = pd.read_excel(REGISTRY_FILE)
    except Exception:
        conn.close()
        return

    required_cols = {"Citizen_ID", "Name", "Account_Status", "Aadhaar_Linked", "Scheme_Eligibility",
                     "Scheme_Amount", "Claim_Count", "Last_Claim_Date"}
    if not required_cols.issubset(set(df.columns)):
        conn.close()
        return

    df["Citizen_ID"] = df["Citizen_ID"].astype(str)
    df["Aadhaar_Linked"] = df["Aadhaar_Linked"].apply(lambda v: 1 if bool(v) else 0)
    df["Scheme_Amount"] = df["Scheme_Amount"].astype(float)
    df["Claim_Count"] = df["Claim_Count"].fillna(0).astype(int)
    df["Last_Claim_Date"] = pd.to_datetime(df["Last_Claim_Date"]).dt.strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT OR REPLACE INTO citizens
            (citizen_id, name, account_status, aadhaar_linked, scheme_eligibility, scheme_amount, claim_count, last_claim_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["Citizen_ID"],
                row["Name"],
                row["Account_Status"],
                int(row["Aadhaar_Linked"]),
                row["Scheme_Eligibility"],
                float(row["Scheme_Amount"]),
                int(row["Claim_Count"]),
                row["Last_Claim_Date"],
            )
        )

    conn.commit()
    conn.close()


# ==============================
# LEDGER HELPERS
# ==============================

def hash_id(citizen_id):
    return hashlib.sha256(citizen_id.encode()).hexdigest()


def amount_hash_value(amount):
    try:
        amount_float = float(amount)
        if amount_float.is_integer():
            return str(int(amount_float))
        return str(amount_float)
    except (TypeError, ValueError):
        return str(amount)


def generate_hash(timestamp, citizen_hash, scheme, amount, previous_hash):
    record = f"{timestamp}{citizen_hash}{scheme}{amount}{previous_hash}"
    return hashlib.sha256(record.encode()).hexdigest()


def get_previous_hash():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT current_hash FROM ledger_entries ORDER BY timestamp DESC, id DESC LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    if not row:
        return "GENESIS"
    return row[0]


def fetch_ledger_rows():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT timestamp, citizen_hash, scheme, amount, previous_hash, current_hash FROM ledger_entries ORDER BY timestamp"
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def verify_ledger_integrity():
    rows = fetch_ledger_rows()
    previous_hash = "GENESIS"

    for row in rows:
        timestamp, citizen_hash, scheme, amount, prev_hash, curr_hash = row
        amount_str = amount_hash_value(amount)
        recalculated_hash = generate_hash(timestamp, citizen_hash, scheme, amount_str, prev_hash)
        if recalculated_hash != curr_hash or prev_hash != previous_hash:
            return False
        previous_hash = curr_hash

    return True


def calculate_remaining_budget():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries")
    total_disbursed = cursor.fetchone()[0]
    conn.close()
    total_disbursed = float(total_disbursed or 0)
    return max(INITIAL_BUDGET - total_disbursed, 0)


# ==============================
# CITIZEN HELPERS
# ==============================

def prepare_citizen_record(row):
    if not row:
        return None
    return {
        "Citizen_ID": row["citizen_id"],
        "Name": row["name"],
        "Account_Status": row["account_status"],
        "Aadhaar_Linked": bool(row["aadhaar_linked"]),
        "Scheme_Eligibility": row["scheme_eligibility"],
        "Scheme_Amount": float(row["scheme_amount"]),
        "Claim_Count": int(row["claim_count"]),
        "Last_Claim_Date": row["last_claim_date"],
    }


def get_citizen_record(citizen_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM citizens WHERE citizen_id = ?", (citizen_id,))
    row = cursor.fetchone()
    conn.close()
    return prepare_citizen_record(row)


def get_expected_scheme_amount(scheme):
    value = SCHEME_AMOUNT_MAP.get(scheme)
    return float(value) if value is not None else None


def get_all_citizens():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM citizens ORDER BY name")
    rows = cursor.fetchall()
    conn.close()
    return [prepare_citizen_record(row) for row in rows]


def normalize_bool_flag(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def validate_citizen_payload(data):
    citizen_id = str(data.get("citizen_id", "")).strip()
    if len(citizen_id) != 12 or not citizen_id.isdigit():
        raise ValueError("Citizen ID must be a 12 digit number")

    name = data.get("name", "").strip()
    if not name:
        raise ValueError("Name is required")

    account_status = data.get("account_status", "Active").strip() or "Active"
    aadhaar_linked = normalize_bool_flag(data.get("aadhaar_linked", False))
    scheme = data.get("scheme_eligibility", "").strip()
    if not scheme:
        raise ValueError("Scheme eligibility is required")

    try:
        scheme_amount = float(data.get("scheme_amount", 0))
    except (TypeError, ValueError):
        raise ValueError("Scheme amount must be a number")
    if scheme_amount <= 0:
        raise ValueError("Scheme amount must be greater than zero")

    expected_amount = get_expected_scheme_amount(scheme)
    if expected_amount is None:
        raise ValueError(f"Unsupported scheme: {scheme}")
    if abs(scheme_amount - expected_amount) > AMOUNT_TOLERANCE:
        raise ValueError(f"Scheme amount must be Rs. {expected_amount:.0f} for {scheme}")

    try:
        claim_count = int(data.get("claim_count", 0))
    except (TypeError, ValueError):
        raise ValueError("Claim count must be an integer")
    if claim_count < 0:
        raise ValueError("Claim count cannot be negative")

    last_claim_date = data.get("last_claim_date") or datetime.today().strftime("%Y-%m-%d")
    try:
        datetime.strptime(last_claim_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Last claim date must be in YYYY-MM-DD format")

    return {
        "citizen_id": citizen_id,
        "name": name,
        "account_status": account_status,
        "aadhaar_linked": 1 if aadhaar_linked else 0,
        "scheme_eligibility": scheme,
        "scheme_amount": scheme_amount,
        "claim_count": claim_count,
        "last_claim_date": last_claim_date,
    }


def upsert_citizen(record):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO citizens
        (citizen_id, name, account_status, aadhaar_linked, scheme_eligibility, scheme_amount, claim_count, last_claim_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(citizen_id) DO UPDATE SET
            name=excluded.name,
            account_status=excluded.account_status,
            aadhaar_linked=excluded.aadhaar_linked,
            scheme_eligibility=excluded.scheme_eligibility,
            scheme_amount=excluded.scheme_amount,
            claim_count=excluded.claim_count,
            last_claim_date=excluded.last_claim_date
        """,
        (
            record["citizen_id"],
            record["name"],
            record["account_status"],
            record["aadhaar_linked"],
            record["scheme_eligibility"],
            record["scheme_amount"],
            record["claim_count"],
            record["last_claim_date"],
        )
    )
    conn.commit()
    conn.close()


# ==============================
# VALIDATION GATES
# ==============================

def eligibility_gate(row, scheme, amount):
    if row["Account_Status"] != "Active":
        return False, "Account Not Active"

    if row["Aadhaar_Linked"] is not True:
        return False, "Aadhaar Not Linked"

    if row["Scheme_Eligibility"] != scheme:
        return False, "Scheme Not Eligible"

    expected_amount = get_expected_scheme_amount(scheme)
    if expected_amount is None:
        return False, "Unsupported Scheme"

    if abs(row["Scheme_Amount"] - expected_amount) > AMOUNT_TOLERANCE:
        return False, "Registry Scheme Amount Mismatch"

    if abs(float(amount) - expected_amount) > AMOUNT_TOLERANCE:
        return False, "Transaction Amount Mismatch"

    if row["Claim_Count"] > 3:
        return False, "Claim Limit Exceeded"

    return True, "Eligible"


def frequency_gate(last_claim_date):
    try:
        last_date = datetime.strptime(str(last_claim_date), "%Y-%m-%d")
    except ValueError:
        return False, "Invalid last claim date"
    today = datetime.today()
    diff = (today - last_date).days

    if diff < 30:
        return False, f"Claim within 30 days not allowed (Last claim: {diff} days ago)"

    return True, "Frequency OK"


def budget_gate(amount):
    global SYSTEM_STATUS
    remaining = calculate_remaining_budget()
    if remaining <= 0:
        SYSTEM_STATUS = "LOCKED"
        return False, "Budget Exhausted. System Locked."
    if amount > remaining:
        return False, "Insufficient Budget"

    return True, "Budget Approved"


# ==============================
# MAIN TRANSACTION FUNCTION
# ==============================

def process_transaction(citizen_id, scheme, amount):
    global SYSTEM_STATUS

    if SYSTEM_STATUS != "ACTIVE":
        return {"success": False, "message": f"System is {SYSTEM_STATUS}. Transaction Blocked.", "gate": "system"}

    if not verify_ledger_integrity():
        SYSTEM_STATUS = "FROZEN"
        return {"success": False, "message": "Ledger Tampering Detected. System Frozen.", "gate": "integrity"}

    citizen_record = get_citizen_record(citizen_id)
    if not citizen_record:
        return {"success": False, "message": "Citizen Not Found", "gate": "lookup"}

    row = citizen_record
    citizen_name = row.get("Name", "Unknown")

    # Gate 1
    eligible, message = eligibility_gate(row, scheme, amount)
    if not eligible:
        return {"success": False, "message": message, "gate": "eligibility", "citizen_name": citizen_name}

    # Gate 2
    budget_ok, message = budget_gate(amount)
    if not budget_ok:
        return {"success": False, "message": message, "gate": "budget", "citizen_name": citizen_name}

    # Gate 3
    freq_ok, message = frequency_gate(row["Last_Claim_Date"])
    if not freq_ok:
        return {"success": False, "message": message, "gate": "frequency", "citizen_name": citizen_name}

    # If all gates pass - Write to ledger
    citizen_hash = hash_id(citizen_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    previous_hash = get_previous_hash()

    amount_str = amount_hash_value(amount)
    current_hash = generate_hash(timestamp, citizen_hash, scheme, amount_str, previous_hash)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO ledger_entries (timestamp, citizen_hash, scheme, amount, previous_hash, current_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (timestamp, citizen_hash, scheme, amount, previous_hash, current_hash)
    )
    conn.commit()
    conn.close()

    remaining_budget = int(calculate_remaining_budget())
    if remaining_budget <= 0:
        SYSTEM_STATUS = "LOCKED"

    return {
        "success": True,
        "message": "Transaction Approved",
        "citizen_name": citizen_name,
        "remaining_budget": remaining_budget,
        "transaction_hash": current_hash[:16] + "..."
    }


# ==============================
# ROUTES
# ==============================

@app.route('/')
def index():
    return render_template('index.html', scheme_amount_map=SCHEME_AMOUNT_MAP)


@app.route('/process', methods=['POST'])
def process():
    data = request.json or {}
    citizen_id = str(data.get('citizen_id', '')).strip()
    scheme = str(data.get('scheme', '')).strip()

    try:
        submitted_amount = float(data.get('amount', 0))
    except (TypeError, ValueError):
        submitted_amount = 0.0

    expected_amount = get_expected_scheme_amount(scheme)
    if expected_amount is None:
        return jsonify({
            "success": False,
            "message": "Unsupported scheme",
            "gate": "eligibility"
        })

    if submitted_amount and abs(submitted_amount - expected_amount) > AMOUNT_TOLERANCE:
        return jsonify({
            "success": False,
            "message": "Amount does not match authorized scheme value",
            "gate": "eligibility"
        })

    result = process_transaction(citizen_id, scheme, expected_amount)
    return jsonify(result)


@app.route('/ledger')
def get_ledger():
    rows = fetch_ledger_rows()[::-1]
    records = []
    for timestamp, citizen_hash, scheme, amount, _, current_hash in rows:
        try:
            amount_float = float(amount)
            amount_value = int(amount_float) if amount_float.is_integer() else amount_float
        except (TypeError, ValueError):
            amount_value = amount
        records.append({
            "timestamp": timestamp,
            "citizen_hash": (citizen_hash or "")[:12] + "...",
            "scheme": scheme,
            "amount": amount_value,
            "block_hash": (current_hash or "")[:12] + "..."
        })
    return jsonify(records)


@app.route('/citizens', methods=['GET', 'POST'])
def citizens_endpoint():
    if request.method == 'GET':
        return jsonify(get_all_citizens())

    data = request.json or {}
    try:
        record = validate_citizen_payload(data)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    upsert_citizen(record)
    return jsonify({"success": True, "message": "Citizen saved successfully"})


@app.route('/status')
def get_status():
    global SYSTEM_STATUS
    integrity = verify_ledger_integrity()
    remaining = int(calculate_remaining_budget())
    return jsonify({
        "budget": remaining,
        "system_status": SYSTEM_STATUS,
        "ledger_integrity": integrity
    })


init_db()


if __name__ == '__main__':
    app.run(debug=True, port=5000)
