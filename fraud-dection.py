import hashlib
import pandas as pd
from datetime import datetime
import os
import sqlite3

# ==============================
# GLOBAL CONFIG
# ==============================

INITIAL_BUDGET = 1000000
SYSTEM_STATUS = "ACTIVE"
DB_FILE = "fraud_system.db"
LEDGER_FILE = "ledger.txt"
REGISTRY_FILE = "jan_dhan_registry_advanced.xlsx"


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
                row["Last_Claim_Date"]
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


def citizen_has_prior_claim(citizen_id):
    citizen_hash = hash_id(citizen_id)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM ledger_entries WHERE citizen_hash = ? LIMIT 1",
        (citizen_hash,)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None


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

    if row["Scheme_Amount"] != amount:
        return False, "Invalid Scheme Amount"

    if row["Claim_Count"] >= 1:
        return False, "Claim already settled"

    return True, "Eligible"


def frequency_gate(last_claim_date):
    try:
        last_date = datetime.strptime(str(last_claim_date), "%Y-%m-%d")
    except ValueError:
        return False, "Invalid last claim date"
    today = datetime.today()
    diff = (today - last_date).days

    if diff < 30:
        return False, "Claim within 30 days not allowed"

    return True, "Frequency OK"


def budget_gate(amount):
    remaining = calculate_remaining_budget()
    if amount > remaining:
        return False, "Insufficient Budget"

    return True, "Budget Approved"


# ==============================
# MAIN TRANSACTION FUNCTION
# ==============================

def process_transaction(citizen_id, scheme, amount):
    global SYSTEM_STATUS

    if SYSTEM_STATUS != "ACTIVE":
        return f"System is {SYSTEM_STATUS}. Transaction Blocked."

    if not verify_ledger_integrity():
        SYSTEM_STATUS = "FROZEN"
        return "Ledger Tampering Detected. System Frozen."

    row = get_citizen_record(citizen_id)
    if not row:
        return "Citizen Not Found"

    if citizen_has_prior_claim(citizen_id):
        return "Claim already settled"

    eligible, message = eligibility_gate(row, scheme, amount)
    if not eligible:
        return message

    freq_ok, message = frequency_gate(row["Last_Claim_Date"])
    if not freq_ok:
        return message

    budget_ok, message = budget_gate(amount)
    if not budget_ok:
        return message

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

    cursor.execute(
        """
        UPDATE citizens
        SET claim_count = claim_count + 1,
            last_claim_date = ?
        WHERE citizen_id = ?
        """,
        (timestamp.split(" ")[0], row["Citizen_ID"])
    )

    conn.commit()
    conn.close()

    remaining_budget = int(calculate_remaining_budget())

    return f"Transaction Approved [SUCCESS] | Remaining Budget: Rs.{remaining_budget}"


# ==============================
# TEST RUN
# ==============================

init_db()


if __name__ == "__main__":
    result = process_transaction("123456789012", "Health_Scheme", 5000)
    print(result)