import hashlib
import pandas as pd
from datetime import datetime
import os

# ==============================
# GLOBAL CONFIG
# ==============================

BUDGET = 1000000
SYSTEM_STATUS = "ACTIVE"
LEDGER_FILE = "ledger.txt"


# ==============================
# UTILITY FUNCTIONS
# ==============================

def hash_id(citizen_id):
    return hashlib.sha256(citizen_id.encode()).hexdigest()


def generate_hash(timestamp, citizen_hash, scheme, amount, previous_hash):
    record = f"{timestamp}{citizen_hash}{scheme}{amount}{previous_hash}"
    return hashlib.sha256(record.encode()).hexdigest()


def get_previous_hash():
    if not os.path.exists(LEDGER_FILE):
        return "GENESIS"

    with open(LEDGER_FILE, "r") as f:
        lines = f.readlines()
        if not lines:
            return "GENESIS"
        last_line = lines[-1].strip().split("|")
        return last_line[-1]  # current hash of last record


def verify_ledger_integrity():
    if not os.path.exists(LEDGER_FILE):
        return True

    with open(LEDGER_FILE, "r") as f:
        lines = f.readlines()

    previous_hash = "GENESIS"

    for line in lines:
        timestamp, citizen_hash, scheme, amount, prev_hash, curr_hash = line.strip().split("|")

        recalculated_hash = generate_hash(timestamp, citizen_hash, scheme, amount, prev_hash)

        if recalculated_hash != curr_hash:
            return False

        previous_hash = curr_hash

    return True


# ==============================
# VALIDATION GATES
# ==============================

def eligibility_gate(row, scheme, amount):
    if row["Account_Status"] != "Active":
        return False, "Account Not Active"

    if row["Aadhaar_Linked"] != True:
        return False, "Aadhaar Not Linked"

    if row["Scheme_Eligibility"] != scheme:
        return False, "Scheme Not Eligible"

    if row["Scheme_Amount"] != amount:
        return False, "Invalid Scheme Amount"

    if row["Claim_Count"] > 3:
        return False, "Claim Limit Exceeded"

    return True, "Eligible"


def frequency_gate(last_claim_date):
    last_date = datetime.strptime(str(last_claim_date), "%Y-%m-%d")
    today = datetime.today()
    diff = (today - last_date).days

    if diff < 30:
        return False, "Claim within 30 days not allowed"

    return True, "Frequency OK"


def budget_gate(amount):
    global BUDGET
    if amount > BUDGET:
        return False, "Insufficient Budget"

    BUDGET -= amount
    return True, "Budget Approved"


# ==============================
# MAIN TRANSACTION FUNCTION
# ==============================

def process_transaction(citizen_id, scheme, amount):
    global SYSTEM_STATUS

    if SYSTEM_STATUS != "ACTIVE":
        return f"System is {SYSTEM_STATUS}. Transaction Blocked."

    # Verify ledger integrity before processing
    if not verify_ledger_integrity():
        SYSTEM_STATUS = "FROZEN"
        return "Ledger Tampering Detected. System Frozen."

    df = pd.read_excel("jan_dhan_registry_advanced.xlsx")
    
    # Convert Citizen_ID to string for comparison
    df["Citizen_ID"] = df["Citizen_ID"].astype(str)

    citizen_row = df[df["Citizen_ID"] == citizen_id]

    if citizen_row.empty:
        return "Citizen Not Found"

    row = citizen_row.iloc[0]

    # Gate 1
    eligible, message = eligibility_gate(row, scheme, amount)
    if not eligible:
        return message

    # Gate 2
    freq_ok, message = frequency_gate(row["Last_Claim_Date"])
    if not freq_ok:
        return message

    # Gate 3
    budget_ok, message = budget_gate(amount)
    if not budget_ok:
        return message

    # If all gates pass → Write to ledger
    citizen_hash = hash_id(citizen_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    previous_hash = get_previous_hash()

    current_hash = generate_hash(timestamp, citizen_hash, scheme, amount, previous_hash)

    with open(LEDGER_FILE, "a") as f:
        f.write(f"{timestamp}|{citizen_hash}|{scheme}|{amount}|{previous_hash}|{current_hash}\n")

    return f"Transaction Approved | Remaining Budget: Rs.{BUDGET}"


# ==============================
# TEST RUN
# ==============================

print("=" * 60)
print("FRAUD DETECTION SYSTEM - TEST RUN")
print("=" * 60)

# Test Case 1: Valid transaction for Rahul Sharma
print("\nTest 1: Processing transaction for citizen 123456789012 (Rahul Sharma)")
result = process_transaction("123456789012", "Health_Scheme", 5000)
print(f"Result: {result}")

# Test Case 2: Invalid citizen
print("\nTest 2: Processing transaction for non-existent citizen")
result = process_transaction("000000000000", "Health_Scheme", 5000)
print(f"Result: {result}")

# Test Case 3: Inactive account (Amit Kumar)
print("\nTest 3: Processing transaction for inactive account (555566667777)")
result = process_transaction("555566667777", "Health_Scheme", 5000)
print(f"Result: {result}")

# Test Case 4: Aadhaar not linked (Sita Devi)
print("\nTest 4: Processing transaction for Aadhaar not linked (111122223333)")
result = process_transaction("111122223333", "Health_Scheme", 5000)
print(f"Result: {result}")

print("\n" + "=" * 60)
print("TEST RUN COMPLETE")
print("=" * 60)
