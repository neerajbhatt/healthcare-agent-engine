"""
Synthetic Healthcare Data Generator
====================================
Generates realistic but fictional data for:
  - providers (200)
  - members (5,000)
  - eligibility (6,000)
  - claims (100,000+)
  - specialty_procedure_map

Includes intentional anomalies for agent testing:
  - 3 providers with upcoding patterns
  - 2 providers with billing spikes
  - Members with doctor-shopping behavior
  - Coverage gap claims
  - Retroactive eligibility additions
  - Weekend billing anomalies
  - Duplicate claims

Output: CSV files ready for Snowflake COPY INTO or INSERT.

Usage:
    python data/generate_data.py
    # Creates CSV files in data/csv/
"""

import csv
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(42)

OUT_DIR = Path(__file__).parent / "csv"
OUT_DIR.mkdir(exist_ok=True)

# ── Reference Data ──

SPECIALTIES = [
    "Family Medicine", "Internal Medicine", "Cardiology", "Orthopedics",
    "Dermatology", "Neurology", "Oncology", "Pediatrics", "Psychiatry",
    "Radiology", "General Surgery", "Emergency Medicine", "Urology",
    "Ophthalmology", "Gastroenterology",
]

STATES = ["TX", "FL", "CA", "NY", "PA", "IL", "OH", "GA", "NC", "MI"]

CITIES_BY_STATE = {
    "TX": ["Houston", "Dallas", "Austin", "San Antonio"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville"],
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento"],
    "NY": ["New York", "Buffalo", "Albany", "Rochester"],
    "PA": ["Philadelphia", "Pittsburgh", "Harrisburg"],
    "IL": ["Chicago", "Springfield", "Naperville"],
    "OH": ["Columbus", "Cleveland", "Cincinnati"],
    "GA": ["Atlanta", "Savannah", "Augusta"],
    "NC": ["Charlotte", "Raleigh", "Durham"],
    "MI": ["Detroit", "Grand Rapids", "Ann Arbor"],
}

EM_CODES = ["99211", "99212", "99213", "99214", "99215"]
# Normal E&M distribution: mostly 99213/99214
NORMAL_EM_WEIGHTS = [5, 15, 40, 30, 10]
# Upcoding distribution: heavy 99214/99215
UPCODING_EM_WEIGHTS = [1, 3, 12, 30, 54]

PROCEDURE_CODES = {
    "Family Medicine":   EM_CODES + ["G0438", "G0439", "99395", "99396"],
    "Cardiology":        EM_CODES + ["93000", "93306", "93452", "93458"],
    "Orthopedics":       EM_CODES + ["27447", "27130", "29881", "20610"],
    "Dermatology":       EM_CODES + ["11102", "17000", "96910", "17110"],
    "Radiology":         ["70553", "74177", "71260", "72148", "77067"],
    "General Surgery":   EM_CODES + ["47562", "49505", "43239", "44120"],
    "Internal Medicine":  EM_CODES + ["99395", "99396", "G0438"],
    "Neurology":         EM_CODES + ["95819", "95909", "95816"],
    "Oncology":          EM_CODES + ["96413", "96415", "96417"],
    "Pediatrics":        EM_CODES + ["99381", "99391", "99382"],
    "Psychiatry":        EM_CODES + ["90834", "90837", "90847"],
    "Emergency Medicine": EM_CODES + ["99281", "99282", "99283", "99284", "99285"],
    "Urology":           EM_CODES + ["52000", "52601", "55700"],
    "Ophthalmology":     EM_CODES + ["92004", "92012", "66984"],
    "Gastroenterology":  EM_CODES + ["43239", "45378", "45380"],
}

DIAGNOSIS_CODES = [
    ("J06.9", "Acute upper respiratory infection"),
    ("M54.5", "Low back pain"),
    ("E11.9", "Type 2 diabetes without complications"),
    ("I10",   "Essential hypertension"),
    ("J45.20","Mild intermittent asthma"),
    ("K21.0", "GERD with esophagitis"),
    ("M17.11","Primary osteoarthritis right knee"),
    ("F32.1", "Major depressive disorder, moderate"),
    ("N39.0", "Urinary tract infection"),
    ("R10.9", "Unspecified abdominal pain"),
    ("Z00.00","General adult medical exam"),
    ("G43.909","Migraine unspecified"),
    ("L70.0", "Acne vulgaris"),
    ("R51.9", "Headache"),
    ("J20.9", "Acute bronchitis"),
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen", "Charles",
    "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret",
    "Mark", "Sandra", "Donald", "Ashley", "Steven", "Dorothy", "Andrew",
    "Kimberly", "Paul", "Emily", "Joshua", "Donna",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]

EMPLOYER_GROUPS = [
    "Acme Corp", "TechStar Inc", "GlobalHealth LLC", "Metro Services",
    "Pacific Enterprises", "Summit Industries", "Apex Solutions",
    "National Logistics", "Pinnacle Group", "Alliance Health",
    None, None, None,  # Some members have no employer (individual/Medicaid)
]


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 1)))


def generate_npi() -> str:
    return "1" + "".join(str(random.randint(0, 9)) for _ in range(9))


# ── Track anomaly providers for intentional patterns ──
ANOMALY_PROVIDERS = {}  # npi -> anomaly_type


# ============================================================
# GENERATE PROVIDERS
# ============================================================
def generate_providers(n=200):
    rows = []
    for i in range(n):
        state = random.choice(STATES)
        npi = generate_npi()
        specialty = random.choice(SPECIALTIES)

        # Mark some for anomalies
        if i < 3:
            ANOMALY_PROVIDERS[npi] = "upcoding"
        elif i < 5:
            ANOMALY_PROVIDERS[npi] = "billing_spike"
        elif i < 7:
            ANOMALY_PROVIDERS[npi] = "weekend_biller"

        rows.append({
            "npi": npi,
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "specialty": specialty,
            "practice_state": state,
            "practice_city": random.choice(CITIES_BY_STATE[state]),
            "practice_zip": f"{random.randint(10000, 99999)}",
            "tax_id": f"{random.randint(10,99)}-{random.randint(1000000,9999999)}",
            "practice_type": random.choice(["solo", "solo", "group", "group", "facility"]),
            "enrollment_date": random_date(date(2018, 1, 1), date(2024, 6, 1)).isoformat(),
            "is_active": True,
        })
    return rows


# ============================================================
# GENERATE MEMBERS
# ============================================================
def generate_members(n=5000):
    rows = []
    for i in range(n):
        state = random.choice(STATES)
        rows.append({
            "member_id": f"MBR{i+1:06d}",
            "date_of_birth": random_date(date(1940, 1, 1), date(2005, 12, 31)).isoformat(),
            "gender": random.choice(["M", "F"]),
            "state": state,
            "city": random.choice(CITIES_BY_STATE[state]),
            "zip": f"{random.randint(10000, 99999)}",
            "plan_type": random.choice(["HMO", "PPO", "EPO", "Medicaid", "PPO", "HMO"]),
            "employer_group": random.choice(EMPLOYER_GROUPS),
            "risk_score": round(random.uniform(0.5, 4.5), 2),
        })
    return rows


# ============================================================
# GENERATE ELIGIBILITY
# ============================================================
def generate_eligibility(members):
    rows = []
    elig_id = 0
    for m in members:
        # 1-2 eligibility periods per member
        periods = random.choices([1, 2], weights=[70, 30])[0]
        coverage_start = random_date(date(2023, 1, 1), date(2024, 6, 1))

        for p in range(periods):
            elig_id += 1
            start = coverage_start + timedelta(days=p * 365)
            end = start + timedelta(days=random.randint(180, 730))
            if end > date(2026, 3, 31):
                end = date(2026, 3, 31)

            # Most records created on or before start date
            created = start - timedelta(days=random.randint(0, 30))

            # ANOMALY: Some retroactive additions (created 30-90 days AFTER start)
            if random.random() < 0.03:
                created = start + timedelta(days=random.randint(31, 90))

            rows.append({
                "eligibility_id": f"ELG{elig_id:06d}",
                "member_id": m["member_id"],
                "coverage_start": start.isoformat(),
                "coverage_end": end.isoformat(),
                "plan_code": f"PLN{random.randint(100,999)}",
                "coverage_type": random.choice(["medical", "medical", "medical", "dental", "pharmacy"]),
                "status": random.choice(["active", "active", "active", "terminated"]),
                "created_date": created.isoformat(),
            })
    return rows


# ============================================================
# GENERATE CLAIMS
# ============================================================
def generate_claims(providers, members):
    rows = []
    claim_id = 0

    member_ids = [m["member_id"] for m in members]
    provider_map = {p["npi"]: p for p in providers}

    # Precompute provider lists by state for referrals
    providers_by_state = {}
    for p in providers:
        providers_by_state.setdefault(p["practice_state"], []).append(p["npi"])

    for provider in providers:
        npi = provider["npi"]
        specialty = provider["specialty"]
        state = provider["practice_state"]
        anomaly = ANOMALY_PROVIDERS.get(npi)

        # Number of claims per provider
        if anomaly == "billing_spike":
            # Normal for most months, then huge spike in Q4 2025
            base_claims = random.randint(80, 150)
            spike_claims = random.randint(400, 700)
        else:
            base_claims = random.randint(100, 500)
            spike_claims = 0

        proc_codes = PROCEDURE_CODES.get(specialty, EM_CODES)

        # Generate normal claims
        for _ in range(base_claims):
            claim_id += 1
            svc_date = random_date(date(2024, 1, 1), date(2025, 9, 30))

            # Weekend anomaly
            if anomaly == "weekend_biller" and random.random() < 0.35:
                # Push to weekend
                dow = svc_date.weekday()
                if dow < 5:
                    svc_date += timedelta(days=(5 - dow))

            member = random.choice(member_ids)
            dx = random.choice(DIAGNOSIS_CODES)

            # Procedure selection
            if anomaly == "upcoding" and random.random() < 0.8:
                proc = random.choices(EM_CODES, weights=UPCODING_EM_WEIGHTS)[0]
            else:
                proc = random.choice(proc_codes)

            # Amount
            base_amount = _get_base_amount(proc)
            allowed = round(base_amount * random.uniform(0.8, 1.3), 2)

            # Referring provider (30% of claims)
            referring = None
            if random.random() < 0.3 and state in providers_by_state:
                candidates = [p for p in providers_by_state[state] if p != npi]
                if candidates:
                    referring = random.choice(candidates)

            rows.append(_make_claim(
                claim_id, member, npi, referring, svc_date,
                proc, dx, allowed, state,
            ))

        # ANOMALY: Billing spike in Q4 2025
        if spike_claims > 0:
            # Also add new members from same employer group (kickback pattern)
            spike_employer = random.choice([e for e in EMPLOYER_GROUPS if e])
            spike_members = [m["member_id"] for m in members
                          if m.get("employer_group") == spike_employer][:50]
            if not spike_members:
                spike_members = random.sample(member_ids, min(50, len(member_ids)))

            for _ in range(spike_claims):
                claim_id += 1
                svc_date = random_date(date(2025, 10, 1), date(2025, 12, 31))
                member = random.choice(spike_members)
                dx = random.choice(DIAGNOSIS_CODES)
                proc = random.choice(proc_codes)
                base_amount = _get_base_amount(proc)
                allowed = round(base_amount * random.uniform(0.9, 1.4), 2)

                rows.append(_make_claim(
                    claim_id, member, npi, None, svc_date,
                    proc, dx, allowed, state,
                ))

    # ANOMALY: Duplicate claims (~0.5% of total)
    num_dupes = max(1, len(rows) // 200)
    for _ in range(num_dupes):
        claim_id += 1
        original = random.choice(rows)
        dupe = dict(original)
        dupe["claim_id"] = f"CLM{claim_id:07d}"
        rows.append(dupe)

    # ANOMALY: Doctor-shopping members (visit 5+ providers for same dx in 30 days)
    shopping_members = random.sample(member_ids, min(20, len(member_ids)))
    shopping_dx = random.choice(DIAGNOSIS_CODES)
    for mbr in shopping_members:
        base_date = random_date(date(2025, 3, 1), date(2025, 9, 1))
        shopping_providers = random.sample(
            [p["npi"] for p in providers], min(6, len(providers))
        )
        for prov_npi in shopping_providers:
            claim_id += 1
            svc_date = base_date + timedelta(days=random.randint(0, 25))
            proc = random.choice(EM_CODES)
            allowed = round(_get_base_amount(proc) * random.uniform(0.8, 1.2), 2)
            rows.append(_make_claim(
                claim_id, mbr, prov_npi, None, svc_date,
                proc, shopping_dx, allowed,
                provider_map.get(prov_npi, {}).get("practice_state", "TX"),
            ))

    # ANOMALY: Claims during coverage gaps
    gap_members = random.sample(member_ids, min(15, len(member_ids)))
    for mbr in gap_members:
        claim_id += 1
        # Service date intentionally outside any eligibility window
        svc_date = date(2023, 6, 15)  # Before most eligibility starts
        prov = random.choice(providers)
        proc = random.choice(EM_CODES)
        allowed = round(_get_base_amount(proc) * random.uniform(0.8, 1.2), 2)
        dx = random.choice(DIAGNOSIS_CODES)
        rows.append(_make_claim(
            claim_id, mbr, prov["npi"], None, svc_date,
            proc, dx, allowed, prov["practice_state"],
        ))

    return rows


def _get_base_amount(proc_code: str) -> float:
    """Rough base allowed amount by procedure type."""
    amounts = {
        "99211": 35, "99212": 65, "99213": 110, "99214": 165, "99215": 230,
        "93000": 45, "93306": 350, "93452": 2800, "93458": 3200,
        "27447": 15000, "27130": 18000, "29881": 4500, "20610": 180,
        "47562": 8000, "49505": 5500, "43239": 2200, "44120": 12000,
    }
    return amounts.get(proc_code, random.uniform(50, 500))


def _make_claim(claim_id, member_id, npi, referring, svc_date, proc, dx, allowed, state):
    paid = round(allowed * random.uniform(0.7, 1.0), 2)
    return {
        "claim_id": f"CLM{claim_id:07d}",
        "member_id": member_id,
        "provider_npi": npi,
        "referring_npi": referring or "",
        "service_date": svc_date.isoformat(),
        "paid_date": (svc_date + timedelta(days=random.randint(14, 60))).isoformat(),
        "procedure_code": proc,
        "diagnosis_code": dx[0],
        "diagnosis_desc": dx[1],
        "place_of_service": random.choice(["11", "21", "22", "23"]),
        "allowed_amount": allowed,
        "paid_amount": paid,
        "member_liability": round(allowed - paid, 2),
        "claim_type": random.choice(["professional", "professional", "institutional"]),
        "service_state": state,
        "modifier_1": random.choice(["", "", "25", "59", "76"]),
        "modifier_2": "",
        "units": random.choices([1, 1, 1, 2, 3], weights=[70, 70, 70, 15, 5])[0],
        "claim_status": random.choice(["paid", "paid", "paid", "paid", "denied", "adjusted"]),
    }


# ============================================================
# GENERATE SPECIALTY PROCEDURE MAP
# ============================================================
def generate_specialty_map():
    rows = []
    for specialty, codes in PROCEDURE_CODES.items():
        for code in codes:
            # E&M codes are valid for all specialties
            if code in EM_CODES:
                continue
            rows.append({
                "procedure_code": code,
                "expected_specialty": specialty,
                "category": specialty,
            })
    return rows


# ============================================================
# WRITE CSV
# ============================================================
def write_csv(filename: str, rows: list[dict]):
    if not rows:
        return
    path = OUT_DIR / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {filename}: {len(rows):,} rows")


# ============================================================
# MAIN
# ============================================================
def main():
    print("Generating synthetic healthcare data...\n")

    providers = generate_providers(200)
    write_csv("providers.csv", providers)

    members = generate_members(5000)
    write_csv("members.csv", members)

    eligibility = generate_eligibility(members)
    write_csv("eligibility.csv", eligibility)

    claims = generate_claims(providers, members)
    write_csv("claims.csv", claims)

    spec_map = generate_specialty_map()
    write_csv("specialty_procedure_map.csv", spec_map)

    # Print anomaly providers for reference
    print(f"\n--- Anomaly Providers (use these NPIs for testing) ---")
    for npi, atype in ANOMALY_PROVIDERS.items():
        p = next(p for p in providers if p["npi"] == npi)
        print(f"  NPI {npi} ({p['first_name']} {p['last_name']}, {p['specialty']}) -> {atype}")

    print(f"\nTotal files written to: {OUT_DIR.resolve()}")
    print("Next steps:")
    print("  1. Upload CSVs to a Snowflake stage")
    print("  2. Run data/01_schema.sql to create tables")
    print("  3. Run data/03_load.sql to load data")


if __name__ == "__main__":
    main()
