import os
import re
import json
import sqlite3
import pandas as pd
import mysql.connector

from pathlib import Path
from collections import defaultdict
from zoneinfo import ZoneInfo
from datetime import datetime
from dotenv import load_dotenv

houston_time = datetime.now(ZoneInfo("America/Chicago"))
formatted_date = houston_time.date().strftime('%Y-%m-%d')

# =========================================================
# EDIT ONLY THIS PART WHEN NEEDED
# =========================================================
INPUT_FILENAME = "Validated Lessors_WY & PA.xlsx"
INPUT_SHEET_NAME = "PA List_Cleaned"

PIPEDRIVE_FILENAME = "deals-20898372-57250.csv"

CONSIDER_DEAL_TITLE = False
DEBUG = False
# =========================================================

INPUT_FOLDER = Path("input_others")
PIPEDRIVE_FOLDER = Path("pipedrive")
C3_FOLDER = Path("local_c3_cache/consolidated_address")
PROD_DB_FOLDER = Path("prod_database")
OUTPUT_FOLDER = Path("output")
OUTPUT_FOLDER.mkdir(exist_ok=True)

ADDRESS_SET_CONFIG = {
    "address_set_1": ("address", "city", "state"),
    "address_set_2": ("address2", "city2", "state2"),
    "address_set_3": ("add_address1", "add_address1_city", "add_address1_state"),
}

MYSQL_RCT_REFRESH_QUERY = """
SELECT *
FROM remove_contacts
WHERE contact_type IN ('contact_addresses', 'contact_skip_traced_addresses')
"""
# -----------------------------
# Basic progress helper
# -----------------------------
class ProgressPrinter:
    def __init__(self, total_steps: int):
        self.total_steps = total_steps
        self.current_step = 0

    def step(self, message: str):
        self.current_step += 1
        print(f"[{self.current_step}/{self.total_steps}] {message}")


def debug(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}")


# -----------------------------
# File helpers
# -----------------------------
def sanitize_filename_part(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r'[\\/*?:"<>|]+', "_", text)
    text = re.sub(r"\s+", "_", text)
    return text


def find_single_db_file(folder: Path) -> Path:
    db_files = list(folder.glob("*.db"))
    if len(db_files) == 0:
        raise FileNotFoundError(f"No .db file found in {folder}")
    if len(db_files) > 1:
        raise ValueError(f"Expected only 1 .db file in {folder}, found {len(db_files)}")
    return db_files[0]


def load_excel_sheet(file_path: Path, sheet_name: str) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str).fillna("")
    return df


def load_pipedrive_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    return df.fillna("")


def load_any_table(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path, dtype=str, keep_default_na=False).fillna("")
    elif suffix in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, dtype=str).fillna("")
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


# -----------------------------
# Normalization helpers
# -----------------------------
ZIP_AT_END_RE = re.compile(r",?\s*\b\d{5}(?:-\d{4})?\b\s*$", re.IGNORECASE)
USA_AT_END_RE = re.compile(r",?\s*USA\s*$", re.IGNORECASE)


def clean_trailing_usa_zip(text: str) -> str:
    """
    Removes trailing ', USA', trailing ZIP, or both repeatedly at the end.
    Examples:
      '123 Main St, Houston, TX, USA, 77001' -> '123 Main St, Houston, TX'
      '123 Main St, Houston, TX 77001' -> '123 Main St, Houston, TX'
      '123 Main St, Houston, TX, USA' -> '123 Main St, Houston, TX'
    """
    if text is None:
        return ""

    s = str(text).strip()
    if not s:
        return ""

    changed = True
    while changed:
        original = s
        s = ZIP_AT_END_RE.sub("", s).strip(" ,")
        s = USA_AT_END_RE.sub("", s).strip(" ,")
        changed = (s != original)

    return s.strip(" ,")


def normalize_for_match(text: str) -> str:
    """
    Case-insensitive, removes spaces and symbols.
    Keeps only alphanumeric chars.
    """
    if text is None:
        return ""
    s = str(text).strip()
    if not s:
        return ""
    s = clean_trailing_usa_zip(s)
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s


def clean_cell(value) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if s.upper() in {"", "NAN", "<NA>", "NONE", "NULL"}:
        return ""
    return s


def build_full_address(address, city, state) -> str:
    address = clean_cell(address)
    city = clean_cell(city)
    state = clean_cell(state)

    parts = []
    for value in [address, city, state]:
        if value:
            parts.append(value)

    return ", ".join(parts)


# -----------------------------
# County / deal title helpers
# -----------------------------
def split_combined_county(combined_county: str) -> list[str]:
    """
    combined_county is space-separated, except SAN AUGUSTINE should stay together.
    Example:
      HOUSTON CADDO SAN AUGUSTINE -> ['HOUSTON', 'CADDO', 'SAN AUGUSTINE']
    """
    if combined_county is None:
        return []

    text = str(combined_county).strip().upper()
    if not text:
        return []

    text = re.sub(r"\s+", " ", text)
    text = text.replace("SAN AUGUSTINE", "SAN_AUGUSTINE")
    parts = text.split(" ")
    parts = [p.replace("SAN_AUGUSTINE", "SAN AUGUSTINE").strip() for p in parts if p.strip()]
    return parts


def generate_deal_titles_for_row(row: pd.Series) -> list[str]:
    """
    Formula variations:
      1. first_name + last_name + county
      2. owner_name + county

    Skip generation if required pieces are blank to avoid inaccuracy.
    """
    titles = set()

    first_name = str(row.get("first_name", "")).strip()
    last_name = str(row.get("last_name", "")).strip()
    owner_name = str(row.get("owner_name", "")).strip()
    combined_county = str(row.get("combined_county", "")).strip()

    counties = split_combined_county(combined_county)
    if not counties:
        return []

    if first_name and last_name:
        full_name = f"{first_name} {last_name}".strip()
        for county in counties:
            titles.add(f"{full_name} {county}".strip())

    if owner_name:
        for county in counties:
            titles.add(f"{owner_name} {county}".strip())

    return sorted(titles)


# -----------------------------
# Mapping builders
# -----------------------------
def build_pipedrive_mailing_address_map(pipedrive_df: pd.DataFrame) -> dict[str, set[str]]:
    required = ["Deal - ID", "Person - Mailing Address"]
    missing = [c for c in required if c not in pipedrive_df.columns]
    if missing:
        raise KeyError(f"Pipedrive file missing required columns: {missing}")

    address_to_ids = defaultdict(set)

    total = len(pipedrive_df)
    for i, row in pipedrive_df.iterrows():
        if i % 5000 == 0 and i > 0:
            debug(f"Pipedrive address scan: {i:,}/{total:,}")

        deal_id = str(row.get("Deal - ID", "")).strip()
        mailing_address = str(row.get("Person - Mailing Address", "")).strip()

        if not deal_id or not mailing_address:
            continue

        norm_addr = normalize_for_match(mailing_address)
        if norm_addr:
            address_to_ids[norm_addr].add(deal_id)

    debug(f"Built pipedrive address map with {len(address_to_ids):,} normalized addresses")
    return address_to_ids


def build_pipedrive_deal_title_list(pipedrive_df: pd.DataFrame) -> list[tuple[str, str]]:
    required = ["Deal - ID", "Deal - Title"]
    missing = [c for c in required if c not in pipedrive_df.columns]
    if missing:
        raise KeyError(f"Pipedrive file missing required columns: {missing}")

    title_records = []

    total = len(pipedrive_df)
    for i, row in pipedrive_df.iterrows():
        if i % 5000 == 0 and i > 0:
            debug(f"Pipedrive deal title scan: {i:,}/{total:,}")

        deal_id = str(row.get("Deal - ID", "")).strip()
        deal_title = str(row.get("Deal - Title", "")).strip()

        if not deal_id or not deal_title:
            continue

        norm_title = normalize_for_match(deal_title)
        if norm_title:
            title_records.append((norm_title, deal_id))

    debug(f"Built pipedrive deal title list with {len(title_records):,} normalized titles")
    return title_records


def build_c3_direct_mail_address_set(c3_folder: Path) -> set[str]:
    normalized_addresses = set()

    if not c3_folder.exists():
        debug(f"{c3_folder} does not exist; skipping C3 matching")
        return normalized_addresses

    files = list(c3_folder.glob("*.csv")) + list(c3_folder.glob("*.xlsx")) + list(c3_folder.glob("*.xls"))
    debug(f"Found {len(files)} C3 files")

    valid_mediums = {"DIRECT MAIL", "MAILING ADDRESS"}

    for idx, file_path in enumerate(files, start=1):
        debug(f"Reading C3 file {idx}/{len(files)}: {file_path.name}")
        try:
            df = load_any_table(file_path)
        except Exception as e:
            print(f"[WARNING] Failed to read {file_path.name}: {e}")
            continue

        required = ["OPT-OUT_MEDIUM", "OPT-OUT_CONTACT"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            debug(f"Skipping {file_path.name} because required columns are missing: {missing}")
            continue

        sub = df[
            df["OPT-OUT_MEDIUM"].astype(str).str.strip().str.upper().isin(valid_mediums)
        ].copy()

        for _, row in sub.iterrows():
            contact = str(row.get("OPT-OUT_CONTACT", "")).strip()
            if not contact:
                continue

            norm_addr = normalize_for_match(contact)
            if norm_addr:
                normalized_addresses.add(norm_addr)

    debug(f"Built C3 address set with {len(normalized_addresses):,} normalized addresses")
    return normalized_addresses

def refresh_rct_addresses_table_from_mysql(prod_db_path: Path):
    """
    Refreshes the local SQLite table `rct_addresses` by pulling fresh data
    from MySQL table `remove_contacts`.

    The refreshed SQLite table will still be named `rct_addresses`.
    """
    load_dotenv(dotenv_path=Path(".env"))

    DB_HOST = os.getenv("MYSQL_HOST")
    DB_USER = os.getenv("MYSQL_USER")
    DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
    DB_NAME = os.getenv("MYSQL_DATABASE")
    DB_PORT = int(os.getenv("MYSQL_PORT") or 3306)

    missing_env = []
    for key, value in {
        "MYSQL_HOST": DB_HOST,
        "MYSQL_USER": DB_USER,
        "MYSQL_PASSWORD": DB_PASSWORD,
        "MYSQL_DATABASE": DB_NAME,
    }.items():
        if not value:
            missing_env.append(key)

    if missing_env:
        raise EnvironmentError(
            f"Missing required .env values: {', '.join(missing_env)}"
        )

    debug("Connecting to MySQL for fresh rct_addresses pull")

    mysql_conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
    )

    try:
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute(MYSQL_RCT_REFRESH_QUERY)
        rows = cursor.fetchall()
        fresh_df = pd.DataFrame(rows)
    finally:
        cursor.close()
        mysql_conn.close()

    debug(f"Pulled {len(fresh_df):,} rows from MySQL remove_contacts")

    sqlite_conn = sqlite3.connect(prod_db_path)
    try:
        # Replace local table with fresh copy
        fresh_df.to_sql("rct_addresses", sqlite_conn, if_exists="replace", index=False)
        sqlite_conn.commit()
    finally:
        sqlite_conn.close()

    debug(f"Refreshed SQLite table 'rct_addresses' in {prod_db_path}")

def build_prod_rct_address_map(prod_db_path: Path) -> dict[str, set[str]]:
    """
    Reads rct_addresses.values JSON from prod DB and builds:
        normalized_address -> set(contact_type)

    Supported contact_type:
      - contact_skip_traced_addresses
      - contact_addresses
    """
    address_to_contact_types = defaultdict(set)

    sql = """
    SELECT contact_type, "values"
    FROM rct_addresses
    WHERE contact_type IN ('contact_skip_traced_addresses', 'contact_addresses')
    """

    conn = sqlite3.connect(prod_db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    debug(f"Fetched {len(rows):,} rows from prod rct_addresses")

    for i, (contact_type, value_text) in enumerate(rows, start=1):
        if i % 5000 == 0:
            debug(f"Prod RCT scan: {i:,}/{len(rows):,}")

        if not value_text:
            continue

        try:
            payload = json.loads(value_text)
        except Exception:
            continue

        address = city = state = ""

        if contact_type == "contact_skip_traced_addresses":
            address = str(payload.get("address") or "").strip()
            city = str(payload.get("city") or "").strip()
            state = str(payload.get("state") or "").strip()

        elif contact_type == "contact_addresses":
            address = str(payload.get("source_address") or "").strip()
            city = str(payload.get("source_city") or "").strip()
            state = str(payload.get("source_state") or "").strip()

        if not address or not city or not state:
            continue

        full_address = build_full_address(address, city, state)
        norm_addr = normalize_for_match(full_address)

        if norm_addr:
            address_to_contact_types[norm_addr].add(contact_type)

    debug(f"Built prod RCT address map with {len(address_to_contact_types):,} normalized addresses")
    return address_to_contact_types


# -----------------------------
# Main processing
# -----------------------------
def process():
    total_steps = 9 if CONSIDER_DEAL_TITLE else 8
    progress = ProgressPrinter(total_steps=total_steps)

    progress.step("Loading input Excel sheet")
    input_path = INPUT_FOLDER / INPUT_FILENAME
    main_df = load_excel_sheet(input_path, INPUT_SHEET_NAME)
    debug(f"Input row count: {len(main_df):,}")

    progress.step("Loading Pipedrive CSV")
    pipedrive_path = PIPEDRIVE_FOLDER / PIPEDRIVE_FILENAME
    if not pipedrive_path.exists():
        raise FileNotFoundError(f"Pipedrive file not found: {pipedrive_path}")
    pipedrive_df = load_pipedrive_csv(pipedrive_path)
    debug(f"Pipedrive row count: {len(pipedrive_df):,}")

    progress.step("Building Pipedrive lookup maps")
    pipedrive_address_map = build_pipedrive_mailing_address_map(pipedrive_df)
    pipedrive_title_list = build_pipedrive_deal_title_list(pipedrive_df) if CONSIDER_DEAL_TITLE else []

    progress.step("Building C3 address lookup set")
    c3_address_set = build_c3_direct_mail_address_set(C3_FOLDER)

    progress.step("Refreshing local rct_addresses table from MySQL")
    prod_db_path = find_single_db_file(PROD_DB_FOLDER)
    debug(f"Using prod DB file: {prod_db_path}")
    refresh_rct_addresses_table_from_mysql(prod_db_path)

    progress.step("Building prod RCT address lookup map")
    prod_rct_address_map = build_prod_rct_address_map(prod_db_path)

    progress.step("Preparing address and deal-title columns")
    for col in ["address_set_1", "address_set_2", "address_set_3", "deal_title_match", "summary_findings"]:
        if col not in main_df.columns:
            main_df[col] = ""

    if "count_of_distinct_address" not in main_df.columns:
        main_df["count_of_distinct_address"] = pd.Series([pd.NA] * len(main_df), dtype="Int64")

    progress.step("Matching addresses and deal titles" if CONSIDER_DEAL_TITLE else "Matching addresses")

    total_rows = len(main_df)

    for i, (idx, row) in enumerate(main_df.iterrows(), start=1):
        if i % 1000 == 0 or i == total_rows:
            print(f"Processing row {i:,}/{total_rows:,}")

        # --- Address set matching ---
        normalized_sets_seen = set()
        distinct_address_count = 0

        for output_col, source_cols in ADDRESS_SET_CONFIG.items():
            addr_col, city_col, state_col = source_cols
            full_address = build_full_address(
                row.get(addr_col, ""),
                row.get(city_col, ""),
                row.get(state_col, "")
            )

            norm_address = normalize_for_match(full_address)
            remarks = []

            if norm_address:
                if norm_address not in normalized_sets_seen:
                    normalized_sets_seen.add(norm_address)
                    distinct_address_count += 1

            if norm_address:
                if norm_address in pipedrive_address_map:
                    deal_ids = sorted(pipedrive_address_map[norm_address], key=lambda x: (len(str(x)), str(x)))
                    remarks.append(f"Found in Deal - ID {'|'.join(map(str, deal_ids))}")

                if norm_address in c3_address_set:
                    remarks.append("Found in C3")

                if norm_address in prod_rct_address_map:
                    contact_types = sorted(prod_rct_address_map[norm_address])
                    remarks.append(f"Found in prod RCT - {'|'.join(contact_types)}")

            main_df.at[idx, output_col] = "; ".join(remarks) if remarks else ""

        main_df.at[idx, "count_of_distinct_address"] = distinct_address_count

        # --- Deal title matching ---
        if CONSIDER_DEAL_TITLE:
            deal_titles = generate_deal_titles_for_row(row)
            matched_deal_ids = set()

            for title in deal_titles:
                norm_generated_title = normalize_for_match(title)
                if not norm_generated_title:
                    continue

                for norm_pipedrive_title, deal_id in pipedrive_title_list:
                    if norm_generated_title in norm_pipedrive_title:
                        matched_deal_ids.add(deal_id)

            if matched_deal_ids:
                matched_ids_sorted = sorted(matched_deal_ids, key=lambda x: (len(str(x)), str(x)))
                main_df.at[idx, "deal_title_match"] = f"Found Deal Title in Deal - ID {'|'.join(map(str, matched_ids_sorted))}"
            else:
                main_df.at[idx, "deal_title_match"] = ""
        else:
            main_df.at[idx, "deal_title_match"] = ""

        # --- Summary findings ---
        address_found = any(
            bool(str(main_df.at[idx, col]).strip())
            for col in ["address_set_1", "address_set_2", "address_set_3"]
        )
        deal_title_found = bool(str(main_df.at[idx, "deal_title_match"]).strip()) if CONSIDER_DEAL_TITLE else False

        if CONSIDER_DEAL_TITLE:
            if not address_found and not deal_title_found:
                main_df.at[idx, "summary_findings"] = "No match found in address and deal title"
            else:
                main_df.at[idx, "summary_findings"] = ""
        else:
            if not address_found:
                main_df.at[idx, "summary_findings"] = "No match found in address"
            else:
                main_df.at[idx, "summary_findings"] = ""

    progress.step("Writing CSV output")
    safe_sheet_name = sanitize_filename_part(INPUT_SHEET_NAME)
    output_filename = f"{formatted_date}_{safe_sheet_name}_address_cleaned.csv"
    output_path = OUTPUT_FOLDER / output_filename
    main_df.to_csv(output_path, index=False)

    print(f"\nFinished.")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    process()