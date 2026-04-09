# Mail Marketing Cleanup

This project cleans and enriches outbound mail-marketing lists by cross-checking contact records against suppression and CRM sources before campaign execution. It helps teams identify addresses or deal-title records that may already exist in internal systems (Pipedrive, C3 or .work), then outputs a cleaned file with match remarks for operational review.

---

![Version](https://img.shields.io/badge/version-1.0.0-ffab4c?style=for-the-badge&logo=python&logoColor=white)
![Python](https://img.shields.io/badge/python-3.11%2B-273946?style=for-the-badge&logo=python&logoColor=ffab4c)
![Status](https://img.shields.io/badge/status-active-273946?style=for-the-badge&logo=github&logoColor=ffab4c)

---

## 🚧 Problem Statement / Motivation
Mail marketing campaigns become expensive and inefficient when records are duplicated, stale, or already suppressed in upstream systems. In practical operations, teams often receive lead lists from multiple sources (database exports, spreadsheets, and Pipedrive downloads) where address formatting is inconsistent and suppression criteria are spread across different tools.

Without a cleanup step, marketing operations risk:
- Mailing to contacts that should be excluded due to prior opt-out or suppression records.
- Sending duplicate mails to the same person/address variant (format differences only).
- Spending marketing campaign budget on records that have already been actioned in the CRM pipeline.
- Introducing compliance and reputation risks when excluded contacts are not detected early.

This repository addresses that gap by normalizing address strings and validating each record against:
- **Pipedrive deal data** (mailing address and deal title signals),
- **C3 consolidated direct-mail suppression files**, and
- **Production RCT suppression address datasets**.

The end result is a campaign-ready export with clear, row-level remarks describing what matched and where it matched.

--- 

## ✨ Features
- Loads and filters NCOP or pooling order data.
- Normalizes addresses (case, spacing, punctuation, trailing ZIP codes/"USA" cleanup).
- Compares up to three address sets per contact for NCOP and one address set for Pooling Order.
- Matches against:
  - Pipedrive mailing addresses,
  - C3 direct-mail suppression contacts,
  - Production RCT address payloads.
- Supports `Deal - Title` matching logic for additional Pipedrive checks.
- Generates remarks per address set and a summary finding field.
- Outputs dated cleaned CSV files to the `output/` folder.

---

## 🧠 Logic Flow
1. **Load inputs**
   - Source dataset (NCOP DB or Pooling Order excel input, depending on script).
   - Pipedrive export CSV.
   - C3 consolidated suppression files.
   - Production DB suppression sources.

2. **Normalize values for robust matching**
   - Remove trailing ZIP and `USA` text.
   - Convert to uppercase.
   - Strip non-alphanumeric characters.

3. **Build lookup structures**
   - Pipedrive normalized mailing address → Deal ID set.
   - C3 normalized contact addresses set (medium filtered).
   - Prod RCT normalized address → contact type set.

4. **Evaluate each row using address-set criteria**
   - **Criteria:** for each of `address_set_1`, `address_set_2`, and `address_set_3`, build a full address from `(address, city, state)` triplets and normalize.
   - **Expected remarks when a match is found:**
     - `Found in Deal - ID <id|id|...>` when matched in Pipedrive deal associated with mailing addresses.
     - `Found in C3` when matched in C3 suppression data.
     - `Found in prod RCT - <contact_type|contact_type|...>` when matched in production RCT.
   - Multiple remarks may appear in the same address set and are joined by `; `.

5. **Apply deal-title criteria**
   - Generate deal-title candidates from name/county combinations.
   - Match candidate titles against normalized Pipedrive deal titles.
   - **Expected remark on match:** `Found Deal Title in Deal - ID <id|id|...>`.

6. **Set summary result**
   - If no address-set match and no deal-title match: `No match found in address and deal title`.
   - Otherwise summary is left blank, since specific match remarks are already present.

7. **Write output**
   - Save cleaned/exported CSV file in `output/` with date-based naming.

---

## 📝 Requirements
- Python 3.11+ recommended.
- Dependencies listed in `requirements.txt`:
  - pandas
  - openpyxl
  - mysql-connector-python
  - python-dotenv
  - and related pinned libraries.
- Input data folders/files expected by scripts (examples):
  - `ncop_database/*.db`
  - `prod_database/*.db`
  - `pipedrive/*.csv`
  - `local_c3_cache/consolidated_address/*.(csv|xlsx|xls)`
  - `input_others/*.xlsx` (for `mail_marketing_others.py`)

---

## 🚀 Installation and Setup
1. **Clone repository**
   ```bash
   git clone <your-repo-url>
   cd mail_marketing_cleanup_ncop
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Place input files in expected folders**
   - Add the appropriate DB, CSV, and Excel files to the paths listed above.

5. **(Optional) Configure environment variables**
   - If using workflows that require DB refresh or external connections, add a `.env` file as needed by your environment.

---

## 🖥️ User Guide
### Run NCOP mail-marketing cleanup (used for BUDB)
1. Open `mail_marketing_ncop.py` and update the editable config block if needed:
   - `PIPEDRIVE_FILENAME`
   - `OUTPUT_FILENAME` (or keep date-based default)
   - `DEBUG`
2. Execute:
   ```bash
   python mail_marketing_ncop.py
   ```
3. Collect output CSV from `output/`.

### Run “others” mail-marketing cleanup (used for Pooling Order)
1. Open `mail_marketing_others.py` and update:
   - `INPUT_FILENAME`
   - `INPUT_SHEET_NAME`
   - `PIPEDRIVE_FILENAME`
   - `CONSIDER_DEAL_TITLE`
2. Execute:
   ```bash
   python mail_marketing_others.py
   ```
3. Review generated CSV in `output/`.

### Interpreting output columns
- `address_set_1`, `address_set_2`, `address_set_3`: match remarks per address source set.
- `deal_title_match`: deal-title hit summary (if available).
- `count_of_distinct_address`: count of unique nonblank normalized addresses across address sets.
- `summary_findings`: high-level no-match flag when no lookup criteria are met.
