# Furnishka Meta Lead Fetcher

This repository contains a complete solution for the Furnishka Meta Lead Fetcher assignment.  
The goal is to build a command‑line tool that retrieves leads from a Meta (Facebook) Lead Ads form, normalizes the data, deduplicates using a local SQLite database, and writes new leads to disk. 
The project supports both live API retrieval and an offline mode for local testing.

## Features

* **Fetch leads via the Graph API** – Queries the `/leads` edge of a lead form, requesting `id`, `created_time` and `field_data`【568102441973552†L491-L534】.  Pagination support follows the `paging.next` URL until all pages have been read.
* **Normalization** – Flattens the nested `field_data` structure into a simple schema with `lead_id`, `name`, `email`, `phone` and `created_time`.  Uses the `full_name` field if present, or falls back to `first_name` and `last_name`.
* **Idempotent behavior** – A local SQLite database (`data/seen_leads.db`) tracks processed lead IDs so that re‑running the tool only returns newly created leads.
* **Retry logic** – Implements exponential backoff on transient server (5xx) errors.
* **Offline mode** – Includes a sample JSON file (`data/meta_leads_sample.json`) to allow you to test the tool without valid Meta credentials.
* **View database in terminal**  
Run with `--view-db` to inspect all saved leads directly in your terminal.


## Project Structure

```
tech_assignment/
├── .env.sample         # Template for your Meta credentials
├── .gitignore
├── README.md           # You are here
├── requirements.txt    # Python dependencies
├── data/
│   └── meta_leads_sample.json  # Sample leads for offline mode
└── src/
    ├── fetcher.py      # Main CLI script
    └── utils/
        └── db.py       # SQLite helper for deduplication
```

## Getting Started

### 1. Prerequisites

* Python 3.8 or higher
* Git (optional, if cloning)
* A Meta (Facebook) developer account with access to Lead Ads (if using live API)

### 2. Set up the repository

Clone this repository or download it as a ZIP and extract it.  Then create a virtual environment (optional but recommended) and install dependencies:

```bash
git clone https://github.com/addyslice-git/tech_assignment.git
cd tech_assignment
python -m venv .venv
.venv\Scripts\activate      # Windows
```

### 3. Configure credentials

Copy the sample environment file and edit it with your own values:

```bash
cp .env.sample .env
```

Open `.env` in your preferred editor and populate:

```
META_APP_ID=your_app_id
META_APP_SECRET=your_app_secret
META_ACCESS_TOKEN=your_long_lived_access_token
LEAD_FORM_ID=your_lead_form_id

# Optional overrides
API_VERSION=24.0
DB_PATH=data/seen_leads.db
OFFLINE_MODE=0
```

* To generate a **System User access token**, follow Meta’s instructions.  You need a token with the `leads_retrieval` and `pages_read_engagement` permissions and the ID of your Lead Ads.
* If you do not have valid credentials yet, leave `OFFLINE_MODE=1` to test using the provided sample data.

### 4. Running the tool

Activate your virtual environment (if using one) and run the fetcher:

```bash
python src/fetcher.py
```

The script will load settings from `.env` by default.  It fetches leads, normalizes them, deduplicates using the SQLite database, and writes new leads to `new_leads.json` (default).  Existing leads are skipped.

#### Command‑line options

* `--since YYYY-MM-DDTHH:MM:SS+0000` – Only fetch leads created after the given ISO timestamp.  For example: `--since 2025-07-01T00:00:00+0000`.
* `--output json|csv` – Choose the output format.  `json` writes to `new_leads.json`, `csv` writes to `new_leads.csv`.
* `--offline` – Force offline mode using the sample JSON file.
* `--limit N` – Number of leads per page (default 25).
* `--log-level LEVEL` – Logging verbosity (e.g. DEBUG, INFO, WARNING, ERROR).
* `--max-retries N` – Number of retries on server errors (default 3).
* `--view-db ` -  Print the contents of the configured SQLite DB to the terminal and exit
* `--help`, `-h` -  shows the list of all the available arguments

For example, to test offline mode with detailed logs:

```bash
python src/fetcher.py --offline --log-level DEBUG
```

### 5. Testing idempotency

1. Run the fetcher: it should produce `new_leads.json` or `new_leads.csv` with all new leads.
2. Run the fetcher again without changing your lead form or the sample file.  No new leads should be written, and the log will report that there are no new leads.
3. If you add a new lead in your Meta form (or add a new record to `meta_leads_sample.json`), the next run will pick up only the new lead.

### Database Schema:
CREATE TABLE seen_leads (
  lead_id TEXT PRIMARY KEY,
  name TEXT,
  email TEXT,
  phone TEXT,
  created_time TEXT
);
*Skip the leads with missing contact information*


### 6. Packaging for submission

While submitting the assignment :

1. **sensitive files have been removed** – as instructed *not* included `.env` or `data/seen_leads.db` in your submission.
2. **Logs have been attached** – Include logs or screenshots demonstrating that idempotency works and that leads are fetched successfully.
3. **Uploaded on github** - The project has been uploaded on github with the requirements 


### 7. Additional Solutions:

1. Works in both live and offline modes
2. Stores and displays full lead details in SQLite
3. Idempotent — no duplicate inserts
4. --view-db shows data clearly
5. .env and DB files excluded from GitHub
6. Clear setup and usage documentation provided
7. Invalid ISO timestamps handled
8. No Tables found cases in view-db are handled by running fetcher atleast once to populate it


### 8. Instructions for creating the Meta Leads fetcher
There are three aspects to having a live form ecosystem.
 1. Having a Meta App
 2. Having a Page 
 3. Having a Lead Form

 *Note: All of these entities must be present under a single business account with developer privileges*
#### Steps for creating the application
 **1. Create an app in the Developer Console note the app id and the app key**
 **2. Create a meta business account and add or create a sample page**
 **3. Create a Leads form with the entries having name, phone, email**
 **4. through all tools go to ads manager and the all tools in ad manager select instant form**
 **5. Note the form id after creating form**
 **6. In business manager, create a system user and make sure they have all the access especially to the following**
    `leads_retrieval`
    `ages_read_engagement`
    `pages_manage_metadata`
    `pages_show_list`
    `business_management`
 **7. get the user token from the screen before closing the dialouge**
 **8. Replace the user access token, app id, app secret key, form id in the .env file**
 
 
 ### 9. Adding testing leads to the values and then running the fetcher to verify:
 Good Data 

   Replace your<PAGE_ACCESS_TOKEN>(note this is different from the meta access token generated as it is page specific, it can be obtained from Graph API tool) and <LEAD_FORM_ID> 

#### Good Data (Both Phone number and the Email)
curl -X POST "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads" \
  -d "full_name=Alice Full" \
  -d "email=alice@example.com" \
  -d "phone_number=+15551234501" \
  -d "access_token=<PAGE_ACCESS_TOKEN>"

#### Data with either phone number or email
curl -X POST "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads" \
  -d "full_name=Bob PhoneOnly" \
  -d "phone_number=+15551234502" \
  -d "access_token=<PAGE_ACCESS_TOKEN>"
curl -X POST "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads" \
  -d "full_name=Carol EmailOnly" \
  -d "email=carol@example.com" \
  -d "access_token=<PAGE_ACCESS_TOKEN>"

Creating duplicate Data:
#### create first lead with same email
curl -X POST "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads" \
  -d "full_name=Eve Duplicate" \
  -d "email=dup@example.com" \
  -d "phone_number=+15551234503" \
  -d "access_token=<PAGE_ACCESS_TOKEN>"

#### create another lead with same email (different lead_id)
curl -X POST "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads" \
  -d "full_name=Eve Duplicate 2" \
  -d "email=dup@example.com" \
  -d "phone_number=+15551234503" \
  -d "access_token=<PAGE_ACCESS_TOKEN>"

#### create a batch of 20 tokens
for i in {1..20}; do
  curl -s -X POST "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads" \
    -d "full_name=BulkUser$i" \
    -d "email=bulk$i@example.com" \
    -d "phone_number=+15551234$(printf "%03d" $i)" \
    -d "access_token=<PAGE_ACCESS_TOKEN>" > /dev/null
done
echo "Created 20 test leads"

#### Cleanup
curl -X DELETE "https://graph.facebook.com/v24.0/<LEAD_FORM_ID>/test_leads?access_token=<PAGE_ACCESS_TOKEN>"

*Note: Depending on the account type, Meta may not allow creating multiple test lead, just like in our case. Modified the offline data and tested on it.*

#### To check the log and outputs: 
<a href="https://docs.google.com/document/d/1MzG3ob9qm1plzdg1p75MtFeldOyX8wO4ygykFJsF8qM/edit?usp=sharing" target="_blank">Click Here</a>



### 10. Author
`Aman Sharma` 
*Submitted as part of Furnishka Tech Assignment*
*Email: amansharma28012002@gmail.com*
