import pandas as pd
import requests
import json
import config

# === Step 1: Load Excel ===
input_file = config.PART1_OUTPUT_XLSX
output_file = config.PART2_OUTPUT_XLSX

df = pd.read_excel(input_file)

# === Step 2: Fetch organisation units from DHIS2 API or fallback ===
url = config.ORG_UNITS_L2_URL

try:
    auth = config.dhis2_auth()
    if auth is None:
        raise ValueError(
            "Missing DHIS2 credentials. Set DHIS2_USERNAME and DHIS2_PASSWORD to fetch from the API."
        )
    response = requests.get(url, auth=auth, timeout=config.DHIS2_TIMEOUT_SECONDS)
    response.raise_for_status()
    if response.text.strip() == "":
        raise ValueError("Empty API response")
    org_data = response.json()
    print("✅ Organisation units fetched successfully from API.")
except Exception as e:
    print(f"⚠️ Could not fetch organisation units from API ({e}). Trying local 'orgUnits.json'...")
    with open(config.ORG_UNITS_FALLBACK_JSON) as f:
        org_data = json.load(f)
    print("✅ Loaded organisation units from local file.")

# === Step 3: Build a lookup dictionary ===
org_units = org_data.get('organisationUnits', [])
org_lookup = {ou['displayName'].strip().lower(): ou['id'] for ou in org_units if 'id' in ou and 'displayName' in ou}

print(f"📘 Loaded {len(org_lookup)} organisation units from API/local file.")

# === Step 4: Map Country to OrgUnit ID ===
def get_orgunit_id(country_name):
    if pd.isna(country_name):
        return None
    return org_lookup.get(str(country_name).strip().lower(), None)

df['orgUnitID'] = df['Country'].apply(get_orgunit_id)

# === Step 5: Optional — replace or retain country column ===
# Uncomment next line if you want to overwrite 'Country' with the ID instead of adding a new column:
# df['Country'] = df['orgUnitID']

# === Step 6: Save output ===
df.to_excel(output_file, index=False)
print(f"✅ Updated file saved as '{output_file}'")

# === Step 7: Quick QA summary ===
matched = df['orgUnitID'].notna().sum()
total = len(df)
print(f"📊 Matched {matched}/{total} countries to orgUnit IDs ({(matched/total)*100:.1f}%)")
