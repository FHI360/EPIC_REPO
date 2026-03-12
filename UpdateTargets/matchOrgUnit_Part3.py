import pandas as pd
import requests
import json
import config

# === Step 1: Load processed Excel ===
input_file = config.PART2_OUTPUT_XLSX
output_file = config.PART3_OUTPUT_CSV

df = pd.read_excel(input_file)

# === Step 2: Fetch organisation units (level 2 = countries) ===
url = config.ORG_UNITS_L2_URL

try:
    auth = config.dhis2_auth()
    if auth is None:
        raise ValueError(
            "Missing DHIS2 credentials. Set DHIS2_USERNAME and DHIS2_PASSWORD to fetch from the API."
        )
    response = requests.get(url, auth=auth, timeout=config.DHIS2_TIMEOUT_SECONDS)
    response.raise_for_status()
    org_data = response.json()
    print("✅ Organisation units fetched successfully.")
except Exception as e:
    print(f"⚠️ Could not fetch organisation units ({e}), using local fallback.")
    with open(config.ORG_UNITS_FALLBACK_JSON) as f:
        org_data = json.load(f)
    print("✅ Loaded organisation units from local file.")

# === Step 3: Create lookup dictionary ===
org_lookup = {ou['displayName'].strip().lower(): ou['id'] for ou in org_data.get('organisationUnits', [])}

def get_orgunit_id(country_name):
    """Match country to orgUnit ID (case-insensitive, with DRC fallback)."""
    if pd.isna(country_name):
        return None
    name = str(country_name).strip().lower()
    if name in org_lookup:
        return org_lookup[name]
    # Special case: Democratic Republic of the Congo → DRC
    if "democratic republic of the congo" in name and "drc" in org_lookup:
        return org_lookup["drc"]
    return None

df['orgunit'] = df['Country'].apply(get_orgunit_id)

# === Step 4: Exclude unwanted countries ===
# exclude = ['tajikistan', 'kyrgyzstan', 'kazakhstan']
# df = df[~df['Country'].str.lower().isin(exclude)]

# === Step 5: Assign DHIS2 import columns ===
df['dataelement'] = df['dataElement']
df['period'] = config.DHIS2_PERIOD
df['categoryoptioncombo'] = config.CATEGORY_OPTION_COMBO_GP
df['attributeoptioncombo'] = df['Indicator'].apply(
    lambda x: config.ATTRIBUTE_OPTION_COMBO_DSD
    if "DSD" in str(x)
    else (config.ATTRIBUTE_OPTION_COMBO_TA if "TA" in str(x) else "")
)
df['value'] = df['Value']

# === Step 6: Keep only DHIS2-required columns ===
df_import = df[['dataelement', 'period', 'orgunit', 'categoryoptioncombo', 'attributeoptioncombo', 'value']]

# === Step 7: Remove incomplete rows ===
df_import = df_import.dropna(subset=['dataelement', 'orgunit', 'value'])

# === Step 8: Export as CSV ===
df_import.to_csv(output_file, index=False)
print(f"✅ DHIS2 import CSV created: '{output_file}'")

# === Step 9: Summary ===
print(f"📊 {len(df_import)} rows ready for DHIS2 import.")
unmatched = df[df['orgunit'].isna()]['Country'].unique()
if len(unmatched) > 0:
    print("⚠️ Unmatched countries:", unmatched)
else:
    print("✅ All countries matched successfully.")
