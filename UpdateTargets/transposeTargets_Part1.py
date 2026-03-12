import pandas as pd
import requests
import json
import re
import config

# === Step 1: Load Excel ===
df = pd.read_excel(config.INDICATOR_XLSX)

# === Step 2: Melt (transpose) ===
df_melted = df.melt(
    id_vars=['Country'],
    var_name='Indicator',
    value_name='Value'
).dropna(subset=['Value'])

# === Step 3: Fetch API or fallback ===
url = config.DATASET_ELEMENTS_URL

try:
    response = requests.get(url, timeout=config.DHIS2_TIMEOUT_SECONDS)
    response.raise_for_status()
    if response.text.strip() == "":
        raise ValueError("Empty API response")
    data = response.json()
    print("✅ Data successfully fetched from API.")
except Exception as e:
    print(f"⚠️ Could not fetch API data ({e}). Trying local 'data.json'...")
    with open(config.DATASET_FALLBACK_JSON) as f:
        data = json.load(f)
    print("✅ Loaded data from local file.")

# === Step 4: Extract data elements ===
data_elements = []
for element in data.get('dataSetElements', []):
    de = element.get('dataElement', {})
    if de.get('name') and de.get('id'):
        name = de['name']
        # Extract full indicator code before first space or parenthesis, including underscores
        root_match = re.match(r'^([A-Za-z0-9_]+)', name)
        root = root_match.group(1).upper() if root_match else name.upper()
        # Extract (N) or (D)
        nd_match = re.search(r'\(([ND])\)', name, flags=re.IGNORECASE)
        nd = nd_match.group(1).upper() if nd_match else None
        data_elements.append({
            'id': de['id'],
            'name': name,
            'root': root,
            'nd': nd
        })

# === Step 5: Define improved matching function ===
def match_data_element(indicator):
    # Extract full root and N/D from Excel indicator
    root_match = re.match(r'^([A-Za-z0-9_]+)', indicator)
    root = root_match.group(1).upper() if root_match else indicator.upper()
    nd_match = re.search(r'\(([ND])', indicator, flags=re.IGNORECASE)
    nd = nd_match.group(1).upper() if nd_match else None

    # Priority 1: exact root + N/D match
    for el in data_elements:
        if el['root'] == root and el['nd'] == nd:
            return el

    # Priority 2: exact root match only
    for el in data_elements:
        if el['root'] == root:
            return el

    # No match found
    return {'id': None, 'name': None, 'root': root, 'nd': nd}

# === Step 6: Apply matching ===
matches = df_melted['Indicator'].apply(match_data_element)
df_melted['dataElement'] = matches.apply(lambda x: x['id'])
df_melted['dataElementName'] = matches.apply(lambda x: x['name'])

# === Step 7: Save output ===
df_melted.to_excel(config.PART1_OUTPUT_XLSX, index=False)
print(f"✅ Output saved to '{config.PART1_OUTPUT_XLSX}'")
