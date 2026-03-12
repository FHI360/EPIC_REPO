import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
import base64
import json
import re
import os
from fuzzysearch import find_near_matches
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# DHIS2 credentials - loaded from .env file
dhis2_url = os.getenv("DHIS2_URL", "https://infolink.fhi360.org")
username = os.getenv("DHIS2_USERNAME", "")
password = os.getenv("DHIS2_PASSWORD", "")

# Data elements dictionary: {key: data_element_id_in_infolink}
dataElements = {
    # "TX_NEW_CD4<200_106a-HC09": "TLYIar0a2BT",
    # "TX_NEW CD4>200_106a-HC08": "TLYIar0a2BT",
    # "TX_NEW_CD4_UNKNOWN_106a-HC06": "TLYIar0a2BT",
    # "TX_CURR_106a-HC11a": "GMQQvx3P5vs",
    # "TX_CURR_106a-HC11b": "GMQQvx3P5vs",
    # "TX_CURR_106a-HC11c": "GMQQvx3P5vs",
    # "TX_CURR_MMD_<3_106a-HC12a": "sy1ruTub9H7",
    # "TX_CURR_MMD_3_5106a-HC12b": "sy1ruTub9H7",
    # "TX_CURR_MMD_6_106a-HC12c": "sy1ruTub9H7",
    # "TX_ML_DIED_106a-HC14d": "LfWnc5htYLx",
    # "TX_ML_Refused_Treatment_106a-HC14b": "vSFqzqEUl19",
    # "TX_ML_TRANSFER_106a-HC14a": "whWcuSRFCfu",
    # "TX_ML_CAUSE_DEATH_TB": "HACJolfaipe",
    # "TX_ML_CAUSE_DEATH_CANCER": "HACJolfaipe",
    # "TX_ML_CAUSE_DEATH_Other_HIV_diseases": "HACJolfaipe",
    # "TX_ML_CAUSE_DEATH_Other_infectious_and_parasitic_disease": "HACJolfaipe",
    # "TX_ML_CAUSE_DEATH_Natural_causes": "HACJolfaipe",
    # "TX_ML_CAUSE_DEATH_Non_Natural_causes": "HACJolfaipe",
    # "TX_ML_CAUSE_DEATH_Unknown": "HACJolfaipe",
    # "TX_PVLS_ELIGIBLE_106a-HC29a":"obTdrBAKbpv",
    # "TX_PVLS_D_106a-HC29b":"QYXCRY1DQYs",
    # "TX_PVLS_N_106a-HC29c":"Jk9XEleg8Al",
    # "PMTCT_FO_105-OE11a":"KH7N6GjsnK2",
    # "PMTCT_FO_105-OE15":"KH7N6GjsnK2",
    # "PMTCT_FO_105-OE16":"KH7N6GjsnK2",
    # "PMTCT_FO_105-OE12":"KH7N6GjsnK2",
    # "PMTCT_STAT_D_105-AN01a":"n2y1Yx6pfO3",
    "PMTCT_STAT_N_105-AN33b_2019":"X4XDlUVECyV",
    "PMTCT_STAT_N_105-AN18a":"X4XDlUVECyV",
    "PMTCT_STAT_N_105-AN17a":"X4XDlUVECyV",
    # "TX_TB_N_106a-HC19d":"ARQ2RardHvk",
    # "TB_STAT_N_106a-TB12a":"Gd11DbAitpI",
    # "TB_STAT_N_106a-TB11b":"Gd11DbAitpI",
    # "TB_STAT_N_106a-TB11a":"Gd11DbAitpI",
    # "TB_ART_106a-TB11d":"ikTmWSLUuW0",
    # "TB_ART_106a-TB12c":"ikTmWSLUuW0",
    # "HTS_SELF_105-HT05a_assisted":"AdRtbdiA8xa",
    # "HTS_SELF_105-HT05a_unassisted":"AdRtbdiA8xa",
    # "HTS_SELF_USED_105-HT05b_assisted":"TvKRwHK1jcd",
    # "HTS_SELF_REACTIVE_105-HT05c1_assisted":"n0Gl6seHJaK",
    # "HTS_SELF_CONFIRMED_105-HT05c2_assisted":"y7dCceXSV5p",
    # "HTS_SELF_LINKED_105-HT05c3_assisted":"ibbebAQBxWz",
    # Add more data elements here as needed
    # "OTHER_DATA_ELEMENT": "OTHER_ID"
} 

# Filter lists dictionary: {data_element_key: filter_dict}
# CoC filter from Infolink
# Each data element can have its own filter criteria
filter_lists = {
    # "TX_NEW_CD4<200_106a-HC09": {"population": "General Population", "CD4": "CD4: <200"},
    # "TX_NEW CD4>200_106a-HC08": {"population": "General Population", "CD4": "CD4: ≥200"},
    # "TX_NEW_CD4_UNKNOWN_106a-HC06": {"population": "General Population", "CD4": "CD4: Unknown"},
    # "TX_CURR_106a-HC11a": {"population": "General Population"},
    # "TX_CURR_106a-HC11b": {"population": "General Population"},
    # "TX_CURR_106a-HC11c": {"population": "General Population"},
    # "TX_CURR_MMD_<3_106a-HC12a": {"population": "General Population", "MMD": "< 3"},
    # "TX_CURR_MMD_3_5106a-HC12b": {"population": "General Population", "MMD": "3-5"},
    # "TX_CURR_MMD_6_106a-HC12c": {"population": "General Population", "MMD": "6 or more"},
    # "TX_ML_DIED_106a-HC14d": {"population": "General Population"},
    # "TX_ML_Refused_Treatment_106a-HC14b": {"population": "General Population"},
    # "TX_ML_TRANSFER_106a-HC14a": {"population": "General Population"},
    # "TX_ML_CAUSE_DEATH_TB": {"population": "General Population", "Cause of death": "TB"},
    # "TX_ML_CAUSE_DEATH_CANCER": {"population": "General Population", "Cause of death": "Cancer"},
    # "TX_ML_CAUSE_DEATH_Other_HIV_diseases": {"population": "General Population", "Cause of death": "Other HIV diseases"},
    # "TX_ML_CAUSE_DEATH_Other_infectious_and_parasitic_disease": {"population": "General Population", "Cause of death": "Other infectious and parasitic disease"},
    # "TX_ML_CAUSE_DEATH_Natural_causes": {"population": "General Population", "Cause of death": "Natural causes"},
    # "TX_ML_CAUSE_DEATH_Non_Natural_causes": {"population": "General Population", "Cause of death": "Non-natural causes"},
    # "TX_ML_CAUSE_DEATH_Unknown": {"population": "General Population", "Cause of death": "Unknown"},
    # "TX_PVLS_ELIGIBLE_106a-HC29a": {"population": "General Population"},
    # "TX_PVLS_D_106a-HC29b": {"population": "General Population"},
    # "TX_PVLS_N_106a-HC29c": {"population": "General Population"},
    # "PMTCT_FO_105-OE11a": {"population": "General Population", "Outcomes": "HIV - infected"},
    # "PMTCT_FO_105-OE15": {"population": "General Population", "Outcomes": "HIV - uninfected"},
    # "PMTCT_FO_105-OE16": {"population": "General Population", "Outcomes": "HIV - final status unknown"},
    # "PMTCT_FO_105-OE12": {"population": "General Population", "Outcomes": "Died without status known"},
    # "PMTCT_STAT_D_105-AN01a": {"population": "General Population"},
    "PMTCT_STAT_N_105-AN18a": {"population": "General Population","test_result": "New Positive", "gender": "Female"},
    "PMTCT_STAT_N_105-AN17a": {"population": "General Population", "test_result": "Negative", "gender": "Female"},
    "PMTCT_STAT_N_105-AN33b_2019": {"population": "General Population","test_result": "Known positive", "gender": "Female"},
    # "TX_TB_N_106a-HC19d": {"population": "General Population", "status": "HIV+ Currently on ART"},
    # "TB_STAT_N_106a-TB12a": {"population": "General Population", "status": "Known positive"},
    # "TB_STAT_N_106a-TB11b": {"population": "General Population", "status": "New Positive"},
    # "TB_STAT_N_106a-TB11a": {"population": "General Population", "status": "Negative"},
    # "TB_ART_106a-TB11d": {"population": "General Population", "status": "New on ART"},
    # "TB_ART_106a-TB12c": {"population": "General Population", "status": "Already on ART"},
    # "HTS_SELF_105-HT05a_assisted": {"population": "General Population", "status": "Assisted HIVST"},
    # "HTS_SELF_105-HT05a_unassisted": {"population": "General Population", "status": "Unassisted HIVST"},
    # "HTS_SELF_USED_105-HT05b_assisted": {"population": "General Population", "status": "Assisted HIVST"},
    # "HTS_SELF_REACTIVE_105-HT05c1_assisted": {"population": "General Population", "status": "Assisted HIVST"},
    # "HTS_SELF_CONFIRMED_105-HT05c2_assisted": {"population": "General Population", "status": "Assisted HIVST"},
    # "HTS_SELF_LINKED_105-HT05c3_assisted": {"population": "General Population", "status": "Assisted HIVST"},
    # Add more filters here as needed
    # "OTHER_DATA_ELEMENT": {"filter_key": "filter_value"}
}

# Master Excel file to load data from
MASTER_EXCEL_FILE = "mergedMetaDataV2.xlsx"

# Filter mapping dictionary: {data_element_key: {"filter_by": filter_value, "filter_by_coc": optional_coc_value}}
# Rows will be extracted from the master file based on:
#   - filter_by: matches values in 'dataElement.name' column (required)
#   - filter_by_coc: optional filter for 'categoryOptionCombos.name' column
# Example: {"filter_by": "105-HT05a", "filter_by_coc": "Assisted HIVST"}
excel_files = {
    # "TX_NEW_CD4<200": {"filter_by": "CD4: <200"},
    # "TX_NEW CD4>200": {"filter_by": "CD4: ≥200"},
    # "TX_NEW_CD4_UNKNOWN": {"filter_by": "CD4: Unknown"},
    # "TX_CURR_106a-HC11a": {"filter_by": "106a-HC11a"},
    # "TX_CURR_106a-HC11b": {"filter_by": "106a-HC11b"},
    # "TX_CURR_106a-HC11c": {"filter_by": "106a-HC11c"},
    # "TX_CURR_MMD_<3_106a-HC12a": {"filter_by": "106a-HC12a"},
    # "TX_CURR_MMD_3_5106a-HC12b": {"filter_by": "106a-HC12b"},
    # "TX_CURR_MMD_6_106a-HC12c": {"filter_by": "106a-HC12c"},
    # "TX_ML_DIED_106a-HC14d": {"filter_by": "106a-HC14d"},
    # "TX_ML_Refused_Treatment_106a-HC14b": {"filter_by": "106a-HC14b"},
    # "TX_ML_TRANSFER_106a-HC14a": {"filter_by": "106a-HC14a"},
    # "TX_ML_CAUSE_DEATH_TB": {"filter_by": "106a-HC28a_2019"},
    # "TX_ML_CAUSE_DEATH_CANCER": {"filter_by": "106a-HC28b_2019"},
    # "TX_ML_CAUSE_DEATH_Other_infectious_and_parasitic_disease": {"filter_by": "106a-HC28c_2019"},
    # "TX_ML_CAUSE_DEATH_Other_HIV_diseases": {"filter_by": "106a-HC28d_2019"},
    # "TX_ML_CAUSE_DEATH_Natural_causes": {"filter_by": "106a-HC28e_2019"},
    # "TX_ML_CAUSE_DEATH_Non_Natural_causes": {"filter_by": "106a-HC28f_2019"},
    # "TX_ML_CAUSE_DEATH_Unknown": {"filter_by": "106a-HC28g_2019"},
    # "TX_PVLS_ELIGIBLE_106a-HC29a": {"filter_by": "106a-HC29a"},
    # "TX_PVLS_D_106a-HC29b": {"filter_by": "106a-HC29b"},
    # "TX_PVLS_N_106a-HC29c": {"filter_by": "106a-HC29c"},
    # "PMTCT_FO_105-OE11a": {"filter_by": "105-OE11a"},
    # "PMTCT_FO_105-OE15": {"filter_by": "105-OE15"},
    # "PMTCT_FO_105-OE16": {"filter_by": "105-OE16"},
    # "PMTCT_FO_105-OE12": {"filter_by": "105-OE12"},
    # "PMTCT_STAT_D_105-AN01a": {"filter_by": "105-AN01a"},
    "PMTCT_STAT_N_105-AN18a": {"filter_by": "105-AN18a"},
    "PMTCT_STAT_N_105-AN17a": {"filter_by": "105-AN17a"},
    "PMTCT_STAT_N_105-AN33b_2019": {"filter_by": "105-AN33b_2019"},
    # "TB_STAT_N_106a-TB12a": {"filter_by": "106a-TB12a"},
    # "TB_STAT_N_106a-TB11b": {"filter_by": "106a-TB11b"},
    # "TB_STAT_N_106a-TB11a": {"filter_by": "106a-TB11a"},
    # "TB_ART_106a-TB11d": {"filter_by": "106a-TB11d"},
    # "TB_ART_106a-TB12c": {"filter_by": "106a-TB12c"},
    #  "HTS_SELF_105-HT05a_assisted": {"filter_by": "105-HT05a", "filter_by_coc": "Directly Assisted"},
    #  "HTS_SELF_105-HT05a_unassisted": {"filter_by": "105-HT05a", "filter_by_coc": "Unassisted"},
    # "HTS_SELF_USED_105-HT05b_assisted": {"filter_by": "105-HT05b"},
    # "HTS_SELF_REACTIVE_105-HT05c1_assisted": {"filter_by": "105-HT05c1"},
    # "HTS_SELF_CONFIRMED_105-HT05c2_assisted": {"filter_by": "105-HT05c2"},
    # "HTS_SELF_LINKED_105-HT05c3_assisted": {"filter_by": "105-HT05c3"},
    
    # Add more filter mappings here as needed
}

def get_dhis2_data_element(url, username, password, data_element_id):
    """
    Fetch data element details from DHIS2 API
    """
    endpoint = f"{url}/api/dataElements/{data_element_id}.json"
    try:
        response = requests.get(
            endpoint,
            auth=HTTPBasicAuth(username, password),
            params={"fields": "id,name,categoryCombo[id,name,categoryOptionCombos[id,name,categoryOptions[id,name]]]"}
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data element: {e}")
        return None

def filter_category_option_combos(coc_list, filter_dict):
    """
    Filter category option combinations based on filter criteria
    Uses word-boundary matching to prevent partial matches (e.g., "Assisted" won't match "Unassisted")
    """
    filtered_list = []
    for coc in coc_list:
        # Check if category options match the filter criteria
        category_options = coc.get('categoryOptions', [])
        coc_name = coc.get('name', '')
        
        # Check if any category option matches the filter values
        matches = True
        for filter_key, filter_value in filter_dict.items():
            filter_value_lower = str(filter_value).strip().lower()
            found = False
            
            # Try to match against category option names and COC name
            for cat_option in category_options:
                cat_option_name = cat_option.get('name', '')
                cat_option_lower = cat_option_name.lower()
                
                # Strategy: Try exact match first, then word-boundary match
                # Step 1: Exact match (after stripping whitespace)
                if cat_option_lower.strip() == filter_value_lower:
                    found = True
                    break
                
                # Step 2: Word-boundary match (prevents "Assisted" matching "Unassisted")
                if not found:
                    escaped_filter = re.escape(filter_value_lower)
                    # Replace escaped spaces with pattern that allows flexible spacing
                    escaped_filter = escaped_filter.replace(r'\ ', r'\s+')
                    # Use negative lookbehind/lookahead to ensure whole word/phrase matching
                    pattern = r'(?<!\w)' + escaped_filter + r'(?!\w)'
                    if re.search(pattern, cat_option_lower):
                        found = True
                        break
            
            # Also check COC name if not found in category options
            if not found:
                coc_name_lower = coc_name.lower()
                # Try exact match
                if coc_name_lower.strip() == filter_value_lower:
                    found = True
                # Try word-boundary match
                elif not found:
                    escaped_filter = re.escape(filter_value_lower)
                    escaped_filter = escaped_filter.replace(r'\ ', r'\s+')
                    pattern = r'(?<!\w)' + escaped_filter + r'(?!\w)'
                    if re.search(pattern, coc_name_lower):
                        found = True
            
            if not found:
                matches = False
                break
        
        if matches:
            filtered_list.append(coc)
    
    return filtered_list

def sanitize_filename(name):
    """
    Sanitize a string to be used as a filename (remove invalid characters)
    """
    if not name:
        return 'Unknown'
    # Replace invalid filename characters with underscore
    invalid_chars = '<>:"/\\|?*'
    sanitized = name
    for char in invalid_chars:
        sanitized = sanitized.replace(char, '_')
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip(' .')
    # Replace multiple spaces/underscores with single underscore
    sanitized = re.sub(r'[\s_]+', '_', sanitized)
    return sanitized if sanitized else 'Unknown'

def get_data_element_name(url, username, password, data_element_id, de_info_cache=None):
    """
    Get data element name from DHIS2 (with caching)
    """
    if de_info_cache is None:
        de_info_cache = {}
    
    if data_element_id in de_info_cache:
        return de_info_cache[data_element_id].get('name', 'Unknown')
    
    # Fetch data element info
    data_element = get_dhis2_data_element(url, username, password, data_element_id)
    if data_element:
        de_info_cache[data_element_id] = data_element
        return data_element.get('name', 'Unknown')
    
    return 'Unknown'

def get_all_category_option_combos(url, username, password, data_element_id):
    """
    Get all category option combinations for a data element
    """
    # First get the data element to find its category combo
    data_element = get_dhis2_data_element(url, username, password, data_element_id)
    
    if not data_element:
        return []
    
    category_combo = data_element.get('categoryCombo', {})
    category_option_combos = category_combo.get('categoryOptionCombos', [])
    
    # If we need more details about each COC, fetch them individually
    detailed_cocs = []
    for coc in category_option_combos:
        coc_id = coc.get('id')
        if coc_id:
            # Fetch detailed COC information
            endpoint = f"{url}/api/categoryOptionCombos/{coc_id}.json"
            try:
                response = requests.get(
                    endpoint,
                    auth=HTTPBasicAuth(username, password),
                    params={"fields": "id,name,categoryOptions[id,name]"}
                )
                response.raise_for_status()
                detailed_coc = response.json()
                detailed_cocs.append(detailed_coc)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching COC {coc_id}: {e}")
                # Use basic info if detailed fetch fails
                detailed_cocs.append(coc)
    
    return detailed_cocs

# Age mapping configurations - easy to extend with new categories
AGE_MAPPERS = {
    'general': [
        # General age mapper for all data elements except TX_CURR_MMD
        ('0-4Yrs', '1-4 Years'),
        ('5-9Yrs', '5-9 Years'),
        ('10-14Yrs', '10-14 Years'),
        ('15-19Yrs', '15-19 Years'),
        ('20-24Yrs', '20-24 Years'),
        ('25-29Yrs', '25-29 Years'),
        ('30-39Yrs', '35-39 Years'),
        ('40-49Yrs', '45-49 Years'),
        ('50+Yrs', '50-54 Years'),
        ('50-54Yrs', '50-54 Years'),
        ('55-59Yrs', '50-54 Years'),
        ('60-64Yrs', '50-54 Years'),
        ('65+Yrs', '50-54 Years')
    ],
    'mmd': [
        # MMD age mapper for TX_CURR_MMD data elements
        ('10-14Yrs', '<15 Years'),
        ('15-19Yrs', '15+ Years'),
        ('20-24Yrs', '15+ Years'),
        ('25-29Yrs', '15+ Years'),
        ('30-39Yrs', '15+ Years'),
        ('40-49Yrs', '15+ Years'),
        ('0-4Yrs', '<15 Years'),
        ('5-9Yrs', '<15 Years'),
        ('50+Yrs', '15+ Years')
    ],
    'pmtct_stat': [
        # PMTCT age mapper for PMTCT data elements
        ('<15Yrs', '10-14yrs'),
        ('15-19Yrs', '15-19Yrs'),
        ('20-24Yrs', '20-24Yrs'),
        ('25-49Yrs', 'Unknown'),
        ('50+Yrs', '50+ Yrs'),
        # Also handle range formats that might map to <15
        # ('0-4Yrs', '10-14yrs'),
        # ('5-9Yrs', '10-14yrs'),
        # ('10-14Yrs', '10-14yrs'),
        # # Handle 25-29, 30-39, 40-49 ranges that should map to Unknown
        # ('25-29Yrs', 'Unknown'),
        # ('30-39Yrs', 'Unknown'),
        # ('40-49Yrs', 'Unknown')
    ]
}

def get_age_mapper_for_data_element(de_key):
    """
    Determine which age mapper to use based on the data element key
    Returns the mapper name ('general', 'mmd', or 'pmtct_stat')
    """
    if 'TX_CURR_MMD' in de_key:
        return 'mmd'
    elif 'PMTCT' in de_key:
        return 'pmtct_stat'
    else:
        return 'general'

def map_age_range(s, mapper_name='general'):
    """
    Map age ranges using the specified mapper
    Args:
        s: String to map
        mapper_name: Name of the mapper to use ('general' or 'mmd')
    """
    if pd.isna(s) or s == '':
        return s
    
    s_str = str(s)
    
    # Get the appropriate age mappings
    if mapper_name not in AGE_MAPPERS:
        print(f"Warning: Unknown mapper '{mapper_name}', using 'general' mapper")
        mapper_name = 'general'
    
    age_mappings = AGE_MAPPERS[mapper_name]
    
    # Replace age ranges in the string (order matters - check longer patterns first)
    # Sort by length descending to match longer patterns first
    age_mappings_sorted = sorted(age_mappings, key=lambda x: len(x[0]), reverse=True)
    
    for age_range, mapped_value in age_mappings_sorted:
        # Pattern to match the age range with Yrs (case-insensitive)
        # Match patterns like "0-4Yrs", "0-4yrs", "0-4 Yrs", "0-4 Years", "<15Yrs", etc.
        age_part = age_range.replace('Yrs', '')
        # Escape special regex characters (re.escape handles + and < correctly)
        age_part_escaped = re.escape(age_part)
        
        # For patterns starting with <, use a more flexible word boundary
        if age_part.startswith('<'):
            # Pattern for <15Yrs, <15 Yrs, etc.
            pattern = r'(?<!\w)' + age_part_escaped + r'(?:\s*)(?:yrs?|years?)(?!\w)'
        else:
            # Standard pattern with word boundaries
            pattern = r'\b' + age_part_escaped + r'(?:\s*)(?:yrs?|years?)\b'
        
        # Replace with mapped value
        s_str = re.sub(pattern, mapped_value, s_str, flags=re.IGNORECASE)
    
    return s_str

def normalize_string(s):
    """
    Normalize string for better matching (handle Yrs/Years, case, etc.)
    """
    s = str(s).lower().strip()
    # Replace common variations
    s = s.replace('yrs', 'years')
    s = s.replace('yr', 'years')
    s = s.replace(',', ' ')
    # Remove extra spaces
    s = ' '.join(s.split())
    return s

def extract_key_components(s):
    """
    Extract key components from a string (age ranges, gender, etc.)
    Returns a dict with 'age_range' and 'gender'
    """
    s_lower = s.lower()
    components = {'age_range': None, 'gender': None}
    
    # First check for mapped age ranges (for TX_CURR_MMD)
    if '<15 years' in s_lower or '<15' in s_lower:
        components['age_range'] = '<15 Years'
    elif '15+ years' in s_lower or '15+' in s_lower:
        components['age_range'] = '15+ Years'
    else:
        # Extract age ranges (e.g., "5-9", "10-14", "0-4", "50+")
        # First try standard range pattern
        age_pattern = r'(\d+)[-\s]+(\d+)'
        age_matches = re.findall(age_pattern, s)
        if age_matches:
            # Take the first age range found
            match = age_matches[0]
            components['age_range'] = f"{match[0]}-{match[1]}"
        else:
            # Check for "50+" or similar patterns
            age_plus_pattern = r'(\d+)\+'
            age_plus_match = re.search(age_plus_pattern, s_lower)
            if age_plus_match:
                components['age_range'] = f"{age_plus_match.group(1)}+"
            else:
                # Check for single age like "0-4" might be written as "0 4"
                single_age_pattern = r'\b(\d+)\s+(\d+)\b'
                single_match = re.search(single_age_pattern, s)
                if single_match:
                    components['age_range'] = f"{single_match.group(1)}-{single_match.group(2)}"
    
    # Extract gender - handle both original (male/female) and replaced (YYY/XXX) values
    # Check for "female" first (before "male") to avoid false matches
    if 'xxx' in s_lower or re.search(r'\bfemale\b', s_lower):
        components['gender'] = 'female'  # XXX represents Female
    elif 'yyy' in s_lower or re.search(r'\bmale\b', s_lower):
        components['gender'] = 'male'  # YYY represents Male
    
    return components

def fuzzy_match_category_option_combo(excel_value, coc_list):
    """
    Use fuzzy matching to find the best matching category option combo using fuzzysearch
    The Excel value (e.g., "5-9Yrs, Female") should match COC names (e.g., "General Population, Female, 5-9 Years, CD4: <200")
    Returns: (matched_name, matched_id, match_score)
    """
    if pd.isna(excel_value) or excel_value == '':
        return None, None, 0.0
    
    excel_value_str = str(excel_value).strip()
    excel_normalized = normalize_string(excel_value_str)
    excel_components = extract_key_components(excel_value_str)
    
    # Create a list of COC names for matching
    coc_names = [coc.get('name', '') for coc in coc_list]
    
    if not coc_names:
        return None, None, 0.0
    
    # Use fuzzysearch to find the best match
    best_match = None
    best_score = 0.0
    max_l_dist = 5  # Maximum Levenshtein distance allowed
    
    for coc_name in coc_names:
        if not coc_name:
            continue
        
        coc_normalized = normalize_string(coc_name)
        coc_components = extract_key_components(coc_name)
        
        # CRITICAL: First check if key components match - age range and gender must match
        # If age range is specified in Excel, it must match in COC
        if excel_components['age_range']:
            if coc_components['age_range'] != excel_components['age_range']:
                continue  # Skip this COC if age range doesn't match
        
        # If gender is specified in Excel, it must match in COC
        if excel_components['gender']:
            if coc_components['gender'] != excel_components['gender']:
                continue  # Skip this COC if gender doesn't match
        
        # If we get here, key components match - now calculate similarity score
        score = 0.0
        
        # Method 1: Check for exact substring match (normalized)
        if excel_normalized in coc_normalized:
            # If the normalized Excel value is contained in the normalized COC name
            shorter = len(excel_normalized)
            longer = len(coc_normalized)
            # High score for substring match
            score = max(score, 0.9)
        
        # Method 2: Try to find excel_value_str as a substring in coc_name (normalized) using fuzzy search
        matches_in_coc = find_near_matches(
            excel_normalized,
            coc_normalized,
            max_l_dist=max_l_dist
        )
        
        if matches_in_coc:
            # Find the best match (longest matched substring)
            best_match_in_coc = max(matches_in_coc, key=lambda m: len(m.matched))
            match_length = len(best_match_in_coc.matched)
            # Score based on how much of the excel value was matched
            match_score = match_length / len(excel_normalized) if excel_normalized else 0
            score = max(score, match_score * 0.85)
        
        # Method 3: Word-based matching - check if key words from Excel are in COC
        excel_words = set(excel_normalized.split())
        coc_words = set(coc_normalized.split())
        if excel_words:
            common_words = excel_words.intersection(coc_words)
            word_score = len(common_words) / len(excel_words)
            score = max(score, word_score * 0.7)
        
        # Method 4: Exact match (case-insensitive)
        if excel_normalized == coc_normalized:
            score = 1.0
        
        # Update best match if this score is better
        if score > best_score:
            best_score = score
            best_match = coc_name
    
    # Only return a match if score is above threshold
    if best_match and best_score >= 0.5:  # Minimum threshold of 50%
        matched_coc = next((coc for coc in coc_list if coc.get('name') == best_match), None)
        if matched_coc:
            return matched_coc.get('name'), matched_coc.get('id'), round(best_score, 3)
    
    return None, None, round(best_score, 3) if best_score > 0 else 0.0

def process_data_element(de_key, de_id, filter_dict, df, cocs_cache=None):
    """
    Process a single data element: fetch COCs, filter, match, and save output files
    Args:
        de_key: Data element key
        de_id: Data element ID
        filter_dict: Filter criteria for COCs
        df: DataFrame containing the data to process (already filtered from master file)
        cocs_cache: Dictionary to cache COCs by de_id to avoid duplicate API calls
    """
    print(f"\n{'='*80}")
    print(f"Processing Data Element: {de_key} (ID: {de_id})")
    print(f"{'='*80}")
    
    print(f"\nProcessing {len(df)} rows from master file")
    
    # Apply age range mapping based on data element type
    if 'categoryOptionCombos.name' in df.columns:
        mapper_name = get_age_mapper_for_data_element(de_key)
        if mapper_name == 'mmd':
            mapper_display = 'MMD'
        elif mapper_name == 'pmtct_stat':
            mapper_display = 'PMTCT'
        else:
            mapper_display = 'General'
        print(f"Applying {mapper_display} age range mapping for data element: {de_key}...")
        df['categoryOptionCombos.name'] = df['categoryOptionCombos.name'].apply(
            lambda x: map_age_range(x, mapper_name)
        )
        print(f"  {mapper_display} age range mapping completed in Excel data")
    
    # Replace Male with YYY and Female with XXX in categoryOptionCombos.name column
    if 'categoryOptionCombos.name' in df.columns:
        print("Replacing 'Male' with 'YYY' and 'Female' with 'XXX' in categoryOptionCombos.name column...")
        df['categoryOptionCombos.name'] = df['categoryOptionCombos.name'].astype(str).str.replace('Male', 'YYY', regex=False)
        df['categoryOptionCombos.name'] = df['categoryOptionCombos.name'].astype(str).str.replace('Female', 'XXX', regex=False)
        print("  Replacement completed in Excel data")
    
    # Get category option combinations for this data element (use cache if available)
    if cocs_cache is None:
        cocs_cache = {}
    
    if de_id in cocs_cache:
        print(f"\nUsing cached category option combinations for data element ID: {de_id}")
        cocs = cocs_cache[de_id]
        print(f"Found {len(cocs)} category option combinations (from cache)")
    else:
        print(f"\nFetching category option combinations for data element: {de_key} (ID: {de_id})")
        cocs = get_all_category_option_combos(dhis2_url, username, password, de_id)
        cocs_cache[de_id] = cocs  # Store in cache for future use
        print(f"Found {len(cocs)} category option combinations (cached for future use)")
    
    # Filter the COCs based on filter criteria for this data element
    if filter_dict:
        print(f"Filtering with criteria: {filter_dict}")
        filtered_cocs = filter_category_option_combos(cocs, filter_dict)
        print(f"After filtering: {len(filtered_cocs)} category option combinations")
    else:
        filtered_cocs = cocs
        print("No filter criteria specified, using all COCs")
    
    # Process COCs: replace Male/Female and remove duplicates
    seen_ids = set()
    unique_cocs = []
    for coc in filtered_cocs:
        coc_id = coc.get('id')
        if coc_id and coc_id not in seen_ids:
            seen_ids.add(coc_id)
            # Replace Male with YYY and Female with XXX in COC names for matching
            coc_name = coc.get('name', '')
            coc_name_replaced = coc_name.replace('Male', 'YYY').replace('Female', 'XXX')
            # Create a copy of the COC dict with replaced name for matching
            coc_copy = coc.copy()
            coc_copy['name'] = coc_name_replaced
            coc_copy['original_name'] = coc_name  # Keep original for reference
            unique_cocs.append(coc_copy)
    
    print(f"\nTotal unique category option combinations after filtering: {len(unique_cocs)}")
    print("Replaced 'Male' with 'YYY' and 'Female' with 'XXX' in filtered COC names for matching")
    
    # Save filtered COCs to CSV (named by data element key)
    if unique_cocs:
        print(f"\nSaving filtered category option combinations to CSV for {de_key}...")
        csv_data = []
        for coc in unique_cocs:
            coc_id = coc.get('id', '')
            coc_name = coc.get('name', '')
            category_options = coc.get('categoryOptions', [])
            
            # Format category options as a string
            cat_option_names = [co.get('name', '') for co in category_options]
            cat_option_ids = [co.get('id', '') for co in category_options]
            cat_options_str = '; '.join([f"{name} ({id})" for name, id in zip(cat_option_names, cat_option_ids)])
            
            # Get original name if available, otherwise use the name
            original_name = coc.get('original_name', coc_name)
            replaced_name = coc_name  # This already has YYY/XXX replacements
            
            csv_data.append({
                'categoryOptionCombo_id': coc_id,
                'categoryOptionCombo_name': replaced_name,  # Show replaced version
                'categoryOptionCombo_name_original': original_name,  # Also show original
                'categoryOptions': cat_options_str,
                'categoryOption_count': len(category_options)
            })
        
        # Create DataFrame and save to CSV
        filtered_df = pd.DataFrame(csv_data)
        csv_filename = f"filtered_categoryOptionCombos_{de_key}.csv"
        filtered_df.to_csv(csv_filename, index=False)
        print(f"Filtered COCs saved to: {csv_filename}")
        print(f"  Total records: {len(filtered_df)}")
    
    # Display some examples
    if unique_cocs:
        print("\nSample category option combinations (with YYY/XXX replacements):")
        for coc in unique_cocs[:5]:
            print(f"  - {coc.get('name')} (ID: {coc.get('id')})")
            if coc.get('original_name'):
                print(f"    Original: {coc.get('original_name')}")
    
    # Perform fuzzy matching for each row
    print("\nPerforming fuzzy matching...")
    matched_names = []
    matched_ids = []
    match_scores = []
    
    for idx, row in df.iterrows():
        excel_coc_name = row.get('categoryOptionCombos.name', '')
        matched_name, matched_id, match_score = fuzzy_match_category_option_combo(excel_coc_name, unique_cocs)
        matched_names.append(matched_name)
        matched_ids.append(matched_id)
        match_scores.append(match_score)
        
        if idx < 5:  # Show first 5 matches
            print(f"Row {idx}: '{excel_coc_name}' -> '{matched_name}' (ID: {matched_id}, Score: {match_score})")
    
    # Add new columns to the dataframe
    # Note: matched_names will have YYY/XXX replacements, let's also add original names
    matched_original_names = []
    for matched_name in matched_names:
        if matched_name:
            # Find the original name from unique_cocs
            matched_coc = next((coc for coc in unique_cocs if coc.get('name') == matched_name), None)
            if matched_coc and matched_coc.get('original_name'):
                matched_original_names.append(matched_coc.get('original_name'))
            else:
                matched_original_names.append(matched_name)
        else:
            matched_original_names.append(None)
    
    df['infolink_categoryOptionCombos_name'] = matched_names  # With YYY/XXX
    df['infolink_categoryOptionCombos_name_original'] = matched_original_names  # Original with Male/Female
    df['infolink_categoryOptionCombos_id'] = matched_ids
    df['infolink_match_score'] = match_scores
    
    # Save the updated Excel file (named by data element key)
    output_file = f"{de_key}_output.xlsx"
    print(f"\nSaving updated file to: {output_file}")
    df.to_excel(output_file, index=False)
    print("File saved successfully!")
    
    # Print summary statistics
    matched_count = sum(1 for x in matched_names if x is not None)
    print(f"\nSummary for {de_key}:")
    print(f"  Total rows: {len(df)}")
    print(f"  Successfully matched: {matched_count}")
    print(f"  Unmatched: {len(df) - matched_count}")
    
    return matched_count, len(df)

def main():
    """
    Main function to process multiple data elements with their respective filters
    """
    print("="*80)
    print("DHIS2 Category Option Combination Matcher")
    print("="*80)
    print(f"\nProcessing {len(dataElements)} data element(s)...")
    
    # Validate that all data elements have corresponding filters
    for de_key in dataElements.keys():
        if de_key not in filter_lists:
            print(f"Warning: No filter specified for data element '{de_key}'. Using empty filter.")
            filter_lists[de_key] = {}
    
    total_matched = 0
    total_rows = 0
    processed_output_files = []  # Track successfully processed output files
    cocs_cache = {}  # Cache to store COCs by de_id to avoid duplicate API calls
    de_info_cache = {}  # Cache to store data element info (name, etc.) by de_id
    
    # Determine the data element name(s) from DHIS2
    # Group keys by their de_id value (keys with same value reference the same data element)
    unique_de_ids = list(set(dataElements.values()))
    data_element_name = None
    
    if unique_de_ids:
        # Get the name for the first unique de_id (or use the one that's most common)
        # If all keys share the same de_id, use that name
        primary_de_id = unique_de_ids[0]
        data_element_name = get_data_element_name(dhis2_url, username, password, primary_de_id, de_info_cache)
        print(f"\nPrimary data element name: {data_element_name} (ID: {primary_de_id})")
        
        # If there are multiple unique de_ids, log them
        if len(unique_de_ids) > 1:
            print(f"Note: Multiple data element IDs found: {unique_de_ids}")
            print(f"Using name from first ID: {primary_de_id}")
    
    # Load master file once (cache it for reuse)
    master_df = None
    master_file_loaded = False
    
    # Process each data element separately
    for de_key, de_id in dataElements.items():
        # Get filter for this data element
        filter_dict = filter_lists.get(de_key, {})
        
        # Get filter configuration from excel_files mapping
        file_config = excel_files.get(de_key, {})
        if not isinstance(file_config, dict):
            print(f"\nWarning: Invalid configuration for data element '{de_key}'. Skipping...")
            continue
        
        filter_by_value = file_config.get('filter_by')
        filter_by_coc = file_config.get('filter_by_coc')  # Optional filter for categoryOptionCombos.name
        
        if not filter_by_value:
            print(f"\nWarning: No 'filter_by' value specified for data element '{de_key}'. Skipping...")
            continue
        
        try:
            # Load master file if not already loaded
            if not master_file_loaded:
                master_df = pd.read_excel(MASTER_EXCEL_FILE)
                master_file_loaded = True
                print(f"\nMaster file loaded: {MASTER_EXCEL_FILE}")
                print(f"  Total rows in master file: {len(master_df)}")
            
            # Filter rows from master file based on filter_by value
            # Use 'dataElement.name' as the filter column (NOT 'dataElement.id')
            filter_column = 'dataElement.name'
            if filter_column not in master_df.columns:
                # Try to find a similar column - must contain 'name' and NOT 'id'
                possible_columns = [
                    col for col in master_df.columns 
                    if 'dataelement' in col.lower() and 'name' in col.lower() and 'id' not in col.lower()
                ]
                if not possible_columns:
                    # Try broader search - still excluding 'id'
                    possible_columns = [
                        col for col in master_df.columns 
                        if ('dataelement' in col.lower() or ('data' in col.lower() and 'element' in col.lower())) 
                        and 'name' in col.lower() 
                        and 'id' not in col.lower()
                    ]
                if possible_columns:
                    filter_column = possible_columns[0]
                    print(f"  Using column '{filter_column}' instead of 'dataElement.name'")
                else:
                    raise ValueError(f"Could not find filter column 'dataElement.name' in master file. Available columns: {', '.join(master_df.columns.tolist())}")
            
            # Verify we're not accidentally using dataElement.id
            if 'id' in filter_column.lower() and 'name' not in filter_column.lower():
                raise ValueError(f"Error: Filter column '{filter_column}' appears to be an ID column, not a name column. Please use 'dataElement.name'.")
            
            print(f"\nFiltering master file for '{de_key}'...")
            # Step 1: Filter rows where dataElement.name starts with the filter_by value
            # (e.g., "106a-HC28a_2019" matches "106a-HC28a_2019. ART patients...")
            filtered_df = master_df[master_df[filter_column].astype(str).str.startswith(str(filter_by_value))].copy()
            print(f"  Step 1 - Filter by '{filter_by_value}' in '{filter_column}': {len(filtered_df)} rows")
            
            # Step 2: Apply optional filter on categoryOptionCombos.name if specified
            if filter_by_coc and len(filtered_df) > 0:
                coc_column = 'categoryOptionCombos.name'
                if coc_column not in filtered_df.columns:
                    # Try to find a similar column
                    possible_coc_columns = [
                        col for col in filtered_df.columns 
                        if 'categoryoptioncombo' in col.lower() and 'name' in col.lower()
                    ]
                    if possible_coc_columns:
                        coc_column = possible_coc_columns[0]
                        print(f"  Using column '{coc_column}' instead of 'categoryOptionCombos.name'")
                    else:
                        print(f"  Warning: Column 'categoryOptionCombos.name' not found. Available columns: {', '.join(filtered_df.columns.tolist()[:10])}")
                        print(f"  Skipping filter_by_coc filter.")
                        filter_by_coc = None
                
                if filter_by_coc:
                    before_count = len(filtered_df)
                    filter_value_lower = str(filter_by_coc).strip().lower()
                    
                    # Filter where categoryOptionCombos.name matches the filter_by_coc value (case-insensitive)
                    # Strategy: Try exact match first, then word-boundary match to avoid partial matches
                    # (e.g., "Assisted HIVST" should NOT match "Unassisted HIVST")
                    
                    # Step 1: Try exact match (after stripping whitespace)
                    mask = filtered_df[coc_column].astype(str).str.strip().str.lower() == filter_value_lower
                    
                    if mask.sum() == 0:
                        # Step 2: Try word-boundary match (prevents "Assisted" matching "Unassisted")
                        # Escape special regex characters in the filter value, but preserve spaces
                        escaped_filter = re.escape(filter_value_lower)
                        # Replace escaped spaces with pattern that allows word boundaries around the phrase
                        # This ensures "Assisted HIVST" matches as a phrase but not "Unassisted HIVST"
                        escaped_filter = escaped_filter.replace(r'\ ', r'\s+')
                        # Use word boundaries at start and end, and allow spaces within
                        pattern = r'(?<!\w)' + escaped_filter + r'(?!\w)'
                        mask = filtered_df[coc_column].astype(str).str.lower().str.contains(pattern, regex=True, na=False)
                    
                    if mask.sum() == 0:
                        # Step 3: As last resort, try simple contains but log a warning
                        print(f"  Warning: No exact or word-boundary match found for '{filter_by_coc}', trying partial match...")
                        mask = filtered_df[coc_column].astype(str).str.lower().str.contains(filter_value_lower, na=False)
                    
                    filtered_df = filtered_df[mask].copy()
                    after_count = len(filtered_df)
                    print(f"  Step 2 - Filter by '{filter_by_coc}' in '{coc_column}': {before_count} -> {after_count} rows")
            
            print(f"  Final filtered rows: {len(filtered_df)}")
            
            # Show sample values for debugging if no matches found
            if len(filtered_df) == 0:
                if filter_by_coc:
                    # Check if first filter worked
                    temp_df = master_df[master_df[filter_column].astype(str).str.startswith(str(filter_by_value))].copy()
                    if len(temp_df) > 0:
                        print(f"  Warning: No rows found after applying both filters.")
                        if 'categoryOptionCombos.name' in temp_df.columns:
                            sample_values = temp_df['categoryOptionCombos.name'].astype(str).unique()[:5]
                            print(f"  Sample values in 'categoryOptionCombos.name': {sample_values.tolist()}")
                    else:
                        sample_values = master_df[filter_column].astype(str).unique()[:5]
                        print(f"  Sample values in '{filter_column}' column: {sample_values.tolist()}")
                else:
                    sample_values = master_df[filter_column].astype(str).unique()[:5]
                    print(f"  Sample values in '{filter_column}' column: {sample_values.tolist()}")
            
            if len(filtered_df) == 0:
                print(f"  Warning: No rows found matching filter criteria. Skipping...")
                continue
            
            # Process the filtered dataframe
            matched, rows = process_data_element(de_key, de_id, filter_dict, filtered_df, cocs_cache)
            total_matched += matched
            total_rows += rows
            # Track the output file name for merging
            output_file = f"{de_key}_output.xlsx"
            processed_output_files.append((de_key, output_file))
        except FileNotFoundError as e:
            print(f"\nError: {e}")
        except Exception as e:
            print(f"\nError processing data element '{de_key}': {e}")
            import traceback
            traceback.print_exc()
    
    # Merge all output files into one combined file
    merged_dataframes = []
    merged_filename = None
    if processed_output_files:
        print(f"\n{'='*80}")
        print("Merging all output files into one combined file...")
        print(f"{'='*80}")
        
        for de_key, output_file in processed_output_files:
            try:
                df = pd.read_excel(output_file)
                # Add a column to identify the data element source
                df['data_element_key'] = de_key
                # Reorder columns to put data_element_key first (after existing key columns if any)
                cols = df.columns.tolist()
                # Move data_element_key to near the beginning
                if 'data_element_key' in cols:
                    cols.remove('data_element_key')
                    # Insert after Dataset or at the beginning
                    if 'Dataset' in cols:
                        dataset_idx = cols.index('Dataset')
                        cols.insert(dataset_idx + 1, 'data_element_key')
                    else:
                        cols.insert(0, 'data_element_key')
                    df = df[cols]
                merged_dataframes.append(df)
                print(f"  Added {len(df)} rows from {output_file}")
            except FileNotFoundError:
                print(f"  Warning: Output file '{output_file}' not found. Skipping...")
            except Exception as e:
                print(f"  Warning: Error reading '{output_file}': {e}. Skipping...")
        
        if merged_dataframes:
            # Concatenate all dataframes
            merged_df = pd.concat(merged_dataframes, ignore_index=True)
            
            # Save merged file with dynamic data element name
            if data_element_name:
                sanitized_name = sanitize_filename(data_element_name)
                merged_filename = f"{sanitized_name}_all_data_elements_merged_output.xlsx"
            else:
                merged_filename = "all_data_elements_merged_output.xlsx"
            
            merged_df.to_excel(merged_filename, index=False)
            print(f"\nMerged file saved: {merged_filename}")
            print(f"  Total rows in merged file: {len(merged_df)}")
            print(f"  Total columns: {len(merged_df.columns)}")
            print(f"  Data elements included: {', '.join(set(merged_df['data_element_key'].unique()))}")
        else:
            print("  No output files were successfully read for merging.")
    
    # Print overall summary
    print(f"\n{'='*80}")
    print("Overall Summary")
    print(f"{'='*80}")
    print(f"  Total data elements processed: {len(dataElements)}")
    print(f"  Total rows processed: {total_rows}")
    print(f"  Total successfully matched: {total_matched}")
    print(f"  Total unmatched: {total_rows - total_matched}")
    if processed_output_files:
        print(f"  Individual output files created: {len(processed_output_files)}")
        if merged_filename:
            print(f"  Merged output file created: {merged_filename}")

if __name__ == "__main__":
    main()
