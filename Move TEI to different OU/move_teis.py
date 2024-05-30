import json
import requests as rq
import os

def replace_org_unit(data, ou_value):
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "orgUnit" or key == "ownerOrgUnit":
                data[key] = ou_value
            else:
                replace_org_unit(value, ou_value)
    elif isinstance(data, list):
        for item in data:
            replace_org_unit(item, ou_value)

def main():
    config_file_path = "config.json"
    
    # Load configuration from the config.json file in the current directory
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Config file not found: {config_file_path}")

    with open(config_file_path, "r") as file:
        config = json.load(file)

    # Extract configuration values
    dhis_uname = config["dhis_uname"]
    dhis_pwd = config["dhis_pwd"]
    base_url = config["base_url"]
    ou_destination = config["ou_destination"]
    teis_to_move = config["teis_to_move"]

    # URL for TEI operations
    tei_url = f"{base_url}api/trackedEntityInstances/"

    for tei in teis_to_move:
        try:
            response = rq.get(f"{tei_url}{tei}.json?fields=*", auth=(dhis_uname, dhis_pwd))
            response.raise_for_status()  # Check if the request was successful
        except rq.exceptions.RequestException as e:
            print(f"Failed to retrieve TEI {tei}: {e}")
            continue  # Skip to the next TEI

        try:
            tei_data = response.json()  # Parse JSON directly from response
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON for TEI {tei}: {e}")
            continue  # Skip to the next TEI

        # Replace orgUnit values in the parsed JSON data
        replace_org_unit(tei_data, ou_destination)

        # Convert the modified JSON data to format for import
        modified_tei_json = {"trackedEntityInstances": [tei_data]}
        tei_uid = modified_tei_json['trackedEntityInstances'][0]['trackedEntityInstance']
        try:
            program = modified_tei_json['trackedEntityInstances'][0]['enrollments'][0]['program']
        except (IndexError, KeyError) as e:
            print(f"Missing enrollment data for TEI {tei_uid}: {e}")
            continue  # Skip to the next TEI

        try:
            # Post TEI
            r = rq.post(url=tei_url, json=modified_tei_json, auth=(dhis_uname, dhis_pwd))
            r.raise_for_status()  # Check if the request was successful
            print(f"Transfer Events for: {tei_uid} Status Code: {r.status_code}")
        except rq.exceptions.RequestException as e:
            print(f"Failed to post TEI {tei_uid}: {e}")
            continue  # Skip to the next TEI

        try:
            # Transfer Ownership
            r2 = rq.put(url=f"{base_url}api/tracker/ownership/transfer", 
                        params={'trackedEntityInstance': tei_uid, 'program': program, 'ou': ou_destination}, 
                        auth=(dhis_uname, dhis_pwd))
            r2.raise_for_status()  # Check if the request was successful
            print(f"Transfer Ownership for: {tei_uid} Status Code: {r2.status_code}")
        except rq.exceptions.RequestException as e:
            print(f"Failed to transfer ownership for TEI {tei_uid}: {e}")
            continue  # Skip to the next TEI

if __name__ == "__main__":
    main()
