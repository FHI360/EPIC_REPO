import re
import json
import pandas as pd
import os

err_file_path = 'error_data.csv'


def clean_log_data(log_file):
    error_data = []
    error_id_list =[]
    with open(log_file, 'r', encoding='utf-8') as file:
        for line in file:
            # Look for the line containing 'Response update:'
            if 'Response update:' in line:
                # Extract the part of the line after 'Response update:'
                raw_json = line.split('Response update:')[-1]
                if "errorReports" in raw_json:
                    # # Clean the unwanted characters
                    cleaned_json = re.sub(r'\\{', '{', raw_json)  # Replace "\{" with "{"
                    cleaned_json = re.sub(r'\\"{', '{', cleaned_json)  # Replace "\{" with "{"
                    cleaned_json = re.sub(r'\\}', '}', cleaned_json)  # Replace "\}" with "}"
                    cleaned_json = re.sub(r'\\"', '"', cleaned_json)  # Replace '\"' with '"'
                    cleaned_json = re.sub(r'\x1b\[[0-9;]*[mG]', '', cleaned_json)  # Remove ANSI escape sequences
                    cleaned_json = cleaned_json.strip()
                    # Check if the string starts and ends with quotes, and remove them
                    if cleaned_json.startswith('"') and cleaned_json.endswith('"'):
                        cleaned_json = cleaned_json[1:-1]  # Remove the extra quotes
                    try:
                        # Parse the cleaned string into a JSON object
                        json_obj = json.loads(cleaned_json)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")
                        return None
                    err_ = json_obj['typeReports'][0]['objectReports'][0]['errorReports']
                    if len(err_) > 0:
                        for err in err_:
                            message = err.get('message', [])
                            error_code = err.get('errorCode', [])
                            main_id = err.get('mainId', [])
                            error_id_list.append(main_id)
                            error_data.append([message, error_code, main_id, "categoryOptionCombo"])
        error_data_saving(error_data)
        print(error_id_list)
        return True


def error_data_saving(error_data):
    try:
        error_data_df = pd.DataFrame(error_data, columns=['message', 'errorCode', 'mainId', 'Property'])

        # Check if file exists
        if not os.path.exists(err_file_path):
            # If file does not exist, write header
            error_data_df.to_csv(err_file_path, index=False)
        else:
            # If file exists, append without writing the header again
            error_data_df.to_csv(err_file_path, mode='a', index=False, header=False)

    except (IOError, OSError) as e:
        # Handle potential I/O errors
        print(f"Error writing to file {err_file_path}: {e}")
    error_data = []


# Usage
log_file_path = 'app_log.log'  # Replace with your actual log file path
cleaned_json_obj = clean_log_data(log_file_path)

if not cleaned_json_obj:
    print("No valid JSON found or error in decoding.")
