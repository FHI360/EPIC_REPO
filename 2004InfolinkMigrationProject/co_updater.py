# -*- coding: UTF-8 -*-
import sys

import pandas as pd
import requests as rq
import json
import csv
import math
from datetime import datetime, timedelta
from logzero import logger, LogFormatter, setup_default_logger
import logzero
import os
import numpy as np
import re
import pickle
import pyAesCrypt
import unicodedata
import getpass
import maskpass  # importing maskpass library

class LogFormat:
    def __init__(self, log_file_name, destination_folder):

        self.log_file_name = log_file_name
        self.destination_folder = destination_folder

    def config(self):
        # Create destination folder if it doesn't exist
        try:
            os.makedirs(self.destination_folder, exist_ok=True)
        except OSError as error:
            pass

        # Set a custom formatter
        level_name = "* %(levelname)1s"
        time = "%(asctime)s,%(msecs)03d"
        message = "%(message)s"
        caller = "%(module)s:%(lineno)d"
        log_format = f'%(color)s[{level_name} {time} {caller}] {message}%(end_color)s'
        formatter = LogFormatter(fmt=log_format)

        # Log file path
        output = f"{self.destination_folder}/{self.log_file_name}.log"

        # Set up default logger for logging to a file
        setup_default_logger(logfile=output, formatter=formatter)

        # Return the global logger for use in other classes
        return logger

class Connection:
    def __init__(self, log=None, timeout=5):
        # Initialize logger, expecting it to be passed from LogFormat
        self.logger = log if log else logzero.logger
        self.source_session = rq.Session()
        self.destination_session = rq.Session()
        self.source_session.auth = None
        self.destination_session.auth = None
        self.source_base_url = None
        self.destination_base_url = None
        self.timeout = timeout
        self.dhis_file = "dhis-credentials.dat"
        self.decrypted = False
        self.connection_type = None

    def setup_credentials(self):
        passphrase = "visit_rwanda"
        if not os.path.exists(f"{self.dhis_file}.aes"):
            # Initial file setup
            #Source credentials and target URL
            dhis_source_username = input("Please enter your source username: ")
            dhis_source_password = input("Please enter your source password: ")
            source_target_url = input("Please enter URL (e.g., https://source_url/api/29/): ")

            #Destination credentials and target URL
            def same_server_responses(res):
                # Keep prompting until the response is valid
                while res.lower() not in ["yes", "y", "no", "n"]:
                    res = input("Please enter a valid response (y/n): ")
                return res.lower() in ["yes", "y"]

            same_server = input("is Source instance the same as Destination instance (y/n): ")
            if same_server_responses(same_server):
                dhis_destination_username = dhis_source_username
                dhis_destination_password = dhis_source_password
                destination_target_url = source_target_url
            else:
                dhis_destination_username = input("Please enter your destination username: ")
                dhis_destination_password = input("Please enter your destination password: ")
                destination_target_url = input("Please enter URL (e.g., https://destination_url/api/29/): ")

            # Save credentials to a file
            credentials = {
                "source": [dhis_source_username, dhis_source_password, source_target_url],
                "destination": [dhis_destination_username, dhis_destination_password, destination_target_url]
            }
            # print(credentials)
            with open(self.dhis_file, "wb") as cred_file:
                pickle.dump(credentials, cred_file)

            # Encrypt the file
            self.logger.debug("Creating Encryption File.")
            pickle_file_enc = f"{self.dhis_file}.aes"
            buffer_size = 64 * 1024  # 64KB buffer size
            pyAesCrypt.encryptFile(self.dhis_file, pickle_file_enc, passphrase, buffer_size)

            # Clean up original file for security
            os.remove(self.dhis_file)

            # Log and ping after encryption is complete
            self.logger.debug("Credentials configured and encrypted.")
            self.decrypt()
        else:
            self.decrypt()

    def decrypt(self):
        self.logger.debug(f"reading configurations")
        pickle_file_enc = f"{self.dhis_file}.aes"
        password_decrypt = "visit_rwanda"
        pyAesCrypt.decryptFile(pickle_file_enc, f"2_{self.dhis_file}", password_decrypt)
        credentials = pickle.load(open(f"2_{self.dhis_file}", "rb"))
        os.remove(f"2_{self.dhis_file}")

        #Setting up source credentials
        source_credentials = credentials["source"]
        self.source_session.auth = (source_credentials[0], source_credentials[1])
        self.source_base_url = source_credentials[2]

        #Setting up destination credentials
        destination_credentials = credentials["destination"]
        self.destination_session.auth = (destination_credentials[0], destination_credentials[1])
        self.destination_base_url = destination_credentials[2]

        self.decrypted = True
        self.ping("source")
        self.ping("destination")

    def ping(self,connection_type):
        if not self.decrypted:
            self.setup_credentials()
        else:
            session = self.source_session if connection_type == "source" else self.destination_session
            base_url = self.source_base_url if connection_type == "source" else self.destination_base_url
            base_url = base_url.replace("://", "TEMP_PLACEHOLDER").replace("//", "/").replace("TEMP_PLACEHOLDER", "://")
            #base_url = re.sub(r"//29.*?/system", "/system", base_url)
            # print(base_url)
            try:
                r = session.get(f'{base_url}system/ping', timeout=self.timeout)
                r.raise_for_status()  # Raise an error for bad responses
            except rq.RequestException as e:
                self.logger.debug(f"[Connection] Error occurred: {e}")
                return False
            else:
                if r.ok:
                    return True
                else:
                    self.logger.debug(f"[Connection] Connection could not be established. {r.text}")
                    return False

    def get_source_session(self):
        return self.source_session  # Expose the session

    def get_destination_session(self):
        return self.destination_session

class Engine:
    def __init__(self, connection=None, log=None, org_unit_group=None, datasource=None, posted_file_path=None, years=None):
        self.logger = log
        self.source_session = None
        self.destination_session = None
        self.connection = connection  # Dependency injection
        # Initialize DataValueProcessing and pass the logger to it
        self.DataValueProcessing = DataValueProcessing(log=self.logger)
        self.df_main = datasource
        self.df = None
        # self.session.auth = ('aejakhegbe', '%Wekgc7345dgfgfq#')
        self.source_base_url = None
        self.destination_base_url = None
        self.klass = self.__class__.__name__
        self.timeout = 240
        self.data_to_process_df = None
        self.data_element_group_id = None
        self.data_element_in_view = None
        self.migration_dataset_id = None
        self.new_data_element = None
        self.config_metadata = None
        self.min_unique_column_to_filter = None
        self.org_obj = None
        self.org_unit_group = org_unit_group
        self.posted_file_path = posted_file_path
        self.process_category_combination_maintenance = True
        self.confirmation_for_specific_coc_ = None
        self.err_file_path = 'conflicts_data.csv'
        self.years_back = years.get('dynamic_years', 0)
        self.specific_years = years.get('specific_years', None)
        self.process_months = years.get('process_months', None)
        self.process_days = years.get('process_days', None)
        self.error_data = []
        self.months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'] #['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        self.ping_connections()

    def ping_connections(self):
        if self.connection.ping("source"):
            self.logger.debug(f"[Source Connection] Connection established.")
            self.source_base_url = self.connection.source_base_url
            self.source_session = self.connection.get_source_session()  # Directly use self.connection.session
            return self.connection.ping("source")

        if self.connection.ping("destination"):
            self.logger.debug("[Destination Connection] Connection established.")
            self.destination_base_url = self.connection.destination_base_url
            self.destination_session = self.connection.get_destination_session()
            return self.connection.ping("destination")

    def process_metadata(self,
                         filter_item=None,
                         co_id=None,
                         old_cc_id=None,
                         new_name=None,
                         new_data_element=None,
                         data_element_in_view=None,
                         update_specific_coc_=None,
                         process_category_combination_maintenance_=False,
                         process_data_values_=True):
        self.new_data_element = new_data_element
        self.data_element_in_view = data_element_in_view
        self.process_category_combination_maintenance = process_category_combination_maintenance_
        if self.config_metadata is not data_element_in_view:
            self.create_update_data_element_group(mode='update')
            # self.logger.debug("Updating dataset")
            self.update_dataset(self.data_element_in_view)
            self.config_metadata = data_element_in_view

        if update_specific_coc_ is None:
            self.data_to_process_df = self.data_to_process(filter_item, data_element_in_view)
        else:
            # Ask for user confirmation before proceeding
            if self.confirmation_for_specific_coc_ is None:
                self.confirmation_for_specific_coc_ = input(
                    f"update_specific_coc_ is not None. Proceed? (yes/no): ").strip().lower()
                if self.confirmation_for_specific_coc_ == 'yes':
                    self.set_filter_column('categoryOptionCombos.id')
                    self.data_to_process_df = self.data_to_process(update_specific_coc_, data_element_in_view)
                    self.logger.debug(self.data_to_process_df)
                else:
                    self.logger.debug("Operation cancelled by the user.")
                    sys.exit()

        if self.data_to_process_df is not None:
            if self.process_category_combination_maintenance:
                with pd.ExcelWriter("filter_xls.xlsx") as writer:
                    self.data_to_process_df.to_excel(writer, sheet_name='data_elements', index=False)
                try:
                    # Loop through each row in the DataFrame along with the index
                    self.data_to_process_df = pd.read_excel("filter_xls.xlsx", sheet_name=0)
                    self.logger.debug(f'self.data_to_process_df length: {len(self.data_to_process_df)} for {filter_item} in {self.data_element_in_view}')
                    # Convert the filtered data to a CSV file and name it 'filter_csv.csv'
                    self.data_to_process_df.to_csv('filter_csv.csv', index=False)
                    if update_specific_coc_ is None:
                        # get old categoryCombos CoCs
                        cc_data_update_ = self.get_url_data(f"{self.source_base_url}categoryCombos/{old_cc_id}.json?"
                                                    f"fields=categoryOptionCombos[id,name,displayShortName,"
                                                    f"displayName,displayFormName,categoryOptions,categoryCombo]",
                                                    connection="source")
                        # Update the categoryCombo id for all items in coc_data_update_["categoryOptionCombos"] to co_id
                        for combo in cc_data_update_["categoryOptionCombos"]:
                            combo["categoryCombo"]["id"] = co_id
                        # get now categoryCombos CoCs
                        cc_data = self.get_url_data(f"{self.source_base_url}categoryCombos/{co_id}.json?",
                                                    connection="source")
                        cc_data['name'] = new_name
                        cc_data['displayName'] = new_name
                        cc_data['categories'] = self.update_co_categories(0)
                        del cc_data["createdBy"], cc_data["lastUpdatedBy"]
                        del cc_data["user"], cc_data["created"]
                        del cc_data["lastUpdated"]
                        del cc_data["href"]
                        data_structured_ = {"categoryCombos": [cc_data],
                                            "categoryOptionCombos": cc_data_update_["categoryOptionCombos"]}
                        # data_structured_ = {"categoryOptionCombos": coc_data_update["categoryOptionCombos"]}
                        data_ = Engine.clean_up(str(data_structured_))
                        response_update_ = self.post_data(url=f"{self.destination_base_url}metadata", data=data_,
                                                        params={'importStrategy': 'CREATE_AND_UPDATE'})
                        self.logger.debug('++ Pushing categoryCombos and categoryOptionCombos data ++')
                        self.logger.debug("Status code: %s", json.dumps(response_update_.status_code))
                        self.logger.debug("Response update: %s", json.dumps(response_update_.text))
                    else:
                        co_data = self.get_url_data(f"{self.source_base_url}categoryCombos/{co_id}.json")
                        co_data['name'] = new_name
                        co_data['displayName'] = new_name
                        # Loop through DataFrame rows once and update accordingly
                        update_co_with_categories_counter = 0
                        for process_index in range(len(self.data_to_process_df)):
                            # self.logger.debug(self.data_to_process_df['categoryOptionCombos.id'].iloc[index])
                            self.logger.debug(f"grouped data for {data_element_in_view} - index value for row : {process_index}/{len(self.data_to_process_df)}")
                            if update_co_with_categories_counter == 0:

                                co_data['categories'] = self.update_co_categories(process_index)
                                co_data_structured = {"categoryCombos": [co_data],
                                                      "categoryOptionCombos": [self.update_coc_category(co_id, process_index)]}
                                # Increment the counter after updating the categories
                                update_co_with_categories_counter += 1
                            else:
                                co_data_structured = {"categoryOptionCombos": [self.update_coc_category(co_id, process_index)]}
                            # with open('co_coc_data.json', 'w') as json_file:
                            #     json.dump(co_data_structured, json_file)
                            #
                            params = {'importStrategy': 'UPDATE'}
                            response_update = self.post_data(url=f"{self.destination_base_url}metadata", json_=co_data_structured, params=params)
                            self.logger.debug('++ Pushing category and COC data ++')
                            self.logger.debug("Status code: %s", json.dumps(response_update.status_code))
                            self.logger.debug("Response update: %s", json.dumps(response_update.text))
                except Exception as e:
                    self.logger.debug(e)
            self.error_data = []
            if process_data_values_:
                self.datavalues()

    def create_check_metadata(self, metadata, mode, target_name, json_obj):
        metadata_data = None
        if mode == 'check':
            check_exist_json = f"{self.source_base_url}{metadata}.json?fields=id%2Cname"
            self.logger.debug(check_exist_json)
            check_exist_json_data = self.get_url_data(check_exist_json, connection="source")
            page_count = check_exist_json_data['pager'].get('pageCount')
            for page in range(1, int(page_count) + 1):
                # Construct the URL for the current page
                page_url = f"{self.source_base_url}{metadata}.json?fields=id%2Cname&page={page}"
                check_exist_json_data = self.get_url_data(page_url, connection="source")
                # Extract categoryCombos from the current page
                obj = check_exist_json_data.get(f'{metadata}', [])
                # Loop through categoryCombos and check for target name
                for values in obj:
                    if values.get('name') == target_name:
                        # self.logger.debug(f'{metadata} exists')
                        if metadata == 'dataElementGroups':
                            self.data_element_group_id = values.get('id')
                        if metadata == 'dataSets':
                            self.migration_dataset_id = values.get('id')
                        if metadata == 'dataElements':
                            self.new_data_element = values.get('id')
                        self.logger.debug(f"CO == > {values}")
                        return values.get('id')
            return None
        if mode == 'create':
            uid = self.get_uid(self.destination_base_url)
            if metadata == 'categoryCombos':
                metadata_data = {
                    "name": target_name,
                    "publicAccess": "rw------",
                    "categories": [],
                    "dataDimensionType": "DISAGGREGATION",
                    "displayName": target_name,
                    "id": uid,
                    "attributeValues": [],
                    "categoryOptionCombos": [],
                }
            if metadata == "dataElementGroups":
                metadata_data = {
                    "name": "Data Migration Group",
                    "shortName": "Data Migration Group",
                    "dimensionItemType": "DATA_ELEMENT_GROUP",
                    "legendSets": [],
                    "aggregationType": "SUM",
                    "groupSets": [],
                    "dimensionItem": uid,
                    "displayShortName": "Data Migration Group",
                    "displayName": "Data Migration Group",
                    "displayFormName": "Data Migration Group",
                    "id": uid,
                    "attributeValues": [],
                    "dataElements": []
                }
                self.data_element_group_id = uid
            if metadata == "dataSets":
                if self.org_obj is None:
                    org_url = f"{self.source_base_url}organisationUnits.json?fields=id&paging=false"
                    org_json_data = self.get_url_data(org_url, connection="destination")
                    self.org_obj = org_json_data.get(f'organisationUnits', [])
                metadata_data = {
                    "name": json_obj.get('name'),
                    "shortName": json_obj.get('name'),
                    "dimensionItemType": "REPORTING_RATE",
                    "legendSets": [],
                    "periodType": "Monthly",
                    "dataInputPeriods": [],
                    "dataSetElements": [],
                    "indicators": [],
                    "compulsoryDataElementOperands": [],
                    "sections": [],
                    "categoryCombo": {
                        "id": "lrq6Qzb9Vyd"
                    },
                    "timelyDays": 15,
                    "openFuturePeriods": 0,
                    "openPeriodsAfterCoEndDate": 0,
                    "formType": "DEFAULT",
                    "displayName": json_obj.get('name'),
                    "dimensionItem": uid,
                    "displayShortName": json_obj.get('name'),
                    "displayFormName": json_obj.get('name'),
                    "id": uid,
                    "attributeValues": [],
                    "organisationUnits": self.org_obj
                }
                self.migration_dataset_id = uid

            if metadata == 'dataElements':
                metadata_data = {
                    "name": json_obj.get('name'),
                    "shortName": json_obj.get("short_name"),
                    "description": json_obj.get("description"),
                    "formName": json_obj.get("form_name"),
                    "dimensionItemType": "DATA_ELEMENT",
                    "aggregationType": "SUM",
                    "valueType": "INTEGER",
                    "domainType": "AGGREGATE",
                    "zeroIsSignificant": False,
                    "optionSetValue": False,
                    "displayShortName": json_obj.get("short_name"),
                    "displayDescription": json_obj.get("description"),
                    "displayName": json_obj.get("name"),
                    "displayFormName": json_obj.get("form_name"),
                    "id": uid,
                    "attributeValues": json_obj.get("attribute_values"),
                    "categoryCombo": {
                        "id": json_obj.get("category_combination")
                    }
                }
                self.new_data_element = uid
            data_structured_ = {f"{metadata}": [metadata_data]}
            data__ = Engine.clean_up(str(data_structured_))
            # self.logger.debug("data_structured: ", data__)
            params = {'importStrategy': 'CREATE_UPDATE'}
            response_update_ = self.post_data(url=f"{self.destination_base_url}metadata", data=data__, params=params)
            self.logger.debug(f'++ Updating or creating {metadata} ++')

            # Assuming the response JSON is stored in a variable called response_update
            response_data = response_update_.json()  # Convert response to JSON
            # Access the value of "ignored"
            ignored_value = response_data['stats']['ignored']
            self.logger.debug(f"ignored_value - {ignored_value}")
            if ignored_value != 0:
                if metadata == 'dataElements':
                    self.logger.debug(f"New data element was not created")
                    # Access the errorReports in the nested structure
                    error_reports = response_data['typeReports'][0]['objectReports'][0]['errorReports']
                    for error in error_reports:
                        if error["errorProperty"] == "shortName" and "already exists" in error["message"]:
                            json_obj["short_name"] = f'{json_obj["short_name"]}_'
                        if error["errorProperty"] == "Name" and "already exists" in error["message"]:
                            json_obj["name"] = f'{json_obj["name"]}_'
                    self.logger.debug(f"Second attempt to create new data element")
                    # Recursive call with incremented attempt value to retry
                    self.create_check_metadata(metadata, mode, target_name, json_obj)
            elif ignored_value == 0:
                self.logger.debug("response_data %s", json.dumps(response_data))
                return uid
            else:
                return None

    def update_co_categories(self, processed_index):
        """
        This function extracts category UIDs (Unique Identifiers) from a specific row in a DataFrame and compiles them
        into a list of dictionaries. Each dictionary represents a category with its associated UID.

        Args:
            processed_index (int): The index of the row in 'self.data_to_process_df' from which data will be extracted.

        Workflow:
            1. Defines the columns representing category UIDs (up to 6 categories: 'Category 1 UID' to 'Category 6 UID').
            2. Initializes an empty list 'cat' to store dictionaries, each containing a category UID as {"id": UID}.
            3. Checks if the provided index is within the valid range of the DataFrame's rows:
                - If the index is out of range, it logs a message and returns an empty list.
            4. Iterates over the defined columns:
                - Skips any column not found in the DataFrame to avoid errors.
                - For each valid column, extracts the UID for the specified row.
                - Adds a dictionary with the UID (if it's not NaN) to the 'cat' list.
            5. If any exception occurs during execution, logs an error message and returns an empty list.

        Returns:
            list: A list of dictionaries, each containing a category UID as {"id": UID}. Returns an empty list if the index is out of range
            or if an error occurs during processing.
        """
        # Specify the columns you want to extract data from
        columns = ['Category 1 UID', 'Category 2 UID', 'Category 3 UID',
                   'Category 4 UID', 'Category 5 UID', 'Category 6 UID'] # 'Category 6 UID'

        # Initialize an empty list to store the category dictionaries
        cat = []

        try:
            # Validate that index is within range
            if processed_index >= len(self.data_to_process_df):
                self.logger.debug(f"Skipping index {processed_index}: Out of range")
                return cat

            # Extract the specified row for the given index
            for col in columns:
                if col not in self.data_to_process_df.columns:
                    # self.logger.debug(f"Column {col} does not exist in the DataFrame at index {index}")
                    continue

                uid = self.data_to_process_df.loc[processed_index, col]  # Access the value based on the index parameter
                if pd.notna(uid):  # Check if the value is not NaN
                    cat.append({"id": uid})
            return cat

        except Exception as e:
            self.logger.debug(f"Error updating categories at index {processed_index}: {e}")
            return cat

    def update_coc_category(self, co_id, processed_index):
        """
        This function updates the categoryOptionCombo (COC) category for a specific record in a DataFrame.

        Args:
            co_id (str): The ID of the categoryOptionCombo to be updated.
            processed_index (int): The index of the row in 'self.data_to_process_df' from which data will be extracted.

        Workflow:
            1. Defines the column(s) ('categoryOptionCombos.id') from which the UID (unique identifier) will be extracted.
            2. Initializes an empty list 'cat_option_combo' to store dictionaries representing the COC category.
            3. Iterates over the specified columns to extract the UID from the specified row (via the 'index' argument).
            4. Appends the 'co_id' as a dictionary to the 'cat_option_combo' list if the UID is not NaN (missing).
            5. Retrieves the full categoryOptionCombo data from a URL using the UID.
            6. Updates the 'categoryCombo' field in the fetched data with the first entry in 'cat_option_combo'.
            7. Returns the updated categoryOptionCombo data.

        Returns:
            dict: A dictionary containing the updated categoryOptionCombo data with the 'categoryCombo' field modified.
        """
        # Specify the columns you want to extract data from
        columns = ['categoryOptionCombos.id']

        # Initialize an empty list to store the category dictionaries
        cat_option_combo = []
        uid = None
        # Extract the first row (index 0) for the specified columns
        for col in columns:
            uid = self.data_to_process_df.loc[processed_index, col]  # Access the value in the first row
            if pd.notna(uid):  # Check if the value is not NaN
                cat_option_combo.append({"id": co_id})
        coc_data_ = self.get_url_data(f"{self.source_base_url}categoryOptionCombos/{uid}.json", connection="source")
        coc_data_["categoryCombo"] = cat_option_combo[0]
        return coc_data_  # Optionally, return the 'cat' list if needed elsewhere

    @staticmethod
    def update_coc_name(coc_data_update_, df_coc):
        # Create a mapping dictionary from the DataFrame
        update_mapping = dict(
            zip(df_coc["categoryOptionCombos.id"], df_coc["updated name for Coc update"]))
        # Update the JSON data
        for combo in coc_data_update_:
            combo_id = combo["id"]  # Extract the id
            if combo_id in update_mapping:
                name_ = update_mapping[combo_id]
                if isinstance(name_, str):
                    name_ = name_.encode('utf-8').decode('utf-8')  # Ensure it's properly encoded
                # Update the 'name, displayName and displayFormName' fields
                combo["name"] = name_
                combo["displayName"] = name_
                combo["displayFormName"] = name_
        return coc_data_update_

    def get_url_data(self, url, connection="source", session=None):
        # Select the appropriate session based on destination
        if session is None:
            session = self.destination_session if connection == "destination" else self.source_session
        try:
            response = session.get(url, timeout=self.timeout)
            get_data_ = json.loads(response.text)
            return get_data_
        except json.JSONDecodeError as e:
            self.logger.debug(f"Failed to parse JSON from response: {e}")
            return {}  # Return empty dictionary instead of None
        except rq.RequestException as e:
            failure_today_date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.debug(f"Request failed at {failure_today_date_time}: {e}")
            return {}  # Return empty dictionary instead of None
        except Exception as e:
            failure_today_date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.debug(f"Unexpected error at {failure_today_date_time}: {e}")
            return {}  # Return empty dictionary instead of None

    def post_data(self, url=None, json_=None, data=None, params=None,
                                                headers={"Content-Type": "application/json; charset=utf-8"}, connection="destination", data_structured_=None):

        # Ensure the URL is provided
        if url is None:
            raise ValueError("The 'url' parameter must be provided.")

        # Select the appropriate session based on destination
        session = self.destination_session if connection == "destination" else self.source_session

        # Choose to send data or json
        if data is not None:
            try:
                response_update_ = session.post(url=url, data=data, params=params,
                                                headers=headers)
            except Exception as e:
                response_update_ = session.post(url=url, json=data_structured_, params=params, headers=headers)

        elif json_ is not None:
            response_update_ = session.post(url=url, json=json_, params=params, headers=headers)
        else:
            raise ValueError("Either 'data' or 'json' must be provided.")

        return response_update_

    def get_uid(self, url):
        get_uid_json = f"{url}system/id?limit=1" #TODO: Update the base url to either source or destination
        get_uid_json_data = self.get_url_data(get_uid_json)
        return get_uid_json_data['codes'][0]

    def data_to_process(self, filter_option1, data_element_in_view=None):
        filtered_results = None
        if data_element_in_view is not None:
            self.df = pd.read_excel("processing_source_engine.xlsx", sheet_name=0)
            filtered_data = self.df[self.df['dataElement.id'] == data_element_in_view]
            filtered_data.to_csv('before_filter.csv', index=False)
            self.logger.debug(f"{self.min_unique_column_to_filter} - column filter enabled")
            if isinstance(filter_option1, list):
                # If filter_option1 is a list, use isin()
                filtered_results = filtered_data[
                    filtered_data[f'{self.min_unique_column_to_filter}'].isin(filter_option1)
                ]
            else:
                filtered_results = filtered_data[
                    filtered_data[f'{self.min_unique_column_to_filter}'].str.contains(filter_option1, regex=False)
                    ]
        if data_element_in_view is None:
            filtered_results = self.df_main[self.df_main['Proposed new Data element Name'] == filter_option1]
            with pd.ExcelWriter("processing_source_engine.xlsx") as writer:
                filtered_results.to_excel(writer, sheet_name='data_elements', index=False)
        if len(filtered_results) > 0:
            return filtered_results
        else:
            return None

    def set_filter_column(self, min_unique_column_to_filter):
        self.min_unique_column_to_filter = min_unique_column_to_filter

    def set_df(self, df_):
        self.df_main = df_

    def create_update_data_element_group(self, mode):
        if mode == 'update':
            data_element_group_json = f"{self.destination_base_url}dataElementGroups/{self.data_element_group_id}.json"
            data_element_group_json_data = self.get_url_data(data_element_group_json)
            del data_element_group_json_data["createdBy"], data_element_group_json_data["lastUpdatedBy"]
            del data_element_group_json_data["user"], data_element_group_json_data["created"]
            del data_element_group_json_data["lastUpdated"]

            if len(data_element_group_json_data["dataElements"]) > 0:
                data_element_group_json_data["dataElements"][0]["id"] = self.data_element_in_view
            else:
                new_element_id = {
                                    "id": self.data_element_in_view
                                    }
                data_element_group_json_data["dataElements"].append(new_element_id)
            data_structured_ = {"dataElementGroups": [data_element_group_json_data]}
            data_ = Engine.clean_up(str(data_structured_))
            self.logger.debug(data_)
            params = {'importStrategy': 'UPDATE'}
            response_update_ = self.post_data(url=f"{self.destination_base_url}metadata", data=data_, params=params)
            self.logger.debug('++ Updating DataElementGroups ++ ')
            self.logger.debug("States %s", json.dumps(response_update_.status_code))
            self.logger.debug("response %s", json.dumps(response_update_.text))
            if response_update_.status_code == 500:
                self.logger.debug("failed to update DataElementGroups")
                self.logger.debug(data)
                sys.exit()

    def update_dataset(self, data_element_in_view):
        dataset_json = f"{self.destination_base_url}dataSets/{self.migration_dataset_id}.json"
        dataset_json_data = self.get_url_data(dataset_json, "destination")
        filtered_elements = [element for element in dataset_json_data["dataSetElements"] if
                             element["dataElement"]["id"] == self.new_data_element]
        if len(filtered_elements) == 0:
            _element_in_view = {
                "dataSet": {
                    "id": self.migration_dataset_id
                },
                "dataElement": {
                    "id": data_element_in_view
                }
            }
            new_element = {
                "dataSet": {
                    "id": self.migration_dataset_id
                },
                "dataElement": {
                    "id": self.new_data_element
                }
            }
            filtered_elements.append(_element_in_view)
            filtered_elements.append(new_element)
        else:
            _element_in_view = {
                "dataSet": {
                    "id": self.migration_dataset_id
                },
                "dataElement": {
                    "id": data_element_in_view
                }
            }
            filtered_elements.append(_element_in_view)
        # Update the original data structure
        dataset_json_data["dataSetElements"] = filtered_elements
        del dataset_json_data["createdBy"], dataset_json_data["lastUpdatedBy"]
        del dataset_json_data["user"], dataset_json_data["created"]
        del dataset_json_data["lastUpdated"]
        data_structured_ = {"dataSets": [dataset_json_data]}
        data_ = Engine.clean_up(str(data_structured_))
        params = {'importStrategy': 'UPDATE'}
        response_update_ = self.post_data(url=f"{self.destination_base_url}metadata", data=data_, params=params)
        self.logger.debug('++ Updating Datasets ++')
        self.logger.debug("Status %s", json.dumps(response_update_.status_code))
        self.logger.debug("response %s", json.dumps(response_update_.text))
        if response_update_.status_code == 500:
            self.logger.debug("failed to update datasets")
            del dataset_json_data['organisationUnits']
            data_structured_ = {"dataSets": [dataset_json_data]}
            data_ = Engine.clean_up(str(data_structured_))
            self.logger.debug(data_)
            sys.exit()

    @staticmethod
    def mth_end(mth, year):
        # Convert year to an integer for leap year calculations
        year = int(year)
        # Check if the month is one of the months with 30 days
        end_day = "30" if mth in ["04", "06", "09", "11"] else "31"

        # Adjust for February (28 or 29 days)
        if mth == "02":
            # Check for leap year for February
            end_day = "29" if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else "28"
        return end_day

    def datavalues(self):
        # Get the unique values in the 'category Option 5' column
        filtered_data_category_option_combos = self.data_to_process_df['categoryOptionCombos.id'].unique()
        today_date = datetime.today().strftime('%Y-%m-%d')
        for year in self.generate_years():
            # self.logger.debug('filtered_data_category_option_combos: ', filtered_data_category_option_combos)
            # data_value_url = None
            # end_year = None
            if self.specific_years is not None:
                end_year = int(year)
                if self.process_months is None:
                    data_value_url = f"{self.source_base_url}dataValueSets?dataSet={self.migration_dataset_id}" \
                                     f"&startDate={year}-01-01&endDate={end_year}-12-31" \
                                     f"&dataElementGroup={self.data_element_group_id}" \
                                     f"&orgUnitGroup={self.org_unit_group}"
                    self.logger.debug(f"{today_date} -startDate={year}-01-01&endDate={end_year}-12-31 - -- {data_value_url}")
                    self.post_values(year, end_year, filtered_data_category_option_combos, data_value_url)
                else:
                    for mth in self.months:
                        if self.process_days is None:
                            data_value_url = f"{self.source_base_url}dataValueSets?dataSet={self.migration_dataset_id}" \
                                             f"&startDate={year}-{mth}-01" \
                                             f"&endDate={end_year}-{mth}-{self.mth_end(mth, year)}" \
                                             f"&dataElementGroup={self.data_element_group_id}" \
                                             f"&orgUnitGroup={self.org_unit_group}"
                            self.logger.debug(f"{today_date} -- {data_value_url}")
                            self.post_values(year, end_year, filtered_data_category_option_combos, data_value_url)
                        else:
                            delta = timedelta(days=3)
                            batch_start_datetime_object = datetime.strptime(f"{year}-{mth}-01", '%Y-%m-%d')
                            batch_end_datetime_object = datetime.strptime(f"{end_year}-{mth}-{self.mth_end(mth, year)}", '%Y-%m-%d')
                            while batch_start_datetime_object <= batch_end_datetime_object:
                                # Calculate the new batch end, but cap it at the end of the month
                                potential_end_date = batch_start_datetime_object + delta
                                new_batch_end_datetime_object = min(potential_end_date, batch_end_datetime_object)

                                data_value_url = f"{self.source_base_url}dataValueSets?dataSet={self.migration_dataset_id}" \
                                                 f"&startDate={batch_start_datetime_object.strftime('%Y-%m-%d')}" \
                                                 f"&endDate={new_batch_end_datetime_object.strftime('%Y-%m-%d')}" \
                                                 f"&dataElementGroup={self.data_element_group_id}" \
                                                 f"&orgUnitGroup={self.org_unit_group}"
                                self.logger.debug(f"{today_date} -- {data_value_url}")
                                self.post_values(year, end_year, filtered_data_category_option_combos, data_value_url)
                                batch_start_datetime_object += delta
            else:
                end_year = int(year) + 3
                if self.process_months is not None:
                    data_value_url = f"{self.source_base_url}dataValueSets?dataSet={self.migration_dataset_id}" \
                                     f"&startDate={year}-01-01&endDate={end_year}-12-31" \
                                     f"&dataElementGroup={self.data_element_group_id}" \
                                     f"&orgUnitGroup={self.org_unit_group}"
                    self.logger.debug(f"{today_date} -- {data_value_url}")
                    self.post_values(f"{year}-01-01", f"{year}-12-31", filtered_data_category_option_combos,
                                     data_value_url)

    def post_values(self, start_date, end_date, filtered_data_category_option_combos, data_value_url):
        _df = None
        data_to_get = None
        try:
            response = self.source_session.get(data_value_url, timeout=600)
            data_to_get = json.loads(response.text)
            response.close()
            self.logger.debug(f"Data pull completed... for &startDate={start_date}&endDate={end_date}")
        except Exception as ex:
            self.logger.debug(f"[{self.klass}] - {ex}")
        if data_to_get is not None:
            if 'dataValues' in data_to_get:
                _df = pd.json_normalize(data_to_get)
                self.logger.debug(f"normalized data been processed... ")
        else:
            self.logger.debug(f"Error getting response data.")

        if _df is not None:
            if not _df.empty:
                try:  ## implemented the exception to catch empty requests
                    self.logger.debug(f"dataValues pulled - {len(_df['dataValues'][0])}")
                    if len(_df['dataValues'][0]) > 0:
                        # self.logger.debug("************ *************** *************")
                        # self.logger.debug(_df['dataValues'][0])
                        # self.logger.debug("************ *************** *************")
                        df0 = pd.DataFrame(_df['dataValues'][0])
                        ## deleter
                        # del df0['value']
                        # df0['value'] = ""

                        df0 = df0[
                            ['dataElement', 'period', 'orgUnit', 'categoryOptionCombo', 'attributeOptionCombo',
                             'value']]
                        self.logger.debug("*** Implementing filtered_data_category_option_combos filter ***")
                        df0_filtered = df0[
                            df0['categoryOptionCombo'].isin(filtered_data_category_option_combos)]
                        # new data element uid
                        df0_filtered = df0_filtered.copy()
                        df0_filtered.loc[:, 'dataElement'] = self.new_data_element
                        df0_filtered.to_csv('after_filter.csv', index=False)
                        with pd.ExcelWriter("after_filter.xlsx") as writer:
                            df0_filtered.to_excel(writer, sheet_name='data_elements', index=False)
                        after_filter_df = pd.read_excel("after_filter.xlsx", sheet_name=0)
                        self.logger.debug("*** after_filter.xlsx file for last post created ***")
                        # Batch size
                        batch_size = 500

                        # Split the DataFrame into chunks of 1000 rows each
                        retries_message = "Normal Post"
                        df_batches = np.array_split(after_filter_df, len(after_filter_df) // batch_size + 1)
                        for i, df_batch in enumerate(df_batches):
                            self.logger.debug(f"*** Processing batch {i + 1} of {len(df_batches)} ***")
                            self.logger.debug(df_batch.head())  # Check the structure of the first few rows
                            with pd.ExcelWriter("filter_df_batch.xlsx") as writer:
                                df_batch.to_excel(writer, sheet_name='data_elements', index=False)
                            filter_df_batch = pd.read_excel("filter_df_batch.xlsx", sheet_name=0)
                            # Check for missing data in this batch
                            missing_data = df_batch[
                                df_batch[
                                    ["dataElement", "period", "orgUnit", "categoryOptionCombo", "value"]].isnull().any(
                                    axis=1)
                            ]

                            if not missing_data.empty:
                                self.logger.debug(f"Batch {i + 1} contains rows with missing data:")
                                continue  # Skip the batch or handle it accordingly
                            # Convert the current batch to JSON
                            converted_to_json = self.DataValueProcessing.get_datavalue(filter_df_batch)

                            # Function to post data with retry logic
                            def push_data(max_retries=2):
                                for attempt in range(1, max_retries + 1):
                                    self.logger.debug(f"Attempt {attempt} of {max_retries} to post data.")

                                    get_data_value = {"dataValues": converted_to_json}
                                    data__ = Engine.clean_up(str(get_data_value))
                                    self.logger.debug("*** Data cleaned ***")

                                    # Post data
                                    r = self.post_data(url=f"{self.destination_base_url}dataValueSets", data=data__)
                                    d = r.json()
                                    conflicts = d.get('conflicts', [])

                                    # If conflicts are found, handle and retry
                                    if conflicts:
                                        for conflict in conflicts:
                                            start_date_ = start_date
                                            end_date_ = end_date
                                            value = conflict.get('value')
                                            error_code = conflict.get('errorCode')
                                            prop = conflict.get('property')

                                            self.logger.debug(f"Value: {value}")
                                            self.logger.debug(f"Error Code: {error_code}")
                                            self.logger.debug(f"Property: {prop}")
                                            self.logger.debug("---")

                                            self.error_data.append([
                                                self.data_element_in_view, start_date_, end_date_, value, error_code,
                                                prop
                                            ])

                                        self.error_data_saving()
                                        fix_errors__ = FixErrors(engine_class=self)
                                        fix_errors__.extract_metadata(triggered='Automatically')
                                    else:
                                        # If no conflicts, log and return
                                        log_message = f"Data posted successfully for batch {i + 1}"
                                        self.logger.debug(log_message)
                                        with open(self.posted_file_path, 'a') as file:
                                            file.write(log_message + "\n")
                                        r.close()
                                        del d, r, get_data_value
                                        return True  # Success

                                    r.close()
                                    del d, r, get_data_value
                                self.logger.debug(
                                    f"Max retries reached for batch {i + 1}. Conflicts remain unresolved.")
                                return False  # Failure after retries
                            push_data()
                        del df0, _df, df0_filtered

                    else:
                        self.logger.debug(f"_df is empty (1)")
                        return True
                except Exception as e:
                    self.logger.debug(f"_df is empty (2) {e}")
                    return True
            else:
                self.logger.debug(f"_df is empty (3)")
                return True
        else:
            self.logger.debug(f" _df is empty")
            return True

    @staticmethod
    def clean_up(clean):
        dictionary = {'\'': '"', '.0': '',
                      'False': 'false',
                        'True': 'true',
                          'nan': '',}
        for key in dictionary.keys():
            clean = clean.replace(key, dictionary[key])
        return clean

    @staticmethod
    def create_csv(data, obj):
        try:
            with open('filter_df.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                headers = data[f'{obj}'][0].keys()
                # Write the headers
                writer.writerow(headers)

                # Write the data
                for row in data[f'{obj}']:
                    writer.writerow([row[key] for key in headers])
        except Exception as e:
            pass

    def generate_years(self):
        # Get the current year
        if self.specific_years is not None:
            # If specific years are provided, return them as strings (ensure they're unique and sorted)
            self.logger.debug(f"Specific year is enabled -- {self.specific_years}")
            return sorted(set(str(year) for year in self.specific_years))
        else:
            # Generate years starting from the current year, and decrement by 3 years
            current_year = int(datetime.today().strftime('%Y'))
            self.logger.debug("Dynamic years enabled")
            return sorted([str(current_year - i * 3) for i in range((self.years_back // 3) + 1) if
                           current_year - i * 3 >= current_year - self.years_back])

    def delete_datavalues(self, data_element_in_view_to_delete):
        self.update_dataset(data_element_in_view_to_delete)
        self.data_element_in_view = data_element_in_view_to_delete
        self.create_update_data_element_group(mode='update')
        for year in self.generate_years():
            duration = f"startDate={year}-01-01&endDate={year}-12-31"
            data_value_url = f"{self.destination_base_url}dataValueSets?dataSet={self.migration_dataset_id}&{duration}" \
                             f"&dataElementGroup={self.data_element_group_id}" \
                             f"&orgUnitGroup={self.org_unit_group}"
            _df = None
            data_to_get = None
            try:
                response = self.destination_session.get(data_value_url, timeout=600)
                data_to_get = json.loads(response.text)
                response.close()
                self.logger.debug(f"Data pull completed for ... {duration}")
            except Exception as ex:
                self.logger.debug(f"[{self.klass}] - {ex}")
            if data_to_get is not None:
                if 'dataValues' in data_to_get:
                    _df = pd.json_normalize(data_to_get)
                    self.logger.debug(f"normalized data been processed... ")
            else:
                self.logger.debug(f"Error getting response data.")

            if _df is not None:
                if not _df.empty:
                    try:  ## implemented the exception to catch empty requests
                        self.logger.debug(f"dataValues pulled - {len(_df['dataValues'][0])}")
                        if len(_df['dataValues'][0]) > 0:
                            df0 = pd.DataFrame(_df['dataValues'][0])
                            df0 = df0[
                                ['dataElement', 'period', 'orgUnit', 'categoryOptionCombo', 'attributeOptionCombo',
                                 'value']]

                            # new data element uid
                            df0 = df0.copy()
                            with pd.ExcelWriter("data_value_to_delete.xlsx") as writer:
                                df0.to_excel(writer, sheet_name='to_delete', index=False)
                            after_filter_df = pd.read_excel("data_value_to_delete.xlsx", sheet_name=0)

                            converted_to_json = self.DataValueProcessing.get_datavalue(after_filter_df)
                            get_datavalue = {"dataValues": converted_to_json}

                            data = Engine.clean_up(str(get_datavalue))
                            self.logger.debug("======== data Before Start ========")
                            self.logger.debug(data)
                            self.logger.debug("======== data End Start ========")
                            params = {'importStrategy': 'DELETE'}
                            r = self.post_data(
                                url=f"{self.destination_base_url}dataValueSets", data=data, params=params
                            )
                            d = r.json()
                            r.close()
                            self.logger.debug(f"-posting-{d}")
                            # del get_datavalue, df0, _df, df0_filtered

                            del d, r, get_datavalue, df0, _df

                        else:
                            self.logger.debug(f"dataset empty, nothing to delete ")
                            pass
                    except Exception as e:
                        self.logger.debug(f"deleting for period : _df is empty ")
                else:
                    self.logger.debug(f" dataset empty, nothing to delete")
                    pass
            else:
                self.logger.debug(f" dataset empty, nothing to delete")
                pass

    def error_data_saving(self):
        try:
            error_data_df = pd.DataFrame(self.error_data, columns=['DataElement', 'Start Date', 'End Date', 'Value',
                                                                   'Error Code', 'Property'])

            # Check if file exists
            if not os.path.exists(self.err_file_path):
                # If file does not exist, write header
                error_data_df.to_csv(self.err_file_path, index=False)
            else:
                # If file exists, append without writing the header again
                error_data_df.to_csv(self.err_file_path, mode='a', index=False, header=False)

        except (IOError, OSError) as e:
            # Handle potential I/O errors
            self.logger.debug(f"Error writing to file {self.err_file_path}: {e}")
        self.error_data = []

class FixErrors:
    def __init__(self, engine_class=None):

        # Engine instance is passed in and can be used selectively
        self.engine = engine_class
        self.count = 6
        self.structured_data = []


    def structure_errors_data(self, org_unit=None, attribute_option_combo=None, category_option_combo=None, data_element=None,
                   error_code=None):
        """
        A placeholder for the actual fix_errors implementation.
        It should take in values such as org_unit, attribute_option_combo, etc.,
        and handle error fixing logic.
        """
        error_data = (
            {"org_unit": org_unit,
             "category_option_combo": category_option_combo,
             "attribute_option_combo": attribute_option_combo,
             "data_element": data_element
             })

        # Filter out keys with None values
        filtered_data = {k: v for k, v in error_data.items() if v is not None}

        # Append the filtered data to structured_data
        if filtered_data:  # Only append if there's data left after filtering
            self.structured_data.append(filtered_data)

    def resolve_errors(self):
        # Grouping the data by attribute_option_combo
        aoc_grouped_data = {}

        # Extract unique items with 'attribute_option_combo' or category_option_combo and then make the list unique
        attribute_combo_items = list({(item['org_unit'], item['attribute_option_combo']): item for item
                                      in self.structured_data if "attribute_option_combo" in item}.values())
        print("attribute_combo_items: ", attribute_combo_items)
        category_options_combo_items = list({(item['data_element'], item['category_option_combo']): item for item
                                             in self.structured_data if "category_option_combo" in item}.values())
        print("category_options_combo_items: ", category_options_combo_items)
        # List of dataElement IDs
        self_test_data_element_and_coc_ids = [
            "BXaeeRcNC1w",
            "GlfMR8YUlsQ",
            "SENLBIzJJi8",
            "ns4Ckbq5FvQ",
            "jLrgY7klJdM",
            "WL1H0ysVO8m",
            "xEVxb6wy4ky",
            "EJxre1x0VgD",
            "ZFPaJ7pQ0Fl",
            "xqpn4UoFXOG",
            "MByYBLdGmqS",
            "LYsAcDNybjW",
            "B27oFNJWjYS",
            "w58CMjcedn3",
            "Cn3JXp4vNyG",
            "HTS_SELF: Continuation",
            "HTS_SELF_CONFIRMED: Continuation",
            "HTS_SELF_LINKED: Continuation",
            "HTS_SELF_REACTIVE: Continuation",
            "HTS_SELF_USED: Continuation"
        ]

        if len(category_options_combo_items) > 0:
            _data = []
            dev_session = rq.Session()  # Consider using a session for connection reuse
            dev_session.auth = ('aejakhegbe', '%Wekgc7345dgfgfq#')
            dev_url = 'https://infolink-dev.baosystems.com/api/29/'
            n = 0
            for entry in category_options_combo_items:
                print(f"Processing category_options_combo_items index {n}")
                if entry['data_element'] not in self_test_data_element_and_coc_ids:
                    coc_data_ = self.engine.get_url_data(f"{self.engine.destination_base_url}categoryOptionCombos/"
                                                         f"{entry['category_option_combo']}.json")
                    de_data_ = self.engine.get_url_data(
                        f"{self.engine.destination_base_url}dataElements/{entry['data_element']}.json")
                    # Load the JSON string into a Python dictionary
                    de_data_json = json.loads(json.dumps(de_data_))

                    # Access the httpStatusCode
                    http_status_code = de_data_json['httpStatusCode']
                    if http_status_code == 404:

                        # print("Pulling   - HTTP Status Code:", http_status_code)

                        de_data_ = self.engine.get_url_data(
                            f"{dev_url}dataElements/{entry['data_element']}.json",
                            session=dev_session)
                        # data_structured_ = {f"dataElements": [de_data_]}
                        # data__ = Engine.clean_up(str(data_structured_))
                        # logger.debug("data_structured: ", data__)
                        # params = {'importStrategy': 'CREATE_UPDATE'}
                        # create_de_response_update_ = self.post_data(url=f"{self.engine.destination_base_url}metadata",
                        #                                             data_=data__, params=params)
                        # logger.debug("Status %s", json.dumps(create_de_response_update_.status_code))
                        # logger.debug("Response %s", json.dumps(create_de_response_update_.text))
                        # logger.debug(f'++ Updating or creating Data Element {de_data_["name"]} ++')
                    if de_data_['name'] not in self_test_data_element_and_coc_ids:
                        if de_data_["categoryCombo"]["id"] not in self_test_data_element_and_coc_ids:
                            try:
                                logger.debug(f"Processing for data element {de_data_['id']} - {de_data_['name']}")
                                coc_data_["categoryCombo"]["id"] = de_data_["categoryCombo"]["id"]

                                del coc_data_["lastUpdatedBy"]
                                del coc_data_["created"]
                                del coc_data_["lastUpdated"]
                                del coc_data_["href"]
                                _data.append(coc_data_)
                            except Exception as e:
                                logger.debug(f"Error for data element {de_data_}")
                                logger.debug(e)
                    n = n + 1
            # Use a set to track unique IDs and filter duplicates
            unique_ids = set()
            unique_combos = []

            for combo in _data:
                if combo['id'] not in unique_ids:
                    unique_ids.add(combo['id'])
                    unique_combos.append(combo)
            coc_data_structured_ = {"categoryOptionCombos": unique_combos}
            category_option_combos_data_ = self.engine.clean_up(str(coc_data_structured_))
            print(category_option_combos_data_)
            params = {'importStrategy': 'UPDATE'}
            category_option_combos_response_update_ = self.post_data(url=f"{self.engine.destination_base_url}metadata",
                                                                     data_=category_option_combos_data_, params=params)
            logger.debug("Status %s", json.dumps(category_option_combos_response_update_.status_code))
            logger.debug("Response %s", json.dumps(category_option_combos_response_update_.text))

        # print("attribute_combo_items: ", attribute_combo_items)
        if len(attribute_combo_items) > 0:
            for entry in attribute_combo_items:
                attribute_combo = entry['attribute_option_combo']
                org_unit = entry['org_unit']

                if attribute_combo not in aoc_grouped_data:
                    aoc_grouped_data[attribute_combo] = []  # Initialize a new list if key doesn't exist

                aoc_grouped_data[attribute_combo].append(org_unit)  # Append the org_unit to the respective list

            for attribute_option_combo, org_units in aoc_grouped_data.items():
                aoc_data_ = self.engine.get_url_data(f"{self.engine.destination_base_url}categoryOptionCombos/"
                                                     f"{attribute_option_combo}.json?fields=categoryOptions[id,name]")
                for option in aoc_data_['categoryOptions']:
                    if option['name'] not in ["COP", "DSD"]:
                        cat_option_data = self.engine.get_url_data(
                            f"{self.engine.destination_base_url}categoryOptions/{option['id']}.json")
                        for org_unit in org_units:
                            cat_option_data['organisationUnits'].append({"id": org_unit})
                        del cat_option_data["createdBy"], cat_option_data["lastUpdatedBy"]
                        del cat_option_data["user"], cat_option_data["created"]
                        del cat_option_data["lastUpdated"]
                        del cat_option_data["href"]
                        # print(cat_option_data)
                        # cat_option_data["startDate"] = FixErrors.correct_date_format(cat_option_data["startDate"])
                        coc_data_structured_ = {"categoryOptions": [cat_option_data]}
                        corrected_data_ = self.engine.clean_up(str(coc_data_structured_))
                        # print(coc_data_structured_)
                        params = {'importStrategy': 'UPDATE'}
                        response_update_category_options = self.post_data(url=f"{self.engine.destination_base_url}metadata",
                                                                          data_=corrected_data_, params=params,
                                                                          data_structured_=coc_data_structured_)
                        if json.dumps(response_update_category_options.status_code) == "200":
                            logger.debug("Status %s", json.dumps(response_update_category_options.status_code))
                            logger.debug("Response %s", json.dumps(response_update_category_options.text))
                        else:
                            # Check if 'startDate' exists
                            if "startDate" in cat_option_data:
                                cat_option_data["startDate"] = FixErrors.correct_date_format(cat_option_data["startDate"])
                            else:
                                print("startDate does not exist.")

                            if "endDate" in cat_option_data:
                                cat_option_data["endDate"] = FixErrors.correct_date_format(
                                    cat_option_data["endDate"])
                            new_name_ = FixErrors.clean_utf8_string(cat_option_data['name'])
                            # Collect problematic entries
                            error_data = []

                            error_data.append([
                                cat_option_data['name'],
                                cat_option_data['id'],
                                new_name_  # Placeholder if you want to suggest a corrected name
                            ])
                            #
                            # # Check if file exists
                            error_names_df = pd.DataFrame(error_data, columns=['name', 'id', 'New Name'])
                            if not os.path.exists("Changed Options Names.csv"):
                                # If file does not exist, write header
                                error_names_df.to_csv("Changed Options Names.csv", index=False)
                            else:
                                # If file exists, append without writing the header again
                                error_names_df.to_csv("Changed Options Names.csv", mode='a', index=False, header=False)

                            cat_option_data["name"] = new_name_
                            cat_option_data["displayName"] = new_name_
                            cat_option_data["displayFormName"] = new_name_
                            cat_option_data["displayShortName"] = new_name_
                            cat_option_data["shortName"] = FixErrors.enforce_shortname_limit(new_name_)
                            coc_data_structured_ = {"categoryOptions": [cat_option_data]}
                            corrected_data_ = self.engine.clean_up(str(coc_data_structured_))
                            print(json.dumps(coc_data_structured_))
                            params = {'importStrategy': 'UPDATE'}
                            response_update_category_options = self.post_data(
                                url=f"{self.engine.destination_base_url}metadata",
                                data_=corrected_data_, params=params, data_structured_=coc_data_structured_)
                            logger.debug("Status Again %s", json.dumps(response_update_category_options.status_code))
                            logger.debug("Response Again %s", json.dumps(response_update_category_options.text))

    def post_data(self, url=None, data_=None, params=None, data_structured_=None):
        return self.engine.post_data(url=url, data=data_, params=params, data_structured_=data_structured_)

    # Function to correct the date format
    @staticmethod
    def correct_date_format(date_str):
        return date_str.split('T')[0]  # Extracts only the date part

    @staticmethod
    def clean_utf8_string(text):
        # Normalize the string (NFC normal form)
        # # Normalize to NFKD form, remove non-ASCII characters
        # normalized_text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        # # Remove any extra spaces created during normalization
        # return re.sub(r'\s+', ' ', normalized_text).strip()

        # Normalize to NFKD form to decompose special characters (é -> e)
        normalized_text = unicodedata.normalize('NFKD', text)

        # Remove all non-alphanumeric characters except spaces, hyphens, and parentheses
        cleaned_text = re.sub(r'[^\w\s\-\(\)]', '', normalized_text)
        # Remove any extra spaces created during normalization
        return re.sub(r'\s+', ' ', cleaned_text).strip()

    # Function to ensure 'shortName' is at most 50 characters
    @staticmethod
    def enforce_shortname_limit(short_name):
        if len(short_name) > 50:
            # Truncate and optionally ensure no word is split
            return short_name[:50].rsplit(' ', 1)[0].strip()  # Truncate to 50 chars, avoid breaking the last word
        return short_name

    def get_structured_data(self):
        return self.structured_data

    def extract_metadata(self, triggered='Manually'):
        # Load the CSV file
        logger.debug(f"Resolving conflict started: triggered {triggered}")
        file_path = 'conflicts_data.csv'  # Replace with your actual file path
        error_data_ = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
        del error_data_["Start Date"]
        del error_data_["End Date"]
        # Keep only unique rows based on 'Value', 'Error Code', and 'Property'
        error_data_unique = error_data_.drop_duplicates(subset=["Value", "Error Code", "Property"])
        logger.debug(f"Unique records in {file_path}  - length => {len(error_data_unique)}")
        # Iterate through each row of the CSV
        for index_, row in error_data_unique.iterrows():
            # Extract the relevant columns: 'Value', 'Error Code', 'Property'
            value = row['Value']
            error_code = row['Error Code']
            # Define regex patterns based on the type of error in the 'Value' column
            org_unit_pattern = r"Organisation unit: `([^`]+)` is not valid for attribute option combo: `([^`]+)`"
            category_combo_pattern = r"Category option combo: `([^`]+)` must be part of category combo of data element: `([^`]+)`"

            if row['Property'] == "orgUnit":
                # Match organisation unit and attribute option combo
                org_unit_match = re.search(org_unit_pattern, value)
                if org_unit_match:
                    org_unit = org_unit_match.group(1)
                    attribute_option_combo = org_unit_match.group(2)
                    # Call fix_errors for Organisation Unit
                    self.structure_errors_data(org_unit=org_unit,
                                               attribute_option_combo=attribute_option_combo,
                                               error_code=error_code)
            if row['Property'] == "categoryOptionCombo":
                # Match category option combo and data element
                category_combo_match = re.search(category_combo_pattern, value)
                if category_combo_match:
                    category_option_combo = category_combo_match.group(1)
                    data_element = category_combo_match.group(2)
                    # Call fix_errors for Category Option Combo
                    self.structure_errors_data(category_option_combo=category_option_combo,
                                               data_element=data_element,
                                               error_code=error_code)

        # unique_data = list({(item['category_option_combo'], item['data_element']): item for item in self.get_structured_data()}.values())
        self.resolve_errors()


class DataValueProcessing:
    def __init__(self, log=None):
        self.url = None
        self.result = None
        self._logger = log

    @property
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, log):
        self._logger = log

    def get_datavalue(self, df):
        json_list = []
        # Convert DataFrame to JSON
        # json_data = df.to_json(orient='records')
        n = 0
        for row in df.values:
            try:
                value = df["value"][n]
                # Exclude only if value is NaN
                if not math.isnan(value):  # This works regardless if value is a float or integer
                    json_list.append({
                        "dataElement": df["dataElement"][n],
                        "period": str(df["period"][n]),
                        "orgUnit": df["orgUnit"][n],
                        "categoryOptionCombo": df["categoryOptionCombo"][n],
                        "attributeOptionCombo": df["attributeOptionCombo"][n],
                        "value": str(df["value"][n])
                    })
            except Exception as e:
                self.logger.debug(f'{n} - Error : {e}')
            n = n + 1
        # return json_data #json_list
        return json_list  # json_list


if __name__ == "__main__":
    # Get current date and time as a string including hours, minutes, and seconds
    # Define the file path
    post_file_path = 'posted data element.txt'

    # Step 1: Delete the file if it exists at the start of the script
    if os.path.exists(post_file_path):
        os.remove(post_file_path)
    # Set up the logger
    log_formatter = LogFormat(log_file_name="app_log", destination_folder="logs")
    logger = log_formatter.config()
    today_date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    logger.debug(f"Started at {today_date_time}")
    # List of sheet names to read
    # sheets_to_read = ["updated CatCombos"]
    # data_source_main = pd.read_excel("Data element by dataset and metadata.xlsx", sheet_name=sheets_to_read)
    # List of sheet names to read

    # df2 = pd.read_csv('Get COC_uid alongside COC_name.csv', encoding="ISO-8859-1")

    # Dynamic generation
    processing_years = {
                            'dynamic_years': 10,
                            'specific_years': [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
                            'process_months': None,
                            'process_days': None
                        }  # 'specific_years=None [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]', #process_months=None or yes,
    update_specific_coc__ = None #["UWYwVkw9n11"] #None
    # default is None  "These are specific CoCs to be processed"  ["rMtYKTzMGAW"]
    process_category_combination_maintenance = False  # default is True (True runs the CC configurations)
    process_data_values = True  # Migrate data Value after metadata functions
    org_unit_group_ = 'DoVcSNLg5rm' # should be automated soon
    connection_ = Connection(logger)
    gen = Engine(connection_, logger, org_unit_group=org_unit_group_,
                 posted_file_path=post_file_path,
                 years=processing_years)
    deletion = False
    maintenance = False  # default is False (False runs the COC configurations)
    specific_push = False
    mode = 'process_metadata_and_process_data_values' #['process_metadata_and_process_data_values', 'import_export_metadata']
    dataSetName = "Migration DataSet"  # "Migrating DataSet Default" #Migrating DataSet
    fix_errors = True  # default is False (False runs the COC configurations)

    if gen.ping_connections():
        if not maintenance:
            df = pd.read_csv('updated CatCombos.csv', low_memory=False)
            gen.set_df(df)
            if not specific_push:
                if mode == 'process_metadata_and_process_data_values':
                    unique_new_data_elements_name = df['Proposed new Data element Name'].unique()
                    all_unique_data_elements_ids_list = set(df['dataElement.id'])
                    index = 0
                    total_elements = len(all_unique_data_elements_ids_list)
                    logger.debug(
                        f"Processing {index}/{total_elements} 0% complete")
                    unique_new_data_elements_name_list = list(unique_new_data_elements_name)
                    for new_data_element_name_filter_ in unique_new_data_elements_name_list:
                        data_to_process_df_first_level = gen.data_to_process(new_data_element_name_filter_)
                        unique_data_elements = data_to_process_df_first_level['dataElement.id'].unique()

                        unique_counts = {}
                        for column in data_to_process_df_first_level.columns:
                            if 'category Option' in column:
                                # Check if the column contains NaN values
                                if data_to_process_df_first_level[column].notna().all():  # Ensures the column has no NaN values
                                    # Calculate unique values using set
                                    unique_counts[column] = len(set(data_to_process_df_first_level[column]))
                        logger.debug(unique_counts)
                        min_unique_column = min(unique_counts, key=unique_counts.get)
                        logger.debug(f"Filtering by column - {min_unique_column}")
                        gen.set_filter_column(min_unique_column)

                        filter_obj = data_to_process_df_first_level[f'{min_unique_column}'].unique()
                        unique_data_elements_list = list(unique_data_elements)
                        category_combination_ = None
                        old_category_combination_ = None
                        data_element_group_ = None
                        data_set_ = None
                        new_data_element_ = None
                        filter_list = list(filter_obj)
                        logger.debug("unique data elements list: %s", json.dumps(unique_data_elements_list))
                        logger.debug("Filters: %s", json.dumps(filter_list))
                        category_combination_name = None
                        obj_checked = set()

                        for dataElement in unique_data_elements_list:
                            # self.logger.debug(f"{new_data_element_name_filter_} --- {dataElement}")
                            for filter_ in filter_list:
                                data_to_process_df = gen.data_to_process(filter_, dataElement)
                                if data_to_process_df is not None:
                                    category_combination_name = data_to_process_df.iloc[0]['Proposed CatCombos']
                                    old_category_combination_ = data_to_process_df.iloc[0]['Current categoryCombo.id']
                                    if category_combination_name not in obj_checked:
                                        logger.debug(f"Processing {category_combination_name}")
                                        category_combination_ = gen.create_check_metadata(metadata='categoryCombos',
                                            mode='check', target_name=category_combination_name, json_obj={})
                                        if category_combination_ is None:
                                            category_combination_ = gen.create_check_metadata(metadata='categoryCombos',
                                                mode='create', target_name=category_combination_name, json_obj={})

                                    if "Data Migration Group" not in obj_checked:
                                        logger.debug(f"Processing Data Migration Group")
                                        data_element_group_ = gen.create_check_metadata(metadata='dataElementGroups',
                                                mode='check', target_name="Data Migration Group", json_obj={})
                                        if data_element_group_ is None:
                                            data_element_group_ = gen.create_check_metadata(metadata='dataElementGroups',
                                                mode='create', target_name="Data Migration Group", json_obj={})

                                    if dataSetName not in obj_checked:
                                        logger.debug(f"Processing {dataSetName}")
                                        data_set_ = gen.create_check_metadata(metadata='dataSets',
                                            mode='check', target_name=dataSetName, json_obj={})
                                        if data_set_ is None:
                                            data_set_ = gen.create_check_metadata(metadata='dataSets',
                                                mode='create', target_name=dataSetName,  json_obj={'name' : dataSetName})

                                    attribute_values_json = [
                                        {
                                            "attribute": {
                                                "id": "HazSRVC04rO"
                                            },
                                            "value": data_to_process_df.iloc[0]['Proposed new Data element Name']
                                        },
                                        {
                                            "attribute": {
                                                "id": "I1UUL3vTmdi"
                                            },
                                            "value": "MER"
                                        }
                                    ]

                                    dataElementName = f'{data_to_process_df.iloc[0]["Proposed new Data element Name"]}: ' \
                                                      f'Continuation'
                                    if dataElementName not in obj_checked:
                                        logger.debug(f"Processing dataElement Name")
                                        new_data_element_ = gen.create_check_metadata(metadata='dataElements',
                                                                                  mode='check',
                                                                                  target_name=dataElementName, json_obj={})
                                        if new_data_element_ is None:
                                            logger.debug(f"dataElement name exists ==> {new_data_element_}")
                                            new_data_element_ = gen.create_check_metadata(
                                                metadata='dataElements',
                                                mode='create',
                                                target_name=dataElementName,
                                                json_obj={
                                                        "name": f'{data_to_process_df.iloc[0]["Proposed new Data element Name"]}'
                                                               f': Continuation',
                                                        "short_name": data_to_process_df.iloc[0]["Proposed new Data element Name"],
                                                        "form_name": data_to_process_df.iloc[0]["Proposed new Data element Name"],
                                                        "description": data_to_process_df.iloc[0]["Proposed new Data element Name"],
                                                        "attribute_values": attribute_values_json,
                                                        "category_combination": category_combination_
                                                }
                                            )
                                    obj_checked.update([category_combination_name, "Data Migration Group", dataSetName,
                                                        dataElementName])
                                    gen.process_metadata(filter_item=filter_,
                                                         co_id=category_combination_,
                                                         old_cc_id=old_category_combination_,
                                                         new_name=category_combination_name,
                                                         new_data_element=new_data_element_,
                                                         data_element_in_view=dataElement,
                                                         update_specific_coc_=update_specific_coc__,
                                                         process_category_combination_maintenance_=
                                                         process_category_combination_maintenance,
                                                         process_data_values_=process_data_values)
                                if process_data_values is False:
                                    break  # Exit the loop because data values are not processed

                            index = index + 1
                            percentage = (index / total_elements) * 100
                            logger.debug(
                                f"Processed {index}/{total_elements} now at - {dataElement}: {percentage:.2f}% complete")
                else:
                    df_coc_ = pd.read_csv('Update CoCs.csv', encoding='utf-8', low_memory=False)
                    unique_cat_combos_names = df['Proposed CatCombos'].unique()
                    index = 0
                    total_cat_combos = len(unique_cat_combos_names)
                    logger.debug(
                        f"Processing {index}/{total_cat_combos} 0% complete")
                    for category_combination_name in unique_cat_combos_names:
                        logger.debug(f"Processing {category_combination_name}")
                        category_combination_id_ = gen.create_check_metadata(metadata='categoryCombos',
                                                                          mode='check',
                                                                          target_name=category_combination_name,
                                                                          json_obj={})
                        if category_combination_id_ is None:
                            category_combination_id_ = gen.create_check_metadata(metadata='categoryCombos',
                                                                              mode='create',
                                                                              target_name=category_combination_name,
                                                                              json_obj={})
                        logger.debug(
                            f"{category_combination_name} exists {category_combination_id_} ")
                        if category_combination_id_ is not None:
                            logger.debug(f"{gen.source_base_url}categoryCombos/{category_combination_id_}.json")
                            coc_data = gen.get_url_data(f"{gen.source_base_url}categoryCombos/{category_combination_id_}.json")

                            logger.debug(coc_data["id"])

                            # category_combination_id = gen.create_check_metadata(target_url=gen.destination_base_url,metadata='categoryCombos',
                            #                                                     mode='create',
                            #                                                     target_name=category_combination_name,
                            #                                                     json_obj={"coc_id": coc_data["id"]})
                            del coc_data["createdBy"], coc_data["lastUpdatedBy"]
                            del coc_data["user"], coc_data["created"]
                            del coc_data["lastUpdated"]
                            del coc_data["href"]
                            #coc_data["shortName"] = category_combination_id

                            data_structured = {"categoryCombos": [coc_data]}
                            data_ = gen.clean_up(str(data_structured))
                            response_update = gen.post_data(url=f"{gen.destination_base_url}metadata", data=data_, params={'importStrategy': 'CREATE_AND_UPDATE'})
                            # logger.debug(coc_data)
                            logger.debug('++ Updating CategoryCombo ++ ')
                            logger.debug(f"{gen.destination_base_url}metadata")
                            logger.debug("Status %s", json.dumps(response_update.status_code))
                            # logger.debug("response %s", json.dumps(response_update.text))
                            coc_data_update = gen.get_url_data(f"{gen.source_base_url}categoryCombos/"
                                                               f"{category_combination_id_}.json?"
                                                               f"fields=categoryOptionCombos[id, name, "
                                                               f"displayShortName, displayName, "
                                                               f"displayFormName, categoryOptions, categoryCombo]")
                            renamed_cat_options_combos = gen.update_coc_name(coc_data_update["categoryOptionCombos"],
                                                                             df_coc_)
                            data_structured = {"categoryOptionCombos": renamed_cat_options_combos}
                            payload = json.dumps(data_structured, ensure_ascii=False).encode('utf-8')
                            data = gen.clean_up(str(data_structured))
                            # print(data)
                            response_update_ = gen.post_data(url=f"{gen.destination_base_url}metadata", data=data,
                                                            params={'importStrategy': 'CREATE_AND_UPDATE'},
                                                            data_structured_=data_structured)
                            response_data_ = response_update_.json()  # Convert response to JSON
                            ignored_value_ = response_data_['stats']['ignored']
                            if ignored_value_ != 0:
                                error_reports = response_data_['typeReports'][0]['objectReports'][0]['errorReports']
                                logger.debug(f"ignored_value - {category_combination_name}")
                                with open("problematics CoCs.txt", 'a') as file:
                                    file.write(f"{category_combination_name} - {error_reports}\n\n")
                            print(json.dumps(data_structured))
                            # logger.debug(data_structured)
                            logger.debug("Status %s", json.dumps(response_update_.status_code))
                            logger.debug("response %s", json.dumps(response_update_.text))
                            index = index + 1
                    percentage = (index / total_cat_combos) * 100
                    logger.debug(
                        f"Processed {index}/{total_cat_combos} {percentage:.2f}% complete")
        else:
            if deletion:
                gen.delete_datavalues(data_element_in_view_to_delete="")
            if fix_errors:
                fix_errors_ = FixErrors(engine_class=gen)  # Pass it to FixErrors
                fix_errors_.extract_metadata()
        today_date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"finished processing at {today_date_time}")
