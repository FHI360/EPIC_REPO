import os
import json
import logging
import requests
from tqdm import tqdm

# Set up logging to display info messages on the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path):
    """
    Load the configuration from a JSON file.
    
    Args:
    config_path (str): The file path to the configuration JSON file.
    
    Returns:
    dict: Configuration dictionary.
    """
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        logging.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error("Configuration file not found.")
        return None
    except json.JSONDecodeError:
        logging.error("Error decoding the configuration file.")
        return None

def configure_url(config):
    """
    Configure the base URL for fetching events using parameters from the configuration.
    This URL will be used as a template for pagination.
    
    Args:
    config (dict): Configuration dictionary containing base_url, program, programStage, and pageSize.
    
    Returns:
    str: Base URL template for event fetching.
    """
    return (f"{config['base_url']}events.json?program={config['program']}&programStage={config['programStage']}"
            "&fields=storedBy,enrollment,event,program,programStage,orgUnit,trackedEntityInstance,"
            "occurredAt,dataValues[dataElement,value],updatedAt&pageSize={config['pageSize']}")


def fetch_events(base_url, config):
    """
    Fetch all events using pagination.
    
    Args:
    base_url (str): Base URL for fetching events, without specific page numbers.
    config (dict): Configuration dictionary containing authentication credentials.
    
    Returns:
    list: All fetched events.
    """
    all_events = []
    page = 1
    while True:
        url = f"{base_url}&page={page}"
        try:
            logging.info(f"Fetching events from page {page}...")
            response = requests.get(url, auth=(config['dhis_uname'], config['dhis_pwd']))
            response.raise_for_status()
            data = response.json()
            all_events.extend(data['events'])
            if len(data['events']) < config['pageSize']:
                break  # Exit loop if last page
            page += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch events: {e}")
            break

    logging.info(f"Total events fetched: {len(all_events)}")
    return all_events


def process_events_with_filters(events, filters):
    """
    Process events based on dynamic filtering conditions from the configuration.
    
    Args:
    events (dict): Dictionary of events fetched from DHIS2.
    filters (list): List of filter conditions specified in the config.
    
    Returns:
    list: Filtered events that meet all the specified conditions.
    """
    logging.info("Processing events based on filters...")
    filtered_events = []
    
    for event in events['events']:
        data_values = event['dataValues']
        data_elements = {dv['dataElement']: dv['value'] for dv in data_values}
        
        match = all(
            (filter['condition'] == 'equals' and data_elements.get(filter['dataElement']) == filter['value']) or
            (filter['condition'] == 'not_equal' and data_elements.get(filter['dataElement']) != filter['value']) or
            (filter['condition'] == 'is_null' and data_elements.get(filter['dataElement']) is None)
            for filter in filters
        )
        
        if match:
            filtered_events.append(event)
    
    logging.info(f"Updated {len(filtered_events)} events to match filter criteria.")
    return filtered_events


def post_event(event, post_url, username, password):
    """
    Post a single event to the DHIS2 using the specified credentials.
    
    Args:
    event (dict): The event to post.
    post_url (str): The URL to post the event to.
    username (str): DHIS2 username.
    password (str): DHIS2 password.

    Returns:
    tuple: status code and response text from the server.
    """
    try:
        response = requests.post(post_url, json={'events': [event]}, auth=(username, password))
        if response.status_code == 409:
            # Parse the response body for more details
            response_data = json.loads(response.text)
            imported = response_data.get('response', {}).get('imported', 0)
            ignored = response_data.get('response', {}).get('ignored', 0)
            descriptions = [summary.get('description') for summary in response_data.get('response', {}).get('importSummaries', [])]
            
            # Conditional handling based on 'imported' and 'ignored' values
            if imported > 0 and ignored == 0:
                logging.info(f"Event {event['event']} posted with warnings, but data was imported.")
                return response.status_code, response.text  # Treat this case as less severe or normal.
            elif ignored > 0:
                logging.error(f"Failed to post event {event['event']}: {response.status_code} - Ignored data elements found.")
                for description in descriptions:
                    logging.error(f"Description: {description}")
                return response.status_code, response.text
            
        elif response.status_code != 200:
            logging.error(f"Failed to post event {event['event']}: {response.status_code} - {response.text}")
            return response.status_code, response.text
        
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error posting event {event['event']}: {str(e)}")
        return None

def post_all_events(events, post_url, username, password):
    """
    Post all events to the DHIS2, showing a progress bar.

    Args:
    events (list): List of events to be posted.
    post_url (str): The URL to post the events to.
    username (str): DHIS2 username.
    password (str): DHIS2 password.
    """
    responses = []
    for event in tqdm(events, desc="Posting events"):
        status, response = post_event(event, post_url, username, password)
        responses.append((status, response))
    return responses

def main():
    config_path = 'config.json'
    config = load_config(config_path)
    if config:
        post_url = config['base_url'] + "events"
        url = configure_url(config)
        events = fetch_events(url, config)
        if events:
            filters = config.get('filters', [])
            processed_events = process_events_with_filters(events, filters)
            responses = post_all_events(processed_events, post_url, config['dhis_uname'], config['dhis_pwd'])
            logging.info("Finished posting events.")

if __name__ == "__main__":
    main()
