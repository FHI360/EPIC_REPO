# DHIS2 Event Processing and Posting Script

This Python script is designed to pull existing DHIS2 events, add data values, then post those events back to the server to update the event.

## Example use
This script has been used when a new data element is added to a program stage. For all old events, this new data element will be blank. You can use this script to set the value of the new data element for each event that was entered prior to the data element being added. 


## General steps
1. Download files
2. Open config_example, update the file with your information, and save with the name config.json
3. Run the script from the command prompt

## Prerequisites (see repo README)

- Python 3.6 or higher

## Installation

1. **Download the files**:
   
   Download and extract the ZIP file from the GitHub repository.

2. **Install required Python libraries**:
   If you do not already have the required libraries installed, do the following:
   1. download requirements.txt from the main repo.
   2. Open the command prompt (windows + R, type cmd > enter)
   3. Change the directory to where you have the requirements.txt file stored using cd. Example:
   ```
   cd Downloads
   ```
   4. Run the following:
   ```
   pip install -r requirements.txt
   ```

## Configuration

1. **Rename the sample configuration file** from `config.sample.json` to `config.json`.
2. **Edit the `config.json` file** to set your parameters:
   - `dhis_uname`: Your DHIS2 username.
   - `dhis_pwd`: Your DHIS2 password.
   - `base_url`: The base URL of your DHIS2 API in this format: https://dhis-epictj.fhi360.org/api/
   - `program`, `programStage`: The unique ID of the program and program stage you want to update the events for.
   - `data_elements`: define the data elements (using their uids) and the values of those data elements that you want added to the events
            `Example`: You want to set "KP_PREV value" to "Known Positive" if "What is your current HIV Status" is "Positive" 
            For the data_elements, you would look up the data element uid of "KP_PREV value" and enter that in the dataElement section. Then you would look up the option set value for Known Positive, and put that value in the value field.
    ```
        {
            "dataElement": "KP_PREV value uid here",
            "value": "option set value for Known Positive here"
        }
    ```
   - `filters`: Define which events you want these data elements added to. There are three options for these filters - equals, not_equal, and is_null
            `Example`: You want to set "KP_PREV value" to "Known Positive" if "What is your current HIV Status" is "Positive"  
            For the filter, you would look up the data element uid of "What is your current HIV Status" and enter that in the dataElement section. Then you would look up the option set value for Positive, and put that value in the value field.
    ```
        {
            "dataElement": "What is your current HIV Status uid here",
            "condition": "equals",
            "value": "option set value for Positive here"
        }
    ```
   - `pageSize`: Specify the number of events to fetch per page for pagination. Default is 10000

## Usage

Run the script from the command line:
```
python "Update Events.py"
```


## Troubleshooting

- **Check the log output** if you encounter issues. The script logs detailed error messages that can help identify configuration mistakes or network issues.
- **Ensure your `config.json` is properly formatted and all required fields are correctly filled out.**

## Contributing

Feel free to fork the repository and submit pull requests. You can also open issues for bugs found or features you think would be beneficial.
