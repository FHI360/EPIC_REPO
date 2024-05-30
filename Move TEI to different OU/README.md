# DHIS2 Move TEIs Script

This Python script is designed to pull existing DHIS2 TEIs, including their enrollments and events, and transfer the entire TEI to a new organization unit.

## Example use
This script has been used when a user accidentally enters data at the wrong org unit, and the entire TEI and everything associated with it needs to be moved. It cannot be used if you need to move just one event, or a subset of data.

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
   - `ou_destination`: The uid of the org unit that you want to move the TEIs to.
   - `teis_to_move`: The uids of any TEIs you want to move. Keep them in this format: ["fyQz0HJ0q5l","k6jtKHbIRQL"]

## Usage

Run the script from the command line:
```
python "move_teis.py"
```


## Troubleshooting

- **Check the log output** if you encounter issues. The script logs error messages that can help identify configuration mistakes or network issues.
- **Ensure your `config.json` is properly formatted and all required fields are correctly filled out.**

## Contributing

Feel free to fork the repository and submit pull requests. You can also open issues for bugs found or features you think would be beneficial.
