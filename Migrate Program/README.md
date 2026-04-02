# Migrate Program

This notebook exports a DHIS2 program and its related metadata from one DHIS2 instance and imports it into another. It is designed to support program migration between environments, such as moving a program from a development server to production.

The script collects the program itself along with the metadata dependencies needed for that program to function, writes the export payload to disk, and then sends that payload to the target DHIS2 instance using the metadata API.

## What it migrates

For a given `PROGRAM_ID`, the notebook collects:

- Program
- Program stages
- Program stage sections
- Data elements used in the program stages
- Option sets linked to those data elements
- Options linked to those option sets
- Data entry forms used by the program stages
- Program rules
- Program rule actions
- Only the program rule variables actually referenced by the program rules

## What it does

1. Connects to the source DHIS2 instance
2. Pulls the selected program and its related metadata
3. Builds a single metadata payload
4. Saves that payload locally as JSON
5. Connects to the target DHIS2 instance
6. Imports the payload into the target using the DHIS2 metadata API
7. Saves the import response locally for review and troubleshooting

## Configuration

Update the configuration section at the top of the notebook before running it:

```python
SOURCE_BASE_URL = "https://your-source-dhis2.org"
TARGET_BASE_URL = "https://your-target-dhis2.org"
PROGRAM_ID = ""

SOURCE_USERNAME = ""
SOURCE_PASSWORD = ""

TARGET_USERNAME = ""
TARGET_PASSWORD = ""

OUTPUT_DIR = r""

DRY_RUN = True
IMPORT_STRATEGY = "CREATE_AND_UPDATE"
ATOMIC_MODE = "ALL"
