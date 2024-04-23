### About this repo
This repo includes scripts developed my EpiC staff to support management of DHIS2 systems.

### Python Installation

Before running any script in this repo, ensure that you have Python installed on your system. Most scripts require Python 3.6 or higher. However, it is recommended to install the latest version of Python to take advantage of security patches and new features.

#### Checking Python Version

To check if you already have Python installed and determine its version, open a command prompt (from windows type cmd, and press enter), then type:

```bash
python --version
```
or
```bash
python3 --version
```

If Python is installed, this command will display the version number. If the version is below 3.6, or if Python is not found, you will need to install or upgrade it.

#### Installing Python

**Windows:**

1. Download the Python installer from the [official Python website](https://www.python.org/downloads/).
2. Run the installer. Make sure to check the box that says "Add Python to PATH" at the beginning of the installation.
3. Follow the instructions to install Python.

**macOS:**

1. The easiest way to install Python on macOS is via the [Homebrew package manager](https://brew.sh/). If you have Homebrew installed, you can install Python by running:
   ```bash
   brew install python
   ```
2. If you do not wish to use Homebrew, you can download the Python installer from the [official Python website](https://www.python.org/downloads/) and follow the instructions.


### pip Installation

**pip** is the Python package installer. It is included by default with Python versions 3.4 and above.

#### Checking pip Version

To check if pip is installed, run:

```bash
pip --version
```
or
```bash
pip3 --version
```

#### Installing pip

If pip is not installed, you can install it by downloading the `get-pip.py` script:

1. Download the script:
   ```bash
   curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
   ```
2. Run the script:
   ```bash
   python get-pip.py
   ```
