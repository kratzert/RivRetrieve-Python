# RivRetrieve (Python)

A Python package for locating and downloading global river gauge data.

This package is a Python translation of the [RivRetrieve R package](https://github.com/Ryan-Riggs/RivRetrieve).

The entire translation into this Python package was performed by Gemini 2.5 Pro, using the Gemini-cli tool. A few API access points seemed to have changed, which was corrected along the way. The Brazil data requires an login creditials, which I don't have at this point, hence this part is untested and most likely needs fixes.

**Disclaimer**

This package is intended solely to simplify access to streamflow data. All data rights remain with the original providers. Users are responsible for reviewing and adhering to the license terms of each data provider, which can be found on their respective homepages. The MIT license in the LICENSE file applies only to the code of this package, not to any data downloaded through it.

## Installation

1. Clone or download the repository to your computer.

2. Setup your evenvironment, e.g. using the following command from within the RivRetrieve-Python directory.

```bash
# Creates a virtual Python environment within the directory.
python3 -m venv .venv
```

3. Install the package and all requirements.


```bash
# The -e makes the installed version editable, in case you want to change some code.
.venv/bin/python3 -m pip install -e .
```

4. Test installation.

```bash
# Downloads data for one gauge from the US and saves a plot with the discharge data.
.venv/bin/python3 examples/test_usa_fetcher.py
```

## Example usage

```python
from rivretrieve import UKEAFetcher

# Create UK-EA specific fetcher object
fetcher = UKEAFetcher()

# Get available sites for the UK
sites = UKEAFetcher.get_gauge_ids()
print(sites.head())

# Fetch data for a specific site
site_id = "3c5cba29-2321-4289-a1fd-c355e135f4cb"  # Example site

discharge_data = fetcher.get_data(variable="discharge", start_date="2023-01-01", end_date="2023-01-31")
print(discharge_data.head())

stage_data = fetcher.get_data(variable="stage", start_date="2023-01-01", end_date="2023-01-31")
print(stage_data.head())
```
