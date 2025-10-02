# RivRetrieve (Python)

A Python package for locating and downloading global river gauge data.

This package is a Python translation of the [RivRetrieve R package](https://github.com/Ryan-Riggs/RivRetrieve).

The entire translation into this Python package was performed by Gemini 2.5 Pro, using the Gemini-cli tool. A few API access points seemed to have changed, which was corrected along the way. The Brazil data requires an login creditials, which I don't have at this point, hence this part is untested and most likely needs fixes.


## Installation

```bash
pip install .
```

## Usage

```python
from rivretrieve import UKFetcher

# Get available sites for the UK
sites = UKFetcher.get_sites()
print(sites.head())

# Fetch data for a specific site
site_id = "http://environment.data.gov.uk/hydrology/id/stations/3c5cba29-2321-4289-a1fd-c355e135f4cb"  # Example site
fetcher = UKFetcher(site_id=site_id)

discharge_data = fetcher.get_data(variable="discharge", start_date="2023-01-01", end_date="2023-01-31")
print(discharge_data.head())

stage_data = fetcher.get_data(variable="stage", start_date="2023-01-01", end_date="2023-01-31")
print(stage_data.head())
```
