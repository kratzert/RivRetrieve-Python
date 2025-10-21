# RivRetrieve (Python)

A Python package for facilitating and unifying access to global streamflow data.

> [!NOTE]
> RivRetrieve-Python is currently under active development and has not yet reached a stable version. APIs and functionality are subject to change.

Welcome to RivRetrieve-Python! We're actively developing this library to make global streamflow data more accessible. As an early-stage project, it's not yet stable, and we appreciate your understanding as things evolve. Contributions and feedback are welcome! Please check out the active development under [Issues](https://github.com/kratzert/RivRetrieve-Python/issues) or start posting under [Discussions](https://github.com/kratzert/RivRetrieve-Python/discussions).

- Documentation: [rivretrieve-python.readthedocs.io](https://rivretrieve-python.readthedocs.io/en/latest/) (Work in Progress)

## Background

This package originated as a Python translation of the [RivRetrieve R package](https://github.com/Ryan-Riggs/RivRetrieve). The initial translation was performed by Gemini, with a few manual adjustments for API changes.

Since then, the package has evolved significantly and is under heavy development. See [Issues](https://github.com/kratzert/RivRetrieve-Python/issues) for more details.

Initially, I ([@kratzert](https://github.com/kratzert)) used this project to experiment with the Gemini-CLI. I was surprised by its effectiveness for this purpose. So far, all code, tests, and everything you see is the output of the Gemini-CLI, including this README.md. Using the Gemini-CLI allows for rapid iteration and integration of new fetchers. I may write a blog post later with more details on my custom `Gemini.md` instruction file. For now, I am focusing on prompting it to resolve all open [Issues](https://github.com/kratzert/RivRetrieve-Python/issues).

## Disclaimer

The purpose of this package is to simplify access to streamflow data. All data rights remain with the original providers. Users are responsible for reviewing and adhering to the license terms of each data provider, which can be found on their respective homepages. The MIT license in the LICENSE file applies only to the code of this package, not to any data downloaded through it.

## Installation

1.  Clone or download the repository to your computer.

2.  Set up your environment, for example, using the following command from within the RivRetrieve-Python directory:

    ```bash
    # Creates a virtual Python environment within the directory.
    python3 -m venv .venv
    ```

3.  Activate the virtual environment:

    ```bash
    source .venv/bin/activate
    ```

4.  Install the package and all requirements:

    ```bash
    # The -e makes the installed version editable, in case you want to change some code.
    pip install -e .
    ```

5.  Test the installation:

    ```bash
    # Downloads data for one gauge from the US and saves a plot with the discharge data.
    python examples/test_usa_fetcher.py
    ```

## Example Usage

```python
from rivretrieve import UKEAFetcher

# Create UK-EA specific fetcher object
fetcher = UKEAFetcher()

# Get available sites for the UK
sites = UKEAFetcher.get_gauge_ids()
print(sites.head())

# Example site.
gauge_id = "3c5cba29-2321-4289-a1fd-c355e135f4cb"

# Fetch discharge data.
discharge_data = fetcher.get_data(
    gauge_id=gauge_id, variable="discharge", start_date="2023-01-01", end_date="2023-01-31"
)
print(discharge_data.head())

# Fetch stage data.
stage_data = fetcher.get_data(gauge_id=gauge_id, variable="stage", start_date="2023-01-01", end_date="2023-01-31")
print(stage_data.head())
```