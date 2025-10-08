"""Downloads all available streamflow data from all sites in all countries."""

import os
import importlib
import pkgutil
import concurrent.futures
import logging
import random
import sys
import argparse

# Add rivretrieve to path
sys.path.append(os.path.join(os.path.dirname(__file__), "rivretrieve"))

import rivretrieve

# Configuration
ROOT_DIR = "downloaded_data"
VARIABLE = "discharge"
START_DATE = "1950-01-01"
END_DATE = "2025-10-03"
N_WORKERS = 8

# Japan specific dates
JAPAN_START_DATE = "1980-01-01"
JAPAN_END_DATE = "2024-12-31"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_fetcher_classes():
    """Dynamically imports all RiverDataFetcher subclasses from rivretrieve."""
    fetchers = {}
    package = rivretrieve
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        if module_name not in ["base", "utils"]:
            module = importlib.import_module(f".{module_name}", package.__name__)
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                try:
                    if issubclass(attribute, rivretrieve.base.RiverDataFetcher) and attribute is not rivretrieve.base.RiverDataFetcher:
                        fetchers[module_name] = attribute
                        logging.info(f"Found fetcher: {attribute_name} in {module_name}")
                except TypeError:
                    continue
    return fetchers

def download_site_data(country, fetcher_class, site_id, start_date, end_date):
    """Downloads and saves data for a single site."""
    output_dir = os.path.join(ROOT_DIR, country)
    os.makedirs(output_dir, exist_ok=True)

    # Sanitize site_id to be used as a filename
    sanitized_site_id = "".join(c if c.isalnum() or c in ['-', '_'] else '_' for c in site_id)
    output_file = os.path.join(output_dir, f"{sanitized_site_id}.csv")

    if os.path.exists(output_file):
        logging.info(f"Skipping {country} - {site_id} (already downloaded)")
        return f"SKIPPED: {country} - {site_id}"

    logging.info(f"Processing {country} - {site_id} with dates {start_date} to {end_date}")
    try:
        fetcher = fetcher_class(site_id=site_id)
        data = fetcher.get_data(variable=VARIABLE, start_date=start_date, end_date=end_date)

        if data is not None and not data.empty:
            data.to_csv(output_file, index=False)
            logging.info(f"Successfully downloaded and saved {country} - {site_id}")
            return f"SUCCESS: {country} - {site_id}"
        else:
            logging.info(f"No data returned for {country} - {site_id}")
            return f"NO DATA: {country} - {site_id}"

    except Exception as e:
        logging.error(f"Error downloading {country} - {site_id}: {e}", exc_info=False)
        return f"FAILED: {country} - {site_id} - {e}"

def main():
    """Main function to download all data."""
    logging.info("Starting data download process...")
    all_fetcher_classes = get_fetcher_classes()
    fetcher_names = list(all_fetcher_classes.keys())

    parser = argparse.ArgumentParser(description="Download river gauge data.")
    parser.add_argument(
        "--fetchers",
        nargs='+',
        choices=fetcher_names + ["all"],
        default=["all"],
        help=f"Specify which fetchers to use. Choices are {fetcher_names + ['all']}"
    )
    args = parser.parse_args()

    selected_fetchers = args.fetchers
    logging.info(f"Selected fetchers: {selected_fetchers}")

    fetcher_classes_to_run = {}
    if "all" in selected_fetchers:
        fetcher_classes_to_run = all_fetcher_classes
    else:
        for fetcher_name in selected_fetchers:
            if fetcher_name in all_fetcher_classes:
                fetcher_classes_to_run[fetcher_name] = all_fetcher_classes[fetcher_name]
            else:
                logging.warning(f"Fetcher '{fetcher_name}' not found, skipping.")

    tasks = []
    for country, fetcher_class in fetcher_classes_to_run.items():
        try:
            sites = fetcher_class.get_sites()
            if sites is None or sites.empty:
                logging.warning(f"No sites found for {country}")
                continue

            current_start_date = START_DATE
            current_end_date = END_DATE
            if country == "japan":
                current_start_date = JAPAN_START_DATE
                current_end_date = JAPAN_END_DATE

            site_id_col = sites.columns[0]  # Assuming the first column is the site ID
            for site_id in sites[site_id_col]:
                tasks.append((country, fetcher_class, site_id, current_start_date, current_end_date))
        except Exception as e:
            logging.error(f"Error getting sites for {country}: {e}")

    random.shuffle(tasks)
    logging.info(f"Found {len(tasks)} total sites to process for fetchers: {list(fetcher_classes_to_run.keys())}.")

    if not tasks:
        logging.info("No tasks to process. Exiting.")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = [executor.submit(download_site_data, *task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                logging.error(f"Error in worker thread: {e}")

    logging.info("Data download process finished.")

if __name__ == "__main__":
    main()

