"""Downloads all available streamflow data from all sites in all countries."""

import argparse
import concurrent.futures
import importlib
import logging
import os
import pkgutil
import random

import rivretrieve
from rivretrieve import constants

# Configuration
ROOT_DIR = "downloaded_data"
VARIABLE = constants.DISCHARGE_DAILY_MEAN  # Default variable to download
START_DATE = "1950-01-01"
END_DATE = "2025-10-03"
N_WORKERS = 1

# Japan specific dates
JAPAN_START_DATE = "1980-01-01"
JAPAN_END_DATE = "2024-12-31"

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_fetcher_classes():
    """Dynamically imports all RiverDataFetcher subclasses from rivretrieve."""
    fetchers = {}
    package = rivretrieve
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        if module_name not in ["base", "utils", "constants"]:
            module = importlib.import_module(f".{module_name}", package.__name__)
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                try:
                    if (
                        issubclass(attribute, rivretrieve.base.RiverDataFetcher)
                        and attribute is not rivretrieve.base.RiverDataFetcher
                    ):
                        fetchers[module_name] = attribute
                        logging.info(f"Found fetcher: {attribute_name} in {module_name}")
                except TypeError:
                    continue
    return fetchers


def download_gauge_data(country, fetcher_instance, gauge_id, variable, start_date, end_date):
    """Downloads and saves data for a single gauge."""
    output_dir = os.path.join(ROOT_DIR, country)
    os.makedirs(output_dir, exist_ok=True)

    # Sanitize gauge_id to be used as a filename
    sanitized_gauge_id = "".join(c if c.isalnum() or c in ["-", "_"] else "_" for c in gauge_id)
    output_file = os.path.join(output_dir, f"{sanitized_gauge_id}_{variable}.csv")

    if os.path.exists(output_file):
        logging.info(f"Skipping {country} - {gauge_id} - {variable} (already downloaded)")
        return f"SKIPPED: {country} - {gauge_id} - {variable}"

    logging.info(f"Processing {country} - {gauge_id} - {variable} with dates {start_date} to {end_date}")
    try:
        if variable not in fetcher_instance.get_available_variables():
            logging.warning(f"Variable {variable} not supported by {country} fetcher, skipping gauge {gauge_id}")
            return f"UNSUPPORTED VARIABLE: {country} - {gauge_id} - {variable}"

        data = fetcher_instance.get_data(
            gauge_id=gauge_id,
            variable=variable,
            start_date=start_date,
            end_date=end_date,
        )

        if data is not None and not data.empty:
            data.to_csv(output_file)
            logging.info(f"Successfully downloaded and saved {country} - {gauge_id} - {variable}")
            return f"SUCCESS: {country} - {gauge_id} - {variable}"
        else:
            logging.info(f"No data returned for {country} - {gauge_id} - {variable}")
            return f"NO DATA: {country} - {gauge_id} - {variable}"

    except Exception as e:
        logging.error(f"Error downloading {country} - {gauge_id} - {variable}: {e}", exc_info=False)
        return f"FAILED: {country} - {gauge_id} - {variable} - {e}"


def main():
    """Main function to download all data."""
    logging.info("Starting data download process...")
    all_fetcher_classes = get_fetcher_classes()
    fetcher_names = list(all_fetcher_classes.keys())

    parser = argparse.ArgumentParser(description="Download river gauge data.")
    parser.add_argument(
        "--fetchers",
        nargs="+",
        choices=fetcher_names + ["all"],
        default=["all"],
        help=f"Specify which fetchers to use. Choices are {fetcher_names + ['all']}",
    )
    parser.add_argument(
        "--variable",
        type=str,
        default=VARIABLE,
        help=f"Variable to download (e.g., {constants.DISCHARGE_DAILY_MEAN}).",
    )
    parser.add_argument("--start_date", type=str, default=START_DATE, help="Start date in YYYY-MM-DD.")
    parser.add_argument("--end_date", type=str, default=END_DATE, help="End date in YYYY-MM-DD.")
    parser.add_argument("--n_workers", type=int, default=N_WORKERS, help="Number of worker threads.")
    args = parser.parse_args()

    selected_fetchers = args.fetchers
    variable_to_download = args.variable
    logging.info(f"Selected fetchers: {selected_fetchers}")
    logging.info(f"Variable to download: {variable_to_download}")

    fetcher_instances = {}
    if "all" in selected_fetchers:
        for country, fetcher_class in all_fetcher_classes.items():
            try:
                fetcher_instances[country] = fetcher_class()
            except Exception as e:
                logging.error(f"Failed to instantiate fetcher for {country}: {e}")
    else:
        for fetcher_name in selected_fetchers:
            if fetcher_name in all_fetcher_classes:
                try:
                    fetcher_instances[fetcher_name] = all_fetcher_classes[fetcher_name]()
                except Exception as e:
                    logging.error(f"Failed to instantiate fetcher for {fetcher_name}: {e}")
            else:
                logging.warning(f"Fetcher '{fetcher_name}' not found, skipping.")

    tasks = []
    for country, fetcher_instance in fetcher_instances.items():
        try:
            sites = fetcher_instance.get_cached_metadata()
            if sites is None or sites.empty:
                logging.warning(f"No sites found for {country}")
                continue

            current_start_date = args.start_date
            current_end_date = args.end_date
            if country == "japan":
                current_start_date = JAPAN_START_DATE
                current_end_date = JAPAN_END_DATE

            for gauge_id in sites.index:
                tasks.append(
                    (
                        country,
                        fetcher_instance,
                        gauge_id,
                        variable_to_download,
                        current_start_date,
                        current_end_date,
                    )
                )
        except Exception as e:
            logging.error(f"Error getting sites for {country}: {e}")

    random.shuffle(tasks)
    logging.info(f"Found {len(tasks)} total sites to process for fetchers: {list(fetcher_instances.keys())}.")

    if not tasks:
        logging.info("No tasks to process. Exiting.")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.n_workers) as executor:
        futures = [executor.submit(download_gauge_data, *task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                logging.error(f"Error in worker thread: {e}")

    logging.info("Data download process finished.")


if __name__ == "__main__":
    main()
