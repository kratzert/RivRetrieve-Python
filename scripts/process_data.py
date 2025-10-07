"""Reads all downloaded CSV files, converts them to xarray Datasets, and concatenates them."""

import os
import glob
import pandas as pd
import xarray as xr
import logging
from tqdm import tqdm

# Configuration
ROOT_DIR = "downloaded_data"
OUTPUT_FILE = "all_streamflow.zarr"
COMMON_START_DATE = "1950-01-01"
COMMON_END_DATE = "2025-10-06"
DATE_RANGE = pd.date_range(start=COMMON_START_DATE, end=COMMON_END_DATE, freq='D')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_csv_to_xarray(file_path):
    """Reads a CSV file and converts it to a a properly formatted xarray Dataset."""
    try:
        # Extract country and site_id
        parts = file_path.split(os.sep)
        country = parts[-2]
        sanitized_site_id = os.path.splitext(parts[-1])[0]

        site_id = sanitized_site_id
        if country == "uk":
            # For UK, extract the UUID part after the last '_'
            site_id = sanitized_site_id.split('_')[-1]

        gauge_id = f"{country}_{site_id}"

        df = pd.read_csv(file_path)

        if 'Date' not in df.columns or 'Q' not in df.columns:
            logging.warning(f"Skipping {file_path}: Missing 'Date' or 'Q' column.")
            return None

        df['Date'] = pd.to_datetime(df['Date'])
        df = df.rename(columns={"Date": "time", "Q": "streamflow"})
        df = df.set_index('time')

        # Reindex to the common date range
        df = df.reindex(DATE_RANGE)

        if df.empty:
            logging.info(f"Skipping {file_path}: No data after processing.")
            return None

        # Convert to xarray Dataset
        ds = xr.Dataset.from_dataframe(df)
        ds = ds.assign_coords(gauge_id=gauge_id)
        ds = ds.expand_dims("gauge_id")

        return ds

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None

def main():
    """Main function to process all CSV files."""
    logging.info("Starting data processing...")

    csv_files = glob.glob(os.path.join(ROOT_DIR, "**", "*.csv"), recursive=True)
    logging.info(f"Found {len(csv_files)} CSV files to process.")

    datasets = []

    for file_path in tqdm(csv_files, desc="Processing CSVs"):

        ds = process_csv_to_xarray(file_path)

        if ds is not None:

            datasets.append(ds)

    if not datasets:

        logging.warning("No datasets were successfully processed.")

        return

    logging.info(f"Successfully processed {len(datasets)} files.")

    # Concatenate all datasets
    try:
        logging.info("Concatenating datasets...")
        combined_ds = xr.concat(datasets, dim="gauge_id")
        logging.info("Concatenation complete.")

        # Save the combined dataset to Zarr
        logging.info(f"Saving combined dataset to {OUTPUT_FILE}...")
        combined_ds.to_zarr(OUTPUT_FILE, mode='w')
        logging.info(f"Successfully saved to {OUTPUT_FILE}.")
        print(combined_ds)

    except Exception as e:
        logging.error(f"Error during concatenation or saving: {e}")

    logging.info("Data processing finished.")

if __name__ == "__main__":
    main()