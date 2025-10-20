import logging

import matplotlib.pyplot as plt

from rivretrieve import PortugalFetcher, constants

logging.basicConfig(level=logging.INFO)

# Example gauge IDs from the SITE_MAP in portugal.py
gauge_ids = [
    "19B/01H",  # A known gauge ID from the map
]
variables = [constants.STAGE_DAILY_MEAN]
start_date = "1980-01-01"
end_date = None  # Defaults to today

fetcher = PortugalFetcher()

for variable in variables:
    plt.figure(figsize=(12, 6))
    print(f"\n--- Testing variable: {variable} ---")
    for gauge_id in gauge_ids:
        print(f"Fetching {variable} for {gauge_id} from {start_date} to {end_date}...")
        data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)
        if not data.empty:
            print(f"Data for {gauge_id}:")
            print(data.head())
            print(f"Time series from {data.index.min()} to {data.index.max()}")
            plt.plot(
                data.index,
                data[variable],
                label=f"{gauge_id} - {variable}",
                marker=".",
                linestyle="-",
            )
        else:
            print(f"No {variable} data found for {gauge_id}")

    if plt.gca().has_data():
        plt.xlabel(constants.TIME_INDEX)
        plt.ylabel(variable)
        plt.title(f"Portugal River Data ({start_date} to {end_date})")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plot_path = f"portugal_{variable}_plot.png"
        plt.savefig(plot_path)
        print(f"Plot saved to {plot_path}")
    else:
        print(f"No data to plot for {variable}.")

print("Test finished.")
