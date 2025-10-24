import logging

import matplotlib.pyplot as plt

from rivretrieve import NorwayFetcher, constants

logging.basicConfig(level=logging.INFO)

# Replace with a valid Norwegian gauge ID, e.g., "12.210.0"
gauge_ids = [
    "100.1.0",
]
variables = [
    constants.DISCHARGE_DAILY_MEAN,
    constants.STAGE_DAILY_MEAN,
    constants.WATER_TEMPERATURE_DAILY_MEAN,
]

fetcher = NorwayFetcher()

for variable in variables:
    plt.figure(figsize=(12, 6))
    has_data = False
    for gauge_id in gauge_ids:
        print(f"Fetching {variable} for {gauge_id}...")
        # Fetching last 5 years for testing
        data = fetcher.get_data(gauge_id=gauge_id, variable=variable)
        if not data.empty:
            print(f"Data for {gauge_id} ({variable}):")
            print(data.head())
            print(f"Time series from {data.index.min()} to {data.index.max()}")
            plt.plot(
                data.index,
                data[variable],
                label=f"{gauge_id} - {variable}",
                marker=".",
                linestyle="-",
            )
            plt.xlim(data.index.min(), data.index.max())
            has_data = True
        else:
            print(f"No data found for {gauge_id} ({variable})")

    if has_data:
        plt.xlabel(constants.TIME_INDEX)
        plt.ylabel(variable)
        plt.title(f"Norway River Data ({gauge_ids[0]})")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plot_path = f"norway_{variable}_plot.png"
        plt.savefig(plot_path)
        print(f"Plot saved to {plot_path}")
    else:
        print(f"No data to plot for {variable}.")

print("Test finished.")
