import logging

import matplotlib.pyplot as plt

from rivretrieve import PolandFetcher
from rivretrieve import constants

logging.basicConfig(level=logging.INFO)

gauge_ids = [
    "149180020",  # CHAŁUPKI on Odra
]
variables = [constants.DISCHARGE, constants.STAGE, constants.WATER_TEMPERATURE]
# Fetch a period from Simon's example
start_date = "1999-01-01"
end_date = "2001-12-31"

for variable in variables:
    plt.figure(figsize=(12, 6))
    print(f"\nTesting variable: {variable}")
    fetcher = PolandFetcher()
    for gauge_id in gauge_ids:
        print(f"Fetching {variable} for {gauge_id} from {start_date} to {end_date}...")
        data = fetcher.get_data(
            gauge_id=gauge_id,
            variable=variable,
            start_date=start_date,
            end_date=end_date,
        )
        if not data.empty:
            print(f"Data for {gauge_id}:")
            print(data.head())
            print(
                f"Time series from {data[constants.TIME_INDEX].min()} to {data[constants.TIME_INDEX].max()}"
            )
            plt.plot(
                data[constants.TIME_INDEX],
                data[variable],
                label=gauge_id,
                marker=".",
                linestyle="-",
            )
        else:
            print(f"No data found for {gauge_id}")

    if "data" in locals() and not data.empty:
        plt.xlabel(constants.TIME_INDEX)
        plt.ylabel(f"{variable}")
        plt.title(
            f"Poland River {variable} ({gauge_ids[0]} - {start_date} to {end_date})"
        )
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plot_path = f"poland_{variable}_plot.png"
        plt.savefig(plot_path)
        print(f"Plot saved to {plot_path}")
    else:
        print(f"No data to plot for {variable}.")

print("Test finished.")
