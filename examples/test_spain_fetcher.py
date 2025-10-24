import logging

import matplotlib.pyplot as plt

from rivretrieve import SpainFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_ids = ["1080"]
variable = constants.DISCHARGE_DAILY_MEAN
start_date = "1900-10-01"
end_date = "2025-09-30"  # One hydrological year

plt.figure(figsize=(12, 6))

fetcher = SpainFetcher()
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
            label=gauge_id,
            marker=".",
            linestyle="-",
        )
    else:
        print(f"No data found for {gauge_id}")

if "data" in locals() and not data.empty:
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(f"{variable} (m3/s)")
    plt.title(f"Spain River Discharge ({gauge_ids[0]} - {start_date} to {end_date})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "spain_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
