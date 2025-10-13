import logging

import matplotlib.pyplot as plt

from rivretrieve import CanadaFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_ids = [
    "01AD003",
]
variable = constants.DISCHARGE

plt.figure(figsize=(12, 6))

fetcher = CanadaFetcher()
for gauge_id in gauge_ids:
    print(f"Fetching data for {gauge_id}...")
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable)
    if not data.empty:
        print(f"Data for {gauge_id}:")
        print(data.head())
        print(f"Time series from {data[constants.TIME_INDEX].min()} to {data[constants.TIME_INDEX].max()}")
        plt.plot(
            data[constants.TIME_INDEX],
            data[constants.DISCHARGE],
            label=gauge_id,
            marker=".",
            linestyle="-",
        )
    else:
        print(f"No data found for {gauge_id}")

if "data" in locals() and not data.empty:
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(f"{constants.DISCHARGE} (m3/s)")
    plt.title(f"Canada River Discharge ({gauge_ids[0]} - Full Time Series)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "canada_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
