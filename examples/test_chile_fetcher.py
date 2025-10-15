import logging

import matplotlib.pyplot as plt

from rivretrieve import ChileFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_ids = [
    "01201005",
]
variable = constants.DISCHARGE

plt.figure(figsize=(12, 6))

fetcher = ChileFetcher()
for gauge_id in gauge_ids:
    print(f"Fetching data for {gauge_id}...")
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable)
    if not data.empty:
        print(f"Data for {gauge_id}:")
        print(data.head())
        print(f"Time series from {data.index.min()} to {data.index.max()}")
        plt.plot(
            data.index,
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
    plt.title(f"Chile River Discharge ({gauge_ids[0]} - Full Time Series)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "chile_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
