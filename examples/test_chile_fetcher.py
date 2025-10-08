import logging

import matplotlib.pyplot as plt

from rivretrieve import ChileFetcher
from rivretrieve import constants

logging.basicConfig(level=logging.INFO)

site_ids = [
    "01201005",
]
variable = constants.DISCHARGE

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = ChileFetcher(site_id=site_id)
    print(f"Fetching data for {site_id}...")
    data = fetcher.get_data(variable=variable)
    if not data.empty:
        print(f"Data for {site_id}:")
        print(data.head())
        print(
            f"Time series from {data[constants.TIME_INDEX].min()} to {data[constants.TIME_INDEX].max()}"
        )
        plt.plot(
            data[constants.TIME_INDEX],
            data[constants.DISCHARGE],
            label=site_id,
            marker=".",
            linestyle="-",
        )
    else:
        print(f"No data found for {site_id}")

if "data" in locals() and not data.empty:
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(f"{constants.DISCHARGE} (m3/s)")
    plt.title(f"Chile River Discharge ({site_id} - Full Time Series)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "chile_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
