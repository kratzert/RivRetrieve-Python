import logging

import matplotlib.pyplot as plt

from rivretrieve import SloveniaFetcher

logging.basicConfig(level=logging.INFO)

site_ids = [
    "1020",  # Cmurek on Mura
]
variable = "discharge"

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = SloveniaFetcher(site_id=site_id)
    print(f"Fetching all data for {site_id}...")
    data = fetcher.get_data(variable=variable)  # Removed start_date and end_date
    if not data.empty:
        print(f"Data for {site_id}:")
        print(data.head())
        print(f"Time series from {data['Date'].min()} to {data['Date'].max()}")
        plt.plot(data["Date"], data["Q"], label=site_id, marker="o")
    else:
        print(f"No data found for {site_id}")

if "data" in locals() and not data.empty:
    plt.xlabel("Date")
    plt.ylabel("Discharge (m3/s)")
    plt.title(f"Slovenia River Discharge ({site_ids[0]} - Full Time Series)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "slovenia_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
