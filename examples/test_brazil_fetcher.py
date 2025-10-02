import logging

import matplotlib.pyplot as plt

from rivretrieve import BrazilFetcher

logging.basicConfig(level=logging.INFO)

site_ids = [
    "12650000",
]
variable = "discharge"

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = BrazilFetcher(site_id=site_id)
    print(f"Fetching data for {site_id}...")
    data = fetcher.get_data(variable=variable)
    if not data.empty:
        print(f"Data for {site_id}:")
        print(data.head())
        print(f"Time series from {data['Date'].min()} to {data['Date'].max()}")
        plt.plot(data['Date'], data['Q'], label=site_id, marker='.', linestyle='-')
    else:
        print(f"No data found for {site_id}")

if 'data' in locals() and not data.empty:
    plt.xlabel("Date")
    plt.ylabel("Discharge (m3/s)")
    plt.title(f"Brazil River Discharge ({site_id} - Full Time Series)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "brazil_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
