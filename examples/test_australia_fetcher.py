import logging

import matplotlib.pyplot as plt

from rivretrieve import AustraliaFetcher

logging.basicConfig(level=logging.INFO)

site_ids = [
    "403213",
]
variable = "discharge"
# Fetch a recent period for testing
start_date = "2023-10-01"
end_date = "2024-03-31"

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = AustraliaFetcher(site_id=site_id)
    print(f"Fetching data for {site_id} from {start_date} to {end_date}...")
    data = fetcher.get_data(variable=variable)
    if not data.empty:
        print(f"Data for {site_id}:")
        print(data.head())
        print(f"Time series from {data['Date'].min()} to {data['Date'].max()}")
        plt.plot(data['Date'], data['Q'], label=site_id, marker='o')
    else:
        print(f"No data found for {site_id}")

plt.xlabel("Date")
plt.ylabel("Discharge (m3/s)")
plt.title(f"Australia River Discharge ({site_id} - Full Time Series)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "australia_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
