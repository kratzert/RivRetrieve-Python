import logging

import matplotlib.pyplot as plt

from rivretrieve import JapanFetcher

logging.basicConfig(level=logging.INFO)

site_ids = [
    "301011281104010",
]
variable = "discharge"
start_date = "2019-01-01"
end_date = "2019-01-31" # Fetching a few months to test

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = JapanFetcher(site_id=site_id)
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
plt.title("Japan River Discharge - Full Time Series")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "japan_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
