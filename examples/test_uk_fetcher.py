import matplotlib.pyplot as plt

from rivretrieve import UKFetcher

site_ids = [
    "http://environment.data.gov.uk/hydrology/id/stations/3c5cba29-2321-4289-a1fd-c355e135f4cb",
    "http://environment.data.gov.uk/hydrology/id/stations/0e34325c-80c4-4528-ac8c-99c4c8b48454"
]
start_date = "2024-01-01"
end_date = "2024-01-31"
variable = "discharge"

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = UKFetcher(site_id=site_id)
    print(f"Fetching data for {site_id}...")
    data = fetcher.get_data(variable=variable)
    if not data.empty:
        print(f"Data for {site_id}:")
        print(data.head())
        plt.plot(data['Date'], data['Q'], label=site_id.split('/')[-1])
    else:
        print(f"No data found for {site_id}")

plt.xlabel("Date")
plt.ylabel("Discharge (m3/s)")
plt.title("UK River Discharge - Full Time Series")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "uk_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
