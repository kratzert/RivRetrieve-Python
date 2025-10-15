import matplotlib.pyplot as plt

from rivretrieve import UKEAFetcher, constants

gauge_ids = [
    "http://environment.data.gov.uk/hydrology/id/stations/3c5cba29-2321-4289-a1fd-c355e135f4cb",
]
start_date = "2024-01-01"
end_date = "2024-01-31"
variable = constants.DISCHARGE

plt.figure(figsize=(12, 6))

fetcher = UKEAFetcher()
for gauge_id in gauge_ids:
    print(f"Fetching data for {gauge_id}...")
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)
    if not data.empty:
        print(f"Data for {gauge_id}:")
        print(data.head())
        plt.plot(
            data.index,
            data[constants.DISCHARGE],
            label=gauge_id.split("/")[-1],
        )
    else:
        print(f"No data found for {gauge_id}")

plt.xlabel(constants.TIME_INDEX)
plt.ylabel(f"{constants.DISCHARGE} (m3/s)")
plt.title("UK River Discharge - Full Time Series")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "uk_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
