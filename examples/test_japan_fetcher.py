import logging

import matplotlib.pyplot as plt

from rivretrieve import JapanFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_ids = [
    "301011281104010",
]
variable = constants.DISCHARGE
start_date = "2019-01-01"
end_date = "2019-12-31"  # Fetching a few months to test

plt.figure(figsize=(12, 6))

fetcher = JapanFetcher()
for gauge_id in gauge_ids:
    print(f"Fetching data for {gauge_id} from {start_date} to {end_date}...")
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)
    if not data.empty:
        print(f"Data for {gauge_id}:")
        print(data.head())
        print(f"Time series from {data[constants.TIME_INDEX].min()} to {data[constants.TIME_INDEX].max()}")
        plt.plot(
            data[constants.TIME_INDEX],
            data[constants.DISCHARGE],
            label=gauge_id,
            marker="o",
        )
    else:
        print(f"No data found for {gauge_id}")

plt.xlabel(constants.TIME_INDEX)
plt.ylabel(f"{constants.DISCHARGE} (m3/s)")
plt.title("Japan River Discharge - Full Time Series")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "japan_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
