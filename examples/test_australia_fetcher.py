import logging

import matplotlib.pyplot as plt

from rivretrieve import AustraliaFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_ids = [
    "403213",
]
variable = constants.DISCHARGE
# Fetch a recent period for testing
start_date = "2023-10-01"
end_date = "2024-03-31"

plt.figure(figsize=(12, 6))

fetcher = AustraliaFetcher()
for gauge_id in gauge_ids:
    print(f"Fetching data for {gauge_id} from {start_date} to {end_date}...")
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)
    if not data.empty:
        print(f"Data for {gauge_id}:")
        print(data.head())
        print(f"Time series from {data.index.min()} to {data.index.max()}")
        plt.plot(
            data.index,
            data[constants.DISCHARGE],
            label=gauge_id,
            marker="o",
        )
    else:
        print(f"No data found for {gauge_id}")

plt.xlabel(constants.TIME_INDEX)
plt.ylabel(f"{constants.DISCHARGE} (m3/s)")
plt.title(f"Australia River Discharge ({gauge_ids[0]} - Full Time Series)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "australia_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
