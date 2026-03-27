import logging

import matplotlib.pyplot as plt

from rivretrieve import EstoniaFetcher, constants

logging.basicConfig(level=logging.INFO)

# Example EstModel gauge ID
gauge_ids = ["SJA8821000"]

# Variable to test
variable = constants.DISCHARGE_DAILY_MEAN

# Period to fetch
start_date = "2024-01-01"
end_date = "2024-03-31"

plt.figure(figsize=(12, 6))

fetcher = EstoniaFetcher()

for gauge_id in gauge_ids:
    print(f"Fetching data for {gauge_id} from {start_date} to {end_date}...")

    data = fetcher.get_data(
        gauge_id=gauge_id,
        variable=variable,
        start_date=start_date,
        end_date=end_date,
    )

    if not data.empty:
        print(f"\nData retrieved for gauge {gauge_id}")
        print(data.head())
        print(f"Time series from {data.index.min()} to {data.index.max()}")

        plt.plot(
            data.index,
            data[variable],
            label=gauge_id,
            marker="o",
        )
    else:
        print(f"\nNo data found for {gauge_id}")

plt.xlabel(constants.TIME_INDEX)
plt.ylabel(f"{variable} (m³/s)")
plt.title(f"Estonia ({gauge_ids[0]}) — {variable} time series")
plt.legend()
plt.grid(True)
plt.tight_layout()

plot_path = "estonia_fetcher_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")

# print(fetcher.get_metadata().head())
