import logging

import matplotlib.pyplot as plt

from rivretrieve import BrazilFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_ids = [
    "15400000",
]
variable = constants.STAGE_DAILY_MEAN
start_date = "2023-01-01"
end_date = "2024-12-31"

plt.figure(figsize=(12, 6))

# Initialize fetcher (credentials are loaded from .env by default)
fetcher = BrazilFetcher()

for gauge_id in gauge_ids:
    print(f"Fetching {variable} for {gauge_id} from {start_date} to {end_date}...")
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)
    if not data.empty:
        print(f"Data for {gauge_id}:")
        print(data.head())
        print(f"Time series from {data.index.min()} to {data.index.max()}")
        plt.plot(
            data.index,
            data[variable],
            label=gauge_id,
            marker=".",
            linestyle="-",
        )
    else:
        print(f"No data found for {gauge_id}")

if "data" in locals() and not data.empty:
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(f"{variable} (m3/s)")
    plt.title(f"Brazil River Discharge ({gauge_ids[0]} - {start_date} to {end_date})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "brazil_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
else:
    print("No data to plot.")

print("Test finished.")
