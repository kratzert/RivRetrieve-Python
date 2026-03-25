import logging

import matplotlib.pyplot as plt

from rivretrieve import SwitzerlandFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_id = "2016"
variable = constants.DISCHARGE_DAILY_MEAN
start_date = "2020-01-01"
end_date = "2020-01-31"

fetcher = SwitzerlandFetcher()
data = fetcher.get_data(
    gauge_id=gauge_id,
    variable=variable,
    start_date=start_date,
    end_date=end_date,
)

if data.empty:
    print(f"No data found for gauge {gauge_id}, variable {variable}")
else:
    print(data.head())
    print(f"Fetched {len(data)} rows from {data.index.min()} to {data.index.max()}")

    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data[variable], label=f"{gauge_id} - {variable}", linewidth=1.8)
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel("Discharge (m3/s)")
    plt.title(f"Switzerland historical series for gauge {gauge_id}")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = "switzerland_historical_plot.png"
    plt.savefig(output_path, dpi=150)
    print(f"Saved plot to {output_path}")
