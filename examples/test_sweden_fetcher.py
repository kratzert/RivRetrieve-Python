import logging

import matplotlib.pyplot as plt

from rivretrieve import SwedenFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_id = "2357"
variable = constants.DISCHARGE_DAILY_MEAN
start_date = "2026-03-01"
end_date = "2026-03-15"

fetcher = SwedenFetcher()
data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)

if data.empty:
    print(f"No data found for {gauge_id} ({variable})")
else:
    print(data.head())
    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data[variable], label=f"{gauge_id} - {variable}")
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel("m³/s")
    plt.title(f"Sweden River Data ({gauge_id})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = "sweden_discharge_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
