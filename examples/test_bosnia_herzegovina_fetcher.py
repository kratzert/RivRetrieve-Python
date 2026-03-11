import logging

import matplotlib.pyplot as plt

from rivretrieve import BosniaHerzegovinaFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_id = "4510"
variables = [
    constants.DISCHARGE_INSTANT,
    constants.STAGE_INSTANT,
    constants.WATER_TEMPERATURE_INSTANT,
]
start_date = "2025-01-01"
end_date = "2025-01-07"

fetcher = BosniaHerzegovinaFetcher()

for variable in variables:
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)
    if data.empty:
        print(f"No data found for {gauge_id} ({variable})")
        continue

    print(data.head())
    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data[variable], label=f"{gauge_id} - {variable}")
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(variable)
    plt.title(f"Bosnia and Herzegovina River Data ({gauge_id})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"bosnia_herzegovina_{variable}_plot.png"
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
