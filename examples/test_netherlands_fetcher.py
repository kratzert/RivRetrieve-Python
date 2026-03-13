import logging

import matplotlib.pyplot as plt

from rivretrieve import NetherlandsFetcher, constants

logging.basicConfig(level=logging.INFO)

fetcher = NetherlandsFetcher()

examples = [
    ("almen", constants.DISCHARGE_DAILY_MEAN, "2025-01-01", "2025-01-07"),
    ("a12", constants.STAGE_INSTANT, "2025-01-01", "2025-01-01"),
    ("ameland.nes", constants.WATER_TEMPERATURE_DAILY_MEAN, "2025-07-01", "2025-07-03"),
]

for gauge_id, variable, start_date, end_date in examples:
    data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)

    if data.empty:
        print(f"No data found for {gauge_id} ({variable})")
        continue

    print(data.head())
    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data[variable], label=f"{gauge_id} - {variable}")
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(variable)
    plt.title(f"Netherlands River Data ({gauge_id})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"netherlands_{gauge_id}_{variable}.png".replace(".", "_")
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")
