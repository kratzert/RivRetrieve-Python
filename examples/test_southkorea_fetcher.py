import logging

import matplotlib.pyplot as plt

from rivretrieve import SouthKoreaFetcher, constants

logging.basicConfig(level=logging.INFO)

gauge_id = "1001602"
variable = constants.DISCHARGE_DAILY_MEAN
start_date = "2023-01-01"
end_date = "2023-01-07"

fetcher = SouthKoreaFetcher()
data = fetcher.get_data(gauge_id=gauge_id, variable=variable, start_date=start_date, end_date=end_date)

if data.empty:
    print(f"No data found for {gauge_id}")
else:
    print(data.head())
    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data[variable], label=gauge_id)
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(f"{variable} (m3/s)")
    plt.title(f"South Korea River Discharge ({gauge_id})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("southkorea_discharge_plot.png")
    print("Plot saved to southkorea_discharge_plot.png")
