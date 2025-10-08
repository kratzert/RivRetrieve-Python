import logging

import matplotlib.pyplot as plt

from rivretrieve import USAFetcher
from rivretrieve import constants

logging.basicConfig(level=logging.INFO)

site_ids = [
    "07374000",
]
variable = constants.DISCHARGE
# Fetch a recent period for testing
start_date = "1950-01-01"
end_date = None

plt.figure(figsize=(12, 6))

for site_id in site_ids:
    fetcher = USAFetcher(site_id=site_id)
    print(f"Fetching data for {site_id} from {start_date} to {end_date}...")
    data = fetcher.get_data(variable=variable, start_date=start_date, end_date=end_date)
    if not data.empty:
        print(f"Data for {site_id}:")
        print(data.head())
        print(
            f"Time series from {data[constants.TIME_INDEX].min()} to {data[constants.TIME_INDEX].max()}"
        )
        plt.plot(
            data[constants.TIME_INDEX],
            data[constants.DISCHARGE],
            label=site_id,
            marker="o",
        )
    else:
        print(f"No data found for {site_id}")

plt.xlabel(constants.TIME_INDEX)
plt.ylabel(f"{constants.DISCHARGE} (m3/s)")
plt.title(f"USA River Discharge ({site_id} - 1950 to Present)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plot_path = "usa_discharge_plot.png"
plt.savefig(plot_path)
print(f"Plot saved to {plot_path}")
