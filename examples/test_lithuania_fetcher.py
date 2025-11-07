import logging

import matplotlib.pyplot as plt

from rivretrieve import LithuaniaFetcher, constants

logging.basicConfig(level=logging.INFO)

# Example gauge ID from Meteo.lt
gauge_ids = ["aunuvenu-vms"]

variables = [constants.DISCHARGE_DAILY_MEAN, constants.STAGE_DAILY_MEAN]

# Period to fetch
start_date = "2024-01-01"
end_date = "2024-01-31"

fetcher = LithuaniaFetcher()

# metadata = fetcher.get_metadata()
# print(metadata.head())

for variable in variables:
    plt.figure(figsize=(12, 6))
    print(f"\n--- Testing variable: {variable} ---")
    has_data = False
    for gauge_id in gauge_ids:
        print(f"Fetching {variable} for {gauge_id} from {start_date} to {end_date}...")
        data = fetcher.get_data(
            gauge_id=gauge_id,
            variable=variable,
            start_date=start_date,
            end_date=end_date,
        )
        if not data.empty:
            print(f"Data for {gauge_id}:")
            print(data.head())
            print(f"Time series from {data.index.min()} to {data.index.max()}")
            plt.plot(
                data.index,
                data[variable],
                label=f"{gauge_id} - {variable}",
                marker=".",
                linestyle="-",
            )
            has_data = True
        else:
            print(f"No {variable} data found for {gauge_id}")

    if has_data:
        plt.xlabel(constants.TIME_INDEX)
        unit = "m³/s" if variable == constants.DISCHARGE_DAILY_MEAN else "m"
        plt.ylabel(f"{variable} ({unit})")
        plt.title(f"Lithuania River Data ({gauge_ids[0]} - {start_date} to {end_date})")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plot_path = f"lithuania_{variable}_plot.png"
        plt.savefig(plot_path)
        print(f"Plot saved to {plot_path}")
    else:
        print(f"No data to plot for {variable}.")

print("Test finished.")
