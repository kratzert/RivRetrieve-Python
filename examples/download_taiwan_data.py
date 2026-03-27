"""Download and plot a short Taiwan discharge series."""

import logging

import matplotlib.pyplot as plt

from rivretrieve import TaiwanFetcher, constants


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    gauge_id = "1140H099"
    variable = constants.DISCHARGE_DAILY_MEAN
    start_date = "2024-01-01"
    end_date = "2024-01-10"

    fetcher = TaiwanFetcher()
    data = fetcher.get_data(
        gauge_id=gauge_id,
        variable=variable,
        start_date=start_date,
        end_date=end_date,
    )

    if data.empty:
        print(f"No data found for {gauge_id}")
        return

    print(data.head())

    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data[variable], marker=".", linestyle="-")
    plt.xlabel(constants.TIME_INDEX)
    plt.ylabel(f"{variable} (m3/s)")
    plt.title(f"Taiwan River Discharge ({gauge_id} - {start_date} to {end_date})")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("taiwan_discharge_plot.png")
    print("Plot saved to taiwan_discharge_plot.png")


if __name__ == "__main__":
    main()
