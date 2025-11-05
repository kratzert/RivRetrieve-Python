import argparse

import matplotlib.pyplot as plt

from rivretrieve import constants
from rivretrieve.japan import JapanFetcher


def main():
    parser = argparse.ArgumentParser(description="Test JapanFetcher")
    parser.add_argument("--gauge_id", type=str, default="301011281104010", help="Gauge ID to test")
    parser.add_argument("--variable", type=str, default=constants.DISCHARGE_DAILY_MEAN, help="Variable to fetch")
    parser.add_argument("--start_date", type=str, default="2004-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, default="2004-12-31", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    fetcher = JapanFetcher()
    print(f"Fetching data for {args.gauge_id} from {args.start_date} to {args.end_date} for {args.variable}...")

    df = fetcher.get_data(
        gauge_id=args.gauge_id, variable=args.variable, start_date=args.start_date, end_date=args.end_date
    )

    if not df.empty:
        print(f"Data for {args.gauge_id}:")
        print(df.head())
        print(f"Time series from {df.index.min()} to {df.index.max()}")
        df.plot(y=args.variable)
        plt.title(f"{args.gauge_id} - {args.variable}")
        plt.xlabel("Time")
        plt.ylabel(args.variable)
        plt.legend()
        plot_filename = f"japan_{args.variable}_plot.png"
        plt.savefig(plot_filename)
        print(f"Plot saved to {plot_filename}")
    else:
        print(f"No data found for {args.gauge_id}")


if __name__ == "__main__":
    main()
