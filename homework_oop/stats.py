import csv
from typing import Any

from csv_hendler import DataProcessor
from csv_reader import CSVReader
import numpy as np


class Stat:
    def __init__(self, csv_reader: CSVReader):
        self.csv_reader = csv_reader
        self.result: list[Any] = []

    def median_size(self):
        self.csv_reader.read_csv()
        data = self.csv_reader.get_data()
        data_processor = DataProcessor(data)

        result = data_processor.select(["Size"]).execute()

        result = np.array([*map(lambda x: x["Size"], result)])
        return np.median(result)

    def most_likely(self) -> list[dict[str, Any]]:
        self.csv_reader.read_csv()
        data = self.csv_reader.get_data()
        data_processor = DataProcessor(data)

        self.result = (
            data_processor.select(["Name", "Description", "Stars"])
            .sort("Stars", reverse=True)
            .limit(1)
            .execute()
        )

        return self.result

    def no_language(self) -> list[dict[str, Any]]:
        self.csv_reader.read_csv()
        data = self.csv_reader.get_data()
        data_processor = DataProcessor(data)

        self.result = (
            data_processor.select(["Name", "Description", "URL"])
            .where(lambda x: x["Language"] is None)
            .execute()
        )

        return self.result

    def most_commit_repos10(self) -> list[dict[str, Any]]:
        self.csv_reader.read_csv()
        data = self.csv_reader.get_data()
        data_processor = DataProcessor(data)

        self.result = (
            data_processor.select(["Name", "Description", "URL", "Forks"])
            .sort("Forks", reverse=True)
            .limit(10)
            .execute()
        )

        return self.result

    def most_watched10(self) -> list[dict[str, Any]]:
        self.csv_reader.read_csv()
        data = self.csv_reader.get_data()
        data_processor = DataProcessor(data)

        self.result = (
            data_processor.select(["Name", "Description", "URL", "Watchers"])
            .sort("Watchers", reverse=True)
            .limit(10)
            .execute()
        )

        return self.result

    def to_csv(self, filename: str):
        with open(filename, "w", encoding="utf-8", newline="") as file:
            fieldnames = self.result[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(len(self.result)):
                writer.writerow(self.result[i])


if __name__ == "__main__":
    csv_reader = CSVReader("repositories.csv")
    stat = Stat(csv_reader)

    print(stat.most_commit_repos10())
    stat.to_csv("saves.csv")
