from csv_hendler import DataProcessor
from csv_reader import CSVReader


def main():
    csv_reader = CSVReader(r"homework_oop/repositories.csv")
    data = csv_reader.read_csv()

    processor = DataProcessor(data)
    result = (processor
              .select(["Name", "Description"])
              .execute())

    for item in result:
        print(item)



if __name__ == "__main__":
    main()
