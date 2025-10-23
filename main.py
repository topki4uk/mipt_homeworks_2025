from homework_oop.csv_hendler import DataProcessor, avg, sum_agg
from homework_oop.csv_reader import CSVReader


def main():
    csv_reader = CSVReader(r"homework_oop/repositories.csv")
    data = csv_reader.read_csv()

    processor = DataProcessor(data)
    result = (processor
              .group_by('Has Pages', {"Stars": avg, "Forks": sum_agg})
              .limit(2))
    print(processor.explain())
    result = result.execute()

    for item in result:
        print(item)

if __name__ == "__main__":
    main()
