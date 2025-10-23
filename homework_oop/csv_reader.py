import csv
from typing import List, Dict, Any
from pathlib import Path


class CSVReader:
    def __init__(self, file_path: str):
        """
        Инициализация CSV ридера

        Args:
            file_path (str): Путь к CSV файлу
        """
        self.file_path = file_path
        self.expected_columns = [
            "Name",
            "Description",
            "URL",
            "Created At",
            "Updated At",
            "Homepage",
            "Size",
            "Stars",
            "Forks",
            "Issues",
            "Watchers",
            "Language",
            "License",
            "Topics",
            "Has Issues",
            "Has Projects",
            "Has Downloads",
            "Has Wiki",
            "Has Pages",
            "Has Discussions",
            "Is Fork",
            "Is Archived",
            "Is Template",
            "Default Branch",
        ]
        self.data: list[Any] = []

    def read_csv(
        self, delimiter: str = ",", encoding: str = "utf-8"
    ) -> List[Dict[str, Any]]:
        """
        Чтение CSV файла

        Args:
            delimiter (str): Разделитель в CSV файле
            encoding (str): Кодировка файла

        Returns:
            List[Dict[str, Any]]: Список словарей с данными

        Raises:
            FileNotFoundError: Если файл не найден
            ValueError: Если структура CSV не соответствует ожидаемой
        """
        if not Path(self.file_path).exists():
            raise FileNotFoundError(f"Файл {self.file_path} не найден")

        self.data = []

        with open(self.file_path, "r", encoding=encoding) as file:
            reader = csv.DictReader(file, delimiter=delimiter)

            # Проверяем наличие всех ожидаемых колонок
            missing_columns = set(self.expected_columns) - set(reader.fieldnames or [])
            if missing_columns:
                raise ValueError(f"Отсутствуют колонки: {missing_columns}")

            for row in reader:
                # Преобразуем данные к правильным типам
                processed_row = self._process_row(row)
                self.data.append(processed_row)

        return self.data

    def _process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Обработка строки данных - преобразование типов

        Args:
            row (Dict[str, str]): Сырая строка из CSV

        Returns:
            Dict[str, Any]: Обработанная строка с правильными типами
        """
        processed: dict[Any, Any] = {}

        for key, value in row.items():
            if value == "" or value is None:
                processed[key] = None
            elif key in ["Size", "Stars", "Forks", "Issues", "Watchers"]:
                processed[key] = int(value) if value.isdigit() else None
            elif key in [
                "Has Issues",
                "Has Projects",
                "Has Downloads",
                "Has Wiki",
                "Has Pages",
                "Has Discussions",
                "Is Fork",
                "Is Archived",
                "Is Template",
            ]:
                processed[key] = value.lower() in ["true", "1", "yes", "y"]
            elif key == "Topics":
                # Предполагаем, что темы разделены запятыми
                processed[key] = (
                    [topic.strip() for topic in value.split(",")] if value else []
                )
            else:
                processed[key] = value

        return processed

    def get_data(self) -> List[Dict[str, Any]]:
        """
        Получить прочитанные данные

        Returns:
            List[Dict[str, Any]]: Список словарей с данными
        """
        return self.data

    def get_column(self, column_name: str) -> List[Any]:
        """
        Получить значения конкретной колонки

        Args:
            column_name (str): Название колонки

        Returns:
            List[Any]: Список значений колонки

        Raises:
            ValueError: Если колонка не существует
        """
        if column_name not in self.expected_columns:
            raise ValueError(f"Колонка {column_name} не существует")

        return [row.get(column_name) for row in self.data]
