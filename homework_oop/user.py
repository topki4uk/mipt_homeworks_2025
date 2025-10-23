from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

from csv_hendler import DataProcessor


class User:
    """
    Упрощенный класс пользователя для сохранения запросов
    """

    def __init__(self, user_id: str, name: str):
        self.user_id = user_id
        self.name = name
        self.saved_queries: Dict[str, Dict] = {}
        self.current_processor: Optional[DataProcessor] = None

    def set_processor(self, processor: DataProcessor):
        """Установка текущего процессора данных"""
        self.current_processor = processor
        return self

    def save_query(self, name: str, description: str = "") -> str:
        """
        Сохранение текущих операций процессора как запроса

        Args:
            name (str): Название запроса
            description (str): Описание запроса

        Returns:
            str: ID сохраненного запроса
        """
        if not self.current_processor:
            raise ValueError(
                "Сначала установите процессор данных с помощью set_processor()"
            )

        if not self.current_processor.operations:
            raise ValueError("Нет операций для сохранения")

        query_id = str(uuid.uuid4())
        self.saved_queries[query_id] = {
            "id": query_id,
            "name": name,
            "description": description,
            "operations": self.current_processor.operations.copy(),
            "created_at": datetime.now().isoformat(),
        }

        return query_id

    def load_query(self, query_id: str) -> DataProcessor:
        """
        Загрузка сохраненного запроса в текущий процессор

        Args:
            query_id (str): ID сохраненного запроса

        Returns:
            DataProcessor: Процессор с загруженными операциями
        """
        if query_id not in self.saved_queries:
            raise ValueError(f"Запрос с ID {query_id} не найден")

        if not self.current_processor:
            raise ValueError("Сначала установите процессор данных")

        # Загружаем операции из сохраненного запроса
        self.current_processor.operations = self.saved_queries[query_id][
            "operations"
        ].copy()
        return self.current_processor

    def load_query_by_name(self, name: str) -> DataProcessor:
        """
        Загрузка запроса по имени

        Args:
            name (str): Название запроса

        Returns:
            DataProcessor: Процессор с загруженными операциями
        """
        for query in self.saved_queries.values():
            if query["name"] == name:
                return self.load_query(query["id"])

        raise ValueError(f"Запрос с именем '{name}' не найден")

    def execute_saved_query(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Выполнение сохраненного запроса

        Args:
            query_id (str): ID запроса

        Returns:
            List[Dict[str, Any]]: Результат выполнения
        """
        processor = self.load_query(query_id)
        return processor.execute()

    def execute_saved_query_by_name(self, name: str) -> List[Dict[str, Any]]:
        """
        Выполнение сохраненного запроса по имени

        Args:
            name (str): Название запроса

        Returns:
            List[Dict[str, Any]]: Результат выполнения
        """
        processor = self.load_query_by_name(name)
        return processor.execute()

    def list_queries(self) -> List[Dict[str, Any]]:
        """Список всех сохраненных запросов"""
        return list(self.saved_queries.values())

    def delete_query(self, query_id: str) -> bool:
        """Удаление запроса"""
        if query_id in self.saved_queries:
            del self.saved_queries[query_id]
            return True
        return False

    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Получение информации о запросе"""
        return self.saved_queries.get(query_id)
