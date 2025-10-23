from typing import List, Dict, Any, Optional, Callable
from operator import itemgetter
from difflib import get_close_matches


class DataProcessor:
    def __init__(self, data: List[Dict[str, Any]]):
        """
        Инициализация процессора данных

        Args:
            data (List[Dict[str, Any]]): Исходные данные для обработки
        """
        self.original_data = data
        self.operations: list[Any] = []
        self.available_fields = list(data[0].keys()) if data else []

    def select(self, fields: List[str]) -> "DataProcessor":
        """
        Выбор определенных полей

        Args:
            fields (List[str]): Список полей для выборки

        Returns:
            DataProcessor: self для цепочки вызовов
        """
        self._validate_fields(fields)
        self.operations.append(("select", fields))
        return self

    def where(self, condition: Callable[[Dict[str, Any]], bool]) -> "DataProcessor":
        """
        Фильтрация данных по условию

        Args:
            condition (Callable): Функция-условие для фильтрации

        Returns:
            DataProcessor: self для цепочки вызовов
        """
        self.operations.append(("where", condition))
        return self

    def sort(self, field: str, reverse: bool = False) -> "DataProcessor":
        """
        Сортировка данных по полю

        Args:
            field (str): Поле для сортировки
            reverse (bool): Порядок сортировки (по убыванию)

        Returns:
            DataProcessor: self для цепочки вызовов
        """
        self._validate_fields([field])
        self.operations.append(("sort", field, reverse))
        return self

    def group_by(
        self, field: str, aggregation: Optional[Dict[str, Callable]] = None
    ) -> "DataProcessor":
        """
        Группировка данных с агрегацией

        Args:
            field (str): Поле для группировки
            aggregation (Dict[str, Callable]): Словарь {поле: функция_агрегации}

        Returns:
            DataProcessor: self для цепочки вызовов
        """
        self._validate_fields([field])
        if aggregation:
            self._validate_fields(list(aggregation.keys()))
        self.operations.append(("group_by", field, aggregation or {}))
        return self

    def limit(self, n: int) -> "DataProcessor":
        """
        Ограничение количества записей

        Args:
            n (int): Максимальное количество записей

        Returns:
            DataProcessor: self для цепочки вызовов
        """
        self.operations.append(("limit", n))
        return self

    def _validate_fields(self, fields: List[str]) -> None:
        """
        Валидация полей с подсказками по похожим полям

        Args:
            fields (List[str]): Список полей для проверки

        Raises:
            ValueError: Если поле не существует
        """
        for field in fields:
            if field not in self.available_fields:
                similar = get_close_matches(
                    field, self.available_fields, n=3, cutoff=0.6
                )
                error_msg = f"Поле '{field}' не существует в данных."
                if similar:
                    error_msg += f" Возможно, вы имели в виду: {', '.join(similar)}"
                else:
                    error_msg += f" Доступные поля: {', '.join(self.available_fields)}"
                raise ValueError(error_msg)

    def _optimize_operations_order(self) -> List[tuple]:
        """
        Оптимизация порядка операций для повышения производительности

        Returns:
            List[tuple]: Оптимизированный список операций
        """
        if not self.operations:
            return []

        # Разделяем операции по типам
        where_ops = [op for op in self.operations if op[0] == "where"]
        sort_ops = [op for op in self.operations if op[0] == "sort"]
        group_ops = [op for op in self.operations if op[0] == "group_by"]
        select_ops = [op for op in self.operations if op[0] == "select"]
        limit_ops = [op for op in self.operations if op[0] == "limit"]

        # Оптимальный порядок: WHERE -> GROUP BY -> SORT -> SELECT -> LIMIT
        optimized = where_ops + group_ops + sort_ops + select_ops + limit_ops

        return optimized

    def _apply_select(
        self, data: List[Dict[str, Any]], fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Применение операции SELECT"""
        return [{field: item.get(field) for field in fields} for item in data]

    def _apply_where(
        self, data: List[Dict[str, Any]], condition: Callable
    ) -> List[Dict[str, Any]]:
        """Применение операции WHERE"""
        return [item for item in data if condition(item)]

    def _apply_sort(
        self, data: List[Dict[str, Any]], field: str, reverse: bool = False
    ) -> List[Dict[str, Any]]:
        """Применение операции SORT"""
        return sorted(data, key=itemgetter(field), reverse=reverse)

    def _apply_group_by(
        self, data: List[Dict[str, Any]], field: str, aggregation: Dict[str, Callable]
    ) -> List[Dict[str, Any]]:
        """Применение операции GROUP BY"""
        groups: dict[Any, Any] = {}
        for item in data:
            group_key = item.get(field)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(item)

        result = []
        for group_key, group_items in groups.items():
            group_result = {field: group_key}

            for agg_field, agg_func in aggregation.items():
                values = [
                    item.get(agg_field)
                    for item in group_items
                    if item.get(agg_field) is not None
                ]
                if values:
                    group_result[agg_field] = agg_func(values)
                else:
                    group_result[agg_field] = None

            result.append(group_result)

        return result

    def _apply_limit(self, data: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
        """Применение операции LIMIT"""
        return data[:n]

    def execute(self) -> List[Dict[str, Any]]:
        """
        Выполнение всех операций в оптимальном порядке

        Returns:
            List[Dict[str, Any]]: Результат обработки данных

        Raises:
            ValueError: Если данные пусты или операции невалидны
        """
        if not self.original_data:
            return []

        # Получаем оптимизированный порядок операций
        optimized_ops = self._optimize_operations_order()

        # Применяем операции последовательно
        result = self.original_data.copy()

        for operation in optimized_ops:
            op_type = operation[0]

            try:
                if op_type == "select":
                    result = self._apply_select(result, operation[1])
                elif op_type == "where":
                    result = self._apply_where(result, operation[1])
                elif op_type == "sort":
                    result = self._apply_sort(result, operation[1], operation[2])
                elif op_type == "group_by":
                    result = self._apply_group_by(result, operation[1], operation[2])
                elif op_type == "limit":
                    result = self._apply_limit(result, operation[1])
            except Exception as e:
                raise RuntimeError(
                    f"Ошибка при выполнении операции {op_type}: {str(e)}"
                )

        # Очищаем операции после выполнения
        self.operations.clear()

        return result

    def explain(self) -> str:
        """
        Показывает план выполнения операций

        Returns:
            str: Описание плана выполнения
        """
        if not self.operations:
            return "Нет операций для выполнения"

        optimized_ops = self._optimize_operations_order()

        explanation = "План выполнения:\n"
        for i, op in enumerate(optimized_ops, 1):
            op_type = op[0]
            if op_type == "select":
                explanation += f"{i}. SELECT: {op[1]}\n"
            elif op_type == "where":
                explanation += f"{i}. WHERE: фильтрация по условию\n"
            elif op_type == "sort":
                explanation += f"{i}. SORT: по полю '{op[1]}' ({'убывание' if op[2] else 'возрастание'})\n"
            elif op_type == "group_by":
                agg_info = f" с агрегацией {list(op[2].keys())}" if op[2] else ""
                explanation += f"{i}. GROUP BY: по полю '{op[1]}'{agg_info}\n"
            elif op_type == "limit":
                explanation += f"{i}. LIMIT: {op[1]} записей\n"

        return explanation


# Вспомогательные функции для агрегации
def avg(values: List[float]) -> float:
    """Среднее значение"""
    return sum(values) / len(values) if values else 0


def sum_agg(values: List[float]) -> float:
    """Сумма"""
    return sum(values)


def count_agg(values: List[Any]) -> int:
    """Количество"""
    return len(values)


def max_agg(values: List[Any]) -> Any:
    """Максимальное значение"""
    return max(values) if values else None


def min_agg(values: List[Any]) -> Any:
    """Минимальное значение"""
    return min(values) if values else None
