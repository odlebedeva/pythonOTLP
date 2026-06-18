import json
import sys
from typing import List, Dict
from pathlib import Path
from tabulate import tabulate
from models import Trace, Span, Process, Tag, ValidationResult
from rules import RuleManager
from validator import TraceValidator


class TraceLoader:
    """Загрузчик трейсов из JSON"""

    @staticmethod
    def load_from_file(file_path: str) -> List[Trace]:
        """Загрузка трейсов из файла"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        traces = []
        for trace_data in data.get("data", []):
            spans = []
            for span_data in trace_data.get("spans", []):
                tags = [Tag(**tag) for tag in span_data.get("tags", [])]

                # Извлекаем span.kind
                span_kind = ""
                for tag in tags:
                    if tag.key == "span.kind":
                        span_kind = str(tag.value)
                        break

                span = Span(
                    trace_id=span_data["traceID"],
                    span_id=span_data["spanID"],
                    operation_name=span_data.get("operationName", ""),
                    references=span_data.get("references", []),
                    start_time=span_data.get("startTime", 0),
                    duration=span_data.get("duration", 0),
                    tags=tags,
                    logs=span_data.get("logs", []),
                    process_id=span_data.get("processID", ""),
                    warnings=span_data.get("warnings"),
                    span_kind=span_kind
                )
                spans.append(span)

            processes = {}
            for proc_id, proc_data in trace_data.get("processes", {}).items():
                proc_tags = [Tag(**tag) for tag in proc_data.get("tags", [])]
                process = Process(
                    service_name=proc_data["serviceName"],
                    tags=proc_tags
                )
                processes[proc_id] = process

            trace = Trace(
                trace_id=trace_data["traceID"],
                spans=spans,
                processes=processes,
                warnings=trace_data.get("warnings")
            )
            traces.append(trace)

        return traces


class ReportGenerator:
    """Генератор отчетов"""

    @staticmethod
    def format_tags_column(all_tags_info: List[Dict[str, str]]) -> str:
        """Форматирование столбца tags с полной информацией"""
        if not all_tags_info:
            return "Нет тегов"

        lines = []
        for tag in all_tags_info:
            lines.append(f"• {tag['key']}")

        return "\n".join(lines)

    @staticmethod
    def format_field_values_column(all_field_values: Dict[str, str]) -> str:
        """Форматирование столбца значений полей с полной информацией"""
        if not all_field_values:
            return "Нет значений"

        lines = []
        for key, value in all_field_values.items():
            # Обрезаем слишком длинные значения для читаемости
            value_str = str(value)
            if len(value_str) > 100:
                value_str = value_str[:97] + "..."
            lines.append(f"• {key}: {value_str}")

        return "\n".join(lines)

    @staticmethod
    def generate_table(results: List[ValidationResult]) -> str:
        """Генерация таблицы с результатами валидации"""
        headers = [
            "Библиотека",
            "Версия",
            "serviceName",
            "spanId",
            "operationName",
            "Метод",
            "span.kind",
            "tags (все ключи)",
            "Соотв. спецификации",
            "Соотв. семантике",
            "Значения полей (все)"
        ]

        table_data = []
        for result in results:
            # Формируем полный список тегов
            tags_str = ReportGenerator.format_tags_column(result.all_tags_info)

            # Формируем полный список значений
            field_values_str = ReportGenerator.format_field_values_column(result.all_field_values)

            # Определяем статус соответствия
            spec_status = "✓" if result.spec_compliance else "✗"
            semantic_status = "✓" if result.semantic_compliance else "✗"

            row = [
                result.library,
                result.version,
                result.service_name,
                result.span_id,
                result.operation_name,
                result.method,
                result.span_kind,
                tags_str,
                spec_status,
                semantic_status,
                field_values_str
            ]
            table_data.append(row)

        # Используем grid формат для лучшего отображения многострочных данных
        return tabulate(
            table_data,
            headers=headers,
            tablefmt="grid",
            maxcolwidths=[15, 10, 20, 20, 17, 8, 12, 40, 15, 15, 50]
        )

    @staticmethod
    def generate_detailed_table(results: List[ValidationResult]) -> str:
        """Генерация детальной таблицы с группировкой тегов по spanId"""
        headers = [
            "spanId / Ключ тега",
            "operationName",
            "Библиотека",
            "Версия",
            "serviceName",
            "Метод",
            "span.kind",
            "Тип тега",
            "Значение тега",
            "Соотв. спецификации",
            "Соотв. семантике"
        ]

        table_data = []

        for idx, result in enumerate(results):
            spec_status = "✓" if result.spec_compliance else "✗"
            semantic_status = "✓" if result.semantic_compliance else "✗"

            if result.all_tags_info:
                # Первая строка - заголовок спана с первым тегом
                first_tag = result.all_tags_info[0]
                header_row = [
                    f"▶ {result.span_id}",
                    result.operation_name,
                    result.library,
                    result.version,
                    result.service_name,
                    result.method,
                    result.span_kind,
                    first_tag['type'],
                    str(first_tag['value'])[:100],
                    spec_status,
                    semantic_status
                ]
                table_data.append(header_row)

                # Подстрока с ключом первого тега
                first_key_row = [
                    f"  └─ {first_tag['key']}",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""
                ]
                table_data.append(first_key_row)

                # Остальные теги (начиная со второго)
                for tag in result.all_tags_info[1:]:
                    tag_row = [
                        f"  └─ {tag['key']}",
                        "", "", "", "", "", "",
                        tag['type'],
                        str(tag['value'])[:100],
                        "",
                        ""
                    ]
                    table_data.append(tag_row)

                # Разделитель между спанами
                if idx < len(results) - 1:
                    separator = ["─" * 30] + [""] * 10
                    table_data.append(separator)

            else:
                row = [
                    f"▶ {result.span_id}",
                    result.operation_name,
                    result.library,
                    result.version,
                    result.service_name,
                    result.method,
                    result.span_kind,
                    "-",
                    "Нет тегов",
                    spec_status,
                    semantic_status
                ]
                table_data.append(row)

                if idx < len(results) - 1:
                    separator = ["─" * 30] + [""] * 10
                    table_data.append(separator)

        return tabulate(
            table_data,
            headers=headers,
            tablefmt="grid",
            maxcolwidths=[35, 17, 15, 10, 20, 8, 12, 10, 55, 18, 18]
        )

    @staticmethod
    def generate_summary(results: List[ValidationResult]) -> str:
        """Генерация сводной информации"""
        total_spans = len(results)
        spec_compliant = sum(1 for r in results if r.spec_compliance)
        semantic_compliant = sum(1 for r in results if r.semantic_compliance)

        # Собираем статистику по типам спанов
        span_types = {}
        for result in results:
            span_type = result.span_kind if result.span_kind else "неизвестный"
            if span_type not in span_types:
                span_types[span_type] = {
                    "total": 0,
                    "spec_compliant": 0,
                    "semantic_compliant": 0
                }
            span_types[span_type]["total"] += 1
            if result.spec_compliance:
                span_types[span_type]["spec_compliant"] += 1
            if result.semantic_compliance:
                span_types[span_type]["semantic_compliant"] += 1

        summary = f"""
═══════════════════════════════════════════════════════════════
                    РЕЗУЛЬТАТЫ ВАЛИДАЦИИ ТРЕЙСОВ
═══════════════════════════════════════════════════════════════
Всего спанов: {total_spans}
Соответствует спецификации: {spec_compliant}/{total_spans} ({spec_compliant / total_spans * 100:.1f}%)
Соответствует семантике: {semantic_compliant}/{total_spans} ({semantic_compliant / total_spans * 100:.1f}%)

Статистика по типам спанов:
"""

        for span_type, stats in span_types.items():
            summary += f"\n  {span_type}:"
            summary += f"\n    Всего: {stats['total']}"
            summary += f"\n    Спецификация: {stats['spec_compliant']}/{stats['total']} ({stats['spec_compliant'] / stats['total'] * 100:.1f}%)"
            summary += f"\n    Семантика: {stats['semantic_compliant']}/{stats['total']} ({stats['semantic_compliant'] / stats['total'] * 100:.1f}%)"

        summary += "\n\nДетали по спанам:\n"
        summary += "=" * 50 + "\n"

        for i, result in enumerate(results, 1):
            summary += f"\nСпан {i}: {result.span_id}"
            summary += f"\n  Сервис: {result.service_name}"
            summary += f"\n  Операция: {result.operation_name}"
            summary += f"\n  Тип: {result.span_kind if result.span_kind else 'неизвестный'}"
            summary += f"\n  Метод: {result.method}"
            summary += f"\n  Спецификация: {'✓' if result.spec_compliance else '✗'}"
            if result.spec_errors:
                for error in result.spec_errors:
                    summary += f"\n    - {error}"

            summary += f"\n  Семантика: {'✓' if result.semantic_compliance else '✗'}"
            if result.semantic_errors:
                for error in result.semantic_errors:
                    summary += f"\n    - {error}"

            # Добавляем информацию о количестве тегов
            summary += f"\n  Всего тегов: {len(result.all_tags_info)}"

            summary += "\n" + "-" * 50

        return summary


def main():
    """Главная функция"""
    if len(sys.argv) < 2:
        print("Использование: python main.py <путь_к_json_файлу>")
        print("Пример: python main.py trace.json")
        sys.exit(1)

    json_file_path = sys.argv[1]

    if not Path(json_file_path).exists():
        print(f"Ошибка: файл {json_file_path} не найден")
        sys.exit(1)

    print("=" * 60)
    print("ЗАГРУЗКА И ВАЛИДАЦИЯ ТРЕЙСОВ")
    print("=" * 60)
    print(f"Файл: {json_file_path}")

    # Инициализация компонентов
    rule_manager = RuleManager()
    validator = TraceValidator(rule_manager)

    # Загрузка трейсов
    print("\nЗагрузка трейсов...")
    traces = TraceLoader.load_from_file(json_file_path)
    print(f"Загружено трейсов: {len(traces)}")

    # Валидация всех спанов
    print("Выполнение валидации...")
    all_results = []
    total_spans = 0
    for trace in traces:
        for span in trace.spans:
            process = trace.processes.get(span.process_id)
            result = validator.validate_span(span, process)
            all_results.append(result)
            total_spans += 1
            print(f"  Обработан спан: {result.span_id} - {result.operation_name}")

    print(f"\nВсего обработано спанов: {total_spans}")

    # Генерация отчетов
    print("\nГенерация отчетов...")

    # Основная таблица со всеми данными в столбцах
    main_table = ReportGenerator.generate_table(all_results)

    # Детальная таблица с разбивкой по тегам
    detailed_table = ReportGenerator.generate_detailed_table(all_results)

    # Сводная информация
    summary = ReportGenerator.generate_summary(all_results)

    # Сохранение результатов в файл
    output_file = "validation_results.txt"
    print(f"\nСохранение результатов в файл: {output_file}")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("РЕЗУЛЬТАТЫ ВАЛИДАЦИИ ТРЕЙСОВ\n")
        f.write("=" * 80 + "\n\n")
        f.write("ОСНОВНАЯ ТАБЛИЦА (сгруппированная)\n")
        f.write("-" * 80 + "\n")
        f.write(main_table)
        f.write("\n\n")
        f.write("ДЕТАЛЬНАЯ ТАБЛИЦА (по тегам)\n")
        f.write("-" * 80 + "\n")
        f.write(detailed_table)
        f.write("\n\n")
        f.write("СВОДНАЯ ИНФОРМАЦИЯ\n")
        f.write("-" * 80 + "\n")
        f.write(summary)

    # Также сохраняем в CSV для удобства анализа
    csv_file = "validation_results.csv"
    print(f"Сохранение CSV в файл: {csv_file}")

    with open(csv_file, 'w', encoding='utf-8') as f:
        # Заголовки CSV
        f.write(
            "Библиотека;Версия;serviceName;spanId;operationName;Метод;span.kind;Ключ тега;Тип;Значение;Соотв. спецификации;Соотв. семантике\n")

        for result in all_results:
            spec_status = "PASS" if result.spec_compliance else "FAIL"
            semantic_status = "PASS" if result.semantic_compliance else "FAIL"

            if result.all_tags_info:
                for tag in result.all_tags_info:
                    # Экранируем значения с точкой с запятой
                    value = str(tag['value']).replace(';', '\\;')
                    f.write(f"{result.library};{result.version};{result.service_name};{result.span_id};"
                            f"{result.operation_name};{result.method};{result.span_kind};"
                            f"{tag['key']};{tag['type']};{value};"
                            f"{spec_status};{semantic_status}\n")
            else:
                f.write(f"{result.library};{result.version};{result.service_name};{result.span_id};"
                        f"{result.operation_name};{result.method};{result.span_kind};"
                        f"Нет тегов;-;-;{spec_status};{semantic_status}\n")

    # Вывод в консоль
    print("\n" + summary)
    print(f"\n{'=' * 60}")
    print(f"Файлы с результатами:")
    print(f"  • {output_file} - полный отчет с таблицами")
    print(f"  • {csv_file} - данные в CSV формате")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()