from typing import Dict, List, Tuple, Optional
from models import Span, Process, Trace, ValidationResult, Tag
from rules import RuleManager, SpanType


class SpanClassifier:
    """Классификатор спанов по типу"""

    @staticmethod
    def classify_span(span: Span) -> Optional[str]:
        """Определение типа спана на основе его тегов"""
        tags_keys = {tag.key for tag in span.tags}

        # Проверяем наличие характерных тегов для каждого типа
        if "http.url" in tags_keys or "http.method" in tags_keys:
            if span.span_kind == "client":
                return SpanType.HTTP_CLIENT.value
            elif span.span_kind == "server":
                return SpanType.HTTP_SERVER.value

        if "kafka.topic" in tags_keys:
            if span.span_kind == "producer":
                return SpanType.KAFKA_PRODUCER.value
            elif span.span_kind == "consumer":
                return SpanType.KAFKA_CONSUMER.value

        # Если span.kind присутствует в тегах
        span_kind_tag = next((tag for tag in span.tags if tag.key == "span.kind"), None)
        if span_kind_tag:
            kind_value = str(span_kind_tag.value).lower()
            if kind_value == "client":
                return SpanType.HTTP_CLIENT.value
            elif kind_value == "server":
                return SpanType.HTTP_SERVER.value
            elif kind_value == "producer":
                return SpanType.KAFKA_PRODUCER.value
            elif kind_value == "consumer":
                return SpanType.KAFKA_CONSUMER.value

        return None


class TraceValidator:
    """Валидатор трейсов"""

    def __init__(self, rule_manager: RuleManager):
        self.rule_manager = rule_manager

    def validate_span(self, span: Span, process: Optional[Process] = None) -> ValidationResult:
        """Валидация одного спана"""
        # Определяем тип спана
        span_type = SpanClassifier.classify_span(span)

        # Получаем правила
        spec_rules = self.rule_manager.get_spec_rules(span_type) if span_type else {}
        semantic_rules = self.rule_manager.get_semantic_rules(span_type) if span_type else {}

        # Проверяем соответствие спецификации
        spec_compliance, spec_errors = self._check_specification(span, spec_rules)

        # Проверяем соответствие семантике
        semantic_compliance, semantic_errors = self._check_semantics(span, semantic_rules)

        # Извлекаем необходимые поля
        method = self._extract_method(span)
        library = process.get_tag("telemetry.sdk.language") if process else None
        library_value = library.value if library else "unknown"

        version_tag = span.get_tag("cm.otel.library.version")
        version_value = version_tag.value if version_tag else "unknown"

        service_name = process.service_name if process else "unknown"

        # Формируем полную информацию о всех тегах
        all_tags_info = []
        for tag in span.tags:
            all_tags_info.append({
                "key": tag.key,
                "type": tag.type,
                "value": str(tag.value)
            })

        # Формируем все значения полей
        all_field_values = {}
        for tag in span.tags:
            all_field_values[tag.key] = f"{tag.value} (type: {tag.type})"

        # Формируем значения полей согласно правилам
        field_values = self._extract_field_values(span, spec_rules)

        return ValidationResult(
            span_id=span.span_id,
            operation_name=span.operation_name[:15],
            span_kind=span.span_kind,
            method=method,
            tags_keys=[tag.key for tag in span.tags],
            spec_compliance=spec_compliance,
            semantic_compliance=semantic_compliance,
            field_values=field_values,
            library=library_value,
            version=version_value,
            service_name=service_name,
            spec_errors=spec_errors,
            semantic_errors=semantic_errors,
            all_tags_info=all_tags_info,
            all_field_values=all_field_values
        )

    def _check_specification(self, span: Span, rules: Dict) -> Tuple[bool, List[str]]:
        """Проверка соответствия спецификации"""
        if not rules:
            return False, ["Тип спана не определен или правила отсутствуют"]

        errors = []
        required_tags = rules.get("required_tags", [])
        tag_types = rules.get("tag_types", {})

        # Проверяем наличие обязательных тегов
        span_tag_keys = {tag.key for tag in span.tags}
        for required_tag in required_tags:
            if required_tag not in span_tag_keys:
                errors.append(f"Отсутствует обязательный тег: {required_tag}")

        # Проверяем типы тегов
        for tag in span.tags:
            if tag.key in tag_types:
                expected_type = tag_types[tag.key]
                if tag.type != expected_type:
                    errors.append(
                        f"Неверный тип для тега {tag.key}: "
                        f"ожидается {expected_type}, получено {tag.type}"
                    )

        return len(errors) == 0, errors

    def _check_semantics(self, span: Span, rules: Dict) -> Tuple[bool, List[str]]:
        """Проверка семантических правил"""
        if not rules:
            return False, ["Тип спана не определен или правила отсутствуют"]

        errors = []
        required_tags = rules.get("required_tags", [])
        tag_types = rules.get("tag_types", {})
        value_constraints = rules.get("value_constraints", {})

        # Проверяем наличие обязательных тегов
        span_tag_keys = {tag.key for tag in span.tags}
        for required_tag in required_tags:
            if required_tag not in span_tag_keys:
                errors.append(f"Отсутствует обязательный тег: {required_tag}")

        # Проверяем типы и значения
        for tag in span.tags:
            if tag.key in tag_types:
                expected_type = tag_types[tag.key]
                if tag.type != expected_type:
                    errors.append(
                        f"Неверный тип для тега {tag.key}: "
                        f"ожидается {expected_type}, получено {tag.type}"
                    )

            # Проверяем допустимые значения
            if tag.key in value_constraints:
                allowed_values = value_constraints[tag.key]
                if str(tag.value) not in allowed_values:
                    errors.append(
                        f"Недопустимое значение для тега {tag.key}: "
                        f"{tag.value}. Допустимые значения: {allowed_values}"
                    )

        return len(errors) == 0, errors

    def _extract_method(self, span: Span) -> str:
        """Извлечение HTTP метода из тегов"""
        for tag in span.tags:
            if tag.key in ["method", "http.method"]:
                return str(tag.value)
        return "N/A"

    def _extract_field_values(self, span: Span, rules: Dict) -> Dict[str, str]:
        """Извлечение значений полей согласно правилам"""
        field_values = {}
        required_tags = rules.get("required_tags", [])

        for tag_name in required_tags:
            tag = span.get_tag(tag_name)
            if tag:
                field_values[tag_name] = str(tag.value)
            else:
                field_values[tag_name] = "отсутствует"

        return field_values