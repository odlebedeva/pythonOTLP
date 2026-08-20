import re
from typing import Dict, Any, List, Tuple, Optional

# Спецификация - это правила, взятые из документации Сайры

class SpecificationValidator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.rules = config.get("specification", {})

    def _get_rules_for_kind(self, kind: str) -> Dict[str, Any]:
        kind_mapping = {
            "client": "http_client",
            "server": "http_server",
            "producer": "kafka_producer",
            "consumer": "kafka_consumer",
        }
        config_key = kind_mapping.get(kind)
        if config_key and config_key in self.rules:
            return self.rules[config_key]
        return {}

    def _check_type(self, value: Any, expected_type: str) -> bool:
        if expected_type == "str":
            return isinstance(value, str)
        elif expected_type == "int":
            return isinstance(value, int)
        elif expected_type == "bool":
            return isinstance(value, bool)
        elif expected_type == "hex16":
            return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{16}", value.lower()) is not None
        elif expected_type == "hex32":
            return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value.lower()) is not None
        return True

    def _check_single_tag(self, key: str, value: Any, rules: Dict[str, Any]) -> Tuple[bool, str]:
        tag_types = rules.get("tag_types", {})
        if key in tag_types:
            expected = tag_types[key]
            if not self._check_type(value, expected):
                return False, f"тип {type(value).__name__} не соответствует {expected}"

        constraints = rules.get("value_constraints", {})
        if key in constraints:
            allowed = constraints[key]
            if value not in allowed:
                return False, f"значение '{value}' не входит в {allowed}"
        return True, ""

    def check_tag(self, key: str, value: Any, kind: str) -> Tuple[bool, str]:
        rules = self._get_rules_for_kind(kind)
        if not rules:
            return True, ""
        return self._check_single_tag(key, value, rules)

    def _evaluate_condition(self, condition: str, tags: Dict[str, Any]) -> bool:
        """
        Простая проверка условия вида: "http.response.status_code >= 400" или "exception != 'none'"
        Поддерживает AND, OR.
        """
        # Для простоты разобьём на части по AND/OR, но пока ограничимся простым парсингом одного условия.
        # Можно использовать eval с заменой имен тегов на значения, но это опасно. Сделаем вручную.
        # Реализуем поддержку: tag operator value, где operator in (==, !=, >=, <=, >, <)
        # и поддержку AND, OR.
        # Упростим: если условие содержит ' AND ', то разделим и проверим оба.
        # Если ' OR ', то разделим и проверим любое.
        # Сначала заменим AND/OR.
        if ' AND ' in condition:
            parts = condition.split(' AND ')
            return all(self._evaluate_simple_condition(p.strip(), tags) for p in parts)
        if ' OR ' in condition:
            parts = condition.split(' OR ')
            return any(self._evaluate_simple_condition(p.strip(), tags) for p in parts)
        return self._evaluate_simple_condition(condition, tags)

    def _evaluate_simple_condition(self, expr: str, tags: Dict[str, Any]) -> bool:
        # expr: "tag operator value"
        # operators: ==, !=, >=, <=, >, <
        import re
        # Разбиваем на tag, operator, value
        # Поддерживаем строки в кавычках
        # Простой вариант: разделим по пробелам? Но значения могут содержать пробелы.
        # Используем регулярное выражение: (\S+)\s+(==|!=|>=|<=|>|<)\s+(.+)
        match = re.match(r'(\S+)\s+(==|!=|>=|<=|>|<)\s+(.+)', expr)
        if not match:
            return False
        tag, op, val_str = match.groups()
        # Получаем значение тега из tags
        tag_value = tags.get(tag)
        if tag_value is None:
            return False  # если тега нет, условие ложно
        # Преобразуем val_str к нужному типу (int, bool, string)
        # Если val_str начинается и заканчивается кавычками, это строка
        if val_str.startswith("'") and val_str.endswith("'"):
            val = val_str[1:-1]
        elif val_str.startswith('"') and val_str.endswith('"'):
            val = val_str[1:-1]
        elif val_str.lower() == 'true':
            val = True
        elif val_str.lower() == 'false':
            val = False
        elif val_str.isdigit():
            val = int(val_str)
        else:
            val = val_str  # строка без кавычек

        # Преобразуем tag_value к тому же типу для сравнения
        if isinstance(val, bool):
            tag_value = bool(tag_value) if isinstance(tag_value, (bool, int)) else tag_value
        elif isinstance(val, int):
            try:
                tag_value = int(tag_value)
            except (ValueError, TypeError):
                return False
        else:
            tag_value = str(tag_value)

        # Сравнение
        if op == '==':
            return tag_value == val
        elif op == '!=':
            return tag_value != val
        elif op == '>=':
            return tag_value >= val
        elif op == '<=':
            return tag_value <= val
        elif op == '>':
            return tag_value > val
        elif op == '<':
            return tag_value < val
        return False

    def validate_span(self, span: Dict[str, Any]) -> Tuple[bool, List[str]]:
        kind = span.get("kind") or span.get("tags", {}).get("span.kind")
        if not kind:
            return False, ["Span kind not specified"]
        kind = kind.lower()
        rules = self._get_rules_for_kind(kind)
        if not rules:
            return True, []

        tags = span.get("tags", {})
        errors = []

        # 1. Проверка обязательных тегов
        required = rules.get("required_tags", [])
        for attr in required:
            if attr not in tags:
                errors.append(f"Отсутствует обязательный атрибут: {attr}")
            else:
                ok, reason = self._check_single_tag(attr, tags[attr], rules)
                if not ok:
                    errors.append(f"Атрибут '{attr}' не соответствует: {reason}")

        # 2. Проверка условно-обязательных тегов
        conditional = rules.get("conditional_required_tags", [])
        for cond_rule in conditional:
            tag = cond_rule.get("tag")
            condition = cond_rule.get("condition", "")
            if not tag or not condition:
                continue
            if self._evaluate_condition(condition, tags):
                # условие истинно -> тег обязателен
                if tag not in tags:
                    errors.append(f"Условно-обязательный атрибут '{tag}' отсутствует при условии: {condition}")
                else:
                    ok, reason = self._check_single_tag(tag, tags[tag], rules)
                    if not ok:
                        errors.append(f"Атрибут '{tag}' не соответствует: {reason}")

        # 3. Проверка рекомендуемых тегов (если присутствуют, проверяем)
        recommended = rules.get("recommended_tags", [])
        for attr in recommended:
            if attr in tags:
                ok, reason = self._check_single_tag(attr, tags[attr], rules)
                if not ok:
                    errors.append(f"Рекомендуемый атрибут '{attr}' не соответствует: {reason}")

        # 4. Проверка Opt-In тегов (аналогично recommended, но могут быть с маской *)
        opt_in = rules.get("opt_in_tags", [])
        for pattern in opt_in:
            # Если паттерн заканчивается на .*, то это маска префикса
            if pattern.endswith(".*"):
                prefix = pattern[:-2]
                for key, value in tags.items():
                    if key.startswith(prefix):
                        ok, reason = self._check_single_tag(key, value, rules)
                        if not ok:
                            errors.append(f"Opt-In атрибут '{key}' не соответствует: {reason}")
            else:
                if pattern in tags:
                    ok, reason = self._check_single_tag(pattern, tags[pattern], rules)
                    if not ok:
                        errors.append(f"Opt-In атрибут '{pattern}' не соответствует: {reason}")

        # Проверка traceID / spanID (как и раньше)
        trace_id = span.get("traceID") or span.get("trace_id")
        if trace_id:
            ok, reason = self._check_single_tag("traceID", trace_id, rules)
            if not ok:
                errors.append(f"traceID {reason}")
        span_id = span.get("spanID") or span.get("span_id")
        if span_id:
            ok, reason = self._check_single_tag("spanID", span_id, rules)
            if not ok:
                errors.append(f"spanID {reason}")

        return len(errors) == 0, errors