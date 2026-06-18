from typing import Dict, List, Any, Optional
from enum import Enum
import json


class SpanType(str, Enum):
    HTTP_CLIENT = "http_client"
    HTTP_SERVER = "http_server"
    KAFKA_PRODUCER = "kafka_producer"
    KAFKA_CONSUMER = "kafka_consumer"


class RuleManager:
    """Менеджер правил валидации с возможностью расширения"""

    def __init__(self, config_path: str = "rules_config.json"):
        self.config_path = config_path
        self.common_tags = {}  # Общие теги для всех типов спанов
        self.spec_rules = {}  # Правила спецификации
        self.semantic_rules = {}  # Семантические правила
        self.conditional_rules = {}  # Условия для условно-обязательных тегов
        self.load_rules()

    def load_rules(self):
        """Загрузка правил из конфигурационного файла"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.common_tags = config.get('common_tags', {})
                self.spec_rules = config.get('specification', {})
                self.semantic_rules = config.get('semantic', {})
                self.conditional_rules = config.get('conditional_rules', {})
        except FileNotFoundError:
            # Используем правила по умолчанию
            self._set_default_rules()
            self.save_rules()

    def _set_default_rules(self):
        """Установка правил по умолчанию"""
        # Общие обязательные теги для всех типов спанов
        self.common_tags = {
            "required": [
                {
                    "key": "cm.otel.fp.id",
                    "type": "string"
                },
                {
                    "key": "cm.otel.fp.module.id",
                    "type": "string"
                }
            ],
            "conditional": [
                {
                    "key": "cm.otel.library.version",
                    "type": "string",
                    "condition": "always_present"  # Условно-обязательный, но почти всегда нужен
                }
            ],
            "optional": [
                {
                    "key": "cm.otel.bs.code",
                    "type": "string"
                },
                {
                    "key": "cm.otel.export.mode",
                    "type": "string"
                },
                {
                    "key": "cm.otel.host.name",
                    "type": "string"
                }
            ]
        }

        self.spec_rules = self._get_default_spec_rules()
        self.semantic_rules = self._get_default_semantic_rules()
        self.conditional_rules = self._get_default_conditional_rules()

    def _get_default_spec_rules(self) -> Dict[str, Any]:
        """Правила спецификации по умолчанию с категориями обязательности"""
        return {
            "http_client": {
                "required": [
                    {"key": "method", "type": "string"},
                    {"key": "http.url", "type": "string"},
                    {"key": "span.kind", "type": "string"}
                ],
                "conditional": [
                    {"key": "status", "type": "string", "condition": "has_response"},
                    {"key": "outcome", "type": "string", "condition": "has_status"},
                    {"key": "uri", "type": "string", "condition": "has_http_url"}
                ],
                "optional": [
                    {"key": "exception", "type": "string"},
                    {"key": "client.name", "type": "string"},
                    {"key": "http.request.header.x-b3-sampled", "type": "string"},
                    {"key": "http.response.body.compress.type", "type": "string"},
                    {"key": "http.response.body", "type": "string"},
                    {"key": "otel.status_code", "type": "string"},
                    {"key": "internal.span.format", "type": "string"}
                ]
            },
            "http_server": {
                "required": [
                    {"key": "method", "type": "string"},
                    {"key": "uri", "type": "string"},
                    {"key": "span.kind", "type": "string"}
                ],
                "conditional": [
                    {"key": "http.url", "type": "string", "condition": "has_uri"},
                    {"key": "status", "type": "string", "condition": "has_response"},
                    {"key": "outcome", "type": "string", "condition": "has_status"}
                ],
                "optional": [
                    {"key": "exception", "type": "string"},
                    {"key": "cm.otel.host.name", "type": "string"},
                    {"key": "otel.status_code", "type": "string"},
                    {"key": "internal.span.format", "type": "string"}
                ]
            },
            "kafka_producer": {
                "required": [
                    {"key": "kafka.topic", "type": "string"},
                    {"key": "span.kind", "type": "string"}
                ],
                "conditional": [
                    {"key": "kafka.key", "type": "string", "condition": "has_key"},
                    {"key": "outcome", "type": "string", "condition": "has_response"}
                ],
                "optional": [
                    {"key": "exception", "type": "string"},
                    {"key": "kafka.partition", "type": "integer"},
                    {"key": "otel.status_code", "type": "string"},
                    {"key": "internal.span.format", "type": "string"}
                ]
            },
            "kafka_consumer": {
                "required": [
                    {"key": "kafka.topic", "type": "string"},
                    {"key": "span.kind", "type": "string"}
                ],
                "conditional": [
                    {"key": "kafka.offset", "type": "string", "condition": "has_offset"},
                    {"key": "kafka.key", "type": "string", "condition": "has_key"},
                    {"key": "outcome", "type": "string", "condition": "has_response"}
                ],
                "optional": [
                    {"key": "exception", "type": "string"},
                    {"key": "kafka.partition", "type": "integer"},
                    {"key": "otel.status_code", "type": "string"},
                    {"key": "internal.span.format", "type": "string"}
                ]
            }
        }

    def _get_default_semantic_rules(self) -> Dict[str, Any]:
        """Семантические правила по умолчанию"""
        return {
            "http_client": {
                "required": [
                    {"key": "method", "type": "string",
                     "allowed_values": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]},
                    {"key": "http.url", "type": "string", "pattern": "^https?://"}
                ],
                "conditional": [
                    {"key": "status", "type": "string", "condition": "has_response"},
                    {"key": "outcome", "type": "string", "condition": "has_status",
                     "allowed_values": ["SUCCESS", "CLIENT_ERROR", "SERVER_ERROR"]}
                ],
                "optional": [
                    {"key": "exception", "type": "string",
                     "allowed_values": ["none", "throwable", "error"]}
                ]
            },
            "http_server": {
                "required": [
                    {"key": "method", "type": "string",
                     "allowed_values": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]},
                    {"key": "uri", "type": "string"}
                ],
                "conditional": [
                    {"key": "status", "type": "string", "condition": "has_response"},
                    {"key": "outcome", "type": "string", "condition": "has_status",
                     "allowed_values": ["SUCCESS", "CLIENT_ERROR", "SERVER_ERROR"]}
                ],
                "optional": []
            },
            "kafka_producer": {
                "required": [
                    {"key": "kafka.topic", "type": "string"}
                ],
                "conditional": [
                    {"key": "outcome", "type": "string", "condition": "has_response",
                     "allowed_values": ["SUCCESS", "FAILURE"]}
                ],
                "optional": []
            },
            "kafka_consumer": {
                "required": [
                    {"key": "kafka.topic", "type": "string"}
                ],
                "conditional": [
                    {"key": "outcome", "type": "string", "condition": "has_response",
                     "allowed_values": ["SUCCESS", "FAILURE"]}
                ],
                "optional": []
            }
        }

    def _get_default_conditional_rules(self) -> Dict[str, Any]:
        """Условия для условно-обязательных тегов"""
        return {
            "conditions": {
                "has_response": {
                    "description": "Тег обязателен, если есть ответ",
                    "check": "tag_exists:status"
                },
                "has_status": {
                    "description": "Тег обязателен, если есть статус",
                    "check": "tag_exists:status"
                },
                "has_http_url": {
                    "description": "Тег обязателен, если есть http.url",
                    "check": "tag_exists:http.url"
                },
                "has_uri": {
                    "description": "Тег обязателен, если есть uri",
                    "check": "tag_exists:uri"
                },
                "has_key": {
                    "description": "Тег обязателен, если есть ключ",
                    "check": "tag_exists:kafka.key"
                },
                "has_offset": {
                    "description": "Тег обязателен, если используется offset",
                    "check": "tag_exists:kafka.offset"
                },
                "always_present": {
                    "description": "Почти всегда должен присутствовать",
                    "check": "always"
                }
            }
        }

    def save_rules(self):
        """Сохранение правил в файл"""
        config = {
            'common_tags': self.common_tags,
            'specification': self.spec_rules,
            'semantic': self.semantic_rules,
            'conditional_rules': self.conditional_rules
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

    def add_span_type(self, type_name: str, spec_config: Dict, semantic_config: Dict, conditional_config: Dict = None):
        """Добавление нового типа спана с правилами"""
        self.spec_rules[type_name] = spec_config
        self.semantic_rules[type_name] = semantic_config
        if conditional_config:
            self.conditional_rules.update(conditional_config)
        self.save_rules()

    def add_common_tag(self, category: str, tag_config: Dict):
        """Добавление общего тега для всех типов спанов"""
        if category not in self.common_tags:
            self.common_tags[category] = []
        self.common_tags[category].append(tag_config)
        self.save_rules()

    def get_spec_rules(self, span_type: str) -> Dict:
        """Получение правил спецификации для типа спана с общими тегами"""
        rules = self.spec_rules.get(span_type, {})
        # Добавляем общие теги к правилам
        return self._merge_with_common_tags(rules)

    def get_semantic_rules(self, span_type: str) -> Dict:
        """Получение семантических правил для типа спана с общими тегами"""
        rules = self.semantic_rules.get(span_type, {})
        # Для семантики тоже добавляем общие теги
        common_semantic = {
            "required": self.common_tags.get("required", []),
            "conditional": self.common_tags.get("conditional", []),
            "optional": self.common_tags.get("optional", [])
        }
        return self._merge_rules(common_semantic, rules)

    def _merge_with_common_tags(self, rules: Dict) -> Dict:
        """Объединение правил с общими тегами"""
        merged = {
            "required": [],
            "conditional": [],
            "optional": []
        }

        # Добавляем общие теги
        for category in ["required", "conditional", "optional"]:
            if category in self.common_tags:
                merged[category].extend(self.common_tags[category])

        # Добавляем специфичные теги
        for category in ["required", "conditional", "optional"]:
            if category in rules:
                merged[category].extend(rules[category])

        return merged

    def _merge_rules(self, common: Dict, specific: Dict) -> Dict:
        """Объединение двух наборов правил"""
        merged = {
            "required": [],
            "conditional": [],
            "optional": []
        }

        for category in ["required", "conditional", "optional"]:
            if category in common:
                merged[category].extend(common[category])
            if category in specific:
                merged[category].extend(specific[category])

        return merged

    def check_condition(self, condition_name: str, span_tags: Dict[str, Any]) -> bool:
        """Проверка выполнения условия для условно-обязательного тега"""
        if condition_name == "always" or condition_name == "always_present":
            return True

        condition_config = self.conditional_rules.get("conditions", {}).get(condition_name, {})
        check = condition_config.get("check", "")

        if check.startswith("tag_exists:"):
            tag_name = check.split(":", 1)[1]
            return tag_name in span_tags

        return False