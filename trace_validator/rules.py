from typing import Dict, List, Any
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
        self.load_rules()

    def load_rules(self):
        """Загрузка правил из конфигурационного файла"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.spec_rules = config.get('specification', {})
                self.semantic_rules = config.get('semantic', {})
        except FileNotFoundError:
            # Используем правила по умолчанию
            self.spec_rules = self._get_default_spec_rules()
            self.semantic_rules = self._get_default_semantic_rules()
            self.save_rules()

    def save_rules(self):
        """Сохранение правил в файл"""
        config = {
            'specification': self.spec_rules,
            'semantic': self.semantic_rules
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

    def _get_default_spec_rules(self) -> Dict[str, Any]:
        """Правила спецификации по умолчанию"""
        return {
            "http_client": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "cm.otel.fp.module.id",
                    "method",
                    "uri",
                    "http.url",
                    "status",
                    "outcome",
                    "exception"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "cm.otel.fp.module.id": "string",
                    "method": "string",
                    "uri": "string",
                    "http.url": "string",
                    "status": "string",
                    "outcome": "string",
                    "exception": "string"
                }
            },
            "http_server": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "cm.otel.fp.module.id",
                    "method",
                    "uri",
                    "http.url",
                    "status",
                    "outcome",
                    "exception"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "cm.otel.fp.module.id": "string",
                    "method": "string",
                    "uri": "string",
                    "http.url": "string",
                    "status": "string",
                    "outcome": "string",
                    "exception": "string"
                }
            },
            "kafka_producer": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "cm.otel.fp.module.id",
                    "kafka.topic",
                    "kafka.key",
                    "outcome",
                    "exception"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "cm.otel.fp.module.id": "string",
                    "kafka.topic": "string",
                    "kafka.key": "string",
                    "outcome": "string",
                    "exception": "string"
                }
            },
            "kafka_consumer": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "cm.otel.fp.module.id",
                    "kafka.topic",
                    "kafka.offset",
                    "outcome",
                    "exception"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "cm.otel.fp.module.id": "string",
                    "kafka.topic": "string",
                    "kafka.offset": "string",
                    "outcome": "string",
                    "exception": "string"
                }
            }
        }

    def _get_default_semantic_rules(self) -> Dict[str, Any]:
        """Семантические правила по умолчанию"""
        return {
            "http_client": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "method",
                    "http.url",
                    "status",
                    "outcome"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "method": "string",
                    "http.url": "string",
                    "status": "string",
                    "outcome": "string"
                },
                "value_constraints": {
                    "method": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
                    "outcome": ["SUCCESS", "CLIENT_ERROR", "SERVER_ERROR"]
                }
            },
            "http_server": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "method",
                    "uri",
                    "status",
                    "outcome"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "method": "string",
                    "uri": "string",
                    "status": "string",
                    "outcome": "string"
                },
                "value_constraints": {
                    "method": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
                    "outcome": ["SUCCESS", "CLIENT_ERROR", "SERVER_ERROR"]
                }
            },
            "kafka_producer": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "kafka.topic",
                    "outcome"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "kafka.topic": "string",
                    "outcome": "string"
                },
                "value_constraints": {
                    "outcome": ["SUCCESS", "FAILURE"]
                }
            },
            "kafka_consumer": {
                "required_tags": [
                    "cm.otel.library.version",
                    "cm.otel.fp.id",
                    "kafka.topic",
                    "outcome"
                ],
                "tag_types": {
                    "cm.otel.library.version": "string",
                    "cm.otel.fp.id": "string",
                    "kafka.topic": "string",
                    "outcome": "string"
                },
                "value_constraints": {
                    "outcome": ["SUCCESS", "FAILURE"]
                }
            }
        }

    def add_span_type(self, type_name: str, spec_config: Dict, semantic_config: Dict):
        """Добавление нового типа спана с правилами"""
        self.spec_rules[type_name] = spec_config
        self.semantic_rules[type_name] = semantic_config
        self.save_rules()

    def get_spec_rules(self, span_type: str) -> Dict:
        """Получение правил спецификации для типа спана"""
        return self.spec_rules.get(span_type, {})

    def get_semantic_rules(self, span_type: str) -> Dict:
        """Получение семантических правил для типа спана"""
        return self.semantic_rules.get(span_type, {})