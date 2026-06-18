from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum


class SpanKind(str, Enum):
    CLIENT = "client"
    SERVER = "server"
    PRODUCER = "producer"
    CONSUMER = "consumer"


@dataclass
class Tag:
    key: str
    type: str
    value: Any


@dataclass
class Span:
    trace_id: str
    span_id: str
    operation_name: str
    references: List[Dict]
    start_time: int
    duration: int
    tags: List[Tag]
    logs: List[Dict]
    process_id: str
    warnings: Optional[List[str]]
    span_kind: str = ""

    def get_tag(self, key: str) -> Optional[Tag]:
        for tag in self.tags:
            if tag.key == key:
                return tag
        return None


@dataclass
class Process:
    service_name: str
    tags: List[Tag]

    def get_tag(self, key: str) -> Optional[Tag]:
        for tag in self.tags:
            if tag.key == key:
                return tag
        return None


@dataclass
class Trace:
    trace_id: str
    spans: List[Span]
    processes: Dict[str, Process]
    warnings: Optional[List[str]]


@dataclass
class ValidationResult:
    span_id: str
    operation_name: str
    span_kind: str
    method: str
    tags_keys: List[str]
    spec_compliance: bool
    semantic_compliance: bool
    field_values: Dict[str, str]
    library: str = ""
    version: str = ""
    service_name: str = ""
    spec_errors: List[str] = field(default_factory=list)
    semantic_errors: List[str] = field(default_factory=list)
    all_tags_info: List[Dict[str, str]] = field(default_factory=list)  # Полная информация о всех тегах
    all_field_values: Dict[str, str] = field(default_factory=dict)  # Все значения полей
