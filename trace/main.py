import json
import sys
import pandas as pd
from typing import List, Dict, Any
from specification_validator import SpecificationValidator
from semantic_validator import SemanticValidator

def load_traces(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    spans = []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data:
        for trace in data["data"]:
            processes = trace.get("processes", {})
            for span in trace.get("spans", []):
                proc_id = span.get("processID")
                if proc_id and proc_id in processes:
                    proc = processes[proc_id]
                    span["serviceName"] = proc.get("serviceName", "")
                    if "tags" in proc and isinstance(proc["tags"], list):
                        if "tags" not in span:
                            span["tags"] = []
                        span["tags"].extend(proc["tags"])
                spans.append(span)
        return spans
    if isinstance(data, dict) and "spans" in data:
        return data["spans"]
    return [data]

def normalize_tags(span: Dict[str, Any]) -> Dict[str, Any]:
    tags = span.get("tags")
    if isinstance(tags, list):
        normalized = {}
        for tag in tags:
            key = tag.get("key")
            value = tag.get("value")
            if key is not None:
                normalized[key] = value
        span["tags"] = normalized
    else:
        span["tags"] = tags or {}
    if "span.kind" in span["tags"]:
        span["kind"] = span["tags"]["span.kind"]
    return span

def validate_span(span: Dict[str, Any],
                  spec_validator: SpecificationValidator,
                  sem_validator: SemanticValidator) -> Dict[str, Any]:
    span = normalize_tags(span)
    kind = span.get("kind") or ""

    spec_ok, spec_errors = spec_validator.validate_span(span)
    sem_ok, sem_errors = sem_validator.validate_span(span)

    tag_results = []
    for key, value in span.get("tags", {}).items():
        spec_ok_tag, spec_reason = spec_validator.check_tag(key, value, kind)
        sem_ok_tag, sem_reason = sem_validator.check_tag(key, value, kind)

        spec_status = "✓" if spec_ok_tag else "✗"
        # если семантика вернула "не описан" – ставим "?", иначе "✓" или "✗"
        if sem_ok_tag and sem_reason == "не описан в семантике":
            sem_status = "?"
        else:
            sem_status = "✓" if sem_ok_tag else "✗"

        tag_results.append({
            "key": key,
            "value": value,
            "spec_status": spec_status,
            "spec_reason": spec_reason if not spec_ok_tag else "",
            "sem_status": sem_status,
            "sem_reason": sem_reason if not sem_ok_tag else ""
        })

    return {
        "span": span,
        "kind": kind,
        "spec_ok": spec_ok,
        "spec_errors": spec_errors,
        "sem_ok": sem_ok,
        "sem_errors": sem_errors,
        "tag_results": tag_results
    }

def create_summary_table(results: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for res in results:
        span = res["span"]
        tags = span.get("tags", {})
        op_name = span.get("operationName", "")
        rows.append([
            tags.get("telemetry.sdk.language", ""),
            tags.get("cm.otel.library.version", ""),
            span.get("serviceName", ""),
            span.get("spanID", ""),
            op_name[:15] + ("..." if len(op_name) > 15 else ""),
            tags.get("method", ""),
            res["kind"],
            ", ".join(tags.keys()) if tags else "",
            "✓" if res["spec_ok"] else "✗",
            "✓" if res["sem_ok"] else "✗",
            f"traceId={span.get('traceID','')}, spanId={span.get('spanID','')}",
            "; ".join(res["spec_errors"] + res["sem_errors"]) if (res["spec_errors"] or res["sem_errors"]) else ""
        ])

    columns = [
        "библиотека", "версия", "serviceName", "spanId", "operationName",
        "метод", "span.kind", "tags (ключи)", "соответствие спецификации",
        "соответствие семантике", "значения полей", "ошибки"
    ]
    return pd.DataFrame(rows, columns=columns)

def print_tag_details(results: List[Dict[str, Any]]):
    print("\n" + "=" * 80)
    print("ДЕТАЛИ ПО ТЕГАМ ДЛЯ КАЖДОГО СПАНА")
    print("=" * 80)
    for idx, res in enumerate(results, 1):
        span = res["span"]
        print(f"\nСпан #{idx} (spanID={span.get('spanID', 'N/A')})")
        print(f"  Service: {span.get('serviceName', 'N/A')}")
        print(f"  Operation: {span.get('operationName', 'N/A')}")
        print(f"  Kind: {res['kind']}")
        print("  Теги:")
        for tag in res["tag_results"]:
            print(f"    {tag['key']}: {tag['value']}")
            print(f"        Спецификация: {tag['spec_status']}" +
                  (f" ({tag['spec_reason']})" if tag['spec_reason'] else ""))
            print(f"        Семантика:    {tag['sem_status']}" +
                  (f" ({tag['sem_reason']})" if tag['sem_reason'] else ""))
        if res["spec_errors"] or res["sem_errors"]:
            print("  Ошибки спана:")
            for err in res["spec_errors"]:
                print(f"    [SPEC] {err}")
            for err in res["sem_errors"]:
                print(f"    [SEM]  {err}")

def print_summary_stats(results: List[Dict[str, Any]]):
    total = len(results)
    spec_ok = sum(1 for r in results if r["spec_ok"])
    sem_ok = sum(1 for r in results if r["sem_ok"])
    both_ok = sum(1 for r in results if r["spec_ok"] and r["sem_ok"])
    print("\n" + "=" * 80)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 80)
    print(f"Всего спанов: {total}")
    print(f"Соответствует спецификации Saira: {spec_ok} ({spec_ok/total*100:.1f}%)")
    print(f"Не соответствует спецификации: {total - spec_ok} ({(total - spec_ok)/total*100:.1f}%)")
    print(f"Соответствует семантике OTel: {sem_ok} ({sem_ok/total*100:.1f}%)")
    print(f"Не соответствует семантике: {total - sem_ok} ({(total - sem_ok)/total*100:.1f}%)")
    print(f"Соответствует и спецификации, и семантике: {both_ok} ({both_ok/total*100:.1f}%)")

def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <trace_file.json> <rules_config.json>")
        sys.exit(1)

    trace_file = sys.argv[1]
    rules_file = sys.argv[2]

    try:
        with open(rules_file, 'r', encoding='utf-8') as f:
            config = json.load(f)

        spec_validator = SpecificationValidator(config)
        sem_validator = SemanticValidator(config)

        print(f"Загрузка спанов из {trace_file}...")
        spans = load_traces(trace_file)
        print(f"Загружено {len(spans)} спанов")

        results = []
        for span in spans:
            results.append(validate_span(span, spec_validator, sem_validator))

        df = create_summary_table(results)
        print("\n=== СВОДНАЯ ТАБЛИЦА ===")
        with pd.option_context('display.max_colwidth', None, 'display.width', None):
            print(df.to_string(index=False))

        # Сохранение сводной таблицы в CSV и TXT
        df.to_csv("trace_report.csv", index=False, encoding="utf-8")
        with open("trace_report.txt", "w", encoding="utf-8") as f:
            f.write(df.to_string(index=False))

        # ---- Сохранение деталей по тегам в отдельный TXT ----
        with open("trace_details.txt", "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("ДЕТАЛИ ПО ТЕГАМ ДЛЯ КАЖДОГО СПАНА\n")
            f.write("=" * 80 + "\n")
            for idx, res in enumerate(results, 1):
                span = res["span"]
                f.write(f"\nСпан #{idx} (spanID={span.get('spanID', 'N/A')})\n")
                f.write(f"  Service: {span.get('serviceName', 'N/A')}\n")
                f.write(f"  Operation: {span.get('operationName', 'N/A')}\n")
                f.write(f"  Kind: {res['kind']}\n")
                f.write("  Теги:\n")
                for tag in res["tag_results"]:
                    spec_status = tag["spec_status"]
                    sem_status = tag["sem_status"]
                    f.write(f"    {tag['key']}: {tag['value']}\n")
                    f.write(f"        Спецификация: {spec_status}")
                    if tag['spec_reason']:
                        f.write(f" ({tag['spec_reason']})")
                    f.write("\n")
                    f.write(f"        Семантика:    {sem_status}")
                    if tag['sem_reason']:
                        f.write(f" ({tag['sem_reason']})")
                    f.write("\n")
                if res["spec_errors"] or res["sem_errors"]:
                    f.write("  Ошибки спана:\n")
                    for err in res["spec_errors"]:
                        f.write(f"    [SPEC] {err}\n")
                    for err in res["sem_errors"]:
                        f.write(f"    [SEM]  {err}\n")
        print("\nДетали по тегам сохранены в trace_details.txt")

        # Также выводим в консоль (как раньше)
        print_tag_details(results)
        print_summary_stats(results)

    except FileNotFoundError as e:
        print(f"Ошибка: файл не найден - {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Ошибка: некорректный JSON - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()