import json
from pathlib import Path
from typing import Any, Dict


def write_json_report(report_path: str, report: Dict[str, Any]) -> str:
    output_path = Path(report_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file_handle:
        json.dump(report, file_handle, indent=2, default=str)

    return str(output_path)
