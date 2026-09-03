import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reports import write_json_report


def test_write_json_report_writes_json_to_disk(tmp_path):
    report = {"status": "ok", "items": [1, 2, 3]}
    output_file = tmp_path / "report.json"

    written_path = write_json_report(str(output_file), report)

    assert written_path == str(output_file)
    assert output_file.exists()
    assert json.loads(output_file.read_text(encoding="utf-8"))["status"] == "ok"
