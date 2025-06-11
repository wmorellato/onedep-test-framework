import yaml
from typing import List, Dict

from odtf.archive import RemoteArchive
from odtf.models import CompareRule, TestEntry, TaskType


class Config:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.remote_archive: RemoteArchive = None
        self.compare_rules: Dict[str, CompareRule] = {}
        self.test_set: Dict[str, TestEntry] = {}  # Changed to a dictionary
        self._parse()

    def _parse(self):
        with open(self.file_path, 'r') as f:
            data = yaml.safe_load(f)

        self.remote_archive = RemoteArchive(**data["remote_archive"])

        for rule_name, rule_data in data["compare_rules"].items():
            self.compare_rules[rule_name] = CompareRule(
                name=rule_name,
                method=rule_data["method"],
                version=rule_data["version"],
                categories=rule_data.get("categories", [])
            )

        for dep_id, entry_data in data["test_set"].items():
            tasks = {
                TaskType(task_name): task_data["compare_files"]
                for task_name, task_data in entry_data["tasks"].items()
            }
            self.test_set[dep_id] = TestEntry(dep_id=dep_id, tasks=tasks)  # Store in dictionary

    def get_remote_archive(self) -> RemoteArchive:
        return self.remote_archive

    def get_compare_rule(self, name: str) -> CompareRule:
        return self.compare_rules.get(name)

    def get_test_entry(self, dep_id: str) -> TestEntry:
        return self.test_set.get(dep_id)  # Access TestEntry by dep_id

    def get_test_set(self) -> List[TestEntry]:
        return self.test_set.values()  # Return all TestEntry objects as a list
