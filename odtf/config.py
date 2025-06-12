import yaml
from typing import List, Dict, Union

from odtf.archive import RemoteArchive
from odtf.models import CompareRule, TestEntry, TaskType, Task, UploadTask, SubmitTask, CompareFilesTask


def parse_task(task_data: Union[str, Dict]) -> Task:
    if isinstance(task_data, str):
        if task_data == "upload":
            return UploadTask()
        elif task_data == "submit":
            return SubmitTask()
    elif isinstance(task_data, dict):
        if "compare_files" in task_data:
            return CompareFilesTask(
                source=task_data.get("source"),
                rules=task_data.get("rules", [])
            )
    raise ValueError(f"Unknown task type: {task_data}")


class Config:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.remote_archive: RemoteArchive = None
        self.compare_rules: Dict[str, CompareRule] = {}
        self.test_set: Dict[str, TestEntry] = {}
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
            tasks = [parse_task(task_data) for task_data in entry_data["tasks"]]
            self.test_set[dep_id] = TestEntry(dep_id=dep_id, tasks=tasks)

    def get_remote_archive(self) -> RemoteArchive:
        return self.remote_archive

    def get_compare_rule(self, name: str) -> CompareRule:
        return self.compare_rules.get(name)

    def get_test_entry(self, dep_id: str) -> TestEntry:
        return self.test_set.get(dep_id)

    def get_test_set(self) -> List[TestEntry]:
        return list(self.test_set.values())