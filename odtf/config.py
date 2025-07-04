import yaml
from typing import List, Dict, Union

from odtf.models import RemoteArchive, CompareRule, TestEntry, Task, UploadTask, SubmitTask, CompareFilesTask

from wwpdb.utils.config.ConfigInfoData import ConfigInfoData

cid = ConfigInfoData()


def parse_task(task_data: Union[str, Dict]) -> Task:
    if isinstance(task_data, str):
        if task_data == "submit":
            return SubmitTask()
    elif isinstance(task_data, dict):
        if "compare_files" in task_data:
            return CompareFilesTask(
                source=task_data.get("source"),
                rules=task_data.get("rules", [])
            )
        elif "upload" in task_data:
            return UploadTask(
                files=task_data.get("files")
            )
    raise ValueError(f"Unknown task type: {task_data}")


class Config:
    CONTENT_TYPE_DICT = cid.getConfigDictionary()["CONTENT_TYPE_DICTIONARY"]
    FORMAT_DICT = cid.getConfigDictionary()["FILE_FORMAT_EXTENSION_DICTIONARY"]

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.remote_archive: RemoteArchive = None
        self.compare_rules: Dict[str, CompareRule] = {}
        self.test_set: Dict[str, TestEntry] = {}
        self.api = {}
        self.report = {}
        self._parse()

    def _parse(self):
        with open(self.file_path, 'r') as f:
            data = yaml.safe_load(f)

        self.api = data.get("api", {})
        self.report = data.get("report", {})
        self.remote_archive = RemoteArchive(**data["remote_archive"])

        for rule_name, rule_data in data.get("compare_rules", {}).items():
            self.compare_rules[rule_name] = CompareRule(
                name=rule_name,
                method=rule_data["method"],
                version=rule_data["version"],
                categories=rule_data.get("categories", [])
            )

        if "test_set" not in data:
            raise ValueError("Test set is missing in the configuration file.")

        for dep_id, entry_data in data["test_set"].items():
            tasks = [parse_task(task_data) for task_data in entry_data["tasks"]]
            self.test_set[dep_id] = TestEntry(dep_id=dep_id, tasks=tasks, skip_fetch=entry_data.get("skip_fetch", False))

    def get_remote_archive(self) -> RemoteArchive:
        return self.remote_archive

    def get_compare_rule(self, name: str) -> CompareRule:
        return self.compare_rules.get(name)

    def get_test_entry(self, dep_id: str) -> TestEntry:
        return self.test_set.get(dep_id)

    def get_test_set(self) -> List[TestEntry]:
        return list(self.test_set.values())