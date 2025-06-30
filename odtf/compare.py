import re
import json
import hashlib
import subprocess
from abc import ABC

from deepdiff import DeepDiff

from odtf.common import get_file_logger

logger = get_file_logger(__name__)


class FileComparer(ABC):
    """
    Abstract base class for comparing files.
    """

    def __init__(self, file1: str, file2: str):
        self.file1 = file1
        self.file2 = file2

    def compare(self, **kwargs) -> bool:
        """
        Compare two files and return True if they are the same, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class HashComparer(FileComparer):
    def compare(self) -> bool:
        """
        Compare files based on their hash (checksum).
        """
        def calculate_hash(file_path):
            hasher = hashlib.blake2b()
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            return hasher.hexdigest()

        return calculate_hash(self.file1) == calculate_hash(self.file2)


class CIFComparer(FileComparer):
    def _cifdiff(self, file1, file2):
        command = ['gemmi', 'cifdiff', '-q', file1, file2]
        logger.debug(f"Running command: {' '.join(command)}")
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"cifdiff failed with error: {result.stderr.strip()}")

        output_lines = result.stdout.splitlines()
        differences = []

        pattern = re.compile(r'^\s*(_\S+)\.\s+rows:\s+(\d+)(\s+->\s+(\d+))?')
        for line in output_lines:
            match = pattern.match(line)
            if match:
                category = match.group(1)
                old_rows = int(match.group(2))
                new_rows = int(match.group(4)) if match.group(4) else old_rows
                if old_rows != new_rows:
                    differences.append((category, old_rows, new_rows))

        return differences

    def compare(self, categories = []) -> bool:
        """
        Compare files based on CIF categories (example logic).
        """
        differences = self._cifdiff(self.file1, self.file2)
        if differences:
            logger.debug(f"Found differences between {self.file1} and {self.file2}: {differences}")

        return not bool(differences)

    def get_report(self) -> list:
        """
        Get a report of the differences found during comparison.
        """
        return self._cifdiff(self.file1, self.file2)


class JSONComparer(FileComparer):
    def compare(self) -> dict:
        """
        Compare files based on JSON keys and items.
        """
        with open(self.file1, 'r') as f1, open(self.file2, 'r') as f2:
            json1 = json.load(f1)
            json2 = json.load(f2)
        return bool(DeepDiff(json1, json2, ignore_order=True))

    def get_report(self) -> dict:
        """
        Get a report of the differences found during comparison.
        """
        with open(self.file1, 'r') as f1, open(self.file2, 'r') as f2:
            json1 = json.load(f1)
            json2 = json.load(f2)
        return DeepDiff(json1, json2, ignore_order=True)


def comparer_factory(comparison_type: str, file1: str, file2: str) -> FileComparer:
    """
    Factory method to create the appropriate comparer based on the comparison type.
    """
    if comparison_type == "hash":
        return HashComparer(file1, file2)
    elif comparison_type == "cifdiff":
        return CIFComparer(file1, file2)
    elif comparison_type == "jsondiff":
        return JSONComparer(file1, file2)
    else:
        raise ValueError(f"Unknown comparison type: {comparison_type}")
