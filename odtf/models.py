from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional
from datetime import datetime

from onedep_deposition.enum import FileType
from onedep_deposition.models import (EMSubType, ExperimentType)


class FileTypeMapping:
    ANY_FORMAT = {
        "parameter-file": FileType.CRYSTAL_PARAMETER,
        "virus-matrix": FileType.VIRUS_MATRIX,
        "topology-file": FileType.NMR_TOPOLOGY_GROMACS,
        "nmr-restraints": FileType.NMR_RESTRAINT_OTHER,
        "nmr-peaks": FileType.NMR_SPECTRAL_PEAK,
    }

    MAP = {
        "layer-lines.txt": FileType.LAYER,
        "fsc.xml": FileType.FSC_XML,
        "model.pdb": FileType.PDB_COORD,
        "model.pdbx": FileType.MMCIF_COORD,
        "em-volume.map": FileType.EM_MAP,
        "img-emdb.jpg": FileType.ENTRY_IMAGE,
        "em-additional-volume.map": FileType.EM_ADDITIONAL_MAP,
        "em-mask-volume.map": FileType.EM_MASK,
        "em-half-volume.map": FileType.EM_HALF_MAP,
        "structure-factors.pdbx": FileType.CRYSTAL_STRUC_FACTORS,
        "structure-factors.mtz": FileType.CRYSTAL_MTZ,
        "nmr-chemical-shifts.nmr-star": FileType.NMR_ACS,
        "nmr-restraints.amber": FileType.NMR_RESTRAINT_AMBER,
        # "nmr-restraints.aria": FileType.NMR_RESTRAINT_ARIA,  # MISSING
        "nmr-restraints.biosym": FileType.NMR_RESTRAINT_BIOSYM,
        "nmr-restraints.charmm": FileType.NMR_RESTRAINT_CHARMM,
        "nmr-restraints.cns": FileType.NMR_RESTRAINT_CNS,
        "nmr-restraints.cyana": FileType.NMR_RESTRAINT_CYANA,
        "nmr-restraints.pales": FileType.NMR_RESTRAINT_DYNAMO,
        "nmr-restraints.talos": FileType.NMR_RESTRAINT_DYNAMO,
        "nmr-restraints.dynamo": FileType.NMR_RESTRAINT_DYNAMO,
        "nmr-restraints.gromacs": FileType.NMR_RESTRAINT_GROMACS,
        "nmr-restraints.isd": FileType.NMR_RESTRAINT_ISD,
        "nmr-restraints.rosetta": FileType.NMR_RESTRAINT_ROSETTA,
        "nmr-restraints.sybyl": FileType.NMR_RESTRAINT_SYBYL,
        "nmr-restraints.xplor-nih": FileType.NMR_RESTRAINT_XPLOR,
        "nmr-data-nef.nmr-star": FileType.NMR_UNIFIED_NEF,
        "nmr-data-str.nmr-star": FileType.NMR_UNIFIED_STAR,
    }


    @staticmethod
    def get_file_type(content_type: str, format: str) -> FileType:
        """Get the FileType enum based on the filename."""
        if f"{content_type}.{format}" in FileTypeMapping.MAP:
            return FileTypeMapping.MAP[f"{content_type}.{format}"]

        if content_type in FileTypeMapping.ANY_FORMAT:
            return FileTypeMapping.ANY_FORMAT[content_type]

        raise ValueError(f"Unknown content type and format combination: {content_type}.{format}")


class TaskType(Enum):
    UPLOAD = "upload"
    SUBMIT = "submit"
    COMPARE_FILES = "compare_files"


class TaskStatus(Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    WARNING = "warning"


@dataclass
class Task:
    type: TaskType
    status: TaskStatus = TaskStatus.PENDING
    stop_on_failure: bool = False
    error_message: Optional[str] = None
    execution_time: Optional[datetime] = None


@dataclass
class UploadTask(Task):
    files: List[str] = None

    def __init__(self, files: List[str]):
        super().__init__(type=TaskType.UPLOAD, stop_on_failure=False)
        self.files = files


@dataclass
class SubmitTask(Task):
    def __init__(self):
        super().__init__(type=TaskType.SUBMIT)


@dataclass
class CompareRule:
    name: str
    method: str
    version: str
    status: TaskStatus = TaskStatus.PENDING
    error_message: Optional[str] = None
    categories: List[str] = field(default_factory=list)


@dataclass
class CompareFilesTask(Task):
    source: Optional[str] = None
    rules: List[CompareRule] = field(default_factory=list)

    def __init__(self, source: Optional[str], rules: List[str]):
        super().__init__(type=TaskType.COMPARE_FILES)
        self.source = source
        self.rules = [CompareRule(name=rule, method="", version="") for rule in rules]


@dataclass
class TestEntry:
    dep_id: str
    copy_dep_id: str = field(default=None)
    tasks: List[Task] = field(default_factory=list)
    log_file: Optional[str] = None
    skip_fetch: bool = False

    def has_task(self, task_type: TaskType) -> bool:
        return any(task.type == task_type for task in self.tasks)


@dataclass
class EntryStatus:
    status: str = "pending"
    arch_dep_id: str = None
    arch_entry_id: str = "?"
    exp_type: ExperimentType = None
    exp_subtype: EMSubType = None
    copy_dep_id: str = "?"
    message: str = "Preparing..."

    def __repr__(self):
        return f"EntryStatus(status={self.status}, arch_dep_id={self.arch_dep_id}, arch_entry_id={self.arch_entry_id}, exp_type={self.exp_type}, test_dep_id={self.copy_dep_id}, message={self.message})"

    def __str__(self):
        exp_type = self.exp_type.value if self.exp_type else "?"
        return f"{self.arch_dep_id} ({self.arch_entry_id}) → {self.copy_dep_id} {exp_type}: {self.message}"


@dataclass
class TestReport:
    """Model for the complete test report"""
    base_url: str
    test_entries: List[TestEntry]
    generation_time: datetime = field(default_factory=datetime.now)
    title: str = "ODTF Test Report"
    summary: Dict[str, int] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate summary statistics"""
        self.summary = {
            'total_entries': len(self.test_entries),
            'total_tasks': sum(len(entry.tasks) for entry in self.test_entries),
            'successful_tasks': 0,
            'failed_tasks': 0,
            'pending_tasks': 0
        }
        
        for entry in self.test_entries:
            for task in entry.tasks:
                if task.status == TaskStatus.SUCCESS:
                    self.summary['successful_tasks'] += 1
                elif task.status == TaskStatus.FAILED:
                    self.summary['failed_tasks'] += 1
                else:
                    self.summary['pending_tasks'] += 1


@dataclass
class RemoteArchive:
    host: str
    user: str
    site_id: str
    key_file: str = None


@dataclass
class LocalArchive:
    site_id: str
