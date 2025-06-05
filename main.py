import os
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import pickle
import concurrent.futures
import hashlib
import logging
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import click
import MySQLdb

from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from onedep_deposition.deposit_api import DepositApi
from onedep_deposition.enum import Country, FileType
from onedep_deposition.models import (DepositError, DepositStatus, EMSubType,
                                      ExperimentType)

from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.depui.constants import uploadDict
from wwpdb.apps.deposit.main.archive import ArchiveRepository, LocalFileSystem
from wwpdb.apps.deposit.main.schemas import ExperimentTypes
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import (ConfigInfoAppBase,
                                              ConfigInfoAppCommon)

file_logger = logging.getLogger(__name__)
file_logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("onedep_test.log")
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
file_logger.handlers.clear()
file_logger.addHandler(file_handler)
file_logger.propagate = False


config = ConfigInfo()
configApp = ConfigInfoAppBase()

for l in ["wwpdb.apps.deposit.main.archive", "onedep_deposition.rest_adapter", "urllib3", "requests"]:
    logger = logging.getLogger(l)
    logger.setLevel(logging.CRITICAL)
    logger.propagate = True


ORCID = "0000-0002-5109-8728"
PROD_SITE_ID = "PDBE_DEV" # ENTER THIS
PROD_HOST = "pdb-002.ebi.ac.uk" # ENTER THIS
PROD_CONFIG = ConfigInfo(siteId=PROD_SITE_ID)
DEBUG = False


class FileTypeMapping:
    ANY_FORMAT = {
        "parameter-file.any": FileType.CRYSTAL_PARAMETER,
        "topology-file.any": FileType.CRYSTAL_TOPOLOGY,
        "virus-matrix.any": FileType.VIRUS_MATRIX,
        "nmr-restraints.any": FileType.NMR_TOPOLOGY_AMBER,
        "nmr-restraints.any": FileType.NMR_RESTRAINT_CNS,
        "topology-file.any": FileType.NMR_TOPOLOGY_GROMACS,
        "nmr-restraints.any": FileType.NMR_RESTRAINT_OTHER,
        "nmr-peaks.any": FileType.NMR_SPECTRAL_PEAK,
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
        "nmr-restraints.aria": FileType.NMR_RESTRAINT_BIOSYM,
        "nmr-restraints.biosym": FileType.NMR_RESTRAINT_CHARMM,
        "nmr-restraints.charmm": FileType.NMR_RESTRAINT_CYANA,
        "nmr-restraints.cns": FileType.NMR_RESTRAINT_DYNAMO,
        "nmr-restraints.cyana": FileType.NMR_RESTRAINT_PALES,
        "nmr-restraints.dynamo": FileType.NMR_RESTRAINT_TALOS,
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
        if content_type in FileTypeMapping.ANY_FORMAT:
            return FileTypeMapping.ANY_FORMAT[content_type]

        if f"{content_type}.{format}" in FileTypeMapping.MAP:
            return FileTypeMapping.MAP[f"{content_type}.{format}"]

        raise ValueError(f"Unknown content type and format combination: {content_type}.{format}")


def parse_voxel_values(filepath):
    """
    Parse a pickle file and extract the first contour_level and pixel_spacing values.
    
    Args:
        dep_id (str): The deposition ID to locate the pickle file.
        
    Returns:
        tuple: (contour_level, pixel_spacing_x/y/z) or None if not found
    """
    try:
        with open(filepath, 'rb') as f:
            data = pickle.load(f)

        item = data['items'][0]
        contour_level = item['contour_level']['value']

        # Get first available pixel spacing
        pixel_spacing = None
        for axis in ['x', 'y', 'z']:
            if f'pixel_spacing_{axis}' in item:
                pixel_spacing = item[f'pixel_spacing_{axis}']['value']
                break
    except:
        file_logger.error("Error reading voxel values from %s", filepath)
        return None, None 
    
    return contour_level, pixel_spacing


@dataclass
class RemoteArchive:
    host: str
    user: str
    root_path: str


@dataclass
class EntryStatus:
    status: str = "pending"
    arch_dep_id: str = None
    arch_entry_id: str = "?"
    exp_type: ExperimentType = None
    exp_subtype: EMSubType = None
    test_dep_id: str = "?"
    message: str = "Starting tests..."

    def __repr__(self):
        return f"EntryStatus(status={self.status}, arch_dep_id={self.arch_dep_id}, arch_entry_id={self.arch_entry_id}, exp_type={self.exp_type}, test_dep_id={self.test_dep_id}, message={self.message})"

    def __str__(self):
        exp_type = self.exp_type.value if self.exp_type else "?"
        return f"{self.arch_dep_id} ({self.arch_entry_id}) → {self.test_dep_id} {exp_type}: {self.message}"


class StatusManager:
    """
    Minimal thread-safety with your exact logic preserved.
    """
    def __init__(self, dep_id_list, callback):
        self.statuses = {dep_id: EntryStatus(arch_dep_id=dep_id) for dep_id in dep_id_list}
        self.callback = callback
        self._lock = threading.RLock()

    def update_status(self, dep_id, **kwargs):
        with self._lock:
            if dep_id not in self.statuses:
                raise KeyError(f"DepID {dep_id} not found in statuses.")

            file_logger.info(str(self.statuses[dep_id]))

            for key, value in kwargs.items():
                if hasattr(self.statuses[dep_id], key):
                    setattr(self.statuses[dep_id], key, value)
                else:
                    raise AttributeError(f"Invalid field '{key}' for EntryStatus.")

            if self.callback:
                self.callback()

    def get_status(self, dep_id):
        with self._lock:
            if dep_id not in self.statuses:
                raise KeyError(f"DepID {dep_id} not found in statuses.")
            return self.statuses[dep_id]

    def __iter__(self):
        with self._lock:
            return iter(list(self.statuses.values()))


prod_host = {
    "host": PROD_HOST,
    "user": PROD_CONFIG.get("LOCAL_SERVICE_OWNER"),
    "onedep_root": PROD_CONFIG.get("TOP_SOFTWARE_DIR"),
}

prod_db = {
    "host": PROD_CONFIG.get("SITE_DB_HOST_NAME"),
    "user": PROD_CONFIG.get("SITE_DB_USER_NAME"),
    "password": PROD_CONFIG.get("SITE_DB_PASSWORD"),
    "port": int(PROD_CONFIG.get("SITE_DB_PORT_NUMBER")),
    "database": "status",
}

# ConfigInfoData._contentTypeInfoBaseD {content-type: ([format], ?)}
# ConfigInfoData._fileFormatExtensionD extensions

test_file_catalogue = {
    # exp type
    "xray": {
        # stage
        "upload": {
            "model": [
                # [milestone, version]
                ["upload", "original"],
                ["upload-convert", "latest"],
                [None, "latest"],
            ],
            "model-issues-report": [
                [None, "latest"],
            ],
            "structure-factors": [
                ["upload", "original"],
                ["upload-convert", "original"],
                [None, "latest"],
            ],
            "structure-factor-report": [
                [None, "latest"],
            ],
            "chem-comp-assign-details": [
                [None, "latest"],
            ],
            "assembly-model": [
                [None, "latest"],
            ]
        },
        "reupload": {
            "merge-xyz-report": [
                [None, "latest"],
            ],
        },
        "pre-submission": {
            "chem-comp-depositor-info": [
                [None, "latest"],
            ]
        }
    },
    "em": {
        "upload": {
            "model": [
                ["upload", "original"],
                ["upload-convert", "latest"],
                [None, "latest"],
            ],
            "em-volume": [
                [None, "latest"],
            ],
            "em-mask-volume": [
                [None, "latest"],
            ],
            "em-additional-volume": [
                [None, "latest"],
            ],
            "em-half-volume": [
                [None, "latest"],
            ],
            "mapfix-header-report": [
                [None, "latest"],
            ]
        }
    },
    "nmr": {

    }
}

BASE_FILES_DIR = "base"
UPLOAD_FILES_DIR = "upload"

api = DepositApi(api_key=create_token(ORCID, expiration_days=1/24), hostname="https://localhost:12000/deposition", ssl_verify=False)


class RemoteFetcher:
    def __init__(self, remote_archive: RemoteArchive, cache_location, cache_size=10):
        self.remote_archive = remote_archive
        self.cache_location = cache_location
        self.cache_size = cache_size

    def fetch(self, dep_id, destination, repository="deposit"):
        """ Fetches a deposition from the remote archive and stores it in the local cache.
        If the deposition is already cached, it copies it to the destination.
        If the cache is full, it evicts the oldest entry.

        Args:
            dep_id (str): The deposition ID to fetch.
            destination (str): The local path where the deposition should be copied. No manipulation is done to this path before copy.
            repository (str): The repository from which to fetch the deposition. Defaults to "deposit".
        """
        cached_dep = os.path.join(self.cache_location, dep_id)

        if self._is_cached(dep_id):
            self._copy_from_cache(dep_id, destination)
            return

        self._evict_oldest_entry()

        os.makedirs(cached_dep, exist_ok=True)

        file_logger.info("Entry %s not found in cache. Downloading from remote host %s", dep_id, self.remote_archive.host)
        self._fetch_from_remote(dep_id, repository, cached_dep)
        self._copy_from_cache(dep_id, destination)

        # TODO: must return a LocalArchiveish object

    def _is_cached(self, dep_id):
        deposition_folder = os.path.join(self.cache_location, dep_id)
        return os.path.exists(deposition_folder)

    def _copy_from_cache(self, dep_id, destination):
        if not self._is_cached(dep_id):
            file_logger.warning("Unable to copy entry from cache (not found)")
            return

        file_logger.info("Entry %s found in cache. Copying to %s", dep_id, destination)

        try:
            shutil.copytree(os.path.join(self.cache_location, dep_id), destination, dirs_exist_ok=True)
        except Exception as e:
            file_logger.error("Error copying files from cache: %s", e)
            raise

    def _remove_from_cache(self, dep_id):
        if not self._is_cached(dep_id):
            file_logger.warning("Unable to remove entry from cache (not found)")
            return
        shutil.rmtree(os.path.join(self.cache_location, dep_id))

    def _evict_oldest_entry(self):
        deposition_folders = [folder for folder in os.listdir(self.cache_location) if folder.startswith("D_")]
        if len(deposition_folders) >= self.cache_size:
            oldest_folder = min(deposition_folders, key=os.path.getctime)
            shutil.rmtree(os.path.join(self.cache_location, oldest_folder))

    def _fetch_from_remote(self, dep_id, repository, local_path):
        # ideally this should be taken from site-config
        # a local copy of the remote site-config would be necessary, so leaving this as is now
        remote_data_path = os.path.join(self.remote_archive.root_path, "data", "dev", repository, dep_id)
        remote_pickles_path = os.path.join(self.remote_archive.root_path, "data", "dev", "deposit", "temp_files", "deposition-v-200", dep_id)
        local_data_path = os.path.join(local_path, "data")
        local_pickles_path = os.path.join(local_path, "pickles")

        os.makedirs(local_data_path, exist_ok=True)
        os.makedirs(local_pickles_path, exist_ok=True)

        if self.remote_archive.host == "localhost":
            file_logger.debug("Copying locally from %s to %s", remote_data_path, local_path)
            shutil.copytree(remote_data_path, os.path.join(local_path, "data"), dirs_exist_ok=True)
            shutil.copytree(remote_pickles_path, os.path.join(local_path, "pickles"), dirs_exist_ok=True)
            return

        rsync_command = ["rsync", "-arvzL", f"{self.remote_archive.user}@{self.remote_archive.host}:{remote_data_path}/", local_path]
        file_logger.debug("Running command %s", ' '.join(rsync_command))
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        rsync_command = ["rsync", "-arvzL", f"{self.remote_archive.user}@{self.remote_archive.host}:{remote_pickles_path}/", local_pickles_path]
        file_logger.debug("Running command %s", ' '.join(rsync_command))
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def cifdiff(file1, file2):
    """Compare two mmCIF files and return a list of differences"""
    bin_location = os.path.join(configApp.get_site_packages_path(), "cifdiff", "coord-file-pack", "bin", "CifDiff")
    command = [bin_location, file1, file2]

    try:
        output = subprocess.check_output(command, universal_newlines=True)
        print(output)
    except subprocess.CalledProcessError as e:
        print("[!] error running cifdiff", e.output)


def create_deposition(etype: ExperimentType, email: str, subtype: EMSubType = None, coordinates: bool = True, related_emdb: str = None, no_map: bool = False):
    """NOTE: if this code ever moves to a separate package, the deposition
    will have to be created using a proper HTTP request.
    """
    users = [ORCID]
    country = Country.UK
    password = "123456"

    if etype == ExperimentType.EM:
        deposition = api.create_em_deposition(email, users, country, subtype, coordinates, related_emdb, password)
    elif etype == ExperimentType.XRAY:
        deposition = api.create_xray_deposition(email, users, country, password)
    elif etype == ExperimentType.FIBER:
        deposition = api.create_fiber_deposition(email, users, country, password)
    elif etype == ExperimentType.NEUTRON:
        deposition = api.create_neutron_deposition(email, users, country, password)
    elif etype == ExperimentType.EC:
        deposition = api.create_ec_deposition(email, users, country, coordinates, password, related_emdb, no_map)
    elif etype == ExperimentType.NMR:
        deposition = api.create_nmr_deposition(email, users, country, coordinates, password)
    elif etype == ExperimentType.SSNMR:
        deposition = api.create_ssnmr_deposition(email, users, country, coordinates, password)
    else:
        raise ValueError(f"Unknown experiment type: {etype}")

    return deposition


def upload_files(test_dep_id: str, arch_dep_id: str, base_files_location: str, status_manager: StatusManager):
    # getting all files with the 'upload' milestone from the archive dir
    previous_files = [f for f in os.listdir(os.path.join(base_files_location, "data")) if "upload_P" in f]
    uploaded_files = []
    contour_level, pixel_spacing = parse_voxel_values(os.path.join(base_files_location, "pickles", "em_map_upload.pkl"))

    for f in previous_files:
        fobj = parse_filename(repository=ArchiveRepository.ARCHIVE.value, filename=f)
        content_type = fobj.getContentType()
        file_format = fobj.getFileFormat()

        status_manager.update_status(arch_dep_id, message=f"Uploading `{content_type}.{file_format}`")

        try:
            filetype = FileTypeMapping.get_file_type(content_type.replace("-upload", ""), file_format)
            file_path = os.path.join(base_files_location, "data", f)
            file = api.upload_file(test_dep_id, file_path, filetype, overwrite=False)
            uploaded_files.append(file)

            if filetype in (FileType.EM_MAP, FileType.EM_ADDITIONAL_MAP, FileType.EM_MASK, FileType.EM_HALF_MAP):
                if contour_level is not None:
                    status_manager.update_status(arch_dep_id, message=f"Updating metadata for {f} with contour level {contour_level} and pixel spacing {pixel_spacing}")
                    api.update_metadata(test_dep_id, file.file_id, contour=contour_level, spacing_x=pixel_spacing, spacing_y=pixel_spacing, spacing_z=pixel_spacing, description="Uploaded from test script")
                else:
                    raise Exception("Contour level or pixel spacing not found in pickle file. Can't continue automatically.")
        except ValueError as e:
            status_manager.update_status(arch_dep_id, status="failed", message=f"Error getting file type for {content_type}.{file_format}: {e}")
            raise
        except:
            status_manager.update_status(arch_dep_id, status="failed", message=f"Error uploading {content_type} {file_format}")
            raise
    
    return uploaded_files


def reupload_files(dep_id: str, base_dep_id: str, base_files_location: str):
    lfs = LocalFileSystem()

    for f in api.get_files(dep_id).files:
        print("[+] removing file", f.file_id)
        api.remove_file(dep_id, f.file_id)

    for f in input_files:
        wdo = lfs.locate(dep_id=base_dep_id, repository=ArchiveRepository.TEMPDEP, content=f[1], format=f[2], version="latest")
        basename = os.path.basename(wdo.getFilePathReference())
        test_file = os.path.join(base_files_location, basename)
        filetype = f[0]

        try:
            file = api.upload_file(dep_id, test_file, filetype, overwrite=True)
        except Exception as e:
            print("[!] failed to upload %s, %s" % (test_file, e))


def compare_files(dep_id: str, base_dep_id: str, base_files_location: str):
    lfs = LocalFileSystem()
    console = Console()

    def calculate_md5(file_path):
        with open(file_path, "rb") as file:
            md5_hash = hashlib.md5()
            while chunk := file.read(4096):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    for f in files_to_compare:
        target_wdo = lfs.locate(dep_id=dep_id, repository=ArchiveRepository.DEPOSIT, content=f[0], format=f[1], version=f[2], milestone=f[3])
        target_path = target_wdo.getFilePathReference()
        # target_version = target_wdo.getFileVersionNumber()

        base_wdo = lfs.locate(dep_id=base_dep_id, repository=ArchiveRepository.TEMPDEP, content=f[0], format=f[1], version=f[2], milestone=f[3])
        base_path = base_wdo.getFilePathReference()
        if not lfs.exists(base_wdo):
            console.print(f"[yellow]base file not found: {base_path}[/yellow]")
            continue
        # base_filename = os.path.basename(base_wdo.getFilePathReference())
        # base_path = os.path.join(base_files_location, base_filename)

        base_md5 = calculate_md5(base_path)
        target_md5 = calculate_md5(target_path)

        if base_md5 == target_md5:
            console.print(f"[green]base: {base_path} deposit: {target_path}[/green]")
        else:
            console.print(f"[red]base: {base_path} deposit: {target_path}[/red]")
            if f[1] == "pdbx":
                cifdiff(base_path, target_path)


def create_and_process(dep_id, fetcher: RemoteFetcher, status_manager: StatusManager, keep_temp=False):
    exp_type = status_manager.get_status(dep_id).exp_type # feels hackish
    exp_subtype = status_manager.get_status(dep_id).exp_subtype # feels hackish

    status_manager.update_status(dep_id, status="working", message="Creating test deposition")    
    try:
        test_dep = create_deposition(etype=exp_type, subtype=exp_subtype, email="wbueno@ebi.ac.uk")
        # time.sleep(random.randint(1, 5))
    except Exception as e:
        raise Exception("Error creating test deposition") from e

    # status_manager.update_status(dep_id, test_dep_id=dep_id, message="Fetching files from archive")
    status_manager.update_status(dep_id, test_dep_id=test_dep.dep_id, message="Fetching files from archive")
    try:
        tmp_dir = tempfile.mkdtemp(prefix="onedep_test_")
        base_dep_dir = os.path.join(tmp_dir, BASE_FILES_DIR, test_dep.dep_id)
        os.makedirs(base_dep_dir, exist_ok=True)

        fetcher.fetch(dep_id, base_dep_dir)
    except Exception as e:
        raise Exception("Error fetching files from archive") from e

    upload_files(test_dep_id=test_dep.dep_id, arch_dep_id=dep_id, base_files_location=base_dep_dir, status_manager=status_manager)

    copy_elements = {"copy_contact": False, "copy_authors": False, "copy_citation": False, "copy_grant": False, "copy_em_exp_data": False}
    response = api.process(test_dep.dep_id, **copy_elements)
    status = "started"

    try:
        while status in ("running", "started", "submit"):
            response = api.get_status(test_dep.dep_id)

            if isinstance(response, DepositStatus):
                if response.details == status_manager.get_status(dep_id).message:
                    continue

                status_manager.update_status(dep_id, status="working", message=f"{response.details}")
                status = response.status

                if status == "error":
                    raise Exception(response.details)
            elif isinstance(response, DepositError):
                raise Exception(response.message)
            else:
                raise Exception(f"Unknown response type {type(response)}")

            time.sleep(5)
    finally:
        if not keep_temp:
            shutil.rmtree(tmp_dir)


def get_entry_info(dep_id, status_manager):
    """Part of the main testing pipeline, hence the status_manager here. Only Jesus can judge me
    """
    connection = MySQLdb.connect(**prod_db)
    status_manager.update_status(dep_id, status="working", message="Getting entry info")

    try:
        with connection.cursor() as cursor:
            sql = "SELECT pdb_id, emdb_id, bmrb_id, exp_method FROM deposition WHERE dep_set_id = '%s'" % dep_id.upper()
            cursor.execute(sql)
            result = cursor.fetchall()
            
            if len(result) == 0:
                return

            eid = '?'
            for i in range(2):
                if result[0][i] and result[0][i] != "?":
                    eid = result[0][i].lower()
                    break

            exp = result[0][3].lower()
            exp_method = None
            exp_submethod = None
            if 'xray' in exp or 'x-ray' in exp:
                exp_method = ExperimentType.XRAY
            elif 'crystallography' in exp:
                exp_method = ExperimentType.EC
            elif 'microscopy' in exp or 'em' in exp:
                exp_method = ExperimentType.EM

                if 'tomography' in exp:
                    exp_submethod = EMSubType.TOMOGRAPHY
                elif 'subtomogram' in exp:
                    exp_submethod = EMSubType.SUBTOMOGRAM
                elif 'helical' in exp:
                    exp_submethod = EMSubType.HELICAL
                else:
                    exp_submethod = EMSubType.SPA
            elif 'solution' in exp:
                exp_method = ExperimentType.SSNMR
            elif 'solid' in exp:
                exp_method = ExperimentType.NMR
            elif 'fiber' in exp:
                exp_method = ExperimentType.FIBER
            elif 'neutron' in exp:
                exp_method = ExperimentType.NEUTRON

            status_manager.update_status(
                dep_id,
                arch_entry_id=eid,
                exp_type=exp_method,
                exp_subtype=exp_submethod,
            )
    finally:
        connection.close()


def generate_table(status_manager):
    """Generate status table for display with spinners"""
    table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
    spinner = Spinner("dots", text="", style="blue")

    for entry_status in sorted(status_manager, key=lambda x: x.arch_dep_id or ""):
        arch_dep_id = entry_status.arch_dep_id
        arch_entry_id = entry_status.arch_entry_id
        exp_type = entry_status.exp_type.value if entry_status.exp_type else "?"
        test_dep_id = entry_status.test_dep_id
        message = entry_status.message

        if entry_status.status in ("working", "pending"):
            table.add_row(spinner, f"{arch_dep_id} [bright_cyan]({arch_entry_id})[/bright_cyan] → {test_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")
        elif entry_status.status == "finished":
            table.add_row("[green]✓[/green]", f"{arch_dep_id} [bright_cyan]({arch_entry_id})[/bright_cyan] → {test_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")
        elif entry_status.status == "failed":
            table.add_row("[red]✗[/red]", f"[red]{arch_dep_id}[/red] [bright_cyan]({arch_entry_id})[/bright_cyan] → {test_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")

    return table


@click.command()
@click.argument('dep_id_list', nargs=-1)
@click.option('--reupload', is_flag=True, help='Test reupload after submission')
@click.option('--keep-temp', is_flag=True, help='Keep temporary files')
@click.option('--cache-location', default="/wwpdb/onedep/testcache", help='Cache location')
def main(dep_id_list, reupload, keep_temp, cache_location):
    if "pro" in getSiteId().lower():
        print("[!] this script should not be run in production. Exiting.")
        return

    remote = RemoteArchive(host="localhost", user="onedep", root_path="/wwpdb/onedep")
    fetcher = RemoteFetcher(remote, cache_location=cache_location)

    with Live(refresh_per_second=15) as live:
        def update_callback():
            """Callback to update the live display"""
            live.update(generate_table(status_manager))

        # Use the simpler thread-safe version
        status_manager = StatusManager(dep_id_list, callback=update_callback)
        live.update(generate_table(status_manager))
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures_info = {executor.submit(get_entry_info, dep_id, status_manager): dep_id for dep_id in dep_id_list}

            for future in concurrent.futures.as_completed(futures_info):
                dep_id = futures_info[future]
                try:
                    future.result()
                except Exception as e:
                    status_manager.update_status(dep_id, status="failed", message=f"Error getting entry info ({e})")

            futures_process = {executor.submit(create_and_process, dep_id, fetcher, status_manager, False): dep_id for dep_id in dep_id_list}

            for future in concurrent.futures.as_completed(futures_process):
                dep_id = futures_process[future]
                try:
                    future.result()
                except Exception as e:
                    file_logger.error("Error processing entry %s: %s", dep_id, e)
                    status_manager.update_status(dep_id, status="failed", message=f"Error processing entry ({e})")


if __name__ == '__main__':
    main()

# D_8233000125 D_8233000126 D_8233000127 D_8233000128 D_8233000129 D_8233000130 D_8233000131 D_8233000132 D_8233000133 D_8233000134 D_8233000135 D_8233000136 D_8233000137 D_8233000138
