import logging
import os
import sys
import shutil
import time

import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import concurrent.futures
import hashlib
import os
import random
import shutil
import subprocess
import tempfile
import traceback
from concurrent.futures import ThreadPoolExecutor

import click
import MySQLdb
from onedep_deposition.deposit_api import DepositApi
from onedep_deposition.enum import Country, FileType
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import (ConfigInfoAppBase,
                                              ConfigInfoAppCommon)

from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.main.archive import ArchiveRepository, LocalFileSystem
from dataclasses import dataclass
from dataclasses import dataclass
from wwpdb.apps.deposit.main.schemas import (ExperimentEMSubtypes,
                                             ExperimentTypes)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("onedep_test.log")
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
logger.removeHandler(logging.StreamHandler(sys.stdout))
logger.removeHandler(logging.StreamHandler(sys.stderr))

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


@dataclass
class RemoteArchive:
    host: str
    user: str
    root_path: str


@dataclass
class EntryStatus:
    status: str = "pending"
    arch_depid: str = None
    arch_entry_id: str = None
    exp_type: str = None
    test_depid: str = None
    message: str = "Starting tests..."

    def __repr__(self):
        return f"EntryStatus(status={self.status}, arch_depid={self.arch_depid}, arch_entry_id={self.arch_entry_id}, exp_type={self.exp_type}, test_depid={self.test_depid}, message={self.message})"


class StatusManager:
    """
    Manages the statuses for all entries in the depid_list.
    """
    def __init__(self, depid_list):
        self.statuses = {depid: EntryStatus() for depid in depid_list}

    def update_status(self, depid, **kwargs):
        """
        Update the status of a specific entry.
        
        Args:
            depid (str): The ID of the entry to update.
            kwargs: Fields to update in the EntryStatus object.
        """
        if depid not in self.statuses:
            raise KeyError(f"DepID {depid} not found in statuses.")

        for key, value in kwargs.items():
            if hasattr(self.statuses[depid], key):
                setattr(self.statuses[depid], key, value)
            else:
                raise AttributeError(f"Invalid field '{key}' for EntryStatus.")

    def get_status(self, depid):
        """
        Retrieve the status of a specific entry.
        
        Args:
            depid (str): The ID of the entry to retrieve.
        
        Returns:
            EntryStatus: The status object for the given depid.
        """
        if depid not in self.statuses:
            raise KeyError(f"DepID {depid} not found in statuses.")
        return self.statuses[depid]

    def __iter__(self):
        """
        Iterate over all EntryStatus objects.
        
        Yields:
            EntryStatus: Each status object in the manager.
        """
        return iter(self.statuses.values())


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

input_files = [
    [FileType.MMCIF_COORD, "model", "pdbx"],
    [FileType.CRYSTAL_MTZ, "structure-factors", "mtz"],
    # [FileType.CRYSTAL_STRUC_FACTORS, "structure-factors", "pdbx"],
]

BASE_FILES_DIR = "base"
UPLOAD_FILES_DIR = "upload"

api = DepositApi(api_key=create_token(ORCID, expiration_days=1/24), hostname="https://localhost:12000/deposition", ssl_verify=False)

def copy_base_files(entry_id: str, destination: str):
    if not entry_id.startswith("D_"):
        entry_id = entry_id_to_dep_id(entry_id)

    if entry_id is None:
        return

    def rsync_copy(source_host, source_path, destination_path):
        rsync_command = f"rsync -arvz {source_host}:{source_path}/ {destination_path}"
        subprocess.run(rsync_command, shell=True)

    source_host = f"{prod_host['user']}@{prod_host['host']}"
    source_path = os.path.join(prod_host["onedep_root"], "data", "production", "deposit", entry_id)

    rsync_copy(source_host, source_path, destination)


class RemoteFetcher:
    def __init__(self, remote_archive: RemoteArchive, cache_location, cache_size=10):
        self.remote_archive = remote_archive
        self.cache_location = cache_location
        self.cache_size = cache_size

    def fetch(self, dep_id, destination, repository="deposit"):
        dep_location = os.path.join(self.cache_location, dep_id)

        if self._is_cached(dep_id):
            logger.info("Entry %s found in cache. Copying to %s", dep_id, destination)
            self._copy_from_cache(dep_id, destination)
            return

        self._evict_oldest_entry()

        os.makedirs(dep_location, exist_ok=True)

        logger.info("Entry %s not found in cache. Downloading from remote host %s", dep_id, self.remote_archive.host)
        self._fetch_from_remote(dep_id, repository, dep_location)
        self._copy_from_cache(dep_id, destination)

    def _is_cached(self, dep_id):
        deposition_folder = os.path.join(self.cache_location, dep_id)
        return os.path.exists(deposition_folder)

    def _copy_from_cache(self, dep_id, destination):
        if not self._is_cached(dep_id):
            logger.warning("Unable to copy entry from cache (not found)")
            return

        try:
            shutil.copytree(os.path.join(self.cache_location, dep_id), destination, dirs_exist_ok=True)
        except Exception as e:
            logger.error("Error copying files from cache: %s", e)

    def _remove_from_cache(self, dep_id):
        if not self._is_cached(dep_id):
            logger.warning("Unable to remove entry from cache (not found)")
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
        remote_path = os.path.join(self.remote_archive.root_path, "data", "production", repository, dep_id)
        rsync_command = ["rsync", "-arvzL", f"{self.remote_archive.user}@{self.remote_archive.host}:{remote_path}/", local_path]
        logger.debug("Running command %s", ' '.join(rsync_command))
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


def create_deposition(etype: ExperimentTypes, email: str, subtype: ExperimentEMSubtypes = None, coordinates: bool = True, related_emdb: str = None, no_map: bool = False):
    """NOTE: if this code ever moves to a separate package, the deposition
    will have to be created using a proper HTTP request.
    """
    users = [ORCID]
    country = Country.UK
    password = "123456"

    if etype == ExperimentTypes.EM:
        deposition = api.create_em_deposition(email, users, country, subtype, coordinates, related_emdb, password)
    elif etype == ExperimentTypes.XRAY:
        deposition = api.create_xray_deposition(email, users, country, password)
    elif etype == ExperimentTypes.FIBER:
        deposition = api.create_fiber_deposition(email, users, country, password)
    elif etype == ExperimentTypes.NEUTRON:
        deposition = api.create_neutron_deposition(email, users, country, password)
    elif etype == ExperimentTypes.EC:
        deposition = api.create_ec_deposition(email, users, country, coordinates, password, related_emdb, no_map)
    elif etype == ExperimentTypes.NMR:
        # deposition = api.create_nmr_deposition(email, users, country, coordinates, password, related_bmrb)
        pass
    elif etype == ExperimentTypes.SSNMR:
        # deposition = api.create_ssnmr_deposition(email, users, country, coordinates, password, related_bmrb)
        pass
    else:
        raise ValueError(f"Unknown experiment type: {etype}")

    return deposition


def upload_files(dep_id: str, base_dep_id: str, base_files_location: str):
    lfs = LocalFileSystem()

    for f in input_files:
        wdo = lfs.locate(dep_id=base_dep_id, repository=ArchiveRepository.TEMPDEP, content=f[1], format=f[2], version="latest", milestone="upload")
        basename = os.path.basename(wdo.getFilePathReference())
        test_file = os.path.join(base_files_location, basename)
        filetype = f[0]

        try:
            file = api.upload_file(dep_id, test_file, filetype, overwrite=False)
        except:
            print("[!] failed to upload %s" % test_file)


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


def create_and_process(fetcher, dep_id):
    if dep_id is None:
        dep = create_deposition(etype=ExperimentTypes(experiment_type), email="wbueno@ebi.ac.uk")
        dep_id = dep.dep_id
    print("[+] dep_id", dep_id)

    tmp_dir = tempfile.mkdtemp()
    base_dir = os.path.join(tmp_dir, BASE_FILES_DIR)

    try:
        os.makedirs(base_dir, exist_ok=True)

        if not reupload:
            dep_cache.copy_files(dep_id, base_dir)
            upload_files(dep_id=dep_id, base_dep_id=dep_id, base_files_location=base_dir)
        else:
            dep_cache.remove(dep_id)
            dep_cache.copy_files(dep_id, base_dir, repository="archive")
            reupload_files(dep_id=dep_id, base_dep_id=dep_id, base_files_location=base_dir)

        copy_elements = {"copy_contact": False, "copy_authors": False, "copy_citation": False, "copy_grant": False, "copy_em_exp_data": False}
        response = api.process(dep_id, voxel=None, copy_from_id=None, **copy_elements)
        print("[+] processing", response)

        status = ""
        while status not in ("finished", "exception", "failed"):
            time.sleep(5)
            response = api.get_status(dep_id)
            status = response.status

        compare_files(dep_id=dep_id, base_dep_id=dep_id, base_files_location=base_dir)

    finally:
        if reupload:
            dep_cache.remove(dep_id)

        print("[+] tmp_dir", tmp_dir)
        if not keep_temp:
            shutil.rmtree(tmp_dir)


def get_entry_info(dep_id):
    connection = MySQLdb.connect(**prod_db)

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

            time.sleep(random.randint(1, 3))

            exp_method = result[0][3]
            if 'xray' in exp_method.lower() or 'x-ray' in exp_method.lower():
                exp_method = "xray"
            elif 'em' in exp_method.lower():
                exp_method = "em"
            elif 'nmr' in exp_method.lower():
                exp_method = "nmr"
            else:
                exp_method = exp_method.lower()

            return {"entry_id": eid, "exp_method": exp_method}
    finally:
        connection.close()


def generate_table(status_manager):
    """Generate status table for display with spinners"""
    table = Table(show_header=False, box=None, padding=(0, 1, 0, 0))
    spinner = Spinner("dots", text="", style="blue")

    for entry_status in sorted(status_manager, key=lambda x: x.arch_depid or ""):
        dep_id = entry_status.arch_depid or "(unknown)"
        info = f"({entry_status.arch_entry_id or '?'}) ({entry_status.exp_type or '?'})"
        message = entry_status.message

        if entry_status.status in ("working", "pending"):
            table.add_row(spinner, f"{dep_id} [bright_cyan]{info}[/bright_cyan] {message}")
        elif entry_status.status == "finished":
            table.add_row("[green]✓[/green]", f"{dep_id} [bright_cyan]{info}[/bright_cyan] {message}")
        elif entry_status.status == "failed":
            table.add_row("[red]✗[/red]", f"{dep_id} {info} {message}")

    return table


@click.command()
@click.argument('depid_list', nargs=-1)
@click.option('--reupload', is_flag=True, help='Test reupload after submission')
@click.option('--keep-temp', is_flag=True, help='Keep temporary files')
@click.option('--cache-location', default=os.path.join(config.get("SITE_ARCHIVE_STORAGE_PATH"), "tempdep"), help='Cache location')
def main(depid_list, reupload, keep_temp, cache_location):
    if "pro" in getSiteId().lower():
        print("[!] this script should not be run in production. Exiting.")
        return

    # (status, exp type, message)
    remote = RemoteArchive(host="local.rcsb.rutgers.edu", user="onedep", root_path="/wwpdb/onedep")
    fetcher = RemoteFetcher(remote, cache_location=cache_location)
    status_manager = StatusManager(depid_list)

    with Live(generate_table(status_manager), refresh_per_second=15) as live:
        live.update(generate_table(status_manager))

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(get_entry_info, dep_id): dep_id for dep_id in depid_list}

            for future in concurrent.futures.as_completed(futures):
                dep_id = futures[future]

                # Update the status to "working" for the current dep_id
                status_manager.update_status(dep_id, status="working", message="Getting entry info")
                live.update(generate_table(status_manager))

                try:
                    info = future.result()
                    # Update the status to "finished" with additional info
                    status_manager.update_status(
                        dep_id,
                        status="finished",
                        arch_entry_id=info["entry_id"],
                        exp_type=info["exp_method"],
                        message="Info read from db"
                    )
                    # Process the entry
                    # create_and_process(fetcher, dep_id)
                except Exception as e:
                    # Update the status to "failed" in case of an exception
                    status_manager.update_status(dep_id, status="failed", message=f"Error getting entry info ({e})")
                    continue
                finally:
                    # Update the live table with the latest statuses
                    live.update(generate_table(status_manager))


if __name__ == '__main__':
    main()

# D_8233000125 D_8233000126 D_8233000127 D_8233000128 D_8233000129 D_8233000130 D_8233000131 D_8233000132 D_8233000133 D_8233000134 D_8233000135 D_8233000136 D_8233000137 D_8233000138