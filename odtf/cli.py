import os
import re
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import pickle
import concurrent.futures
import logging
import threading
import time
from typing import List
from concurrent.futures import ThreadPoolExecutor

import click
import MySQLdb

from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from odtf.common import get_file_logger
from odtf.models import EntryStatus, FileTypeMapping, TestEntry, TaskType, Task
from odtf.archive import RemoteFetcher, LocalArchive
from odtf.compare import FileComparer, comparer_factory
from odtf.config import Config

from onedep_deposition.deposit_api import DepositApi
from onedep_deposition.enum import Country, FileType
from onedep_deposition.models import (DepositError, DepositStatus, EMSubType,
                                      ExperimentType)

from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.main.archive import ArchiveRepository, LocalFileSystem
from wwpdb.io.locator.PathInfo import PathInfo
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import (ConfigInfoAppBase)

file_logger = get_file_logger(__name__)


ci = ConfigInfo()
pi = PathInfo()
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

prod_db = {
    "host": PROD_CONFIG.get("SITE_DB_HOST_NAME"),
    "user": PROD_CONFIG.get("SITE_DB_USER_NAME"),
    "password": PROD_CONFIG.get("SITE_DB_PASSWORD"),
    "port": int(PROD_CONFIG.get("SITE_DB_PORT_NUMBER")),
    "database": "status",
}

api = DepositApi(api_key=create_token(ORCID, expiration_days=1/24), hostname="https://localhost:12000/deposition", ssl_verify=False)


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


class StatusManager:
    """
    Minimal thread-safety with your exact logic preserved.
    """
    def __init__(self, test_set: List[TestEntry], callback):
        self.statuses = {te.dep_id: EntryStatus(arch_dep_id=te.dep_id) for te in test_set}
        self.callback = callback
        self._lock = threading.RLock()

    def update_status(self, test_entry: TestEntry, **kwargs):
        with self._lock:
            if test_entry.dep_id not in self.statuses:
                raise KeyError(f"DepID {test_entry.dep_id} not found in statuses.")

            file_logger.info(str(self.statuses[test_entry.dep_id]))

            for key, value in kwargs.items():
                if hasattr(self.statuses[test_entry.dep_id], key):
                    setattr(self.statuses[test_entry.dep_id], key, value)
                else:
                    raise AttributeError(f"Invalid field '{key}' for EntryStatus.")

            if self.callback:
                self.callback()

    def get_status(self, test_entry: TestEntry):
        with self._lock:
            if test_entry.dep_id not in self.statuses:
                raise KeyError(f"DepID {test_entry.dep_id} not found in statuses.")
            return self.statuses[test_entry.dep_id]

    def __iter__(self):
        with self._lock:
            return iter(list(self.statuses.values()))


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


def upload_files(copy_dep_id: str, test_entry: TestEntry, status_manager: StatusManager):
    # getting all files with the 'upload' milestone from the archive dir
    arch_data = pi.getTempDepPath(dataSetId=test_entry.dep_id)
    arch_pickles = pi.getDirPath(dataSetId=test_entry.dep_id, fileSource="pickles")
    previous_files = [f for f in os.listdir(arch_data) if "upload_P" in f]
    uploaded_files = []
    contour_level, pixel_spacing = parse_voxel_values(os.path.join(arch_pickles, "em_map_upload.pkl"))

    for f in previous_files:
        fobj = parse_filename(repository=ArchiveRepository.ARCHIVE.value, filename=f)
        content_type = fobj.getContentType()
        file_format = fobj.getFileFormat()

        status_manager.update_status(test_entry, message=f"Uploading `{content_type}.{file_format}`")

        try:
            filetype = FileTypeMapping.get_file_type(content_type.replace("-upload", ""), file_format)
            file_path = os.path.join(arch_data, f)
            file = api.upload_file(copy_dep_id, file_path, filetype, overwrite=False)
            uploaded_files.append(file)

            if filetype in (FileType.EM_MAP, FileType.EM_ADDITIONAL_MAP, FileType.EM_MASK, FileType.EM_HALF_MAP):
                if contour_level:
                    status_manager.update_status(test_entry, message=f"Updating metadata for {f} with contour level {contour_level} and pixel spacing {pixel_spacing}")
                    api.update_metadata(copy_dep_id, file.file_id, contour=contour_level, spacing_x=pixel_spacing, spacing_y=pixel_spacing, spacing_z=pixel_spacing, description="Uploaded from test script")
                else:
                    raise Exception("Contour level or pixel spacing not found in pickle file. Can't continue automatically.")
        except ValueError as e:
            status_manager.update_status(test_entry, status="failed", message=f"Error getting file type for {content_type}.{file_format}: {e}")
            raise
        except:
            status_manager.update_status(test_entry, status="failed", message=f"Error uploading {content_type} {file_format}")
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


def compare_files(test_entry: TestEntry, task: Task, config: Config, status_manager: StatusManager, source=None):
    lfs = LocalFileSystem()

    for rname in task.rules:
        if task.source == "copy":
            if not source:
                raise ValueError("Deposition ID source must be provided for comparison either in config.yaml or as a parameter.")
            task.source = source

        rule = config.get_compare_rule(rname)
        content_type, format = rule.name.split(".")

        copy_wdo = lfs.locate(dep_id=test_entry.dep_id, repository=ArchiveRepository.DEPOSIT_UI, content=content_type, format=format, version=rule.version)
        copy_file_path = copy_wdo.getFilePathReference()

        test_entry_wdo = lfs.locate(dep_id=task.source, repository=ArchiveRepository.TEMPDEP, content=content_type, format=format, version=rule.version)
        test_entry_file_path = test_entry_wdo.getFilePathReference()
        if not lfs.exists(test_entry_wdo):
            raise FileNotFoundError(f"Test entry file {test_entry_file_path} not found in local archive.")

        comparer = comparer_factory(rule.method, test_entry_file_path, copy_file_path)
        if not comparer.compare():
            status_manager.update_status(test_entry, status="warning", message=f"Comparison failed for {content_type}.{format} using {rule.method}")
        else:
            status_manager.update_status(test_entry, message=f"Files are the same for {content_type}.{format} using {rule.method}")


def create_and_process(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    remote = config.get_remote_archive()
    local = LocalArchive(site_id=getSiteId())
    fetcher = RemoteFetcher(remote, local, cache_size=10, force=False)

    exp_type = status_manager.get_status(test_entry).exp_type # feels hackish
    exp_subtype = status_manager.get_status(test_entry).exp_subtype # feels hackish

    status_manager.update_status(test_entry, status="working", message="Creating test deposition")    
    try:
        copy_dep = create_deposition(etype=exp_type, subtype=exp_subtype, email="wbueno@ebi.ac.uk")
    except Exception as e:
        raise Exception("Error creating test deposition") from e

    status_manager.update_status(test_entry, copy_dep_id=copy_dep.dep_id, message="Fetching files from archive")
    try:
        fetcher.fetch(test_entry.dep_id)
    except Exception as e:
        raise Exception("Error fetching files from archive") from e

    upload_files(copy_dep_id=copy_dep.dep_id, test_entry=test_entry, status_manager=status_manager)

    copy_elements = {"copy_contact": False, "copy_authors": False, "copy_citation": False, "copy_grant": False, "copy_em_exp_data": False}
    response = api.process(copy_dep.dep_id, **copy_elements)
    status = "started"

    while status in ("running", "started", "submit"):
        response = api.get_status(copy_dep.dep_id)

        if isinstance(response, DepositStatus):
            if response.details == status_manager.get_status(test_entry).message:
                continue

            status_manager.update_status(test_entry, status="working", message=f"{response.details}")
            status = response.status

            if status == "error":
                raise Exception(response.details)
        elif isinstance(response, DepositError):
            raise Exception(response.message)
        else:
            raise Exception(f"Unknown response type {type(response)}")

        time.sleep(5)


def get_entry_info(test_entry: TestEntry, status_manager: StatusManager):
    """Part of the main testing pipeline, hence the status_manager here. Only Jesus can judge me
    This should be using the API.
    """
    connection = MySQLdb.connect(**prod_db)
    status_manager.update_status(test_entry, status="working", message="Getting entry info")

    try:
        with connection.cursor() as cursor:
            sql = "SELECT pdb_id, emdb_id, bmrb_id, exp_method FROM deposition WHERE dep_set_id = '%s'" % test_entry.dep_id.upper()
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
                test_entry,
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
        copy_dep_id = entry_status.copy_dep_id
        message = entry_status.message

        if entry_status.status in ("working", "pending"):
            table.add_row(spinner, f"{arch_dep_id} [bright_cyan]({arch_entry_id})[/bright_cyan] → {copy_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")
        elif entry_status.status == "finished":
            table.add_row("[green]✓[/green]", f"{arch_dep_id} [bright_cyan]({arch_entry_id})[/bright_cyan] → {copy_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")
        elif entry_status.status == "warning":
            table.add_row("[yellow]⊙[/yellow]", f"[red]{arch_dep_id}[/red] [bright_cyan]({arch_entry_id})[/bright_cyan] → {copy_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")
        elif entry_status.status == "failed":
            table.add_row("[red]✗[/red]", f"[red]{arch_dep_id}[/red] [bright_cyan]({arch_entry_id})[/bright_cyan] → {copy_dep_id} [bright_cyan]{exp_type:<5}[/bright_cyan] {message}")

    return table


def run_entry_tasks(entry, config, status_manager):
    """Run all tasks for a single entry sequentially."""
    get_entry_info(entry, status_manager)

    for task in entry.tasks:
        if task.type == TaskType.UPLOAD:
            create_and_process(entry, config, status_manager)
        elif task.type == TaskType.COMPARE_FILES:
            compare_files(entry, task, config, status_manager)
        else:
            file_logger.warning("Unknown task type %s for entry %s", task.type, entry.dep_id)
    
    status_manager.update_status(entry, status="finished", message=f"Completed all tasks for entry {entry.dep_id}")


@click.command()
@click.argument('test_config', type=click.Path(exists=True, dir_okay=False, readable=True), required=True)
# @click.option('--config-file', default=None, help='Path to the configuration file. Defaults to ~/.odtf/config.yaml.')
@click.option('--force-fetch', is_flag=True, help="Force fetch from remote archive")
@click.option('--keep-temp', is_flag=True, help='Keep temporary files')
@click.option('--cache-location', default="/wwpdb/onedep/testcache", help='Cache location')
def main(test_config, force_fetch, keep_temp, cache_location):
    """TEST_CONFIG is the path to the test configuration file.
    """
    if "pro" in getSiteId().lower(): # get the production ids from config
        click.echo("This command is not allowed on production sites. Exiting.", err=True)
        return

    config = Config(test_config)
    test_set = config.get_test_set()

    with Live(refresh_per_second=15) as live:
        def update_callback():
            """Callback to update the live display"""
            live.update(generate_table(status_manager))

        status_manager = StatusManager(test_set, callback=update_callback)
        live.update(generate_table(status_manager))

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(run_entry_tasks, te, config, status_manager): te.dep_id for te in test_set}

            for future in concurrent.futures.as_completed(futures):
                dep_id = futures[future]
                try:
                    future.result()
                except Exception as e:
                    file_logger.error("Error processing entry %s: %s", dep_id, e)
                    status_manager.update_status(config.get_test_entry(dep_id=dep_id), status="failed", message=f"Error processing entry ({e})")


if __name__ == '__main__':
    main()

# D_8233000125 D_8233000126 D_8233000127 D_8233000128 D_8233000129 D_8233000130 D_8233000131 D_8233000132 D_8233000133 D_8233000134 D_8233000135 D_8233000136 D_8233000137 D_8233000138
