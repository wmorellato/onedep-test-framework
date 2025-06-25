import os
import re
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import pickle
import concurrent.futures
import requests
import logging
import threading
import time
import shutil
from typing import List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import MySQLdb

from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from odtf.common import get_file_logger
from odtf.models import EntryStatus, FileTypeMapping, TestEntry, TaskType, Task, UploadTask, CompareFilesTask, TaskStatus
from odtf.wwpdb_uri import WwPDBResourceURI, FilesystemBackend
from odtf.archive import RemoteFetcher, LocalArchive
from odtf.compare import  comparer_factory
from odtf.config import Config
from odtf.report import TestReportGenerator, TestReportIntegration

from onedep_deposition.deposit_api import DepositApi
from onedep_deposition.enum import Country, FileType
from onedep_deposition.models import (DepositError, DepositStatus, EMSubType,
                                      ExperimentType)

from wwpdb.io.locator.PathInfo import PathInfo
from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.main.archive import ArchiveRepository
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import (ConfigInfoAppBase)

from django.conf import settings
from django.utils.module_loading import import_string
from django.utils.encoding import force_bytes

file_logger = get_file_logger(__name__)

# globals
pi = PathInfo()
ci = ConfigInfo()
configApp = ConfigInfoAppBase()
filesystem = FilesystemBackend(pi, content_type_config=Config.CONTENT_TYPE_DICT, format_extension_d=Config.FORMAT_DICT)

for l in ["wwpdb.apps.deposit.main.archive", "onedep_deposition.rest_adapter", "urllib3", "requests", "wwpdb.io.locator.PathInfo", "odtf.report"]:
    logger = logging.getLogger(l)
    logger.setLevel(logging.CRITICAL)
    logger.propagate = True


ORCID = "0000-0002-5109-8728"
api = None


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
    def __init__(self, test_set: List[TestEntry], callback, report_integration=None):
        self.statuses = {te.dep_id: EntryStatus(arch_dep_id=te.dep_id) for te in test_set}
        self.callback = callback
        self._lock = threading.RLock()
        self.report_integration = report_integration

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

    def track_task_result(self, test_entry: TestEntry, task_type: TaskType, success: bool, error_message: str = None):
        """Track individual task results for report generation"""
        if not self.report_integration:
            return
            
        # Find the task in the test entry and update its status
        for task in test_entry.tasks:
            if task.type == task_type:
                task.status = TaskStatus.SUCCESS if success else TaskStatus.FAILED
                task.error_message = error_message
                task.execution_time = datetime.now()
                break
    
    def track_comparison_result(self, test_entry: TestEntry, rule_name: str, success: bool, error_message: str = None):
        """Track comparison rule results"""
        if self.report_integration:
            self.report_integration.track_comparison_result(
                test_entry.dep_id, rule_name, success, error_message
            )


def create_deposition(orcid: str, country: Country, etype: ExperimentType, email: str, subtype: EMSubType = None, coordinates: bool = True, related_emdb: str = None, no_map: bool = False):
    """NOTE: if this code ever moves to a separate package, the deposition
    will have to be created using a proper HTTP request.
    """
    users = [orcid]
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


def monitor_processing(test_entry: TestEntry, status_manager: StatusManager):
    status = "started"

    while status in ("running", "started", "submit"):
        response = api.get_status(test_entry.copy_dep_id)

        if isinstance(response, DepositStatus):
            if response.details == status_manager.get_status(test_entry).message:
                time.sleep(5)
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


def submit_deposition(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    def get_cookie_signer(salt="django.core.signing.get_cookie_signer"):
        Signer = import_string(settings.SIGNING_BACKEND)
        key = force_bytes(settings.SECRET_KEY)  # SECRET_KEY may be str or bytes.
        return Signer(b"django.http.cookies" + key, salt=salt)

    test_pickles_location = pi.getDirPath(dataSetId=test_entry.dep_id, fileSource="pickles")
    copy_pickles_location = pi.getDirPath(dataSetId=test_entry.copy_dep_id, fileSource="pickles")
    status_manager.update_status(test_entry, status="working", message="Copying pickles from test deposition to copy deposition")

    for file_name in os.listdir(test_pickles_location):
        if file_name.endswith(".pkl"):
            source_path = os.path.join(test_pickles_location, file_name)
            destination_path = os.path.join(copy_pickles_location, file_name)
            shutil.copy(source_path, destination_path)

    # writing the submitOK.pkl file
    for ppath in [copy_pickles_location, test_pickles_location]:
        with open(os.path.join(ppath, "submitOK.pkl"), "wb") as f:
            pickle.dump({
                'annotator_initials': 'TST',
                'date': '2025-06-24 10:53:30',
                'reason': 'Submission test'
            }, f)

    orcid_cookie = get_cookie_signer(salt=settings.AUTH_COOKIE_KEY).sign(config.api.get("orcid"))

    # get the csrftoken from an arbitrary request
    session = requests.Session()
    session.verify = False

    response = session.get(
        url=os.path.join(config.api.get("base_url"), "api", "v1", "depositions", test_entry.copy_dep_id, "view"),
        cookies={"depositor-orcid": orcid_cookie},
    )
    csrftoken = response.cookies.get("csrftoken")

    status_manager.update_status(test_entry, message="Submitting deposition")
    response = session.post(
        url=os.path.join(config.api.get("base_url"), "submitRequest"),
        json={},
        cookies={"depositor-orcid": orcid_cookie},
        headers={"x-csrftoken": csrftoken, "content-type": "application/x-www-form-urlencoded; charset=UTF-8", "referer": config.api.get("base_url")}
    )

    file_logger.info("Submit response: %s", response.text)


def upload_files(test_entry: TestEntry, task: UploadTask, status_manager: StatusManager):
    # getting all files with the 'upload' milestone doesn't work as I've seen
    # some depositions with multiple versions of -upload type
    arch_pickles = pi.getDirPath(dataSetId=test_entry.dep_id, fileSource="pickles")
    uploaded_files = []
    contour_level, pixel_spacing = parse_voxel_values(os.path.join(arch_pickles, "em_map_upload.pkl"))

    for f in task.files:
        content_type, file_format = f.split(".")
        content_type = f"{content_type}-upload"
        file_uri = WwPDBResourceURI.for_file(repository="tempdep", dep_id=test_entry.dep_id, content_type=content_type, format=file_format, version="latest")

        status_manager.update_status(test_entry, message=f"Uploading `{content_type}.{file_format}`")

        try:
            filetype = FileTypeMapping.get_file_type(content_type.replace("-upload", ""), file_format)
            filepath = str(filesystem.locate(file_uri))
            file = api.upload_file(test_entry.copy_dep_id, filepath, filetype, overwrite=False)
            uploaded_files.append(file)

            if filetype in (FileType.EM_MAP, FileType.EM_ADDITIONAL_MAP, FileType.EM_MASK, FileType.EM_HALF_MAP):
                if contour_level:
                    status_manager.update_status(test_entry, message=f"Updating metadata for {f} with contour level {contour_level} and pixel spacing {pixel_spacing}")
                    api.update_metadata(test_entry.copy_dep_id, file.file_id, contour=contour_level, spacing_x=pixel_spacing, spacing_y=pixel_spacing, spacing_z=pixel_spacing, description="Uploaded from test script")
                else:
                    raise Exception("Contour level or pixel spacing not found in pickle file. Can't continue automatically.")
        except ValueError as e:
            status_manager.update_status(test_entry, status="failed", message=f"Error getting file type for {content_type}.{file_format}: {e}")
            raise
        except:
            status_manager.update_status(test_entry, status="failed", message=f"Error uploading {content_type} {file_format}")
            raise

    return uploaded_files


def compare_files(test_entry: TestEntry, task: Task, config: Config, status_manager: StatusManager):
    overall_success = True

    for cr in task.rules:
        if ":copy:" in task.source:
            if not test_entry.copy_dep_id:
                raise ValueError("Deposition ID source must be provided for comparison either in config.yaml or as a parameter.")
            task.source = task.source.replace(":copy:", test_entry.copy_dep_id)

        rule = config.get_compare_rule(cr.name)
        content_type, format = rule.name.split(".")

        copy_uri = WwPDBResourceURI(task.source).join_file(content_type=content_type, format=format, version=rule.version)
        copy_file_path = str(filesystem.locate(copy_uri))

        test_entry_uri = WwPDBResourceURI.for_file(repository="tempdep", dep_id=test_entry.dep_id, content_type=content_type, format=format, version=rule.version)
        if not filesystem.exists(test_entry_uri):
            error_msg = f"Test entry file {test_entry_uri} not found in local archive."
            status_manager.track_comparison_result(test_entry, rule.name, False, error_msg)
            raise FileNotFoundError(error_msg)

        test_entry_file_path = str(filesystem.locate(test_entry_uri))

        status_manager.update_status(test_entry, message=f"Comparing {copy_uri} with {test_entry_uri} using {rule.method}")

        try:
            comparer = comparer_factory(rule.method, test_entry_file_path, copy_file_path)
            success = comparer.compare()
            
            # Track the individual rule result
            error_msg = None if success else f"Comparison failed for {content_type}.{format} using {rule.method}"
            status_manager.track_comparison_result(test_entry, rule.name, success, error_msg)
            
            if not success:
                overall_success = False
                status_manager.update_status(test_entry, status="warning", message=f"Comparison failed for {content_type}.{format} using {rule.method}")
            else:
                status_manager.update_status(test_entry, message=f"Files are the same for {content_type}.{format} using {rule.method}")

        except Exception as e:
            overall_success = False
            error_msg = f"Error during comparison: {str(e)}"
            status_manager.track_comparison_result(test_entry, rule.name, False, error_msg)

    status_manager.track_task_result(test_entry, TaskType.COMPARE_FILES, overall_success)


def fetch_files(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    remote = config.get_remote_archive()
    local = LocalArchive(site_id=getSiteId())
    fetcher = RemoteFetcher(remote, local, cache_size=10, force=False)

    status_manager.update_status(test_entry, copy_dep_id=test_entry.copy_dep_id, message="Fetching files from archive")
    try:
        fetcher.fetch_repository(test_entry.dep_id, repository=ArchiveRepository.DEPOSIT.value)
    except Exception as e:
        raise Exception("Error fetching files from archive") from e


def create_and_process(test_entry: TestEntry, task: Task, config: Config, status_manager: StatusManager):
    try:
        exp_type = status_manager.get_status(test_entry).exp_type # feels hackish
        exp_subtype = status_manager.get_status(test_entry).exp_subtype # feels hackish

        status_manager.update_status(test_entry, status="working", message="Creating test deposition")
        try:
            copy_dep = create_deposition(orcid=config.api.get("orcid"), country=Country(config.api.get("country")), etype=exp_type, subtype=exp_subtype, email="wbueno@ebi.ac.uk")
            test_entry.copy_dep_id = copy_dep.dep_id
        except Exception as e:
            raise Exception(f"Error creating test deposition: {str(e)}") from e

        status_manager.update_status(test_entry, copy_dep_id=copy_dep.dep_id)
        upload_files(test_entry=test_entry, task=task, status_manager=status_manager)

        copy_elements = {"copy_contact": False, "copy_authors": False, "copy_citation": False, "copy_grant": False, "copy_em_exp_data": False}
        api.process(test_entry.copy_dep_id, **copy_elements)
        monitor_processing(test_entry, status_manager)

        status_manager.track_task_result(test_entry, TaskType.UPLOAD, True)
    except Exception as e:
        status_manager.update_status(test_entry, status="failed", message=str(e))
        status_manager.track_task_result(test_entry, TaskType.UPLOAD, False, str(e))
        raise


def get_entry_info(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    """Part of the main testing pipeline, hence the status_manager here. Only Jesus can judge me
    This should be using the API.
    """
    prod_ci = ConfigInfo(siteId=config.remote_archive.site_id)
    prod_db = {
        "host": prod_ci.get("SITE_DB_HOST_NAME"),
        "user": prod_ci.get("SITE_DB_USER_NAME"),
        "password": prod_ci.get("SITE_DB_PASSWORD"),
        "port": int(prod_ci.get("SITE_DB_PORT_NUMBER")),
        "database": "status",
    }

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
    get_entry_info(entry, config, status_manager)
    fetch_files(entry, config, status_manager)

    for task in entry.tasks:
        try:
            if task.type == TaskType.UPLOAD:
                create_and_process(entry, task, config, status_manager)
            elif task.type == TaskType.COMPARE_FILES:
                compare_files(entry, task, config, status_manager)
            elif task.type == TaskType.SUBMIT:
                submit_deposition(entry, config, status_manager)
            else:
                file_logger.warning("Unknown task type %s for entry %s", task.type, entry.dep_id)
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error_message = str(e)
            task.execution_time = datetime.now()
            if task.stop_on_failure:
                status_manager.update_status(entry, status="failed", message=f"Task {task.type} failed: {e}")
                return

    status_manager.update_status(entry, status="finished", message=f"Completed all tasks for entry {entry.dep_id}")


def setup_report_generation(output_dir: str = "reports"):
    """Setup report generation components"""
    Path(output_dir).mkdir(exist_ok=True)
    template_dir = Path(__file__).parent / "templates"
    template_path = template_dir / "test_report.html"
    if not template_path.exists():
        # You would copy the HTML template content here
        # For now, we'll assume it exists
        file_logger.warning(f"Template file not found: {template_path}")
    
    # Initialize report generator
    generator = TestReportGenerator(template_dir=template_dir, output_dir=output_dir)
    integration = TestReportIntegration(generator)
    
    return generator, integration


@click.command()
@click.argument('test_config', type=click.Path(exists=True, dir_okay=False, readable=True), required=True)
@click.option('--generate-report', is_flag=True, help='Generate HTML test report', default=True)
@click.option('--report-dir', default="reports", help='Directory for generated reports')
def main(test_config, generate_report, report_dir):
    """TEST_CONFIG is the path to the test configuration file.
    """
    global api

    if "pro" in getSiteId().lower(): # get the production ids from config
        click.echo("This command is not allowed on production sites. Exiting.", err=True)
        return

    config = Config(test_config)
    test_set = config.get_test_set()
    api = DepositApi(api_key=create_token(config.api.get("orcid"), expiration_days=1/24), hostname=config.api.get("base_url"), ssl_verify=False)

    report_integration = None
    if generate_report:
        try:
            _, report_integration = setup_report_generation(
                output_dir="/wwpdb/onedep/deployments/dev/source/onedep-webfe/webapps/htdocs/"
            )
            click.echo(f"Report generation enabled. Reports will be saved to: {report_dir}")
        except Exception as e:
            click.echo(f"Warning: Could not setup report generation: {e}", err=True)
            generate_report = False

    with Live(refresh_per_second=15) as live:
        def update_callback():
            """Callback to update the live display"""
            live.update(generate_table(status_manager))

        status_manager = StatusManager(test_set, callback=update_callback, report_integration=report_integration)
        live.update(generate_table(status_manager))

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(run_entry_tasks, te, config, status_manager): te.dep_id for te in test_set}

            for future in concurrent.futures.as_completed(futures):
                dep_id = futures[future]
                try:
                    future.result()
                except Exception as e:
                    file_logger.error("Error processing entry %s: %s", dep_id, e, exc_info=True)
                    status_manager.update_status(config.get_test_entry(dep_id=dep_id), status="failed", message=f"Error processing entry ({e})")

    if generate_report and report_integration:
        try:
            click.echo("Generating test report...")
            report_path = report_integration.generate_final_report(
                test_set, 
                status_manager,
                output_filename=f"report.html"
            )
            click.echo(f"Test report generated: {report_path}")
        except Exception as e:
            click.echo(f"Failed to generate test report: {e}", err=True)
            file_logger.error(f"Report generation failed: {e}")


if __name__ == '__main__':
    main()

# D_8233000125 D_8233000126 D_8233000127 D_8233000128 D_8233000129 D_8233000130 D_8233000131 D_8233000132 D_8233000133 D_8233000134 D_8233000135 D_8233000136 D_8233000137 D_8233000138
