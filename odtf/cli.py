import os
import re
import django
import asyncio
import aiohttp

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import pickle
import logging
import threading
import time
import shutil
import pprint
from typing import List
from datetime import datetime
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
from odtf.compare import comparer_factory
from odtf.config import Config
from odtf.report import TestReportGenerator, TestReportIntegration
from odtf.aioapi import AsyncDepositApi as DepositApi

from onedep_deposition.enum import Country, FileType
from onedep_deposition.models import (DepositError, DepositStatus, EMSubType,
                                      ExperimentType)

from wwpdb.io.locator.PathInfo import PathInfo
from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.main.archive import ArchiveRepository
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppBase

from django.conf import settings
from django.utils.module_loading import import_string
from django.utils.encoding import force_bytes

file_logger = get_file_logger(__name__)

# globals
pi = PathInfo()
ci = ConfigInfo()
configApp = ConfigInfoAppBase()
filesystem = FilesystemBackend(pi, content_type_config=Config.CONTENT_TYPE_DICT, format_extension_d=Config.FORMAT_DICT)

for l in ["wwpdb.apps.deposit.main.archive", "urllib3", "requests", "wwpdb.io.locator.PathInfo", "odtf.report"]:
    logger = logging.getLogger(l)
    logger.setLevel(logging.CRITICAL)
    logger.propagate = True

for l in ["asyncio", "aiohttp", "odtf.aioapi"]:
    logger = logging.getLogger(l)
    logger.setLevel(logging.WARNING)
    logger.propagate = True


def parse_voxel_values(filepath):
    """
    Parse a pickle file and extract the first contour_level and pixel_spacing values.
    
    Args:
        filepath (str): Path to the pickle file
        
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
    except Exception as e:
        file_logger.error("Error reading voxel values from %s: %s", filepath, e)
        return None, None 
    
    return contour_level, pixel_spacing


def get_cookie_signer(salt="django.core.signing.get_cookie_signer"):
    Signer = import_string(settings.SIGNING_BACKEND)
    key = force_bytes(settings.SECRET_KEY)  # SECRET_KEY may be str or bytes.
    return Signer(b"django.http.cookies" + key, salt=salt)


class StatusManager:
    """
    Thread-safe status manager for tracking test entry status.
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


async def create_deposition(api: DepositApi, orcid: str, country: Country, etype: ExperimentType, 
                           email: str, subtype: EMSubType = None, coordinates: bool = True, 
                           related_emdb: str = None, no_map: bool = False):
    """Create a deposition using the async API"""
    users = [orcid]
    password = "123456"

    if etype == ExperimentType.EM:
        return await api.create_em_deposition(email, users, country, subtype, coordinates, related_emdb, password)
    elif etype == ExperimentType.XRAY:
        return await api.create_xray_deposition(email, users, country, password)
    elif etype == ExperimentType.FIBER:
        return await api.create_fiber_deposition(email, users, country, password)
    elif etype == ExperimentType.NEUTRON:
        return await api.create_neutron_deposition(email, users, country, password)
    elif etype == ExperimentType.EC:
        return await api.create_ec_deposition(email, users, country, coordinates, password, related_emdb, no_map)
    elif etype == ExperimentType.NMR:
        return await api.create_nmr_deposition(email, users, country, coordinates, password)
    elif etype == ExperimentType.SSNMR:
        return await api.create_ssnmr_deposition(email, users, country, coordinates, password)
    else:
        raise ValueError(f"Unknown experiment type: {etype}")


async def monitor_processing(test_entry: TestEntry, config: Config, status_manager: StatusManager, timeout_minutes=30):
    """Monitor processing using the async API"""
    start_time = time.time()
    status = "started"
    sleep_time = 5
    max_sleep = 30
    
    file_logger.info(f"Starting async monitor_processing for {test_entry.dep_id}")
    
    # Create API instance for monitoring
    api = DepositApi(
        api_key=create_token(config.api.get("orcid"), expiration_days=7),
        hostname=config.api.get("base_url"),
        ssl_verify=False
    )
    await api._ensure_adapter()
    
    try:
        while status in ("running", "started", "submit"):
            if time.time() - start_time > timeout_minutes * 60:
                raise Exception(f"Processing timeout after {timeout_minutes} minutes for {test_entry.dep_id}")
            
            file_logger.debug(f"Checking status for {test_entry.copy_dep_id}, current sleep_time: {sleep_time}")
            
            try:
                response = await api.get_status(test_entry.copy_dep_id)
                
                if hasattr(response, 'status'):
                    status = response.status
                    details = getattr(response, 'details', "")
                else:
                    status = "error"
                    details = getattr(response, 'message', str(response))
                    
            except asyncio.TimeoutError:
                file_logger.error(f"Timeout checking status for {test_entry.copy_dep_id}")
                await asyncio.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, max_sleep)
                continue
            except Exception as e:
                file_logger.error(f"Status check failed for {test_entry.copy_dep_id}: {e}")
                await asyncio.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, max_sleep)
                continue

            current_status_message = status_manager.get_status(test_entry).message
            if details == current_status_message:
                file_logger.debug(f"No status change for {test_entry.dep_id}, sleeping {sleep_time}s")
                await asyncio.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.2, max_sleep)
                continue

            sleep_time = 5
            status_manager.update_status(test_entry, status="working", message=details)
            file_logger.info(f"Status update for {test_entry.dep_id}: {status} - {details}")

            if status == "error":
                raise Exception(details)

            await asyncio.sleep(sleep_time)
                
    except Exception as e:
        raise Exception(f"Error monitoring processing for {test_entry.dep_id}: {str(e)}") from e


async def unlock_deposition(dep_id: str, config: Config):
    """Unlock a deposition by sending a POST request to the unlock endpoint."""
    orcid_cookie = get_cookie_signer(salt=settings.AUTH_COOKIE_KEY).sign(config.api.get("orcid"))

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=10,
        ttl_dns_cache=300,
        use_dns_cache=True,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        connector_owner=False
    ) as session:
        try:
            view_url = os.path.join(config.api.get("base_url"), "api", "v1", "depositions", dep_id, "view")
            async with session.get(
                url=view_url,
                cookies={"depositor-orcid": orcid_cookie},
                ssl=False
            ) as response:
                if response.status != 200:
                    raise Exception(f"Failed to get deposition view {dep_id}: {response.status} - {await response.text()}")

            unlock_url = os.path.join(config.api.get("base_url"), "stage", "unlock")
            async with session.post(
                url=unlock_url,
                cookies={"depositor-orcid": orcid_cookie},
                ssl=False
            ) as response:
                if response.status != 200:
                    raise Exception(f"Failed to unlock deposition {dep_id}: {response.status} - {await response.text()}")
                    
        except asyncio.TimeoutError:
            raise Exception(f"Timeout while unlocking deposition {dep_id}")
        except Exception as e:
            raise Exception(f"Error unlocking deposition {dep_id}: {str(e)}") from e
        finally:
            await connector.close()


async def _upload_all_files(api: DepositApi, test_entry: TestEntry, task: UploadTask, 
                           status_manager: StatusManager, source_repository: str = "tempdep"):
    """Upload all files for a test entry using async API"""
    arch_pickles = pi.getDirPath(dataSetId=test_entry.dep_id, fileSource="pickles")
    uploaded_files = []
    type_dict = {}
    contour_level, pixel_spacing = parse_voxel_values(os.path.join(arch_pickles, "em_map_upload.pkl"))

    for f in task.files:
        content_type, file_format = f.split(".")
        content_type = f"{content_type}-upload"
        if content_type not in type_dict:
            type_dict[content_type] = 1
        else:
            type_dict[content_type] += 1

        file_uri = WwPDBResourceURI.for_file(
            repository=source_repository, 
            dep_id=test_entry.dep_id, 
            content_type=content_type, 
            format=file_format, 
            part_number=type_dict[content_type], 
            version="latest"
        )

        try:
            filetype = FileTypeMapping.get_file_type(content_type.replace("-upload", ""), file_format)
            filepath = str(filesystem.locate(file_uri))
            status_manager.update_status(test_entry, message=f"Uploading `{content_type}.{file_format}` from {filepath}")
            
            file = await api.upload_file(test_entry.copy_dep_id, filepath, filetype, overwrite=False)
            uploaded_files.append(file)

            if filetype in (FileType.EM_MAP, FileType.EM_ADDITIONAL_MAP, FileType.EM_MASK, FileType.EM_HALF_MAP):
                if contour_level:
                    status_manager.update_status(
                        test_entry, 
                        message=f"Updating metadata for {f} with contour level {contour_level} and pixel spacing {pixel_spacing}"
                    )
                    await api.update_metadata(
                        test_entry.copy_dep_id, 
                        file.file_id, 
                        contour=contour_level, 
                        spacing_x=pixel_spacing, 
                        spacing_y=pixel_spacing, 
                        spacing_z=pixel_spacing, 
                        description="Uploaded from test script"
                    )
                else:
                    raise Exception("Contour level or pixel spacing not found in pickle file. Can't continue automatically.")
                    
        except ValueError as e:
            status_manager.update_status(test_entry, status="failed", message=f"Error getting file type for {content_type}.{file_format}: {e}")
            raise
        except Exception as e:
            status_manager.update_status(test_entry, status="failed", message=f"Error uploading {content_type} {file_format}")
            raise

    return uploaded_files


async def submit_task(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    """Submit a deposition for processing"""
    test_pickles_location = pi.getDirPath(dataSetId=test_entry.dep_id, fileSource="pickles")

    if test_entry.copy_dep_id:
        copy_pickles_location = pi.getDirPath(dataSetId=test_entry.copy_dep_id, fileSource="pickles")
        status_manager.update_status(test_entry, status="working", message="Copying pickles from test deposition to copy deposition")

        for file_name in os.listdir(test_pickles_location):
            if file_name.endswith(".pkl"):
                source_path = os.path.join(test_pickles_location, file_name)
                destination_path = os.path.join(copy_pickles_location, file_name)
                shutil.copy(source_path, destination_path)
    else:
        # standalone testing
        test_entry.copy_dep_id = test_entry.dep_id

    # copying the test pickle
    copy_pickles_location = pi.getDirPath(dataSetId=test_entry.copy_dep_id, fileSource="pickles")
    pklpath = Path(__file__).parent / "resources" / "pdbx_contact_author.pkl"
    file_logger.info("Copying pickle file %s to %s", pklpath, copy_pickles_location)
    shutil.copy(pklpath, copy_pickles_location)

    # writing the submitOK.pkl file
    for ppath in [copy_pickles_location, test_pickles_location]:
        with open(os.path.join(ppath, "submitOK.pkl"), "wb") as f:
            pickle.dump({
                'annotator_initials': 'TST',
                'date': '2025-06-24 10:53:30',
                'reason': 'Submission test'
            }, f)

    orcid_cookie = get_cookie_signer(salt=settings.AUTH_COOKIE_KEY).sign(config.api.get("orcid"))
    base_url = config.api.get("base_url")
    
    timeout = aiohttp.ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=10,
        ttl_dns_cache=300,
        use_dns_cache=True,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        connector_owner=False
    ) as session:
        try:
            # get the view to obtain csrftoken
            view_url = os.path.join(base_url, "api", "v1", "depositions", test_entry.copy_dep_id, "view")
            async with session.get(
                url=view_url,
                cookies={"depositor-orcid": orcid_cookie},
                ssl=False
            ) as response:
                if response.status != 200:
                    raise Exception(f"Failed to get deposition view {test_entry.copy_dep_id}: {response.status} - {await response.text()}")
                
                # extract CSRF token from response cookies
                csrftoken = None
                if "csrftoken" in response.cookies:
                    csrftoken = response.cookies["csrftoken"].value
                else:
                    raise Exception("CSRF token not found in response cookies")
            
            # unlock stage
            unlock_url = os.path.join(base_url, "stage", "unlock")
            async with session.post(
                url=unlock_url,
                cookies={"depositor-orcid": orcid_cookie, "csrftoken": csrftoken},
                ssl=False
            ) as response:
                if response.status != 200:
                    file_logger.warning(f"Unlock request returned {response.status}, continuing anyway")
            
            # submit the deposition
            status_manager.update_status(test_entry, message="Submitting deposition")
            submit_url = os.path.join(base_url, "submitRequest")
            
            headers = {
                "x-csrftoken": csrftoken,
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "referer": base_url
            }
            
            async with session.post(
                url=submit_url,
                json={},
                cookies={"depositor-orcid": orcid_cookie, "csrftoken": csrftoken},
                headers=headers,
                ssl=False
            ) as response:
                response_text = await response.text()
                file_logger.info("Submit response: %s", response_text)
                
                if response.status != 200:
                    raise Exception(f"Failed to submit deposition {test_entry.copy_dep_id}: {response.status} - {response_text}")

        except asyncio.TimeoutError:
            raise Exception(f"Timeout while submitting deposition {test_entry.copy_dep_id}")
        except Exception as e:
            raise Exception(f"Error submitting deposition {test_entry.copy_dep_id}: {str(e)}") from e
        finally:
            await connector.close()


def compare_repos_task(test_entry: TestEntry, task: Task, config: Config, status_manager: StatusManager):
    """Compare repositories (sync operation)"""
    overall_success = True

    try:
        source_repo = WwPDBResourceURI(task.source)
        if source_repo.dep_id == ":copy:":
            if not test_entry.copy_dep_id:
                raise ValueError("Deposition ID source must be provided for comparison either in config.yaml or as a parameter.")
            source_repo.dep_id = test_entry.copy_dep_id

        test_repo = WwPDBResourceURI.for_directory(repository=source_repo.repository, dep_id=test_entry.dep_id)
        if not filesystem.exists(test_repo):
            error_msg = f"Repository {test_repo} not found in local archive."
            status_manager.track_comparison_result(test_entry, "repository", False, error_msg)
            raise FileNotFoundError(error_msg)

        status_manager.update_status(test_entry, message=f"Comparing {test_repo} with {source_repo}")

        comparer = comparer_factory("repository", str(filesystem.locate(test_repo)), str(filesystem.locate(source_repo)))
        diffs = comparer.get_report()

        error_msg = None if not diffs else "Comparison failed for repository files"
        status_manager.track_comparison_result(test_entry, "repository", not bool(diffs), pprint.pformat(diffs, indent=2))
        
        if diffs:
            overall_success = False
            status_manager.update_status(test_entry, status="warning", message="Repository files differ")
        else:
            status_manager.update_status(test_entry, message="Repository files are the same")

    except Exception as e:
        overall_success = False
        error_msg = f"Error during repository comparison: {str(e)}"
        status_manager.track_comparison_result(test_entry, "repository", False, error_msg)
        status_manager.update_status(test_entry, status="failed", message=error_msg)
        raise
    finally:
        status_manager.track_task_result(test_entry, TaskType.COMPARE_REPOS, overall_success)


def compare_files_task(test_entry: TestEntry, task: Task, config: Config, status_manager: StatusManager):
    """Compare individual files (sync operation)"""
    overall_success = True

    for cr in task.rules:
        try:
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

            comparer = comparer_factory(rule.method, test_entry_file_path, copy_file_path)
            diffs = comparer.get_report()

            error_msg = None if not diffs else f"Comparison failed for {content_type}.{format} using {rule.method}"
            status_manager.track_comparison_result(test_entry, rule.name, not bool(diffs), pprint.pformat(diffs, indent=4))
            
            if diffs:
                overall_success = False
                status_manager.update_status(test_entry, status="warning", message=f"Comparison failed for {content_type}.{format} using {rule.method}")
            else:
                status_manager.update_status(test_entry, message=f"Files are the same for {content_type}.{format} using {rule.method}")

        except Exception as e:
            overall_success = False
            error_msg = f"Error during comparison: {str(e)}"
            status_manager.track_comparison_result(test_entry, rule.name, False, error_msg)

    status_manager.track_task_result(test_entry, TaskType.COMPARE_FILES, overall_success)


async def create_dep_task(api: DepositApi, test_entry: TestEntry, task: Task, config: Config, status_manager: StatusManager):
    """Create a deposition (async)"""
    try:
        exp_type = status_manager.get_status(test_entry).exp_type
        exp_subtype = status_manager.get_status(test_entry).exp_subtype

        status_manager.update_status(test_entry, status="working", message="Creating test deposition")
        try:
            copy_dep = await create_deposition(
                api=api, 
                orcid=config.api.get("orcid"), 
                country=Country(config.api.get("country")), 
                etype=exp_type, 
                subtype=exp_subtype, 
                email="wbueno@ebi.ac.uk"
            )
            test_entry.copy_dep_id = copy_dep.dep_id
            status_manager.track_task_result(test_entry, TaskType.CREATE, True)
        except Exception as e:
            status_manager.track_task_result(test_entry, TaskType.CREATE, False, str(e))
            raise Exception(f"Error creating test deposition: {str(e)}") from e
    except Exception as e:
        status_manager.update_status(test_entry, status="failed", message=str(e))
        raise


async def upload_task(api: DepositApi, test_entry: TestEntry, task: UploadTask, config: Config, status_manager: StatusManager):
    """Upload files to deposition (async)"""
    source_repository = "tempdep"

    try:
        if not test_entry.copy_dep_id:
            # it's a standalone test, so we use the original dep_id
            test_entry.copy_dep_id = test_entry.dep_id
            source_repository = "deposit-ui"
            await unlock_deposition(test_entry.copy_dep_id, config)

        status_manager.update_status(test_entry, copy_dep_id=test_entry.copy_dep_id)
        await _upload_all_files(api=api, test_entry=test_entry, task=task, status_manager=status_manager, source_repository=source_repository)

        copy_elements = {"copy_contact": False, "copy_authors": False, "copy_citation": False, "copy_grant": False, "copy_em_exp_data": False}
        await api.process(test_entry.copy_dep_id, **copy_elements)
        await monitor_processing(test_entry, config, status_manager)

        status_manager.track_task_result(test_entry, TaskType.UPLOAD, True)
    except Exception as e:
        status_manager.update_status(test_entry, status="failed", message=str(e))
        status_manager.track_task_result(test_entry, TaskType.UPLOAD, False, str(e))
        raise


def fetch_files(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    """Fetch files from archive (sync operation)"""
    remote = config.get_remote_archive()
    local = LocalArchive(site_id=getSiteId())
    fetcher = RemoteFetcher(remote, local, cache_size=10, force=False)

    status_manager.update_status(test_entry, copy_dep_id=test_entry.copy_dep_id, message="Fetching files from archive")
    try:
        fetcher.fetch_repository(test_entry.dep_id, repository=ArchiveRepository.DEPOSIT.value)
    except Exception as e:
        raise Exception("Error fetching files from archive") from e


def get_entry_info(test_entry: TestEntry, config: Config, status_manager: StatusManager):
    """Get entry information from database (sync operation)"""
    prod_ci = ConfigInfo(siteId=config.remote_archive.site_id)
    prod_db = {
        "host": prod_ci.get("SITE_DB_HOST_NAME"),
        "user": prod_ci.get("SITE_DB_USER_NAME"),
        "password": prod_ci.get("SITE_DB_PASSWORD"),
        "port": int(prod_ci.get("SITE_DB_PORT_NUMBER")),
        "database": "status",
    }

    connection = MySQLdb.connect(**prod_db)
    try:
        status_manager.update_status(test_entry, status="working", message="Getting entry info")
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
                exp_method = ExperimentType.NMR
            elif 'solid' in exp:
                exp_method = ExperimentType.SSNMR
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


async def run_entry_tasks(entry, config, status_manager):
    """Run all tasks for a single entry sequentially using async API."""
    try:
        # Get entry info (sync operation)
        get_entry_info(entry, config, status_manager)
        
        # Create async API instance
        api = DepositApi(
            api_key=create_token(config.api.get("orcid"), expiration_days=7), 
            hostname=config.api.get("base_url"), 
            ssl_verify=False
        )
        await api._ensure_adapter()

        # Fetch files if needed (sync operation)
        if not entry.skip_fetch:
            fetch_files(entry, config, status_manager)

        # Process tasks
        for task in entry.tasks:
            try:
                if task.type == TaskType.CREATE:
                    await create_dep_task(api, entry, task, config, status_manager)
                elif task.type == TaskType.UPLOAD:
                    await upload_task(api, entry, task, config, status_manager)
                elif task.type == TaskType.COMPARE_FILES:
                    compare_files_task(entry, task, config, status_manager)
                elif task.type == TaskType.COMPARE_REPOS:
                    compare_repos_task(entry, task, config, status_manager)
                elif task.type == TaskType.SUBMIT:
                    await submit_task(entry, config, status_manager)
                else:
                    file_logger.warning("Unknown task type %s for entry %s", task.type, entry.dep_id)
            except Exception as e:
                file_logger.error("Error processing task %s for entry %s: %s", task.type, entry.dep_id, e, exc_info=True)
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                task.execution_time = datetime.now()
                if task.stop_on_failure:
                    status_manager.update_status(entry, status="failed", message=f"Task {task.type} failed: {e}")
                    return

        status_manager.update_status(entry, status="finished", message=f"Completed all tasks for entry {entry.dep_id}")
        
    except Exception as e:
        file_logger.error("Error processing entry %s: %s", entry.dep_id, e, exc_info=True)
        status_manager.update_status(entry, status="failed", message=f"Error processing entry: {e}")


def setup_report_generation(config, output_dir: str = "reports"):
    """Setup report generation components"""
    Path(output_dir).mkdir(exist_ok=True)
    template_dir = Path(__file__).parent / "templates"
    template_path = template_dir / "test_report.html"
    if not template_path.exists():
        file_logger.warning(f"Template file not found: {template_path}")
    
    # Initialize report generator
    generator = TestReportGenerator(config=config, template_dir=template_dir, output_dir=output_dir)
    integration = TestReportIntegration(generator)
    
    return generator, integration


async def run_all_entries(test_set, config, status_manager, max_concurrent=3):
    """Run all test entries with controlled concurrency"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def run_with_semaphore(entry):
        async with semaphore:
            await run_entry_tasks(entry, config, status_manager)
    
    # Create tasks for all entries
    tasks = [run_with_semaphore(entry) for entry in test_set]
    
    # Run all tasks concurrently with controlled concurrency
    await asyncio.gather(*tasks, return_exceptions=True)


@click.command()
@click.argument('test_config', type=click.Path(exists=True, dir_okay=False, readable=True), required=True)
@click.option('--generate-report', is_flag=True, help='Generate HTML test report', default=True)
@click.option('--report-dir', default="reports", help='Directory for generated reports')
@click.option('--max-concurrent', default=3, help='Maximum number of concurrent test entries')
def main(test_config, generate_report, report_dir, max_concurrent):
    """TEST_CONFIG is the path to the test configuration file."""
    if getSiteId() in ["PDBE_PROD"]:  # get the production ids from config
        click.echo("This command is not allowed on production sites. Exiting.", err=True)
        return

    config = Config(test_config)
    test_set = config.get_test_set()

    report_integration = None
    if generate_report:
        try:
            webapps_dir = ci.get("TOP_WWPDB_WEBAPPS_DIR")
            _, report_integration = setup_report_generation(
                config=config,
                output_dir=os.path.join(webapps_dir, "htdocs")
            )
            click.echo(f"Report generation enabled. Reports will be saved to: {report_dir}")
        except Exception as e:
            click.echo(f"Warning: Could not setup report generation: {e}", err=True)
            generate_report = False

    async def run_tests():
        """Main async function to run all tests"""
        try:
            with Live(refresh_per_second=3) as live:
                def update_callback():
                    """Callback to update the live display"""
                    live.update(generate_table(status_manager))

                status_manager = StatusManager(test_set, callback=update_callback, report_integration=report_integration)
                live.update(generate_table(status_manager))

                # Run all entries with controlled concurrency
                await run_all_entries(test_set, config, status_manager, max_concurrent)

        except KeyboardInterrupt:
            click.echo("🚫 Test interrupted by user.", err=True)
        finally:
            if generate_report and report_integration:
                try:
                    report_path = report_integration.generate_final_report(
                        test_set, 
                        status_manager,
                        output_filename=f"report.html"
                    )
                    click.echo(f"📝 Test report generated. Read it in {config.report.get('depui_url')}/report.html")
                except Exception as e:
                    click.echo(f"Failed to generate test report: {e}", err=True)
                    file_logger.error(f"Report generation failed: {e}")

    # Run the async main function
    asyncio.run(run_tests())


if __name__ == '__main__':
    main()