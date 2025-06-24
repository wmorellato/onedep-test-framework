import os
import shutil
import subprocess

from dataclasses import dataclass
from typing import List, Dict, Any, List, Union
from pathlib import Path

from wwpdb.io.locator.PathInfo import PathInfo

from odtf.wwpdb_uri import WwPDBResourceURI, FilesystemBackend, FileNameBuilder
from odtf.models import RemoteArchive, LocalArchive
from odtf.common import get_file_logger
from odtf.config import Config

file_logger = get_file_logger(__name__)


class RemoteFetcher:
    """Fetches depositions from a remote archive and stores them in a local archive.
    """
    def __init__(self, remote_archive: RemoteArchive, local_archive: LocalArchive, cache_size=10, force=False):
        self.remote_archive = remote_archive
        self.local_archive = local_archive
        self.remote_pi = PathInfo(siteId=self.remote_archive.site_id)
        self.local_pi = PathInfo(siteId=self.local_archive.site_id)
        self.cache_size = cache_size
        self.force = force

    def fetch_file(self, file_uri: WwPDBResourceURI):
        """ Fetches a file from the remote archive and stores it in the local tempdep directory."""
        filesystem = FilesystemBackend(self.remote_pi, Config.CONTENT_TYPE_DICT, Config.FORMAT_DICT)
        filepath = str(filesystem.locate(file_uri))

        if not filepath:
            file_logger.error("File %s not found in remote archive", file_uri)
            return None

        local_path = self.local_pi.getTempDepPath(dataSetId=file_uri.dep_id)

        if self.remote_archive.host == "localhost":
            # Copy the file locally
            file_logger.debug("Copying locally from %s to %s", filepath, local_path)
            os.makedirs(local_path, exist_ok=True)
            shutil.copy(filepath, local_path)
            return

        rsync_command = self._build_rsync_command(filepath, local_path)
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def fetch_repository(self, dep_id, repository="deposit"):
        """ Fetches a deposition from the remote archive and stores it in the local
        tempdep directory.

        Args:
            dep_id (str): The deposition ID to fetch.
            repository (str): The repository from which to fetch the deposition.
        """
        if self._local_exists(dep_id) and not self.force:
            return

        # self._evict_oldest_entry()

        file_logger.info("Entry %s not found locally. Downloading from remote host %s", dep_id, self.remote_archive.host)
        self._fetch_data_and_pickles(dep_id, repository)

    def _local_exists(self, dep_id):
        return os.path.exists(self.local_pi.getTempDepPath(dataSetId=dep_id))

    def _remove_from_local(self, dep_id):
        if not self._local_exists(dep_id):
            file_logger.warning("Unable to remove entry from cache (not found)")
            return
        shutil.rmtree(os.path.join(self.cache_location, dep_id))

    def _evict_oldest_entry(self):
        deposition_folders = [folder for folder in os.listdir(self.cache_location) if folder.startswith("D_999")]
        if len(deposition_folders) >= self.cache_size:
            oldest_folder = min(deposition_folders, key=os.path.getctime)
            shutil.rmtree(os.path.join(self.cache_location, oldest_folder))
    
    def _build_rsync_command(self, remote_path, local_path):
        rsync_command = ["rsync", "-arvzL", "--ignore-existing"]

        if self.remote_archive.key_file:
            rsync_command.append("-e")
            rsync_command.append(f"ssh -i {self.remote_archive.key_file}")

        rsync_command.append(f"{self.remote_archive.user}@{self.remote_archive.host}:{remote_path}/")
        rsync_command.append(local_path)

        return rsync_command

    def _fetch_data_and_pickles(self, dep_id, repository):
        remote_data_path = self.remote_pi.getDirPath(dataSetId=dep_id, fileSource=repository)
        remote_pickles_path = self.remote_pi.getDirPath(dataSetId=dep_id, fileSource="pickles")
        local_data_path = self.local_pi.getTempDepPath(dataSetId=dep_id)
        local_pickles_path = self.local_pi.getDirPath(dataSetId=dep_id, fileSource="pickles")

        os.makedirs(local_data_path, exist_ok=True)
        os.makedirs(local_pickles_path, exist_ok=True)

        if self.remote_archive.host == "localhost":
            # we'll have to build the pickles path manually
            file_logger.debug("Copying locally from %s to %s", remote_data_path, local_data_path)
            shutil.copytree(remote_data_path, local_data_path, dirs_exist_ok=True)

            if repository == "deposit":
                remote_pickles_path = os.path.join(os.path.dirname(self.local_pi.getDepositPath(dataSetId=dep_id)), "temp_files", "deposition-v-200", dep_id)
            elif repository == "deposit-ui":
                remote_pickles_path = os.path.join(os.path.dirname(self.local_pi.getDepositUIPath(dataSetId=dep_id)), "temp_files", "deposition-v-200", dep_id)

            if remote_pickles_path == local_pickles_path:
                return

            shutil.copytree(remote_pickles_path, local_pickles_path, dirs_exist_ok=True)
            return

        rsync_command = self._build_rsync_command(remote_data_path, local_data_path)
        file_logger.debug("Running command %s", ' '.join(rsync_command))
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        rsync_command = self._build_rsync_command(remote_pickles_path, local_pickles_path)
        file_logger.debug("Running command %s", ' '.join(rsync_command))
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
