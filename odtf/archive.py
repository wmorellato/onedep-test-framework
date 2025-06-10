import os
import shutil
import subprocess

from typing import Callable, List

from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.main.archive import ArchiveRepository

from wwpdb.io.locator.PathInfo import PathInfo
from wwpdb.io.locator.DataReference import DataFileReference

from odtf.common import get_file_logger
from odtf.models import RemoteArchive, LocalArchive

file_logger = get_file_logger(__name__)


class FileFinder:
    """This implementation is not good, but I'm in a hurry to get this done.
    We need a way of finding any WF file regardless of its location, which is
    what I tried to do here, but it's clunky and conflicts with the LFS implementation.
    """
    def __init__(self, location: str):
        self._location = location
        self._content_group = {}

    def find(self, filters: List[Callable[[str], bool]] = None) -> List[str]:
        """
        Find files in the archive directory based on the deposition ID and apply filters.
        
        :param location: The path to the location where files are stored.
        :param filters: List of filter functions to apply to the files.
        :return: List of filtered file paths.
        """
        files = []
        for filename in os.listdir(self._location):
            if os.path.isfile(os.path.join(self._location, filename)):
                # the repository doesn't matter here
                fo = parse_filename(ArchiveRepository.ARCHIVE.value, filename)
                if not (fo and fo.getFilePathReference()):
                    continue
                files.append(fo)

        if filters:
            for filter_func in filters:
                files = list(filter(filter_func, files))

        return [f._DataFileReference__getInternalFileNameVersioned() for f in files]

    @staticmethod
    def filter_by_content_type(content_type: str) -> Callable[[str], bool]:
        """
        Create a filter function to filter files by content type.
        """
        def filter_func(dfr: DataFileReference) -> bool:
            return dfr.getContentType() == content_type
        return filter_func

    @staticmethod
    def filter_by_format(file_format: str) -> Callable[[str], bool]:
        """
        Create a filter function to filter files by format.
        """
        def filter_func(dfr: DataFileReference) -> bool:
            return dfr.getFileFormat() == file_format
        return filter_func

    @staticmethod
    def filter_by_partition(partition: str) -> Callable[[str], bool]:
        """
        Create a filter function to filter files by partition number.
        """
        def filter_func(dfr: DataFileReference) -> bool:
            return dfr.getPartitionNumber() == partition
        return filter_func

    @staticmethod
    def filter_by_version(version: str) -> Callable[[str], bool]:
        """
        Create a filter function to filter files by version.
        
        WARNING: This only accepts numbers (in str), not the string identifiers
        (latest, next, etc.). If you want the first, latest etc, remove
        this filter and sort the results after filtering.
        """
        def filter_func(dfr: DataFileReference) -> bool:
            return dfr.getVersionId() == version
        return filter_func

    @staticmethod
    def filter_by_filename_contains(substring: str) -> Callable[[str], bool]:
        """
        Create a filter function to filter files by substring in filename.
        """
        def filter_func(dfr: DataFileReference) -> bool:
            filepath = dfr.getFilePathReference()

            if not filepath:
                return False

            return substring in os.path.basename(dfr.getFilePathReference())
        return filter_func


class RemoteFetcher:
    """This needs refactoring. Quick and dirty without thinking about the local
    directory.
    """
    def __init__(self, remote_archive: RemoteArchive, local_archive: LocalArchive, cache_size=10, force=False):
        self.remote_archive = remote_archive
        self.local_archive = local_archive
        self.remote_pi = PathInfo(siteId=self.remote_archive.site_id)
        self.local_pi = PathInfo(siteId=self.local_archive.site_id)
        self.cache_size = cache_size
        self.force = force

    def fetch(self, dep_id, repository="deposit"):
        """ Fetches a deposition from the remote archive and stores it in the local cache.
        If the deposition is already cached, it copies it to the destination.
        If the cache is full, it evicts the oldest entry.

        Args:
            dep_id (str): The deposition ID to fetch.
            destination (str): The local path where the deposition should be copied. No manipulation is done to this path before copy.
            repository (str): The repository from which to fetch the deposition. Defaults to "deposit".
        """
        if self._local_exists(dep_id) and not self.force:
            return

        # self._evict_oldest_entry()

        file_logger.info("Entry %s not found locally. Downloading from remote host %s", dep_id, self.remote_archive.host)
        self._fetch_from_remote(dep_id, repository)

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

    def _fetch_from_remote(self, dep_id, repository):
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
            remote_pickles_path = os.path.join(os.path.dirname(self.local_pi.getDepositPath(dataSetId=dep_id)), "temp_files", "deposition-v-200", dep_id)
            shutil.copytree(remote_pickles_path, local_pickles_path, dirs_exist_ok=True)
            return

        rsync_command = ["rsync", "-arvzL"]

        if self.remote_archive.key_file:
            rsync_command.append("-e")
            rsync_command.append(f"ssh -i {self.remote_archive.key_file}")

        rsync_command.append(f"{self.remote_archive.user}@{self.remote_archive.host}:{remote_data_path}/")
        rsync_command.append(local_data_path)

        file_logger.debug("Running command %s", ' '.join(rsync_command))
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        rsync_command.pop()  # Remove the last argument (local_data_path)
        rsync_command.pop()  # Remove the remote_data_path argument
        rsync_command.append(f"{self.remote_archive.user}@{self.remote_archive.host}:{remote_pickles_path}/")
        rsync_command.append(local_pickles_path)

        file_logger.debug("Running command %s", ' '.join(rsync_command))
        subprocess.run(rsync_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
