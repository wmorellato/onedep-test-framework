import os
import shutil
import subprocess

from typing import Callable, List

from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.main.archive import ArchiveRepository

from wwpdb.io.locator.DataReference import DataFileReference
from wwpdb.utils.wf.WfDataObject import WfDataObject

from odtf.common import get_file_logger
from odtf.models import RemoteArchive

file_logger = get_file_logger(__name__)


class FileFinder:
    """This implementation is not good, but I'm in a hurry to get this done.
    We need a way of finding any WF file regardless of its location, which is
    what I tried to do here, but it's clunky and conflicts with the LFS implementation.
    """
    def __init__(self, location: str):
        self._location = location
        self._content_group = {}
    
    def find_single(self, content_type: str, format: str, version: str = "latest",
                milestone=None, partition: str = None) -> str:
        if milestone:
            content_type = content_type + '-' + milestone

        filters=[
            FileFinder.filter_by_content_type(content_type),
            FileFinder.filter_by_format(format),
        ]

        if partition:
            filters.append(FileFinder.filter_by_partition(partition))

        if version not in ("latest", "original"):
            filters.append(FileFinder.filter_by_version(version))

        files = self.find(filters=filters)

        if not files:
            return None

        if version == "latest":
            return max(files)
        elif version == "original":
            return min(files)

        return files[0]

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
