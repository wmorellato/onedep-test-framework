import os

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Union
from urllib.parse import urlparse, parse_qs, urlencode
from pathlib import Path
import re

from wwpdb.io.locator.PathInfo import PathInfo


# TODO: Implement RemoteBackend


class WwPDBResourceURI:
    """
    wwPDB Resource URI that supports both directory and file identification

    Examples:
    - wwpdb://archive/D_8233000142/
    - wwpdb://deposit/D_8233000142/
    - wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2
    - wwpdb://tempdep/D_1292123393/model.pdbx?version=latest
    """

    def __init__(self, uri: str):
        self.uri = uri
        self.parsed = urlparse(uri)

        # Parse path components
        path_parts = [p for p in self.parsed.path.split('/') if p]

        if not self.parsed.netloc:
            raise ValueError("URI must specify at least a repository")

        self.repository = self.parsed.netloc  # archive, deposit, tempdep, etc. TODO: create enum
        self.dep_id = path_parts[0] if len(path_parts) > 0 else None

        # Check if this is a file (has content.format in path)
        if len(path_parts) > 1:
            # File path: wwpdb://repo/dep_id/content.format
            file_part = path_parts[1]
            if '.' in file_part:
                parts = file_part.split('.')
                self.content_type = parts[0]
                self.format = parts[1]
                self.is_directory = False
            else:
                raise ValueError("File path must be in format content.format")
        else:
            # Directory path
            self.content_type = None
            self.format = None
            self.is_directory = self.parsed.path.endswith('/') or not self.dep_id

        # Parse query parameters for additional file metadata
        self.params = parse_qs(self.parsed.query)
        # Convert single-item lists to strings for convenience
        self.params = {k: v[0] if len(v) == 1 else v for k, v in self.params.items()}

    @classmethod
    def for_directory(cls, repository: str, dep_id: Optional[str] = None) -> 'WwPDBResourceURI':
        """Create URI for a directory"""
        if dep_id:
            return cls(f"wwpdb://{repository}/{dep_id}/")
        else:
            return cls(f"wwpdb://{repository}/")

    @classmethod
    def for_file(cls, repository: str, dep_id: str, content_type: str, 
                 format: str, part_number: Optional[int] = None, 
                 version: Optional[str] = None) -> 'WwPDBResourceURI':
        """Create URI for a specific file based on parameters"""
        # Build path with content.format
        path = f"wwpdb://{repository}/{dep_id}/{content_type}.{format}"

        # Add query parameters for metadata
        params = {}
        if part_number is not None:
            params['part'] = str(part_number)
        if version:
            params['version'] = version

        if params:
            query_string = urlencode(params)
            return cls(f"{path}?{query_string}")
        else:
            return cls(path)

    def join_file(self, content_type: str, format: str, 
                  part_number: Optional[int] = 1, version: Optional[str] = "latest") -> 'WwPDBResourceURI':
        """Create a file URI by extending this directory URI"""
        if not self.is_directory:
            raise ValueError("Can only join files to directory URIs")
        
        return WwPDBResourceURI.for_file(
            repository=self.repository,
            dep_id=self.dep_id,
            content_type=content_type,
            format=format,
            part_number=part_number,
            version=version
        )

    def __str__(self):
        return self.uri

    def get_content_type(self) -> Optional[str]:
        """Get content type from path"""
        return self.content_type

    def get_format(self) -> Optional[str]:
        """Get file format from path"""
        return self.format

    def get_part_number(self) -> Optional[int]:
        """Get part number from parameters"""
        part = self.params.get('part')
        return int(part) if part else None

    def get_version(self) -> Optional[str]:
        """Get version from parameters"""
        return self.params.get('version')

    def is_file_uri(self) -> bool:
        """Check if this URI represents a file (has content.format in path)"""
        return self.content_type is not None and self.format is not None


class FileNameBuilder:
    """
    Builds wwPDB file names from parameters and vice versa
    """

    def __init__(self, content_type_config: Dict[str, tuple], format_extension_d: Dict[str, str]):
        """
        Initialize with content type and format extension mappings

        Args:
            content_type_config: Maps content types to (allowed_formats, filename_content_type)
                Example: {"model": (["pdbx", "pdb"], "model"), "nmr-chemical-shifts": (["nmr-star"], "cs")}
            format_extension_d: Maps format names to their extensions
                Example: {"pdbx": "pdbx", "nmr-star": "str"}
        """
        self.content_type_config = content_type_config
        self.format_extension_d = format_extension_d

        # Build reverse mappings for parsing
        self.extension_to_format = {ext: fmt for fmt, ext in format_extension_d.items()}
        self.filename_content_to_type = {filename_type: content_type 
                                        for content_type, (_, filename_type) in content_type_config.items()}

    def build_filename(self, dep_id: str, content_type: str, format: str,
                      part_number: int = 1, version: Optional[str] = None) -> str:
        """
        Build filename from parameters

        Example: D_8233000142_cs-upload_P1.str.V2 (for nmr-chemical-shifts content type)
        """
        # Validate content type and format
        if content_type not in self.content_type_config:
            raise ValueError(f"Unknown content type: {content_type}")

        allowed_formats, filename_content_type = self.content_type_config[content_type]

        # Check if format is allowed for this content type (unless "any" is specified)
        if "any" not in allowed_formats and format not in allowed_formats:
            raise ValueError(f"Format '{format}' not allowed for content type '{content_type}'. "
                           f"Allowed formats: {allowed_formats}")

        if format not in self.format_extension_d:
            raise ValueError(f"Unknown file format: {format}")

        extension = self.format_extension_d[format]

        # Use the filename content type (may be different from logical content type)
        content_part = filename_content_type

        # Build filename
        filename = f"{dep_id}_{content_part}_P{part_number}.{extension}"

        if version:
            filename += f".V{version}"

        return filename

    def parse_filename(self, filename: str) -> Dict[str, Any]:
        """
        Parse filename to extract parameters

        Returns dict with: dep_id, content_type, format, part_number, version
        """
        # Pattern: D_XXXXXXX_content_P#.format[.V#]
        # milestone can be treated as a separate content-type
        pattern = r'^(D_\d+)_(.+)_P(\d+)\.([^.]+)(?:\.V(\d+))?$'
        match = re.match(pattern, filename)

        if not match:
            raise ValueError(f"Invalid filename format: {filename}")

        dep_id, content_part, part_str, extension, version = match.groups()

        # Parse content type
        filename_content_type = content_part

        # Map filename content type back to logical content type
        content_type = self.filename_content_to_type.get(filename_content_type)
        if not content_type:
            raise ValueError(f"Unknown filename content type: {filename_content_type}")

        # Find file format from extension
        format = self.extension_to_format.get(extension)
        if not format:
            raise ValueError(f"Unknown file extension: {extension}")

        return {
            'dep_id': dep_id,
            'content_type': content_type,
            'format': format,
            'part_number': int(part_str),
            'version': version,
        }


class StorageBackend(ABC):
    """
    Abstract storage backend interface.
    Extendable for filesystem, S3, etc. without modifying existing code.
    """
    @abstractmethod
    def locate(self, uri: WwPDBResourceURI) -> Path:
        """Get the filesystem path for this URI"""
        pass

    @abstractmethod
    def exists(self, uri: WwPDBResourceURI) -> bool:
        """Check if resource exists"""
        pass

    @abstractmethod
    def list_directory(self, uri: WwPDBResourceURI, **filters) -> List[WwPDBResourceURI]:
        """List files in directory with optional filters"""
        pass

    @abstractmethod
    def get_metadata(self, uri: WwPDBResourceURI) -> Dict[str, Any]:
        """Get resource metadata (size, modified time, etc.)"""
        pass


class FilesystemBackend(StorageBackend):
    """
    Filesystem storage backend implementation
    """

    def __init__(self, path_info: PathInfo, content_type_config: Dict[str, tuple], 
                 format_extension_d: Dict[str, str]):
        self.path_info = path_info
        self.filename_builder = FileNameBuilder(content_type_config, format_extension_d)

    def _resolve_path(self, uri: WwPDBResourceURI) -> Path:
        """Resolve URI to filesystem path"""
        if uri.is_file_uri():
            return Path(self.path_info.getFilePath(
                dataSetId=uri.dep_id,
                contentType=uri.content_type,
                fileFormat=uri.format,
                fileSource=uri.repository,
                versionId=uri.get_version(),
                partNumber=uri.get_part_number() or 1
            ))
        else:
            if uri.dep_id:
                return self.path_info.getDirPath(
                    dataSetId=uri.dep_id,
                    fileSource=uri.repository
                )
            else:
                path = self.path_info.getDirPath(
                    dataSetId='D_000001',
                    fileSource=uri.repository
                )

                if path:
                    return Path(path).parent

                # raise?

    def locate(self, uri: WwPDBResourceURI) -> Path:
        """Get the filesystem path for this URI"""
        return self._resolve_path(uri)

    def exists(self, uri: WwPDBResourceURI) -> bool:
        """Check if resource exists"""
        return self._resolve_path(uri).exists()

    def list_directory(self, uri: WwPDBResourceURI, **filters) -> List[WwPDBResourceURI]:
        """List files in directory with optional filters"""
        if uri.is_file_uri():
            raise ValueError("URI must be a directory")

        directory = self._resolve_path(uri)
        if not directory.exists() or not directory.is_dir():
            return []

        results = []
        for file_path in directory.glob("D_*"):
            if not file_path.is_file():
                continue

            try:
                parsed = self.filename_builder.parse_filename(file_path.name)

                # Apply filters
                if filters:
                    match = True
                    for key, value in filters.items():
                        if key == 'content_type' and parsed['content_type'] != value:
                            match = False
                            break
                        elif key == 'format' and parsed['format'] != value:
                            match = False
                            break
                        elif key == 'version' and parsed['version'] != value:
                            match = False
                            break

                    if not match:
                        continue

                # Create URI for this file
                file_uri = WwPDBResourceURI.for_file(
                    repository=uri.repository,
                    dep_id=parsed['dep_id'],
                    content_type=parsed['content_type'],
                    format=parsed['format'],
                    part_number=parsed['part_number'],
                    version=parsed['version']
                )
                results.append(file_uri)

            except ValueError:
                continue

        return results

    def get_metadata(self, uri: WwPDBResourceURI) -> Dict[str, Any]:
        """Get resource metadata"""
        path = self._resolve_path(uri)
        if not path.exists():
            return {}

        stat = path.stat()
        return {
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'is_directory': path.is_dir()
        }


class WWPDBResourceManager:
    """
    Main interface for WWPDB resource management.
    Extensible design - add new backends without modification.
    """

    def __init__(self, default_backend: StorageBackend):
        self.backends = {}
        self.default_backend = default_backend

    def register_backend(self, repository: str, backend: StorageBackend):
        """Register a storage backend for a specific repository"""
        self.backends[repository] = backend

    def _get_backend(self, uri: WwPDBResourceURI) -> StorageBackend:
        """Get the appropriate backend for a URI"""
        return self.backends.get(uri.repository, self.default_backend)

    def exists(self, uri: Union[str, WwPDBResourceURI]) -> bool:
        if isinstance(uri, str):
            uri = WwPDBResourceURI(uri)
        return self._get_backend(uri).exists(uri)

    def list_directory(self, uri: Union[str, WwPDBResourceURI], **filters) -> List[WwPDBResourceURI]:
        if isinstance(uri, str):
            uri = WwPDBResourceURI(uri)
        return self._get_backend(uri).list_directory(uri, **filters)

    def read_file(self, uri: Union[str, WwPDBResourceURI]) -> bytes:
        if isinstance(uri, str):
            uri = WwPDBResourceURI(uri)
        return self._get_backend(uri).read_file(uri)

    def write_file(self, uri: Union[str, WwPDBResourceURI], data: bytes) -> None:
        if isinstance(uri, str):
            uri = WwPDBResourceURI(uri)
        return self._get_backend(uri).write_file(uri, data)

    def get_metadata(self, uri: Union[str, WwPDBResourceURI]) -> Dict[str, Any]:
        if isinstance(uri, str):
            uri = WwPDBResourceURI(uri)
        return self._get_backend(uri).get_metadata(uri)


def main():
    # this can be found in ConfigInfoData
    content_type_config = {
        "model": (["pdbx", "pdb", "pdbml", "cifeps"], "model"),
        "nmr-chemical-shifts": (["nmr-star", "pdbx", "any"], "cs"),
        "em-volume": (["map"], "em-volume"),
    }

    format_extension_d = {
        'pdbx': 'cif',
        'nmr-star': 'str',
        'json': 'json',
    }

    fs_backend = FilesystemBackend('/data', content_type_config, format_extension_d)

    # rm should be used to manage resources as it will search in the registered
    # backends (eg local, s3 bucket, etc.)
    rm = WWPDBResourceManager(fs_backend)
    rm.exists('wwpdb://archive/D_8233000142/')

    # repository URIs
    archive_dir = WwPDBResourceURI.for_directory('archive', 'D_8233000142')
    deposit_dir = WwPDBResourceURI.for_directory('deposit', 'D_8233000142')

    # file URI
    model_file = WwPDBResourceURI.for_file(
        repository='deposit',
        dep_id='D_8233000142',
        content_type='model-upload', # with milestone
        format='pdbx',
        part_number=1,
        version='2',
    )
    print(f"Archive directory: {archive_dir}")
    print(f"Deposit directory: {deposit_dir}")
    print(f"Model file: {model_file}")

    simple_file = WwPDBResourceURI('wwpdb://deposit/D_8233000142/model.pdbx')
    print(f"Simple file: {simple_file}")


if __name__ == "__main__":
    main()