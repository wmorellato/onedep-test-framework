#!/usr/bin/env python3
"""
Unit tests for WWPDB URI system
"""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch

from odtf.wwpdb_uri import (
    WwPDBResourceURI, 
    FileNameBuilder, 
    FilesystemBackend, 
    WWPDBResourceManager
)

# Test fixtures and configuration
@pytest.fixture
def content_type_config():
    """Standard content type configuration for tests"""
    return {
        "model": (["pdbx", "pdb", "pdbml", "cifeps"], "model"),
        "model-upload": (["pdbx", "pdb"], "model-upload"),
        "nmr-chemical-shifts": (["nmr-star", "pdbx", "any"], "cs"),
        "em-volume": (["map"], "em-volume"),
        "em-half-volume": (["map"], "em-half-volume"),
        "structure-factors": (["mtz", "pdbx"], "structure-factors"),
    }

@pytest.fixture
def format_extension_d():
    """Standard format extension mappings for tests"""
    return {
        'pdbx': 'cif',
        'pdb': 'pdb',
        'pdbml': 'xml',
        'cifeps': 'eps',
        'nmr-star': 'str',
        'map': 'map',
        'mtz': 'mtz',
        'json': 'json',
    }

@pytest.fixture
def temp_data_dir():
    """Create temporary directory structure for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create repository directories
        repos = ['deposit', 'archive', 'tempdep', 'workflow']
        dep_ids = ['D_8233000142', 'D_8233000143']
        
        for repo in repos:
            for dep_id in dep_ids:
                repo_dir = Path(temp_dir) / repo / dep_id
                repo_dir.mkdir(parents=True, exist_ok=True)
                
                # Create some test files
                if repo == 'deposit' and dep_id == 'D_8233000142':
                    # Create files for version testing
                    test_files = [
                        'D_8233000142_em-volume_P1.map.V1',
                        'D_8233000142_em-volume_P1.map.V2', 
                        'D_8233000142_em-volume_P1.map.V3',
                        'D_8233000142_model_P1.cif.V1',
                        'D_8233000142_cs_P1.str.V1',
                    ]
                    for filename in test_files:
                        (repo_dir / filename).write_text(f"Test content for {filename}")
                        
        yield temp_dir

@pytest.fixture
def mock_path_info(temp_data_dir):
    """Mock PathInfo that returns paths in temp directory"""
    mock = Mock()
    
    def mock_get_file_path(dataSetId, contentType, fileFormat, fileSource, 
                          versionId=None, partNumber=1, **kwargs):
        # Simulate PathInfo behavior
        base_path = Path(temp_data_dir) / fileSource / dataSetId
        
        # Build filename based on content type config
        content_type_config = {
            "model": "model",
            "model-upload": "model-upload", 
            "nmr-chemical-shifts": "cs",
            "em-volume": "em-volume",
        }
        
        format_extensions = {
            'pdbx': 'cif',
            'nmr-star': 'str',
            'map': 'map',
        }
        
        filename_content = content_type_config.get(contentType, contentType)
        extension = format_extensions.get(fileFormat, fileFormat)
        
        filename = f"{dataSetId}_{filename_content}_P{partNumber}.{extension}"
        if versionId and versionId != "latest":
            filename += f".V{versionId}"
        elif versionId == "latest":
            # Find latest version
            existing_files = list(base_path.glob(f"{dataSetId}_{filename_content}_P{partNumber}.{extension}.V*"))
            if existing_files:
                versions = []
                for f in existing_files:
                    try:
                        version = int(f.name.split('.V')[-1])
                        versions.append(version)
                    except ValueError:
                        continue
                if versions:
                    latest_version = max(versions)
                    filename += f".V{latest_version}"
        
        return base_path / filename
    
    def mock_get_dir_path(dataSetId, fileSource, **kwargs):
        return Path(temp_data_dir) / fileSource / dataSetId
    
    mock.getFilePath = mock_get_file_path
    mock.getDirPath = mock_get_dir_path
    
    return mock

@pytest.fixture
def filename_builder(content_type_config, format_extension_d):
    """FileNameBuilder instance for testing"""
    return FileNameBuilder(content_type_config, format_extension_d)

@pytest.fixture
def filesystem_backend(mock_path_info, content_type_config, format_extension_d):
    """FilesystemBackend instance for testing"""
    return FilesystemBackend(mock_path_info, content_type_config, format_extension_d)


class TestWwPDBResourceURI:
    """Test WwPDBResourceURI class"""
    
    def test_init_directory_uri(self):
        """Test creating directory URI"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        
        assert uri.repository == "deposit"
        assert uri.dep_id == "D_8233000142"
        assert uri.is_directory is True
        assert uri.content_type is None
        assert uri.format is None
    
    def test_init_file_uri(self):
        """Test creating file URI"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2")
        
        assert uri.repository == "deposit"
        assert uri.dep_id == "D_8233000142"
        assert uri.content_type == "em-volume"
        assert uri.format == "map"
        assert uri.is_directory is False
        assert uri.get_part_number() == 1
        assert uri.get_version() == "2"
    
    def test_init_repository_only(self):
        """Test creating repository-only URI"""
        uri = WwPDBResourceURI("wwpdb://deposit/")
        
        assert uri.repository == "deposit"
        assert uri.dep_id is None
        assert uri.is_directory is True
    
    def test_init_invalid_uri(self):
        """Test invalid URI formats"""
        with pytest.raises(ValueError, match="URI must specify at least a repository"):
            WwPDBResourceURI("wwpdb://")
        
        with pytest.raises(ValueError, match="File path must be in format content.format"):
            WwPDBResourceURI("wwpdb://deposit/D_123/invalidfile")
    
    def test_for_directory_classmethod(self):
        """Test for_directory class method"""
        # With dep_id
        uri = WwPDBResourceURI.for_directory("archive", "D_8233000142")
        assert str(uri) == "wwpdb://archive/D_8233000142/"
        
        # Without dep_id
        uri = WwPDBResourceURI.for_directory("deposit")
        assert str(uri) == "wwpdb://deposit/"
    
    def test_for_file_classmethod(self):
        """Test for_file class method"""
        # With all parameters
        uri = WwPDBResourceURI.for_file(
            repository="deposit",
            dep_id="D_8233000142",
            content_type="em-volume",
            format="map",
            part_number=1,
            version="2"
        )
        assert str(uri) == "wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2"
        
        # Minimal parameters
        uri = WwPDBResourceURI.for_file(
            repository="deposit",
            dep_id="D_8233000142", 
            content_type="model",
            format="pdbx"
        )
        assert str(uri) == "wwpdb://deposit/D_8233000142/model.pdbx"
    
    def test_join_file(self):
        """Test join_file method"""
        dir_uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        file_uri = dir_uri.join_file("model", "pdbx", part_number=1, version="2")
        
        assert str(file_uri) == "wwpdb://deposit/D_8233000142/model.pdbx?part=1&version=2"
    
    def test_join_file_invalid(self):
        """Test join_file on non-directory URI"""
        file_uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/model.pdbx")
        
        with pytest.raises(ValueError, match="Can only join files to directory URIs"):
            file_uri.join_file("model", "pdbx")
    
    def test_getters(self):
        """Test getter methods"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=2&version=latest")
        
        assert uri.get_content_type() == "em-volume"
        assert uri.get_format() == "map"
        assert uri.get_part_number() == 2
        assert uri.get_version() == "latest"
    
    def test_is_file_uri(self):
        """Test is_file_uri method"""
        file_uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/model.pdbx")
        dir_uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        
        assert file_uri.is_file_uri() is True
        assert dir_uri.is_file_uri() is False


class TestFileNameBuilder:
    """Test FileNameBuilder class"""
    
    def test_build_filename_basic(self, filename_builder):
        """Test basic filename building"""
        filename = filename_builder.build_filename(
            dep_id="D_8233000142",
            content_type="model",
            format="pdbx",
            part_number=1,
            version="2"
        )
        
        assert filename == "D_8233000142_model_P1.cif.V2"
    
    def test_build_filename_mapped_content_type(self, filename_builder):
        """Test filename building with mapped content type"""
        filename = filename_builder.build_filename(
            dep_id="D_8233000142",
            content_type="nmr-chemical-shifts",
            format="nmr-star",
            part_number=1,
            version="1"
        )
        
        assert filename == "D_8233000142_cs_P1.str.V1"
    
    def test_build_filename_no_version(self, filename_builder):
        """Test filename building without version"""
        filename = filename_builder.build_filename(
            dep_id="D_8233000142",
            content_type="em-volume",
            format="map",
            part_number=1
        )
        
        assert filename == "D_8233000142_em-volume_P1.map"
    
    def test_build_filename_unknown_content_type(self, filename_builder):
        """Test error handling for unknown content type"""
        with pytest.raises(ValueError, match="Unknown content type: unknown"):
            filename_builder.build_filename(
                dep_id="D_8233000142",
                content_type="unknown",
                format="pdbx"
            )
    
    def test_build_filename_invalid_format_for_content_type(self, filename_builder):
        """Test error handling for invalid format/content type combination"""
        with pytest.raises(ValueError, match="Format 'map' not allowed for content type 'model'"):
            filename_builder.build_filename(
                dep_id="D_8233000142",
                content_type="model",
                format="map"
            )
    
    def test_build_filename_unknown_format(self, filename_builder):
        """Test error handling for unknown format"""
        with pytest.raises(ValueError, match="Format 'unknown' not allowed"):
            filename_builder.build_filename(
                dep_id="D_8233000142",
                content_type="model",
                format="unknown"
            )
    
    def test_parse_filename_basic(self, filename_builder):
        """Test basic filename parsing"""
        parsed = filename_builder.parse_filename("D_8233000142_model_P1.cif.V2")
        
        expected = {
            'dep_id': 'D_8233000142',
            'content_type': 'model',
            'format': 'pdbx',
            'part_number': 1,
            'version': '2'
        }
        
        assert parsed == expected
    
    def test_parse_filename_mapped_content_type(self, filename_builder):
        """Test parsing filename with mapped content type"""
        parsed = filename_builder.parse_filename("D_8233000142_cs_P1.str.V1")
        
        expected = {
            'dep_id': 'D_8233000142',
            'content_type': 'nmr-chemical-shifts',
            'format': 'nmr-star',
            'part_number': 1,
            'version': '1'
        }
        
        assert parsed == expected
    
    def test_parse_filename_no_version(self, filename_builder):
        """Test parsing filename without version"""
        parsed = filename_builder.parse_filename("D_8233000142_em-volume_P1.map")
        
        expected = {
            'dep_id': 'D_8233000142',
            'content_type': 'em-volume',
            'format': 'map',
            'part_number': 1,
            'version': None
        }
        
        assert parsed == expected
    
    def test_parse_filename_invalid_format(self, filename_builder):
        """Test error handling for invalid filename format"""
        with pytest.raises(ValueError, match="Invalid filename format"):
            filename_builder.parse_filename("invalid_filename")
    
    def test_parse_filename_unknown_content_type(self, filename_builder):
        """Test error handling for unknown filename content type"""
        with pytest.raises(ValueError, match="Unknown filename content type: unknown"):
            filename_builder.parse_filename("D_8233000142_unknown_P1.cif")
    
    def test_parse_filename_unknown_extension(self, filename_builder):
        """Test error handling for unknown file extension"""
        with pytest.raises(ValueError, match="Unknown file extension: xyz"):
            filename_builder.parse_filename("D_8233000142_model_P1.xyz")


class TestFilesystemBackend:
    """Test FilesystemBackend class"""
    
    def test_resolve_path_directory(self, filesystem_backend, temp_data_dir):
        """Test resolving directory URI to path"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        path = filesystem_backend._resolve_path(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142"
        assert path == expected
    
    def test_resolve_path_file(self, filesystem_backend, temp_data_dir):
        """Test resolving file URI to path"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2")
        path = filesystem_backend._resolve_path(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142" / "D_8233000142_em-volume_P1.map.V2"
        assert path == expected
    
    def test_resolve_path_file_latest_version(self, filesystem_backend, temp_data_dir):
        """Test resolving file URI with 'latest' version"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=latest")
        path = filesystem_backend._resolve_path(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142" / "D_8233000142_em-volume_P1.map.V3"
        assert path == expected
    
    def test_locate(self, filesystem_backend, temp_data_dir):
        """Test locate method"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2")
        path = filesystem_backend.locate(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142" / "D_8233000142_em-volume_P1.map.V2"
        assert path == expected
    
    def test_exists_true(self, filesystem_backend):
        """Test exists method returns True for existing resource"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        assert filesystem_backend.exists(uri) is True
    
    def test_exists_false(self, filesystem_backend):
        """Test exists method returns False for non-existing resource"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_9999999999/")
        assert filesystem_backend.exists(uri) is False
    
    def test_list_directory(self, filesystem_backend):
        """Test list_directory method"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        files = filesystem_backend.list_directory(uri)
        
        # Should find the mock files we created
        assert len(files) > 0
        
        # Check that all returned URIs are files
        for file_uri in files:
            assert file_uri.is_file_uri() is True
            assert file_uri.repository == "deposit"
            assert file_uri.dep_id == "D_8233000142"
    
    def test_list_directory_with_filters(self, filesystem_backend):
        """Test list_directory with filters"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        
        # Filter by content type
        em_files = filesystem_backend.list_directory(uri, content_type="em-volume")
        for file_uri in em_files:
            assert file_uri.get_content_type() == "em-volume"
        
        # Filter by format
        map_files = filesystem_backend.list_directory(uri, format="map")
        for file_uri in map_files:
            assert file_uri.get_format() == "map"
    
    def test_list_directory_invalid_uri(self, filesystem_backend):
        """Test list_directory with file URI raises error"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/model.pdbx")
        
        with pytest.raises(ValueError, match="URI must be a directory"):
            filesystem_backend.list_directory(uri)
    
    def test_list_directory_nonexistent(self, filesystem_backend):
        """Test list_directory returns empty list for non-existent directory"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_9999999999/")
        files = filesystem_backend.list_directory(uri)
        
        assert files == []
    
    def test_get_metadata_existing_file(self, filesystem_backend):
        """Test get_metadata for existing file"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=1")
        metadata = filesystem_backend.get_metadata(uri)
        
        assert 'size' in metadata
        assert 'modified' in metadata
        assert 'is_directory' in metadata
        assert metadata['is_directory'] is False
        assert metadata['size'] > 0
    
    def test_get_metadata_existing_directory(self, filesystem_backend):
        """Test get_metadata for existing directory"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        metadata = filesystem_backend.get_metadata(uri)
        
        assert 'size' in metadata
        assert 'modified' in metadata
        assert 'is_directory' in metadata
        assert metadata['is_directory'] is True
    
    def test_get_metadata_nonexistent(self, filesystem_backend):
        """Test get_metadata for non-existent resource"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_9999999999/nonexistent.pdbx")
        metadata = filesystem_backend.get_metadata(uri)
        
        assert metadata == {}


class TestWWPDBResourceManager:
    """Test WWPDBResourceManager class"""
    
    def test_init(self, filesystem_backend):
        """Test manager initialization"""
        manager = WWPDBResourceManager(filesystem_backend)
        
        assert manager.default_backend == filesystem_backend
        assert manager.backends == {}
    
    def test_register_backend(self, filesystem_backend):
        """Test registering a backend"""
        manager = WWPDBResourceManager(filesystem_backend)
        mock_backend = Mock()
        
        manager.register_backend("archive", mock_backend)
        
        assert manager.backends["archive"] == mock_backend
    
    def test_get_backend_default(self, filesystem_backend):
        """Test getting default backend"""
        manager = WWPDBResourceManager(filesystem_backend)
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        
        backend = manager._get_backend(uri)
        
        assert backend == filesystem_backend
    
    def test_get_backend_registered(self, filesystem_backend):
        """Test getting registered backend"""
        manager = WWPDBResourceManager(filesystem_backend)
        mock_backend = Mock()
        manager.register_backend("archive", mock_backend)
        
        uri = WwPDBResourceURI("wwpdb://archive/D_8233000142/")
        backend = manager._get_backend(uri)
        
        assert backend == mock_backend
    
    def test_exists_string_uri(self, filesystem_backend):
        """Test exists method with string URI"""
        manager = WWPDBResourceManager(filesystem_backend)
        
        result = manager.exists("wwpdb://deposit/D_8233000142/")
        
        assert result is True
    
    def test_exists_uri_object(self, filesystem_backend):
        """Test exists method with URI object"""
        manager = WWPDBResourceManager(filesystem_backend)
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        
        result = manager.exists(uri)
        
        assert result is True
    
    def test_list_directory_with_filters(self, filesystem_backend):
        """Test list_directory with filters"""
        manager = WWPDBResourceManager(filesystem_backend)
        
        files = manager.list_directory("wwpdb://deposit/D_8233000142/", content_type="em-volume")
        
        for file_uri in files:
            assert file_uri.get_content_type() == "em-volume"
    
    def test_get_metadata(self, filesystem_backend):
        """Test get_metadata method"""
        manager = WWPDBResourceManager(filesystem_backend)
        
        metadata = manager.get_metadata("wwpdb://deposit/D_8233000142/")
        
        assert 'is_directory' in metadata
        assert metadata['is_directory'] is True


class TestIntegrationScenarios:
    """Integration tests for specific scenarios"""
    
    def test_repository_directory_resolution(self, filesystem_backend, temp_data_dir):
        """Test: wwpdb://repository/dep_id/ must resolve to /data/repository/dep_id"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/")
        path = filesystem_backend.locate(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142"
        assert path == expected
        assert filesystem_backend.exists(uri) is True
    
    def test_file_with_version_resolution(self, filesystem_backend, temp_data_dir):
        """Test: wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2 resolves correctly"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=2")
        path = filesystem_backend.locate(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142" / "D_8233000142_em-volume_P1.map.V2"
        assert path == expected
    
    def test_file_with_latest_version_resolution(self, filesystem_backend, temp_data_dir):
        """Test: wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=latest resolves to highest version"""
        uri = WwPDBResourceURI("wwpdb://deposit/D_8233000142/em-volume.map?part=1&version=latest")
        path = filesystem_backend.locate(uri)
        
        expected = Path(temp_data_dir) / "deposit" / "D_8233000142" / "D_8233000142_em-volume_P1.map.V3"
        assert path == expected
    
    def test_end_to_end_workflow(self, filesystem_backend):
        """Test complete workflow from URI creation to file operations"""
        # Create directory URI
        dir_uri = WwPDBResourceURI.for_directory("deposit", "D_8233000142")
        assert filesystem_backend.exists(dir_uri) is True
        
        # List files in directory
        files = filesystem_backend.list_directory(dir_uri)
        assert len(files) > 0
        
        # Create file URI
        file_uri = dir_uri.join_file("em-volume", "map", part_number=1, version="1")
        assert filesystem_backend.exists(file_uri) is True
        
        # Get metadata
        metadata = filesystem_backend.get_metadata(file_uri)
        assert metadata['is_directory'] is False
        assert metadata['size'] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])