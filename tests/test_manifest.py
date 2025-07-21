"""
Tests for manifest functionality
"""

import pytest
import json
import tempfile
from pathlib import Path

from dbtlist.manifest import ManifestReader, ManifestComparator


def create_test_manifest(nodes_data):
    """Helper to create a test manifest file"""
    manifest_data = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.5.0",
            "generated_at": "2023-01-01T00:00:00Z",
            "invocation_id": "test-id",
        },
        "nodes": nodes_data,
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "selectors": {},
        "disabled": {}
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(manifest_data, f)
        return f.name


def test_manifest_reader():
    """Test basic manifest reading functionality"""
    nodes = {
        "model.test.model1": {
            "name": "model1",
            "resource_type": "model",
            "package_name": "test",
            "checksum": {"name": "sha256", "checksum": "abc123"},
            "raw_code": "select 1 as id",
            "depends_on": {"nodes": []}
        }
    }
    
    manifest_path = create_test_manifest(nodes)
    
    try:
        reader = ManifestReader(manifest_path)
        manifest = reader.load()
        
        assert len(manifest.nodes) == 1
        assert "model.test.model1" in manifest.nodes
        
        node_names = reader.get_node_names()
        assert node_names == {"model.test.model1"}
        
    finally:
        Path(manifest_path).unlink()


def test_manifest_comparator():
    """Test manifest comparison functionality"""
    old_nodes = {
        "model.test.model1": {
            "name": "model1",
            "resource_type": "model", 
            "package_name": "test",
            "checksum": {"name": "sha256", "checksum": "abc123"},
            "raw_code": "select 1 as id",
            "depends_on": {"nodes": []}
        }
    }
    
    new_nodes = {
        "model.test.model1": {
            "name": "model1",
            "resource_type": "model",
            "package_name": "test", 
            "checksum": {"name": "sha256", "checksum": "xyz789"},
            "raw_code": "select 2 as id",
            "depends_on": {"nodes": []}
        },
        "model.test.model2": {
            "name": "model2",
            "resource_type": "model",
            "package_name": "test",
            "checksum": {"name": "sha256", "checksum": "def456"},
            "raw_code": "select 3 as id", 
            "depends_on": {"nodes": []}
        }
    }
    
    old_manifest_path = create_test_manifest(old_nodes)
    new_manifest_path = create_test_manifest(new_nodes)
    
    try:
        old_reader = ManifestReader(old_manifest_path)
        new_reader = ManifestReader(new_manifest_path)
        comparator = ManifestComparator(old_reader, new_reader)
        
        modified = comparator.get_modified_nodes()
        assert "model.test.model1" in modified
        
        added = comparator.get_added_nodes()
        assert "model.test.model2" in added
        
    finally:
        Path(old_manifest_path).unlink()
        Path(new_manifest_path).unlink()