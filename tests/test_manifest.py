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
    # Create more complete node structures
    complete_nodes = {}
    for node_id, node_data in nodes_data.items():
        complete_nodes[node_id] = {
            "alias": node_data["name"],
            "checksum": node_data["checksum"],
            "config": {},
            "database": None,
            "schema": "test_schema",
            "name": node_data["name"],
            "resource_type": node_data["resource_type"],
            "package_name": node_data["package_name"],
            "path": f"{node_data['name']}.sql",
            "original_file_path": f"models/{node_data['name']}.sql",
            "unique_id": node_id,
            "fqn": ["test", node_data["name"]],
            "raw_code": node_data["raw_code"],
            "refs": [],
            "sources": [],
            "metrics": [],
            "depends_on": node_data["depends_on"],
            "compiled_path": None,
            "tags": [],
            "meta": {},
            "docs": {"show": True, "node_color": None},
            "patch_path": None,
            "build_path": None,
            "deferred": False,
            "unrendered_config": {},
            "created_at": 1234567890.0,
            "relation_name": f"test_db.test_schema.{node_data['name']}",
            "language": "sql",
            "description": "",
            "columns": {},
            "group": None,
            "constraints": [],
            "version": None,
            "latest_version": None,
            "access": "protected",
            "contract": {"enforced": False, "alias_types": True, "checksum": None}
        }
    
    manifest_data = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.5.0",
            "generated_at": "2023-01-01T00:00:00Z",
            "invocation_id": "test-id",
            "env": {},
            "project_name": "test_project",
            "project_id": "test-project-id",
            "user_id": "test-user",
            "send_anonymous_usage_stats": False,
            "adapter_type": "postgres"
        },
        "nodes": complete_nodes,
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": {},
        "child_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {}
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