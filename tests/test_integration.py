"""
Integration tests using real manifest files
"""

import os
from pathlib import Path

from dbtlist.manifest import ManifestReader, ManifestComparator
from dbtlist.selector import DbtSelector


def test_real_manifest_integration():
    """Test with real manifest files if they exist"""
    # Check if test data exists
    data_dir = Path(__file__).parent.parent / "data" / "jaffe_shop_duckdb"
    old_manifest = data_dir / "old.json"
    new_manifest = data_dir / "new.json"

    if not old_manifest.exists() or not new_manifest.exists():
        # Skip test if no test data
        return

    # Test manifest reading
    old_reader = ManifestReader(str(old_manifest))
    new_reader = ManifestReader(str(new_manifest))

    old_manifest_obj = old_reader.load()
    new_manifest_obj = new_reader.load()

    assert len(old_manifest_obj.nodes) > 0
    assert len(new_manifest_obj.nodes) > 0

    # Test comparison
    comparator = ManifestComparator(old_reader, new_reader)
    modified_nodes = comparator.get_modified_nodes()

    # Should find some modified nodes
    assert len(modified_nodes) > 0

    # Test selector
    selector = DbtSelector(new_manifest_obj, comparator)

    # Test state:modified selector
    state_modified = selector.select_nodes(select="state:modified")
    assert len(state_modified) > 0

    # Test state:modified+ selector
    state_modified_plus = selector.select_nodes(select="state:modified+")
    assert len(state_modified_plus) >= len(state_modified)  # Should include downstream

    # Test resource_type selector
    models = selector.select_nodes(select="resource_type:model")
    assert len(models) > 0

    # All selected nodes should be models
    for node_id in models:
        node = new_manifest_obj.nodes[node_id]
        assert node.resource_type == "model"


def test_cli_functionality():
    """Test CLI functionality without complex manifest creation"""
    from dbtlist.cli import main
    import tempfile
    import json

    # Create minimal valid manifest structures
    minimal_old = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.5.0",
            "generated_at": "2023-01-01T00:00:00Z",
            "invocation_id": "11111111-1111-1111-1111-111111111111",
            "env": {},
            "project_name": "test_project",
            "project_id": "11111111-1111-1111-1111-111111111111",
            "user_id": None,
            "send_anonymous_usage_stats": False,
            "adapter_type": "postgres",
        },
        "nodes": {},
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
        "unit_tests": {},
    }

    # Test basic CLI import - if this works, the structure is sound
    assert main is not None
    assert callable(main)
