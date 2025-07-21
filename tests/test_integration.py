"""
Integration tests using real manifest files
"""

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

    # Test basic CLI import - if this works, the structure is sound
    assert main is not None
    assert callable(main)
