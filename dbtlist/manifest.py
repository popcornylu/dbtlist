"""
Manifest file handling and comparison functionality
"""

import json
from typing import Dict, Set, Any, Optional
from pathlib import Path

from dbt.contracts.graph.manifest import Manifest


class ManifestReader:
    """Handle reading and processing dbt manifest files"""

    def __init__(self, manifest_path: str):
        self.manifest_path = Path(manifest_path)
        self._manifest: Optional[Manifest] = None

    def load(self) -> Manifest:
        """Load manifest from file"""
        if self._manifest is None:
            with open(self.manifest_path, "r") as f:
                manifest_data = json.load(f)
            self._manifest = Manifest.from_dict(manifest_data)
        return self._manifest

    @property
    def manifest(self) -> Manifest:
        """Get loaded manifest"""
        if self._manifest is None:
            self.load()
        return self._manifest

    def get_nodes(self) -> Dict[str, Any]:
        """Get all nodes from manifest"""
        return self.manifest.nodes

    def get_node_names(self) -> Set[str]:
        """Get set of all node names"""
        return set(self.manifest.nodes.keys())


class ManifestComparator:
    """Compare two manifest files to find differences"""

    def __init__(self, old_manifest: ManifestReader, new_manifest: ManifestReader):
        self.old_manifest = old_manifest
        self.new_manifest = new_manifest

    def get_modified_nodes(self) -> Set[str]:
        """Get nodes that have been modified between manifests"""
        old_nodes = self.old_manifest.get_nodes()
        new_nodes = self.new_manifest.get_nodes()

        modified = set()

        for node_name in new_nodes.keys():
            if node_name not in old_nodes:
                modified.add(node_name)
            elif self._is_node_modified(old_nodes[node_name], new_nodes[node_name]):
                modified.add(node_name)

        return modified

    def _is_node_modified(self, old_node: Any, new_node: Any) -> bool:
        """Check if a node has been modified"""
        if old_node.checksum != new_node.checksum:
            return True

        if old_node.raw_code != new_node.raw_code:
            return True

        return False

    def get_added_nodes(self) -> Set[str]:
        """Get nodes that were added in new manifest"""
        old_node_names = self.old_manifest.get_node_names()
        new_node_names = self.new_manifest.get_node_names()
        return new_node_names - old_node_names

    def get_removed_nodes(self) -> Set[str]:
        """Get nodes that were removed from old manifest"""
        old_node_names = self.old_manifest.get_node_names()
        new_node_names = self.new_manifest.get_node_names()
        return old_node_names - new_node_names
