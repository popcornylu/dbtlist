"""
Selector handling using dbt-core built-in functionality
"""

from typing import Set, Optional, List
from dbt.contracts.graph.manifest import Manifest

from .manifest import ManifestComparator


class DbtSelector:
    """Use dbt-core's built-in selector functionality"""

    def __init__(self, manifest: Manifest, comparator: ManifestComparator = None):
        self.manifest = manifest
        self.comparator = comparator

    def select_nodes(
        self,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        packages: Optional[List[str]] = None,
    ) -> Set[str]:
        """Select nodes using basic selector syntax"""

        # Handle state selectors
        if select and "state:modified" in select:
            if not self.comparator:
                raise ValueError("State selector requires manifest comparison")

            modified_nodes = self.comparator.get_modified_nodes()

            if "state:modified+" in select:
                # Include modified nodes and their downstream dependencies
                selected_nodes = set(modified_nodes)
                selected_nodes.update(self._get_downstream_nodes(modified_nodes))
            else:
                # Just the modified nodes
                selected_nodes = modified_nodes

        elif select:
            # Apply basic selector
            selected_nodes = self._apply_selector(select)
        else:
            # No selector - return all nodes
            selected_nodes = set(self.manifest.nodes.keys())

        # Apply exclusions
        if exclude:
            excluded = self._apply_selector(exclude)
            selected_nodes -= excluded

        # Filter by packages
        if packages:
            filtered_nodes = set()
            for node_name in selected_nodes:
                node = self.manifest.nodes.get(node_name)
                if node and node.package_name in packages:
                    filtered_nodes.add(node_name)
            selected_nodes = filtered_nodes

        return selected_nodes

    def _apply_selector(self, selector: str) -> Set[str]:
        """Apply a basic selector and return matching nodes"""
        selected = set()

        if selector.startswith("resource_type:"):
            resource_type = selector.split(":", 1)[1]
            for node_id, node in self.manifest.nodes.items():
                if node.resource_type == resource_type:
                    selected.add(node_id)

        elif selector.startswith("tag:"):
            tag = selector.split(":", 1)[1]
            for node_id, node in self.manifest.nodes.items():
                if hasattr(node, "tags") and tag in node.tags:
                    selected.add(node_id)

        elif "+" in selector and not selector.startswith("state:"):
            # Handle downstream expansion for non-state selectors
            base_selector = selector.replace("+", "")
            base_nodes = self._apply_selector(base_selector)
            selected.update(base_nodes)
            selected.update(self._get_downstream_nodes(base_nodes))

        else:
            # Simple name matching
            for node_id in self.manifest.nodes:
                node_name = node_id.split(".")[-1]
                if selector in node_name:
                    selected.add(node_id)

        return selected

    def _get_downstream_nodes(self, base_nodes: Set[str]) -> Set[str]:
        """Get all downstream nodes from a set of base nodes"""
        downstream = set()

        for node_id, node in self.manifest.nodes.items():
            if hasattr(node, "depends_on") and node.depends_on:
                # Safely access depends_on.nodes
                try:
                    dep_nodes = getattr(node.depends_on, "nodes", [])
                    if dep_nodes:
                        for dep in dep_nodes:
                            if dep in base_nodes:
                                downstream.add(node_id)
                                break
                except AttributeError:
                    # Skip if depends_on doesn't have the expected structure
                    continue

        # Recursively get downstream of downstream nodes
        if downstream:
            further_downstream = self._get_downstream_nodes(downstream)
            downstream.update(further_downstream)

        return downstream
