"""
CLI interface for dbtlist tool
"""

import click
from typing import Optional

from .manifest import ManifestReader, ManifestComparator
from .selector import DbtSelector


@click.command()
@click.option(
    "-s", "--select", help="Selector string (e.g., 'state:modified+', 'tag:my_tag')"
)
@click.option("--exclude", help="Exclude selector string")
@click.option("--packages", help="Comma-separated list of package names to filter")
@click.argument("old_manifest", type=click.Path(exists=True))
@click.argument("new_manifest", type=click.Path(exists=True))
def main(
    select: Optional[str],
    exclude: Optional[str],
    packages: Optional[str],
    old_manifest: str,
    new_manifest: str,
) -> None:
    """
    dbtlist - Compare two dbt manifest files and list selected nodes

    Usage:
        dbtlist -s "state:modified+" manifest1.json manifest2.json
        dbtlist -s "tag:staging" --exclude "resource_type:test" old.json new.json
    """
    try:
        # Load manifests
        old_reader = ManifestReader(old_manifest)
        new_reader = ManifestReader(new_manifest)

        # Create comparator for state selectors
        comparator = ManifestComparator(old_reader, new_reader)

        # Create selector with new manifest as base
        selector = DbtSelector(new_reader.manifest, comparator)

        # Parse packages if provided
        package_list = None
        if packages:
            package_list = [pkg.strip() for pkg in packages.split(",")]

        # Select nodes
        selected_nodes = selector.select_nodes(
            select=select, exclude=exclude, packages=package_list
        )

        # Output results as unique IDs
        for node_unique_id in sorted(selected_nodes):
            click.echo(node_unique_id)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    main()
