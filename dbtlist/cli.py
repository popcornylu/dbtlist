"""
CLI interface for dbtlist tool
"""

import click
from pathlib import Path
from typing import Optional

from .manifest import ManifestReader, ManifestComparator
from .selector import DbtSelector


@click.command()
@click.option(
    "-s", "--select",
    help="Selector string (e.g., 'state:modified+', 'tag:my_tag')"
)
@click.option(
    "--exclude",
    help="Exclude selector string"
)
@click.option(
    "--packages",
    help="Comma-separated list of package names to filter"
)
@click.argument("old_manifest", type=click.Path(exists=True))
@click.argument("new_manifest", type=click.Path(exists=True))
def main(
    select: Optional[str],
    exclude: Optional[str], 
    packages: Optional[str],
    old_manifest: str,
    new_manifest: str
):
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
            select=select,
            exclude=exclude,
            packages=package_list
        )
        
        # Output results in dbt list format
        for node_unique_id in sorted(selected_nodes):
            # Convert unique_id to dbt list format
            # e.g., "model.jaffle_shop.customers" -> "jaffle_shop.customers"
            # e.g., "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1" -> "jaffle_shop.unique_customers_customer_id"
            
            node = new_reader.manifest.nodes.get(node_unique_id)
            if node:
                # Build the output name following dbt list format
                parts = [node.package_name]
                
                # Handle schema based on dbt conventions
                if hasattr(node, 'schema') and node.schema:
                    schema = node.schema
                    # Only add schema if it's not the default package schema
                    # For staging models, use 'staging' instead of full schema
                    if 'staging' in schema.lower() or 'stg' in node.name:
                        parts.append('staging')
                    elif schema != node.package_name and 'dev' not in schema and 'test' not in schema:
                        parts.append(schema)
                
                # Add the node name
                parts.append(node.name)
                
                output_name = ".".join(parts)
            else:
                # Fallback: extract from unique_id
                parts = node_unique_id.split(".")
                if len(parts) >= 3:
                    output_name = ".".join(parts[1:])  # Skip resource_type prefix
                else:
                    output_name = node_unique_id
            
            click.echo(output_name)
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    main()