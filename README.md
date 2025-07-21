# dbtlist

A tool that extends dbt-core's list functionality to support manifest file comparison for state selectors.

## Installation

```bash
pip install -e .
```

## Usage

```bash
# List models modified between two manifest files
dbtlist -s "state:modified+" old_manifest.json new_manifest.json

# List modified models and their downstream dependencies
dbtlist -s "state:modified+" old_manifest.json new_manifest.json

# Combine with other selectors
dbtlist -s "state:modified+" --exclude "resource_type:test" old_manifest.json new_manifest.json

# Filter by package
dbtlist -s "state:modified+" --packages "my_package" old_manifest.json new_manifest.json
```

## Features

- Uses dbt-core's built-in selector functionality
- Supports all standard dbt selectors (tag:, resource_type:, etc.)
- Adds state:modified and state:modified+ for manifest comparison
- Compatible with dbt manifest.json files

## Arguments

- `old_manifest`: Path to the old/reference manifest.json file
- `new_manifest`: Path to the new/current manifest.json file

## Options

- `-s, --select`: Selector string (supports all dbt selector syntax)
- `--exclude`: Exclude selector string  
- `--packages`: Comma-separated list of package names to filter