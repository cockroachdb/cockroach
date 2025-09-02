# ASIM Test Data Viewer

The viewer for ASIM test results has moved to a standalone tool with enhanced features.

## Using the New Viewer

```bash
# From the repository root:
cd pkg/kv/kvserver/asim/tests/cmd/asimview
go run .

# Then open http://localhost:8080 in your browser
```

## Features

The new viewer provides:
- **Fuzzy search** - Quickly find test files by typing any part of the name
- **Multiple file selection** - Compare multiple test runs side-by-side
- **Auto-discovery** - Automatically finds all JSON files in the generated/ directory
- **Synchronized zoom** - Zoom actions apply to all charts for easy comparison
- **Copy data** - Export timeseries data from any chart as JSON

## Custom Directory or Port

```bash
# Use a different directory
go run . /path/to/json/files

# Use a different port
go run . -port 8081
```

For more details, see the [asimview README](../cmd/asimview/README.md).