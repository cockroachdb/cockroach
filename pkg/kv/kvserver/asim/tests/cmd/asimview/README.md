# ASIM Test Results Viewer

Interactive web viewer for ASIM test JSON output files with fuzzy search and multi-file comparison.

## Usage

```bash
# Default: serves files from repo's testdata/generated directory
go run .

# Or specify a custom directory
go run . /path/to/json/files

# Custom port
go run . -port 8081
```

Then open http://localhost:8080 in your browser.

## Features

- **Fuzzy Search**: Type any part of test name or file name to filter
- **Multiple Selection**: Select multiple test files to compare side-by-side
- **Synchronized Zoom**: Drag to zoom on any chart, all charts sync automatically
- **Copy Data**: Click the clipboard button on any chart to copy its timeseries data as JSON
- **Auto-discovery**: Recursively finds all JSON files in the specified directory
- **Local Storage**: Remembers your last selection and zoom state. (Can refresh the browser window to update loaded files).
