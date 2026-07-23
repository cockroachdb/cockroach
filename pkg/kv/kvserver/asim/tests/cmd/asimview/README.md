# ASIM Test Results Viewer

Interactive web viewer for ASIM test JSON output files with fuzzy search and multi-file comparison.

## Usage

### Regular File Viewer Mode

```bash
# Default: serves files from repo's testdata/generated directory
go run .

# Or specify a custom directory
go run . /path/to/json/files

# Custom port
go run . -port 8081
```

Then open http://localhost:8080 in your browser.

### SHA Comparison Mode

```bash
# Enable SHA comparison mode
go run . -sha-compare

# With custom port
go run . -sha-compare -port 8081
```

Then open http://localhost:8080 in your browser. This mode allows you to:

1. **Generate test data for different Git SHAs**: Enter two commit SHAs and click "Generate Comparison" to run ASIM tests for both commits
2. **Compare results side-by-side**: Select test files to see JSON data plotted side-by-side for visual comparison
3. **Automatic Git workflow**: The tool handles switching between SHAs, running tests, and storing results automatically

**Note**: SHA comparison mode requires a clean Git working directory and will temporarily switch between commits to generate test data.

## Features

- **Fuzzy Search**: Type any part of test name or file name to filter
- **Multiple Selection**: Select multiple test files to compare side-by-side
- **Synchronized Zoom**: Drag to zoom on any chart, all charts sync automatically
- **Copy Data**: Click the clipboard button on any chart to copy its timeseries data as JSON
- **Auto-discovery**: Recursively finds all JSON files in the specified directory
- **Local Storage**: Remembers your last selection and zoom state. (Can refresh the browser window to update loaded files).
