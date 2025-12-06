# ibazel Development Setup for CockroachDB

This setup enables automatic rebuilding and restarting of a CockroachDB single-node server whenever source files change.

**Performance Note:** This configuration uses the `cockroach-short` build (without embedded UI assets) for significantly faster build times during development cycles.

## Prerequisites

Install ibazel if you haven't already:
```bash
# macOS
brew install ibazel

# Or using Go
go install github.com/bazelbuild/bazel-watcher/cmd/ibazel@latest
```

## Usage

From the root of the CockroachDB repository, run:

```bash
ibazel run //build/dev:cockroach-single-node
```

This will:
1. Build the cockroach-short binary (faster, without embedded UI)
2. Start a single-node CockroachDB server with `--insecure` flag
3. Watch for source file changes
4. Automatically rebuild and restart the server when changes are detected

## Server Details

- **SQL Port**: `localhost:26257`
- **Web UI**: `http://localhost:8080`
- **Data Directory**: `/tmp/cockroach-single-node-dev` (cleaned on each restart)
- **Logs**: Output to console (stderr)

## Connecting to the Server

Once running, you can connect using:

```bash
# Using cockroach sql client
./cockroach sql --insecure --host=localhost:26257

# Using psql
psql -h localhost -p 26257 -U root -d defaultdb
```

## Configuration

The setup includes:
- `.ibazelrc` - ibazel configuration with a 500ms debounce delay
- `build/dev/BUILD.bazel` - Bazel target definition
- `build/dev/cockroach-single-node.sh` - Server startup script

## Tips

1. The server data is cleared on each restart for a clean state
2. Use Ctrl+C to stop ibazel and the server
3. The server runs with INFO level logging to the console
4. For persistent data, modify the DATA_DIR in the shell script

## Troubleshooting

If ibazel isn't rebuilding on file changes:
- Check that the file you're editing is a dependency of the cockroach binary
- Ensure ibazel has proper file watching permissions
- Try increasing the debounce_delay in `.ibazelrc` if builds are triggering too frequently