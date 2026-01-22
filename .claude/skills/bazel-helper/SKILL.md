---
name: bazel-helper
description: Diagnose and resolve common Bazel build and test issues.
---

# Bazel Helper
This skill provides standard recovery steps for resolving Bazel build and test failures caused by stale state or server conflicts.

## Stop the Bazel Server
Shut down the Bazel server to clear in-memory state and avoid conflicts during cleanup.

```bash
bazel shutdown
```

## Clean the Bazel Output Base
Remove generated build artifacts and cached state.

### Standard Clean
Removes most generated files.

```bash
bazel clean
```

### Full Clean (Expunge)
Forces deletion of the entire output base, including:

- Action cache
- Content-addressable storage
- Persistent Bazel state

Use this when standard cleaning does not resolve issues.

```bash
bazel clean --expunge
```

## Recommended Recovery Workflow
- Stop the Bazel server.
- Run a standard clean.
- Retry the build or tests.
- If issues persist, run a full clean with --expunge and retry.