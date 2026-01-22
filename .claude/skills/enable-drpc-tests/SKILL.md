---
name: enable-drpc-tests
description: Enable or disable DRPC for tests, either randomly at the package level or explicitly at the test level.
---

# Enable DRPC Tests

This skill helps enable or disable DRPC while running tests, either randomly at the package level or explicitly for individual tests.

## Enable DRPC Randomly at the Package Level

DRPC can be enabled randomly for any package that:
1. Contains a `main_test.go` file, and
2. Initializes a test server.

### Recommended Pattern (Preferred)

```go
serverutils.InitTestServerFactory(
    server.TestServerFactory,
        serverutils.WithDRPCOption(base.TestDRPCEnabledRandomly),
)
```

### Alternative Pattern

```go
serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
serverutils.TestingGlobalDRPCOption(base.TestDRPCEnabledRandomly)
```

## Disable DRPC at the Package Level

DRPC can also be disabled at the package level using the same `main_test.go` setup.

### Pattern 1

```go
serverutils.InitTestServerFactory(
    server.TestServerFactory,
        serverutils.WithDRPCOption(base.TestDRPCDisabled),
)
```

### Pattern 2

```go
serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
serverutils.TestingGlobalDRPCOption(base.TestDRPCDisabled)
```

## Verify All Tests in the Package

To validate correctness, all tests in the package must pass with DRPC enabled.

### Requirements
- Set `COCKROACH_TEST_DRPC=true`
- Run tests for all tenant modes:
  - shared
  - external
- Run tests multiple times to ensure stability (recommended: 10 runs)

### Commands

```bash
./dev test <pkg> --count=10 --ignore-cache -- --test_env="COCKROACH_TEST_DRPC=true"

./dev test <pkg> --count=10 --ignore-cache -- \
  --test_env="COCKROACH_TEST_DRPC=true" \
  --test_env="COCKROACH_TEST_TENANT=shared"

./dev test <pkg> --count=10 --ignore-cache -- \
  --test_env="COCKROACH_TEST_DRPC=true" \
  --test_env="COCKROACH_TEST_TENANT=external"
```

## Enable or Disable DRPC for a Specific Test

DRPC behavior can be overridden at the test level using `ServerArgs`. Using `ServerArgsPerNode` will not work for this purpose.

### Pattern 1

```go
ServerArgs: base.TestServerArgs{
    DefaultDRPCOption: base.TestDRPCDisabled,
},
```

### Pattern 2

```go
base.TestClusterArgs{
    ServerArgs: base.TestServerArgs{
        DefaultDRPCOption: base.TestDRPCDisabled,
    },
}
```

## Recommended Workflow for Enabling DRPC Randomly

1. Ensure the package has a `main_test.go` that initializes a test server.
2. Enable DRPC randomly at the package level.
3. Format the file modified using `crlfmt` tool.
3. Run and verify all tests in the package.
4. If any test fails:
    a. Disable DRPC for the failing test specifically.
    b. Re-run and verify all tests.
5. Repeat until all tests pass consistently.