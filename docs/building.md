# Building and developing CockroachDB

At least 4GB of RAM is required to build from source and run tests.

## Prerequisites

**macOS:** Install XCode (full installation, not just command-line tools), then:

```bash
brew install autoconf cmake bazelisk make gpatch
```

**Linux (Debian/Ubuntu):**

```bash
sudo apt install build-essential cmake autoconf bison patch libncurses-dev
```

Install [Bazelisk](https://github.com/bazelbuild/bazelisk) separately on Linux.

Install Go matching the version in `go.mod` (not strictly required for Bazel builds, but needed for IDE development).

**Docker note:** CRL employees must not use Docker Desktop (restricted software). Use [Rancher Desktop](https://rancherdesktop.io/) or [Podman](https://podman.io/) instead.

## First steps

Run `./dev doctor` before attempting any build -- it checks your environment and tells you how to fix problems.

## The `dev` tool

`dev` is a light wrapper around Bazel whose source lives in `pkg/cmd/dev`. The top-level `./dev` script builds and runs the version matching your checked-out commit. It supplements `bazel` by providing aliases for common targets and copying binaries out of Bazel's output tree into your workspace. You can always pass extra flags to the underlying `bazel` invocation after `--`.

## Building

| Command | What it does |
|---------|-------------|
| `./dev build short` | Build cockroach without DB Console (faster) |
| `./dev build` | Build full cockroach binary (includes JS/UI) |
| `./dev build crlfmt` | Build crlfmt formatter |
| `./dev build roachprod` | Build roachprod |
| `./dev build pkg/util/log` | Compile a package (useful as a check) |
| `./dev build pkg/util/log:log_test` | Compile tests for a package |

The built `cockroach` binary is staged at `./cockroach` in the repo root.

## Testing

| Command | What it does |
|---------|-------------|
| `./dev test pkg/sql` | Run all tests in a package |
| `./dev test pkg/sql -f=TestParse -v` | Run matching tests with verbose output |
| `./dev test pkg/sql --count=5` | Run tests multiple times |
| `./dev test pkg/sql --stress` | Run tests under stress |
| `./dev test pkg/sql --race` | Run with race detector |
| `./dev testlogic --files=F --subtests=S --config=C` | Run logic tests |

**Tips:**
- Always use `-v` with `-f` so you see `testing: warning: no tests to run` if your filter matches nothing.
- Add `-v -- --test_output streamed` to print results as tests run (reduces parallelism).
- To debug a hung test: append `-- -c dbg` to disable symbol stripping for `dlv`.

## Code generation

| Command | When to use |
|---------|-------------|
| `./dev generate bazel` | After adding/removing files or changing imports. Use `--mirror` when vendoring new deps. |
| `./dev generate protobuf` | After editing `.proto` files (relatively fast) |
| `./dev generate go` | Regenerate all Go code (protobuf, cgo stubs, etc.) |
| `./dev generate` | Regenerate everything (slow) |

Set `ALWAYS_RUN_GAZELLE=1` in your shell to auto-run gazelle before every `dev test` or `dev build` (adds a small delay).

## Cross-compilation

```bash
./dev build --cross          # default: linux
./dev build --cross=windows  # also: macos, linuxarm, macosarm
```

Requires a Docker-compatible runtime (Rancher Desktop, Podman, etc.). Outputs go to the `artifacts` directory.

## IDE / non-Bazel development (escape hatch)

Generated code is not checked into the repo, so IDEs need it produced locally. Run `./dev gen go` to generate all `.go` files (protobuf, cgo stubs, etc.) into the worktree. If your IDE still complains, try the slower `./dev gen` to regenerate everything. This escape hatch is explicitly supported -- file a bug if tests pass under Bazel but fail under `go test` after generating.

## Local dependency overrides

To iterate on a dependency without updating commits in `DEPS.bzl`, use `--override_repository` in `.bazelrc.user`:

```bash
echo 'build --override_repository=com_github_google_btree=/path/to/local/btree' >> .bazelrc.user
```

The repo name (e.g. `com_github_google_btree`) matches the `name` field in the `go_repository` rule in `DEPS.bzl`. The local clone needs a `WORKSPACE` file (can be empty) and `BUILD.bazel` files (generate with `gazelle -go_prefix=<import-path> -repo_root=.`). Remove the override line when done.

For a simpler approach (no local clone), you can edit the `go_repository` in `DEPS.bzl` directly to point to a custom `remote` and `commit`. See the comment at the top of `DEPS.bzl`.

## Bazel troubleshooting

**Missing proto dependencies:** Gazelle cannot detect dependencies introduced by `gogoproto.customtype` annotations in `.proto` files. You must manually add them to the `go_proto_library` in `BUILD.bazel` with a `# keep` comment so Gazelle does not remove them:

```python
go_proto_library(
    name = "execinfrapb_go_proto",
    deps = [
        "//pkg/ccl/streamingccl",  # keep
    ],
)
```

**`go:generate` directives:** New `go:generate` calls are not run inside the Bazel sandbox. If you add one, ensure equivalent generation logic exists in the Bazel build.

**`broken_in_bazel` tag:** If a test cannot practically support Bazel sandboxing in the same PR, tag it `broken_in_bazel` and follow up with the dev-infra team.
