# DB Console

This directory contains the client-side code for CockroachDB's web-based DB
Console, which provides details about a cluster's performance and health. See the
[DB Console docs](https://www.cockroachlabs.com/docs/stable/ui-overview.html)
for an expanded overview.

## Getting Started

To start developing the UI, be sure you're able to build and run a CockroachDB
node. Instructions for this are located in the top-level README. Every Cockroach
node serves the UI, by default on port 8080, but you can customize the port with
the `--http-port` flag. If you've started a node with the default options,
you'll be able to access the UI at <http://localhost:8080>. If you've started
a node using `demo`, the default port is 8081 and you'll be able to access the UI
at <http://localhost:8081>.

Immdiately upon cloning this repo, your editor may report errors from
[eslint](https://eslint.org/) claiming that `@cockroachlabs/eslint-plugin-crdb`
failed to load. Solve this issue by running the linter once to build the
CRDB-specific plugin, which lives in this repo:

```shell
$ ./dev gen js
```

or by building the plugin manually:
```shell
$ pushd pkg/ui/workspaces/eslint-plugin-crdb; pnpm install && pnpm build; popd
```

Behind the scenes, our UI is compiled using a collection of tools that depends on
[Node.js](https://nodejs.org/) and are managed with
[pnpm](https://pnpm.io), a package manager that offers more deterministic
package installation than NPM. LTS versions of NodeJS (16.x) and pnpm (8.6.x)
are known to work. [Chrome](https://www.google.com/chrome/), Google's internet
browser. Unit tests are run using Chrome's "Headless" mode.

## Developing

When making changes to the UI, it is desirable to see those changes with data
from an existing cluster without rebuilding and relaunching the cluster for each
change. This is useful for rapidly visualizing local development changes against
a consistent and realistic dataset.

We've created a simple NodeJS proxy to accomplish this. This server serves all
requests for web resources (JavaScript, HTML, CSS) out of the code in this
directory, while proxying all API requests to the specified CockroachDB node.

To use this proxy, in Cockroach's root directory run:
```shell
$ ./dev ui watch --db=<target-cluster-http-uri>
```

then navigate to `http://localhost:3000` to access the UI.

To proxy to a cluster started up in secure mode, in Cockroach's root directory run:
```shell
$ ./dev ui watch --db=<target-cluster-http-uri> --secure
```

While the proxy is running, any changes you make in the `src` directory will
trigger an automatic recompilation of the UI. This recompilation should be much
faster than a cold compile—usually less than one second—as Webpack can reuse
in-memory compilation artifacts from the last compile.

Note that calling `./dev` above will hold the bazel lock, but only for the duration of the
initial build. The watcher itself releases the bazel lock, so it's perfectly reasonable to
run `./dev ui watch` in one shell and `./dev build` in another.


### Working with the `cluster-ui` dependency

Many page-level components have been extracted into a
separate repository for sharing with other applications.
You can read all about this division in the [README for the
package](https://github.com/cockroachdb/cockroach/blob/master/pkg/ui/workspaces/cluster-ui/README.md)
which describes a dev workflow that fits well with this package.

### Clearing the local cache
If the UI cache becomes corrupted, clear it with:
```shell
$ ./dev ui clean --all
$ make ui-maintainer-clean  # Deprecation soon.
```

If all else fails, run
```shell
bazel clean --expunge
```
though be warned your next build will take a while.

## CCL Build

In CCL builds, code in `pkg/ui/ccl/src` overrides code in `pkg/ui/src` at build
time, via a Webpack import resolution rule. E.g. if a file imports
`src/views/shared/components/licenseType`, it'll resolve to
`pkg/ui/src/views/shared/components/licenseType` in an OSS build, and
`pkg/ui/ccl/src/views/shared/components/licenseType` in a CCL build.

CCL code can import OSS code by prefixing paths with `oss/`, e.g.
`import "oss/src/myComponent"`. By convention, this is only done by a CCL file
importing the OSS version of itself, e.g. to render the OSS version of itself
when the trial period has expired.

## Running tests

To run the tests outside of CI:

```shell
$ ./dev ui test
```

## Managing dependencies

The NPM registry (and the Yarn proxy in front of it, registry.yarnpkg.com)
have historically proven quite flaky. Errors during `yarn install` were the
leading cause of spurious CI failures in the first half of 2018. We used yarn's
[offline mirror](https://classic.yarnpkg.com/blog/2016/11/24/ offline-mirror/)
functionality through December 2022 (and for the initial 22.2 release), but
Bazel support for that feature was poor to non-existent and the workflow
involved was complicated. Worse, upgrades to Bazel and the deprecation of
rules_nodejs (in favor of rules_js) meant a yarn-vendor submodule prevented
necessary maintenance.

As-of January 2023, NPM dependencies are mirrored to a world-readable Google
Cloud Storage bucket maintained by Cockroach Labs, similar to the Go
dependencies (see [build/README.md](../../build/README.md#dependencies)). This
allows for nearly standard pnpm package management workflows, with only one
caveat:

1. When running `pnpm install`, dependencies are installed from the public
   registry. Bazel builds install dependencies from the Cockroach Labs mirror.
   Any net-new dependencies must be uploaded to Google Cloud Storage before a
   Bazel build will succeed. See below for details.

### Adding, Removing, or Updating a dependency
Besides the above wrinkle w.r.t. CLI flags, the standard workflows apply:

```sh
# Add left-pad
pnpm add left-pad

# Or upgrade to a specific version
pnpm add left-pad@1.3.0
# or
pnpm update left-pad

# Then remove it (it's deprecated, after all)
pnpm remove left-pad
```

These respectively add, upgrade, or remove dependencies using the default
registry for pnpm (registry.npmjs.org). Before merging, new dependencies must
be mirrored to GCS so that a Bazel build can succeed.

As always, be sure to commit modifications resulting from dependency changes,
like updates to `package.json` and `pnpm-lock.yaml`.

### Mirroring Dependencies
To upload new dependencies to Google Cloud Storage, you'll need to be a
Cockroach Labs employee signed into the `gcloud` CLI. Simply run
`./dev ui mirror-deps` from the root of `cockroach.git`, and any new
dependencies will be uploaded:

```sh
# Upload new dependencies to GCS
./dev ui mirror-deps
```

### Testing if Dependencies Need to be Mirrored
The default UI lint suite includes testing for unmirrored dependencies:

```sh
./dev ui lint
```

To run _only_ the dependency-mirroring tests, use `bazel` directly:

```sh
bazel test //pkg/cmd/mirror/npm:list_unmirrored_dependencies
```

Either way, a failed test will produce a list of unmirrored dependencies, with a
reminder to run `./dev ui mirror-deps`.