# Admin UI

This directory contains the client-side code for CockroachDB's web-based admin
UI, which provides details about a cluster's performance and health. See the
[Admin UI docs](https://www.cockroachlabs.com/docs/stable/explore-the-admin-ui.html)
for an expanded overview.

## Getting Started

To start developing the UI, be sure you're able to build and run a CockroachDB
node. Instructions for this are located in the top-level README. Every Cockroach
node serves the UI, by default on port 8080, but you can customize the port with
the `--http-port` flag. If you've started a node with the default options,
you'll be able to access the UI at <http://localhost:8080>.

Our UI is compiled using a collection of tools that depends on
[Node.js](https://nodejs.org/) and are managed with
[Yarn](https://yarnpkg.com), a package manager that offers more deterministic
package installation than NPM. NodeJS 6.x and Yarn 0.22.0 are known to work.

With Node and Yarn installed, bootstrap local development by running `make` in
this directory. This will run `yarn install` to install our Node dependencies,
run the tests, and compile the assets. Asset compilation happens in two steps.
First, [Webpack](https://webpack.github.io) runs the TypeScript compiler and CSS
preprocessor to assemble assets into the `dist` directory. Then, we package
those assets into `embedded.go` using
[go-bindata](https://github.com/jteeuwen/go-bindata). When you later run `make
build` in the parent directory, `embedded.go` is linked into the `cockroach`
binary so that it can serve the admin UI when you run `cockroach start`.

## Developing

When making changes to the UI, it is desirable to see those changes with data
from an existing cluster without rebuilding and relaunching the cluster for each
change. This is useful for rapidly visualizing local development changes against
a consistent and realistic dataset.

We've created a simple NodeJS proxy to accomplish this. This server serves all
requests for web resources (JavaScript, HTML, CSS) out of the code in this
directory, while proxying all API requests to the specified CockroachDB node.

To use this proxy, run

```shell
$ make watch TARGET=<target-cluster-http-uri>
```

then navigate to `http://localhost:3000` to access the UI.

While the proxy is running, any changes you make in the `src` directory will
trigger an automatic recompilation of the UI. This recompilation should be much
faster than a cold compile—usually less than one second—as Webpack can reuse
in-memory compilation artifacts from the last compile.

If you get cryptic TypeScript compile/lint failures upon running `make` that
seem completely unrelated to your changes, try removing `yarn.installed` and
`node_modules` before re-running `make` (do NOT run `yarn install` directly).

Be sure to also commit modifications resulting from dependency changes, like
updates to `package.json` and `yarn.lock`.

### DLLs for speedy builds

To improve Webpack compile times, we split the compile output into three
bundles, each of which can be compiled independently. The feature that enables
this is [Webpack's DLLPlugin](https://webpack.js.org/plugins/dll-plugin/), named
after the Windows term for shared libraries ("**d**ynamic-**l**ink
**l**ibraries").

Third-party dependencies, which change infrequently, are contained in the
[vendor DLL]. Generated protobuf definitions, which change more frequently, are
contained in the [protos DLL]. First-party JavaScript and TypeScript are
compiled in the [main app bundle], which is then "linked" against the two DLLs.
This means that updating a dependency or protobuf only requires rebuilding the
appropriate DLL and the main app bundle, and updating a UI source file doesn't
require rebuilding the DLLs at all. When DLLs were introduced, the time required
to start the proxy was reduced from over a minute to under five seconds.

DLLs are not without costs. Notably, the development proxy cannot determine when
a DLL is out-of-date, so the proxy must be manually restarted when dependencies
or protobufs change. (The Make build system, however, tracks the DLLs'
dependencies properly, so a top-level `make build` will rebuild exactly the
necessary DLLs.) DLLs also make the Webpack configuration rather complicated.
Still, the tradeoff seems well worth it.

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
$ make test
```

[main app bundle]: ./webpack.app.js
[protos DLL]: ./webpack.protos.js
[vendor DLL]: ./webpack.vendor.js
