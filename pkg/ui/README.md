# Admin UI

This directory contains the client-side code for CockroachDB's web-based admin
UI, which provides details about a cluster's performance and health. See the
[Admin UI docs](https://www.cockroachlabs.com/docs/explore-the-admin-ui.html)
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

To use this proxy, run `./proxy.js <target-cluster-http-uri>` and navigate to
`http://localhost:3000` to access the UI.

When you're ready to submit your changes, be sure to run `make` in this
directory to regenerate the on-disk assets so that your commit includes the
updated `embedded.go`. This is enforced by our build system, but forgetting to
do this will result in wasted time waiting for CI. We commit this generated file
so that CockroachDB can be compiled with minimal [non-go
dependencies](#dependencies).

Be sure to also commit modifications resulting from dependency changes, like
updates to `package.json` and `yarn.lock`.

## Running tests

If you'd like to run the tests directly you can run `make test`. If you're
having trouble debugging tests, we recommend using `make test-debug` which
prettifies the test output and runs the tests in Chrome. When a webpage opens,
you can press the debug button in the top right-hand corner to run tests and set
breakpoints directly in the browser.
