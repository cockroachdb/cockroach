# Embedded UI

This directory contains the client-side code for cockroach's web admin
console. These files are embedded into the cockroach binary via the
[go-bindata](https://github.com/jteeuwen/go-bindata) package, which is used to
generate the `embedded.go` file in this directory.

## Getting Started

To get started with the UI, be sure you're able to build and run the
CockroachDB server. Instructions for this are located in the top-level README.

To bootstrap local development, you'll need to run `make` in this directory;
this will download the dependencies, run the tests, and build the web console
assets.

## Modification

As mentioned above, be sure to run the CockroachDB server in UI debug mode
while developing the web console. This causes the CockroachDB server to serve
assets directly from the disk, rather than use the compiled-in assets. These
assets will be compiled in the browser each time the page is reloaded.

NOTE: styles are not yet compiled in the browser. As a workaround, `make
watch` is available; it automatically watches for style changes and recompiles
them, though a browser reload is still required. Note that if you add a new
file, you'll need to restart `make watch`.

When you're ready to submit your changes, be sure to run `make` in this
directory to regenerate the on-disk assets so that your commit includes the
updated `embedded.go`. This is enforced by our build system, but forgetting to
do this will result in wasted time waiting for the build.

We commit the generated file so that CockroachDB can be compiled with minimal
[non-go dependencies](#dependencies).

## Running tests

If you'd like to run the tests directly you can run `make test`. If you're
having trouble debugging tests, we recommend using `make test-debug` which
prettifies the test output and runs the tests in Chrome. When a webpage opens,
you can press the debug button in the top righthand corner to run tests and set
breakpoints directly in the browser.

## Proxying

When prototyping changes to the CockroachDB Admin UI, it is desirable to see
those changes with data from an existing cluster without the headache of having
to redeploy a cluster. This is useful for rapidly visualizing local development
changes against a consistent and realistic dataset.

We have created a simple NodeJS reverse-proxy server to accomplish this; this
server proxies all requests for web resources (javascript, HTML, CSS) to a local
CockroachDB server, while proxying all requests for actual data to a remote
CockroachDB server.

To use this server, navigate to the `pkg/ui/proxy` directory, install the
dependencies using `yarn` or `npm`, then run `./proxy.js <existing- instance-
ui-url> --local <development-instance-ui-url>` and navigate to
`http://localhost:3000` to access the UI.

## Dependencies

Our web console is compiled using a collection of tools that depends on
[Node.js](https://nodejs.org/), so you'll want to have that installed.

We use [yarn](https://yarnpkg.com) to manage various dependencies. It is also
possible to use `npm`, though you may run into problems as `npm` does not
respect yarn's yarn.lock.

Be sure to commit any changes resulting from your dependency changes.
