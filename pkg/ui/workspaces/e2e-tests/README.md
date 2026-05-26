# End-to-end Tests for CockroachDB UI
This package contains end-to-end tests for the CockroachDB web UI served (by
default) on port 8080. Tests are run via [Cypress](https://cypress.io) and
use a combination of the default Cypress API and the dom-testing-library queries
provided by [Cypress Testing
Library](https://testing-library.com/docs/cypress-testing-library/intro).

## Running Tests Locally
### Via `pnpm`
If you've already built a `cockroach` binary and installed the dependencies in
this directory with `pnpm`, these tests can be run with `pnpm` directly:

```sh
pnpm test # run all tests
pnpm test ./path/to/some/file.cy.ts # run only a single test file
```

For "headed" test development, in which the Cypress UI and a browser are
visible, run `pnpm test:debug` and choose a test file in the Cypress UI.

### Via `dev`
The standard `dev` CLI used for other CockroachDB tasks can be used to run all
(or a selected subset) of end-to-end tests via:

```sh
dev ui e2e # run all tests
dev ui e2e ./path/to/some/file.cy.ts # run only a single test file
```

This will ensure the relevant NPM dependencies are installed and build
CockroachDB (if necessary) before running any tests, which is especially helpful
during a bisect. For more information, see `dev ui e2e --help`. 

For "headed" test development, in which the Cypress UI and a browser are
visible, run `dev ui e2e --headed` and choose a test file in the Cypress UI.

## Running Tests in TeamCity
For CI purposes, these tests are also run in TeamCity via Docker's [Compose
V2](https://docs.docker.com/compose) by combining the [CockroachDB Docker
image](https://github.com/cockroachdb/cockroach/tree/master/build/deploy) from a
[previous build
step](https://teamcity.cockroachdb.com/buildConfiguration/Cockroach_ScratchProjectPutTcExperimentsInHere_JaneDockerImage)
with an [upstream Cypress docker
image](https://hub.docker.com/r/cypress/browsers). Testing this configuration
requires a bit of manual file movement and is out of scope for this document.

## Cluster Management in Tests
Regardless of how tests are launched, each test suite spins up a single-node
cluster with an empty 'movr' database  that listens only on localhost. The test
scripts waits for the HTTP server to be listening on port 8080 before invoking
`cypress`, and tears down the cluster when `cypress` exits.

## Health Checks vs All Tests
Each CockroachDB pull request results in only the tests in
`./cypress/e2e/health-checks` being executed. These are intended to confirm only
the most basic functionality of the UI, to ensure it hasn't been completely
broken or accidentally removed.  A more comprehensive suite including all tests
is run in a separate TeamCity job.

In general: put only simple, short-lived tests in the health-checks tree.
