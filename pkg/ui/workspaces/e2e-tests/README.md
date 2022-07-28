# End-to-end Tests for CockroachDB UI
This package contains end-to-end tests for the CockroachDB web UI served (by
default) on port 8080. Tests are run via [Cypress](https://cypress.io) and
use a combination of the default Cypress API and the dom-testing-library queries
provided by [Cypress Testing
Library](https://testing-library.com/docs/cypress-testing-library/intro).

## Running Tests Locally
### Via `yarn`
If you've already built a `cockroach` binary and installed the dependencies in
this directory with `yarn`, these tests can be run with `yarn` directly:

```sh
yarn test # run all tests
yarn test ./path/to/some/file.cy.ts # run only a single test file
```

For "headed" test development, in which the Cypress UI and a browser are
visible, run `yarn test:debug` and choose a test file in the Cypress UI.

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
For CI purposes, these tests are also run in TeamCity via Docker. Debugging that
container is slightly more complicated, and is out-of-scope for this README. See
[Dockerfile](./Dockerfile) for more details and examples.

## Cluster Management in Tests
Regardless of how tests are launched, each test suite spins up a single-node
cluster using the [MovR
dataset](https://www.cockroachlabs.com/docs/stable/movr.html) that listens only
on localhost. The test scripts waits for the HTTP server to be listening on port
8080 before invoking `cypress`. Since the credentials for demo clusters change
for each invocation, the path to a file containing a connection URL is passed to
Cypress to allow testing of both authenticated and unauthenticated flows.

## Health Checks vs All Tests
Each CockroachDB pull request results in only the tests in
`./cypress/e2e/health-checks` being executed. These are intended to confirm only
the most basic functionality of the UI, to ensure it hasn't been completely
broken or accidentally removed.  A more comprehensive suite including all tests
is run in a separate TeamCity job.

In general: put only simple, short-lived tests in the health-checks tree.
