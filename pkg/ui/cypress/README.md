### How to run regression visual tests

- start `movr` demo server with command and dev env

```
cockroach demo movr --nodes 4 --insecure
make ui-watch TARGET=http://localhost:{PORT}
```

- run tests

```
yarn cypress:run-visual-regression
```

#### To update test snapshots:

```
yarn cypress:update-snapshots
```

# CockroachDB Cypress Tests

This directory contains e2e tests driven by Cypress for CockroachDB

## How to run

For running tests on a local development environment, first ensure you have CockroachDB `cockroach start-single-node --insecure` running.

After that you have two options, set it up and run it manually or use the commands in the Makefile

### Option 1: Makefile

This command will install cypress and open cypress tests.

```
make db-console-e2e-test
```

### Option 2: Manual process

If this is your first time running Cypress, install it and dependencies:

```
cd pkg/ui/opt
yarn
```

launch Cypress with a local configuration:

```
 cd pkg/ui
 yarn cypress:open
```

The Cypress window will open with a list of test files. Click on a test to run it in a controlled browser window.

Once you click on a test it will re-trigger automatically if you modify the test file. This is a great way to interactively develop tests.

## How to add a test

Generally, following the documentation on [the Cypress website](https://docs.cypress.io/api/api/table-of-contents.html) is a good idea.

Tests are always added to the `./cypress/integration` directory under this directory.

Copying an existing test and modifying it is a great way to start.

Cypress-testing-library has been installed to make writing testing much easier so CSS selectors can be ignored.

[Cypress-testing-library](https://testing-library.com/docs/cypress-testing-library/intro) provides better API to write cypress-tests.

Example for cypress-testing-library can be found under `./cypress/integration/statements.visual.spec.ts` directory.
