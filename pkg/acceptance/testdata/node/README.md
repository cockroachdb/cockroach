# Node Acceptance Test

This test runs the `pg` driver against Cockroach.
If it detects it's running in an acceptance test it will run against the
assigned env vars, otherwise it will run against a locally running Cockroach
(`localhost:26257`) to allow quicker feedback cycles.

## To run tests locally:

* Run a Cockroach instance on the default port
* `yarn && yarn test`

## To add a dependency

We don't want to install the deps on every CI run, so to add a dependency to
this test, you must:

* Add it to the `package.json` so that anyone running the tests locally will be
  able to install it.
* Rebuild the cockroach-acceptance container (see [../Dockerfile]), which will
  automatically bake in the new dependency.

[../Dockerfile]: ../Dockerfile
