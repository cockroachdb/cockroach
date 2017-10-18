# Node Acceptance Test

This test runs the `pg` driver against Cockroach.
If it detects it's running in an acceptance test it will run against the
assigned env vars, otherwise it will run against a locally running Cockroach
(`localhost:26257`) to allow quicker feedback cycles.

To run tests locally:

* Run a Cockroach instance on the default port
* `npm install && npm test`
