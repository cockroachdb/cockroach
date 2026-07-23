# bincheck

bincheck verifies the sanity of CockroachDB release binaries. At present, the
sanity checks are:

  * starting a one-node server and running a simple SQL query, and
  * verifying the output of `cockroach version`.

## Testing a new release

bincheck action is triggered when a new tag with `v` prefix is created. Check
https://github.com/cockroachdb/cockroach/actions after a release is published.
