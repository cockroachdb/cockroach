Roachtest fixtures
----

To regenerate these fixtures:

1. Set `FIXTURE_VERSION` to the release or pre-release version you want to
   generate fixtures for:
```
export FIXTURE_VERSION=v23.2.0-beta.1
```

Note that this version must exist in the
[Releases](https://www.cockroachlabs.com/docs/releases) page.


2. Make clean builds of CRDB, roachtest, workload, roachprod. Note that
   roachtest needs to be run on amd64 (if you are using a Mac it's recommended to
   use a gceworker).

```
./dev build cockroach roachprod workload roachtest
## Clear out roachprod remnants, if any
./bin/roachprod destroy local
```

3. Run roachtest against the `FIXTURE_VERSION` binary and generate the updated
   fixtures:

```
./bin/roachtest run generate-fixtures --local --debug --cockroach ./cockroach tag:fixtures
```

This should produce an intentional failure, where you see something like:

```
--- FAIL: generate-fixtures (19.73s)
test artifacts and logs in: artifacts/generate-fixtures/run_1
versionupgrade.go:516,versionupgrade.go:189,versionupgrade.go:527,versionupgrade.go:102,acceptance.go:58,acceptance.go:95,test_runner.go:755: successfully created checkpoints; failing test on purpose.
Invoke the following to move the archives to the right place and commit the
result:
for i in 1 2 3 4; do
mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
pkg/cmd/roachtest/fixtures/${i}/
done
```

Follow the directions in the test failure output to copy the checkpoint files.
Make sure the filenames are as expected and rename if necessary.
