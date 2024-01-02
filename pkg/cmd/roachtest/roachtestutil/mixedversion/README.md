# Mixed-version roachtest framework

The `mixedversion` package provides an API that teams can use to test behaviour in _mixed-version_ context. "Mixed-version," as used in this document, refers to the following scenarios:

* not every node in the cluster is running the same binary. One example of this would be a 4-node cluster where nodes 1 and 2 are running v22.2.7, and nodes 3 and 4 are running v23.1.2.
* every node in the cluster is running the same binary version, but the cluster version is still at some previous version due to the use of [`cluster.preserve_downgrade_option`](https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html#step-3-decide-how-the-upgrade-will-be-finalized). For example, an operator ran `SET CLUSTER SETTING cluster.preserve_downgrade_option = '22.2'`, then restarted every node with a v23.1.4 binary.
* every node in the cluster is running the same binary version _and_ the upgrade is happening in the background (i.e., relevant migrations are running). Also known as _finalizing_ the upgrade. For example: in the scenario above, the operator ran `RESET CLUSTER SETTING cluster.preserve_downgrade_option`.

### High Level Overview

Every run of a roachtest that uses the `mixedversion` API follows this high-level structure:

* cluster is initialized at a certain previously released version (possibly but not necessarily the immediate predecessor of the current binary);
* nodes are stopped and restarted with a different, newer binary. User functions (_hooks_ -- see below) run at arbitrary points during this process. The order in which nodes are restarted is unspecified;
* a downgrade _may_ happen -- that is, nodes are once again restarted, and the newer binary is replaced by an older binary. This simulates the need for the operator to perform a downgrade in case the cluster cannot be upgraded for some reason. In case a downgrade is performed, the cluster will set the `cluster.preserve_downgrade_option` cluster setting accordingly prior to any node being upgraded;
* eventually, every node in the cluster is restarted with the most recent binary (i.e., the one passed to roachtest with the `--cockroach` flag). The upgrade is then finalized.

The framework's main goal is to provide an abstraction that allows tests to focus on the functionality being tested rather than on setting up a mixed-version test. The following example illustrates this with a basic example.

### Terminology

Before we dive into the example, it's useful to define some terms used by the `mixedversion` API:

* **Test plan**: `mixedversion` tests are _randomized_: each run of the same test will exercise a different ordering of certain events, represented by a different test plan. Before the cluster is set up, a test plan is generated that outlines the _steps_ the test will take. The test plan is printed at the beginning of execution.
* **Test step**: a test plan is comprised of many test steps. Some of these steps are implemented by the framework (e.g., restarting a node with a certain binary, installing pre-existing fixtures, finalizing the upgrade, etc) while others are implemented by the test author (through _hooks_). Each step has a numeric ID; the test plan displays these IDs.
* **Hooks**: mixed-version tests are ultimately about testing some database feature or expected behaviour. Hooks are what tell the `mixedversion` API what should be tested. Hooks can be run at different points in the test, and often multiple times in the same test run.

### Walkthrough of a simple `mixedversion` test

In this simple example, we write a `mixedversion` test to make sure we are able to drop tables in mixed-version. The entire code is below (you can try it by including the snippet that follows in a file in `pkg/cmd/roachtest/tests`, and calling `registerSimpleMixedVersionTest` in [`pkg/cmd/roachtest/tests/registry.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/tests/registry.go)).

``` go
// droppableTables keeps track of all tables created in the test.
type droppableTables struct {
	mu      syncutil.Mutex
	tables  []string
	counter int
}

// add generates a new unique table name, adds it to the underlying
// list of tables, and returns the generated name.
func (dt *droppableTables) add() string {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	dt.counter++
	name := fmt.Sprintf("t_%d", dt.counter)
	dt.tables = append(dt.tables, name)
	return name
}

// randomSample generates a random sample of a given size. If there
// aren't enough tables as requested, all tables are returned, in
// random order.
func (dt *droppableTables) randomSample(rng *rand.Rand, size int) []string {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	rng.Shuffle(len(dt.tables), func(i, j int) { dt.tables[i], dt.tables[j] = dt.tables[j], dt.tables[i] })
	sampleSize := size
	if numTables := len(dt.tables); numTables < size {
		sampleSize = numTables
	}

	sample := dt.tables[:sampleSize]
	dt.tables = dt.tables[sampleSize:]
	return sample
}

func registerSimpleMixedVersionTest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "simple-test/mixed-version",
		Owner:   registry.OwnerTestEng,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All())

			// Maxium number of tables we will attempt to create or drop
			// each time our mixedversion hooks are called.
			const maxTablesPerCycle = 5
			tables := droppableTables{}

			// Some helper functions to create and drop tables.
			createSomeTablesStep := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				numTables := rng.Intn(maxTablesPerCycle)
				l.Printf("creating %d tables", numTables)

				for j := 0; j < numTables; j++ {
					name := tables.add()
					if err := h.Exec(rng, fmt.Sprintf("CREATE TABLE %s (n INT)", name)); err != nil {
						return errors.Wrapf(err, "error creating table %s", name)
					}
				}

				return nil
			}

			// Drops each table in the `tables` parameter passed.
			dropTables := func(tables []string, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				for _, name := range tables {
					if err := h.Exec(rng, fmt.Sprintf("DROP TABLE %s", name)); err != nil {
						return errors.Wrapf(err, "error dropping table %s", name)
					}
				}
				return nil
			}

			// Drops a random sample of tables.
			dropSomeTablesStep := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				numTables := rng.Intn(maxTablesPerCycle)
				l.Printf("dropping (at most) %d tables", numTables)
				return dropTables(tables.randomSample(rng, numTables), l, rng, h)
			}

			// Drops _all_ tables  created thus far.
			dropAllTablesStep := func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
				l.Printf("dropping %d tables", len(tables.tables))
				return dropTables(tables.tables, l, rng, h)
			}

			// Create some tables when the cluster starts up (in the initial version).
			mvt.OnStartup("create some tables on the previous version", createSomeTablesStep)

			// Create more tables in mixed version -- we should be able to drop
			// these tables too.
			mvt.InMixedVersion("create more tables", createSomeTablesStep)
			// Drop some of the tables we have created up to this point.
			mvt.InMixedVersion("drop some tables", dropSomeTablesStep)

			// Drop all remaining tables (if any) when the upgrade is finalized.
			mvt.AfterUpgradeFinalized("drop all remaining tables", dropAllTablesStep)

			// Run the test.
			mvt.Run()
		},
	})
}
```

Before we get into a few details about the code above, one point is worth highlighting: all of the code relates to testing the feature we are interested in (dropping tables). Specifically, the test does not have any logic related to: setting up the cluster, finding out which version is the predecessor, staging releases, deciding whether we want to test a downgrade, among other things. **This abstraction is the main advantage of using the `mixedversion` API**: not only does it allow test authors to focus on the feature being tested, but it also allows us to extend our coverage with different scenarios without having to change each mixed-version test.

Now, on to an overview of the test itself.

#### Setting up the test

``` go
mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All())
```

The first step is to create a `*mixedversion.Test` struct: this object enables test authors to specify what functionality they want tested in mixed-version. This is done primarily by the use of _hooks_, such as `InMixedVersion` (described below).

The 5th argument to the call above (`c.All()`) indicates to the framework what nodes are running `cockroach` in the cluster. Since this is a simple test, all nodes in the test spec are cockroach nodes. It's also common to have a separate `workload` node, in which case we wouldn't use `c.All()`.

#### Running logic as soon as the cluster is ready

``` go
mvt.OnStartup("create some tables on the previous version", createSomeTables)
```

This is the first _user hook_ we encounter: `OnStartup`. [Hooks](#terminology) are functions that are invoked at specific points in the test and is where test logic and assertions must live. The `OnStartup` function lets the test register a function to be executed as soon as the cluster is ready.

One important point to note is that mixed-version tests start the cluster at a [_predecessor version_ (not necessarily immediate predecessor)](#high-level-overview). As such, if a feature was just introduced in the current release, it won't be available at this point.

#### Testing code in mixed-version

``` go
mvt.InMixedVersion("create more tables", createSomeTables)
mvt.InMixedVersion("drop some tables", dropSomeTables)
```

The function passed to `InMixedVersion` is called when the cluster is in some mixed-version state (see opening paragraph). This is where the main assertions of our test lives: in this case, we assert that we are able to create and drop tables in mixed-version.

Also notice how there are _two_ `InMixedVersion` calls: while we could have created a single hook that both creates and drops tables in one function, that would have forced an ordering on these operations. Instead, by defining them separately, we allow the framework to explore different orderings for these functions (including running them concurrently).

#### Making final assertions

``` go
mvt.AfterUpgradeFinalized("drop all remaining tables", dropAllTables)
```

As the name implies, `AfterUpgradeFinalized` allows the test to register code to be executed once an upgrade is finalized. At this point, the binary version on every node matches the cluster version. Note, however, that this does _not_ mean that the cluster is running the latest binary (the one passed to roachtest's `--cockroach` flag). The `mixedversion` framework may decide to start the cluster from two versions prior to the current one and perform a full upgrade to the current version's predecessor. In that case, the `AfterUpgradeFinalized` hook is called two times, once for each upgrade.

#### Running the test

``` go
mvt.Run()
```

Once all hooks are registered, all that is left to do is to actually run the test. The `Run()` function will run the mixed-version test and fail the roachtest in case one of the user-hooks returns an error or panics. Naturally, failures can also happen due to bugs in the framework itself: report it to `#test-eng` in that case!

### Best Practices

#### Embrace randomness

The `mixedversion` framework itself randomizes a lot of its operations: the order in which nodes are restarted with a new binary, whether to use fixtures when setting up the cluster, inserting a delay before certain operations, etc. Similarly, tests are encouraged to take advantage of randomization to get more coverage out of a single test.

Every `mixedversion` hook is passed a `*rand.Rand` instance. Tests that wish to perform random operations **should use that random number generator instead of the global rand instance**. Doing so makes [reproducing failures](#reproducing-failures) much easier. Whenever we want to test an operation, it's useful to think of different ways to achieve it and use randomization to make sure we cover different scenarios and orderings over time.

In the [example](#walkthrough-of-a-simple-mixedversion-test) above, notice how SQL statements were run with `h.Exec()` instead of using `cluster.Conn()` as most roachtests do. The former picks a random node to send the query to; this is especially important in mixed-version, as we want to test that our feature works regardless of the node (and therefore, binary version) we are connecting to.

##### Note: non-deterministic use of the `*rand.Rand` instance

In order to take [full advantage](#reproducing-failures) of randomness in mixedversion tests, it's highly advisable that every use of the `*rand.Rand` instance passed to test steps happen either in deterministic code, or in non-deterministic code controlled by the same rng instance. Conversely, _uses of `*rand.Rand` that are influenced by non-determinism sources other than the `*rand.Rand` instance itself should be avoided_.

Consider the following example:

``` go
func myTestStep(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	// ...
	for {
		select {
		case <-someWork.Done():
			if rng.Float64() < scatterProbability {
				if err := h.Exec(rng, "ALTER TABLE t1 SCATTER"); err != nil {
					return errors.Wrap(err, "failed to scatter")
				}
			}

			return nil

		case <-time.After(time.Second):
			var amount int
			if err := h.QueryRow(rng, "SELECT amount FROM t1").Scan(&amount); err != nil {
				return errors.Wrap(err, "failed to query amount")
			}

			l.Printf("still waiting for work to be done, %d left", amount)
		}
	}
}
```

In the loop above, we wait for `someWork` to be done, and when that happens, we perform an operation (`SCATTER`) randomly based on a certain `scatterProbability`. Every 1s, the loop also prints a log message to display some progress towards the goal. Notice how we use `h.QueryRow` and pass the `rng` argument to that function.

Consider two runs of this test using the same random seed: in the first, `someWork` takes 800ms to complete; in the second, it takes 1.2s. By the time we are evaluating whether to scatter, our `rng` instance will be different in the two runs and our test runs will diverge.

To avoid this situation, one must avoid using the `*rand.Rand` instance in non-deterministic situations outside of the control of the rand instance itself; in this case, the source of non-determinisim is how long it takes to finish `someWork`. To deal with situations like this, the test author could have picked a node _before_ the loop, as in the updated code below:

``` go
// ...
_, db := h.RandomDB(rng, crdbNodes)
for {
	select {
	case <-someWork.Done():
		// ...

	case <-time.After(time.Second):
		var amount int
		if err := db.QueryRow("SELECT amount FROM t1").Scan(&amount); err != nil {
			return errors.Wrap(err, "failed to query amount")
		}
	}
}
```

#### Use test helpers

Every test hook receives as parameter a `*mixedversion.Helper` instance. This helper struct contains convenience functions that make it easy to perform certain operations while automatically implementing some of the best practices described here. For a full list of helpers, check out the [`helper.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/helper.go) file in the framework's source tree.

##### Aside: background functions

One particularly important helper functionality to highlight relates to the management of functions that need to run in the background during a test. Typically, roachtests are expected to use a [monitor](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/monitor.go) for that purpose; not only does the monitor protect from panics inadvertently crashing the test, it also preempts their execution (via context cancelation) if a node dies unexpectedly.

However, if a mixedversion test needs to perform a task in the background, they **must not use the roachtest monitor**. The reason for this is that mixedversion tests are [randomized](#embrace-randomness); as such, user-created monitors would not be able to predict when a death is expected since it does not know, by design, when a node restarts.

To run functions in the background, use the API provided by the mixedversion framework. Long running tasks that run throughout the test can be defined with `(*mixedversion.Test).BackgroundFunc`. If a test hook needs to perform a task in the background, the `*mixedversion.Helper` instance has a `Background` function that can be used for that purpose. As usual, check the documentation for the public API for more details on usage and behaviour of these functions.

#### Log progress

Logging events in the test as it runs can make debugging failures a lot easier. All hooks passed to the `mixedversion` API receive a `*logger.Logger` instance as parameter. **Functions should use that logger instead of the test logger** (`t.L()`). Doing so has two main advantages:

* Messages logged will be prefixed with the step name (in a format similar to `[{step_num}_{step_name}] {message}`;
* All messages logged by a single function are grouped in the same file, making it easier to follow the sequence of events in a hook, especially when it is running concurrently with other hooks or test steps.

In a `mixedversion` test, roachtest's `test.log` will include the logs for all steps in the same file. Alternatively, it is also possible to focus on the logs of a single hook by inspecting the `mixed-version-test` directory in the test artifacts. That directory will contain a series of log files named `{step_num}_{step_name}.log`; if one of these steps failed, the log filename will have the `FAILED_` prefix.

| ![mixed-version-test-dir](https://github.com/cockroachdb/cockroach/assets/103441181/4161dd3c-1460-4f89-9a48-b42da3eaad77) |
| :--: |
| *The `mixed-version-test` directory in a `mixedversion` test failure.* |

#### Return errors

Hooks passed to the `mixedversion` API are expected to return errors on failure. These errors ultimately cause the test to fail with information about the cluster's state (binary and cluster versions on each node), along with the random seed used in that run.


``` go
// Better
mvt.InMixedVersion(
 	"test my feature",
 	func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
 		if err := testFeature(); err != nil {
 			return errors.Wrapf(err, "failed to test feature")
 		}

 		// ...
 		return nil
 	})

// Worse
mvt.InMixedVersion(
 	"test my feature",
 	func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
 		if err := testFeature(); err != nil {
 			t.Fatal(err)
 		}

 		// ...
 		return nil
 	})
```

### Debugging failures

No matter how robust our test, chances are it will fail at some point. When that happens, the `test.log` file is the first artifact to check, as it contains all the logs for the test leading up to the failure. At the bottom of that file, we should see the `mixedversion` test failure.

Note that `mixedversion` failures are still roachtest failures, so make sure you are familiar with [debugging roachtest failures](https://cockroachlabs.atlassian.net/wiki/spaces/TE/pages/2208629098/How+to+investigate+a+roachtest+failure) in general as well.

#### Understanding the test failure

Test failures will look something like the error below (extracted from a real failure -- [#105032](https://github.com/cockroachdb/cockroach/issues/105032)):

``` text
[mixed-version-test/29_run-test-schema-change-step] 05:57:24 runner.go:328: mixed-version test failure while running step 29 (run "test schema change step"): output in run_055721.704588390_n3_workload-run-schemac: ./workload run schemachange {pgurl:1-4} --concurrency 2 --max-ops 10 --verbose 1 returned: COMMAND_PROBLEM: ssh verbose log retained in ssh_055722.380585675_n3_workload-run-schemac.log: exit status 1
test random seed:                       7357315251706229449
binary versions:                        [1: 23.1, 2: 23.1, 3: 23.1, 4: 23.1]
cluster versions before failure:        [1: 22.2, 2: 22.2, 3: 22.2, 4: 22.2]
cluster versions after failure:         [1: 22.2-10, 2: 22.2-10, 3: 22.2-10, 4: 22.2-10]
```

The error message includes a few pieces of information worth highlighting:

* The _random seed_ used this run: `7357315251706229449` (see [reproducing-failures](#reproducing-failures));
* The _binary version_ reported by each node at the time of the failure. Note that this is not the released binary version (i.e., `v23.1.3`); it is, instead, the value reported by `crdb_internal.node_executable_version()`.
* The _cluster version_ reported by each node before (when the test step started) and after the failure. This is useful in case of failures that happen during upgrade finalization. Note, however, that if the upgrade _is_ finalizing, these cluster versions are there for extra visibility, but there is no guarantee that the cluster versions were at exactly those values by the time the test step started or ended. During finalization, the cluster version is advancing, as migrations run, concurrently with the test step itself.

Finally, notice that the error message includes which step caused the failure (`mixed-version test failure while running step 29 (run "test schema change step")`). If we refer to the test plan for this execution (included below), it is also possible to understand what happened in the test up to this point which should provide some insight into possible reasons for the failure.

``` text
[mixed-version-test] 05:50:39 mixedversion.go:407: mixed-version test plan for upgrading from 22.2.10 to <current>:
├── starting cluster from fixtures for version "22.2.10" (1)
├── upload current binary to all cockroach nodes (:1-4) (2)
├── wait for nodes :1-4 to all have the same cluster version (same as binary version of node 1) (3)
├── preventing auto-upgrades by setting `preserve_downgrade_option` (4)
├── run "setup schema changer workload" (5)
├── upgrade nodes :1-4 from "22.2.10" to "<current>"
│   ├── restart node 3 with binary version <current> (6)
│   ├── run "run backup" (7)
│   ├── restart node 4 with binary version <current> (8)
│   ├── run "test features" (9)
│   ├── restart node 1 with binary version <current> (10)
│   ├── restart node 2 with binary version <current> (11)
│   └── run "test schema change step" (12)
├── downgrade nodes :1-4 from "<current>" to "22.2.10"
│   ├── restart node 3 with binary version 22.2.10 (13)
│   ├── run "run backup" (14)
│   ├── restart node 1 with binary version 22.2.10 (15)
│   ├── run "test features" (16)
│   ├── restart node 4 with binary version 22.2.10 (17)
│   ├── run "test schema change step" (18)
│   └── restart node 2 with binary version 22.2.10 (19)
├── upgrade nodes :1-4 from "22.2.10" to "<current>"
│   ├── restart node 4 with binary version <current> (20)
│   ├── restart node 3 with binary version <current> (21)
│   ├── run mixed-version hooks concurrently
│   │   ├── run "run backup", after 500ms delay (22)
│   │   └── run "test features", after 200ms delay (23)
│   ├── restart node 1 with binary version <current> (24)
│   ├── restart node 2 with binary version <current> (25)
│   └── run "test schema change step" (26)
├── finalize upgrade by resetting `preserve_downgrade_option` (27)
├── run mixed-version hooks concurrently
│   ├── run "run backup", after 100ms delay (28)
│   └── run "test schema change step", after 0s delay (29) <--- STEP WHERE FAILURE HAPPENED
├── wait for nodes :1-4 to all have the same cluster version (same as binary version of node 1) (30)
└── run "check if GC TTL is pinned" (31)
```

#### Reproducing failures

Needless to say, CockroachDB's behaviour in a roachtest is highly non-deterministic. In that sense, there is no guaranteed way to reproduce a roachtest failure, `mixedversion` or otherwise.

That said, there are measures we can take to make failures _more likely_ to be reproducible. When a [failure occurs](#understanding-the-test-failure), the `mixedversion` framework logs the _random seed_ used in that particular run. By re-running the test with the same seed, we guarantee that:

* the same test plan is generated (i.e., ordering of events, as performed by the test, is the same). Note that, at the moment, this is only guaranteed if `roachtest` is built on the same SHA as the one used when the failure occurred;
* random values used by user hooks are also the same. Note that this only holds if user hooks are diligent about [using the `*rand.Rand` instance passed to them](#embrace-randomness).

In practice, we have observed that this goes a long way towards reproducing issues. To run a `mixedversion` test with a specific seed, set the `COCKROACH_RANDOM_SEED` environment variable accordingly, as illustrated below:

``` shell
$ COCKROACH_RANDOM_SEED=7357315251706229449 roachtest run acceptance/version-upgrade
```

#### Using instrumented binaries

To help understand what is happening inside cockroach when a failure happens, it's common to run tests with instrumented binaries (e.g., including changes to increase log verbosity, enabling different profiling options, etc.) In order to use a different binary for what represents the "current" cockroach binary, use roachtest's `--cockroach` flag.

In addition, it is also possible to use custom binaries for _previous_ cockroach versions (for instance, if we are debugging a failure that happens during a test that exercises the `22.2.x` to `23.1.x` upgrade path, we might want to use a custom build of the `22.2.x` binary used in the test). In order to achieve that,

* Identify the patch release used in the test. For example, in the test plan [above](#understanding-the-test-failure), the test used the `22.2.10` release (see message at the top of the test plan: `mixed-version test plan for upgrading from 22.2.10 to <current>`);
* Build the modified binary, preferably by applying the desired patch on top of the patch release used in the test. Let's say you call this binary `cockroach-22.2.10-custom`.
* Invoke roachtest with the `versions-binary-override` flag.

The following command can be used as a reproduction attempt for the failure [above](#understanding-the-test-failure), using a custom build of the 22.2.10 release:

``` shell
$ COCKROACH_RANDOM_SEED=7357315251706229449 roachtest run --versions-binary-override "22.2.10=./cockroach-22.2.10-custom" acceptance/version-upgrade
```

### Final Notes

* This is a high level document and does not include API documentation. The `mixedversion` package includes a lot of documentation in the form of source code comments, and that should be the source of truth when it comes to finding out what functionality is available and how to use it. Most of the public API is in the [`mixedversion.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go) and [`helper.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/helper.go) files.
* For a simple application of the `mixedversion` framework, check out the `acceptance/version-upgrade` roachtest. For a more complex example, see `backup-restore/mixed-version`.
* For any other questions, please reach out to `#test-eng`!
