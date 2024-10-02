# Mixed-version roachtest framework

The `mixedversion` package provides an API that teams can use to test behaviour in _mixed-version_ context. "Mixed-version," as used in this document, refers to the following scenarios:

* not every node in the cluster is running the same binary. One example of this would be a 4-node cluster where nodes 1 and 2 are running v22.2.7, and nodes 3 and 4 are running v23.1.2.
* every node in the cluster is running the same binary version, but the cluster version is still at some previous version due to the use of [`cluster.preserve_downgrade_option`](https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html#step-3-decide-how-the-upgrade-will-be-finalized). For example, an operator ran `SET CLUSTER SETTING cluster.preserve_downgrade_option = '22.2'`, then restarted every node with a v23.1.4 binary.
* every node in the cluster is running the same binary version _and_ the upgrade is happening in the background (i.e., relevant migrations are running). Also known as _finalizing_ the upgrade. For example: in the scenario above, the operator ran `RESET CLUSTER SETTING cluster.preserve_downgrade_option`.
* in [multi-tenant](#deployment-modes) deployments: the storage cluster is running a given cluster version, but the tenant is still in the previous verison.

### High Level Overview

Every run of a roachtest that uses the `mixedversion` API follows this high-level structure:

* cluster is initialized at a certain previously released version (possibly but not necessarily the immediate predecessor of the current binary);
* if the test is running in a _multi-tenant_ deployment, the test tenant is created at some point after the cluster is initialized, but before any user functions run.
* nodes are stopped and restarted with a different, newer binary. User functions (_hooks_ -- see below) run at arbitrary points during this process. The order in which nodes are restarted is unspecified;
* a downgrade (or rollback) _may_ happen -- that is, nodes are once again restarted, and the newer binary is replaced by an older binary. This simulates the need for the operator to perform a downgrade in case the cluster cannot be upgraded for some reason. In case a downgrade is performed, the cluster will set the `cluster.preserve_downgrade_option` cluster setting accordingly prior to any node being upgraded;
* eventually, every node in the cluster is upgraded all the way to the most recent binary (i.e., the one passed to roachtest with the `--cockroach` flag). That upgrade is then finalized (i.e., downgrade is no longer allowed).

The framework's main goal is to provide an abstraction that allows tests to focus on the functionality being tested rather than on setting up a mixed-version test. The following example illustrates this with a basic example.

### Terminology

Before we dive into the example, it's useful to define some terms used by the `mixedversion` API:

* **Test plan**: `mixedversion` tests are _randomized_: each run of the same test will exercise a different ordering of certain events, represented by a different test plan. Before the cluster is set up, a test plan is generated that outlines the _steps_ the test will take. The test plan is printed at the beginning of execution.
* **Test step**: a test plan is comprised of many test steps. Some of these steps are implemented by the framework (e.g., restarting a node with a certain binary, installing pre-existing fixtures, finalizing the upgrade, etc) while others are implemented by the test author (through _hooks_). Each step has a numeric ID; the test plan displays these IDs.
* **Deployment mode**: the deployment mode indicates what kind of cluster configuration the test will run on: it is one of `system-only`, `shared-process`, or `separate-process`. As the name suggests, the cluster can be running in [multi-tenant](#deployment-modes) mode. In these cases, the functionality that tests exercise will be invoked on the tenant created for the test.
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
			mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.CRDBNodes())

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
mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.CRDBNodes())
```

The first step is to create a `*mixedversion.Test` struct: this object enables test authors to specify what functionality they want tested in mixed-version. This is done primarily by the use of _hooks_, such as `InMixedVersion` (described below).

The 5th argument to the call above (`c.CRDBNodes()`) indicates to the framework what nodes should be part of the cockroachdb cluster created for this test. See the [Upgrading multiple clusters](#upgrading-multiple-clusters) section for information on tests that exercise features across clusters.

#### Running setup logic as soon as the cluster is ready

``` go
mvt.OnStartup("create some tables on the previous version", createSomeTables)
```

This is the first _user hook_ we encounter: `OnStartup`. [Hooks](#terminology) are functions that are invoked at specific points in the test and is where test logic and assertions must live. The `OnStartup` function lets the test register a hook to be executed as soon as the cluster is ready (i.e., when it is running a version greater than the test's _minimum supported version_). In addition, in multi-tenant tests, the test tenant has also been created and is ready to serve requests at this point.

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

As the name implies, `AfterUpgradeFinalized` allows the test to register code to be executed once an upgrade is finalized. At this point, the binary version on every node matches the cluster version. In multi-tenant deployments, the test tenant has also been fully upgraded to the same cluster version as the storage cluster. Note, however, that this does _not_ mean that the cluster is running the latest binary (the one passed to roachtest's `--cockroach` flag). The `mixedversion` framework may decide to start the cluster from two versions prior to the current one and perform a full upgrade to the current version's predecessor. In that case, the `AfterUpgradeFinalized` hook is called two times, once for each upgrade.

#### Running the test

``` go
mvt.Run()
```

Once all hooks are registered, all that is left to do is to actually run the test. The `Run()` function will run the mixed-version test and fail the roachtest in case one of the user-hooks returns an error or panics. Naturally, failures can also happen due to bugs in the framework itself: report it to `#test-eng` in that case!

### Best Practices

#### Embrace randomness

The `mixedversion` framework itself randomizes a lot of its operations: the order in which nodes are restarted with a new binary, whether to use fixtures when setting up the cluster, the deployment mode used in each test run, the timing of user functions, etc. Similarly, tests are encouraged to take advantage of randomization to get more coverage out of a single test.

Every `mixedversion` hook is passed a `*rand.Rand` instance. Tests that wish to perform random operations **should use that random number generator instead of the global rand instance** (or another `rand` instance created elsewhere). Doing so makes [reproducing failures](#reproducing-failures) a lot easier. Whenever we want to test an operation, it's useful to think of different ways to achieve it and use randomization to make sure we cover different scenarios and orderings over time.

In the [example](#walkthrough-of-a-simple-mixedversion-test) above, notice how SQL statements were run with `h.Exec()` instead of using `c.Conn()` as most roachtests do. The former picks a random node to send the query to; this is especially important in mixed-version, as we want to test that our feature works regardless of the node (and therefore, binary version) we are connecting to.

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

Hooks passed to the `mixedversion` API are expected to return errors on failure. These errors ultimately cause the test to fail with information about the cluster's state (binary and cluster versions on each node and tenant), along with the random seed used in that run.


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

### Test options

`mixedversion` tests can be configured by passing a list of _options_ to `mixedversion.NewTest()`. These options expose some control on the types of test plans generated and specific runtime behaviour. Common options are explained below -- for a full list, see the [mixedversion.go](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go) file.

``` go
mixedversion.NeverUseFixtures
```

This option tells the test planner to never use cockroachdb [fixtures](https://github.com/cockroachdb/cockroach/tree/master/pkg/cmd/roachtest/fixtures) when setting up the cluster. These fixtures are useful as they have been maintained by upgrading a cockroachdb cluster all the way back since v1.0, and they sometimes expose bugs. However, a key limitation of the fixtures is that they only work in 4-node clusters. If the test uses a differently sized cluster, this option is necessary.

``` go
mixedversion.MaxUpgrades(2)
```

This option instructs the planner to generate test plans that run no more than the given amount of version upgrades. Useful to limit the test's running time, as upgrades can take a long time to run if the test has long running user hooks. See also: `mixedversion.MinUpgrades()` and `mixedversion.NumUpgrades()`.

``` go
mixedversion.MinimumSupportedVersion("v24.1.0")
```

`mixedversion` tests may perform [several upgrades](#high-level-overview) before finalizing the upgrade to the current version. If the feature being tested was introduced in a specific, supported version then the test should use this option to indicate that. Note that this **does not** mean that the cluster will bootstrapped in this version. It could be created in an older version and upgraded to the minimum supported version. User hooks are only called once the cluster reaches the minimum supported version.

``` go
mixedversion.AlwaysUseLatestPredecessors
```

By default, `mixedversion` tests will use random predecessors when upgrading the cluster to the current version. Sometimes, however, this can lead to flakes if the test exposes a bug already fixed in a patch release. By using this option, the test planner will always pick the latest available release for each release series.

``` go
mixedversion.EnabledDeploymentModes(mixedversion.SystemOnly)
```

Sometimes, a certain test is incompatible (or flakes) with a particular [deployment mode](#deployment-modes). This option allows the test author to specify the deployments modes compatible with a test.

``` go
mixedversion.DisableMutators(mixedversion.PreserveDowngradeOptionRandomizerMutator)
```

Use this option to disable specific [mutators](#mutators) that are incompatible with the test.

### Deployment Modes

By default, each run of a `mixedversion` test happens in one of 3 possible _deployment modes_: `system-only`, `shared-process`, and `separate-process`. In the latter two options, the framework will create a test tenant, and tests should exercise the feature they are testing by invoking it on the tenant.

The deployment mode used in a test run is included in `test.log`, before the test begins (before the test plan itself):

``` text
[mixed-version-test] 2024/09/27 11:52:50 mixedversion.go:734: mixed-version test:
Seed:               374411616843294270
Upgrades:           v24.2.2 → master
Deployment mode:    separate-process
Plan:
...
```

In tests that can run in multi-tenant deployments, it is especially important to use the [helpers](#use-test-helpers) provided by the framework: when using them, tests do not need to care about the deployment mode active in a test run. For instance, the following code:

```go
h.Exec(rng, "CREATE TABLE mytable")
```

will run the `CREATE TABLE` statement above directly on the system in `system-only` deployments, but on the test tenant in `shared-process` or `separate-process` deployments.

In some cases, however, it is necessary to be explicit about the service we need to connect to when performing a certain action. The most common scenario is when the test needs to update a system cluster setting. This can be achieved by specifying that the test needs to talk to the system directly:

``` go
h.System.Exec(rng, "SET CLUSTER SETTING kv.rangefeed.enabled = $1", true)
```

The code above is safe as it will connect to the system tenant in all deployment modes: `h.System` is never `nil` in any deployment mode, as even in multi-tenant deployments we still have the the system interface (storage cluster).

### Upgrading multiple clusters

Sometimes, the feature being tested involves the interaction between more than a single CockroachDB cluster. In order to test the behaviour of such features when some or all of the clusters are in mixed-version, it is possible to create _multiple instances_ of `*mixedversion.Test` and run the upgrades concurrently:

``` go
mvt1 := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(1, 3),
 	mixedversion.WithTag("cluster 1"),
)
mvt2 := mixedversion.NewTest(ctx, t, t.L(), c, c.Range(4, 6),
 	mixedversion.WithTag("cluster 2"),
)
```

In the example above, the two `mixedversion` instances `mvt1` and `mvt2` are responsible for carrying out upgrades on the two cockroachdb clusters involved in the test: `cluster 1`, running on nodes `1-3` and `cluster 2`, running on nodes `4-6`. Additionally, it is useful to use the `WithTag` [option](#test-options) to make it easier to identify which log lines map to each upgrade in `test.log`, especially if the upgrades are meant to run concurrently.

For an example of this setup in practice, see the `c2c/mixed-version` test (an upgrade test for the Physical Cluster Replication feature).

### Mutators

_Mutators_ in the `mixedversion` framework implement changes to an upgrade test plan that should not impact the behaviour of the cluster or the upgrade itself. If enabled, they are applied on a subset of test runs. Mutators are able to add, remove, or reorder steps in the plan. Tests are able to disable specific mutators by using a [test option](#test-options).

The list of mutators enabled in a particular run is included in the logs before the test runs, along with the [deployment mode](#deployment-modes):

``` text
[mixed-version-test] 06:17:32 mixedversion.go:707: mixed-version test:
Seed:               3313508076011511280
Upgrades:           v23.2.11 → release-24.1
Deployment mode:    system-only
Mutators:           preserve_downgrade_option_randomizer, cluster_setting[kv.expiration_leases_only.enabled], cluster_setting[storage.ingest_split.enabled], cluster_setting[storage.sstable.compression_algorithm]
Plan:
...
```

At the time of writing, the two main mutators available in the framework are:

#### `preserve_downgrade_option_randomizer`

This mutator randomizes the point in time in which the `preserve_downgrade_option` is reset, which controls auto-upgrade behaviour on the cluster. When performing auto-upgrades, this setting can be reset at any point in time when we know we are no longer going to rollback an upgrade. When this mutator is not enabled, the resetting of this cluster setting always happens when every node is running the newer binary.

#### `cluster_setting[$name]`

When enabled, this mutator sets the cluster setting `$name` to one of its possible values. It may also update the setting later, or reset it back to its original value. Typically used for cluster settings introduced in recent releases.

### Debugging failures

No matter how robust our test is, chances are it will fail at some point. When that happens, the `test.log` file is the first artifact to check, as it contains all the logs for the test leading up to the failure. At the bottom of that file, we should see the `mixedversion` test failure.

Note that `mixedversion` failures are still roachtest failures, so make sure you are familiar with [debugging roachtest failures](https://cockroachlabs.atlassian.net/wiki/spaces/TE/pages/2208629098/How+to+investigate+a+roachtest+failure) in general as well.

#### Understanding the test failure

Test failures will look something like the error below (extracted from [#131501](https://github.com/cockroachdb/cockroach/issues/131501)):

``` text
[mixed-version-test/22_run-run-backup] 2024/09/27 12:00:17 runner.go:386: mixed-version test failure while running step 22 (run "run backup"): pq: failed to create blob client: connecting to node 1: failed to connect to n1 at 10.142.1.165:26257: grpc: node unavailable; try another peer [code 2/Unknown]
(1) test failed:
  | test random seed: 374411616843294270 (use COCKROACH_RANDOM_SEED to reproduce)
  |
  |                                                    n1          n2          n3          n4
  | released versions (system)                         v24.2.2     v24.2.2     v24.2.2     master
  | binary versions (system)                           24.2        24.2        24.2        24.2-upgrading-to-24.3-step-020
  | cluster versions (system)                          24.2        24.2        24.2        24.2
  | released versions (mixed-version-tenant-2rzqh)     v24.2.2     v24.2.2     v24.2.2     v24.2.2
  | binary versions (mixed-version-tenant-2rzqh)       24.2        24.2        24.2        24.2
  | cluster versions (mixed-version-tenant-2rzqh)      24.2        24.2        24.2        24.2
```

The error message includes a few pieces of information worth highlighting:

* The _random seed_ used this run: `374411616843294270` (see [reproducing-failures](#reproducing-failures));
* The _released versions_ running on each service (system and tenant).
* The _binary versions_ reported by each service in the test. Note that this different from the released binary version (i.e., `v23.1.3`); it is, instead, the value reported by `crdb_internal.node_executable_version()`.
* The _cluster version_ reported by each service before the step that failed started. Note that if the upgrade is _finalizing_ at the time of the failure, these cluster versions may not be exactly as reported by the time the test failed. During finalization, the cluster version is advancing, as migrations run, concurrently with the test step itself.

Finally, notice that the error message includes which step caused the failure (`mixed-version test failure while running step 22 (run "run backup")`). If we refer to the test plan for this execution (included below), it is also possible to understand what happened in the test up to that point which should provide some insight into possible reasons for the failure.

``` text
[mixed-version-test] 2024/09/27 11:52:50 mixedversion.go:734: mixed-version test:
Seed:               374411616843294270
Upgrades:           v24.2.2 → master
Deployment mode:    separate-process
Plan:
├── install fixtures for version "v24.2.2" (1)
├── start cluster at version "v24.2.2" (2)
├── wait for all nodes (:1-4) to acknowledge cluster version '24.2' on system tenant (3)
├── start separate process virtual cluster mixed-version-tenant-2rzqh with binary version v24.2.2 (4)
├── wait for all nodes (:1-4) to acknowledge cluster version '24.2' on mixed-version-tenant-2rzqh tenant (5)
├── set cluster setting "spanconfig.tenant_limit" to '50000' on mixed-version-tenant-2rzqh tenant (6)
├── set cluster setting "server.secondary_tenants.authorization.mode" to 'allow-all' on system tenant (7)
└── upgrade cluster from "v24.2.2" to "master"
   ├── upgrade storage cluster
   │   ├── prevent auto-upgrades on system tenant by setting `preserve_downgrade_option` (8)
   │   ├── upgrade nodes :1-4 from "v24.2.2" to "master"
   │   │   ├── restart system server on node 3 with binary version master (9)
   │   │   ├── restart system server on node 1 with binary version master (10)
   │   │   ├── restart system server on node 2 with binary version master (11)
   │   │   ├── run mixed-version hooks concurrently
   │   │   │   ├── run "run backup", after 5s delay (12)
   │   │   │   └── run "test features", after 30s delay (13)
   │   │   └── restart system server on node 4 with binary version master (14)
   │   ├── downgrade nodes :1-4 from "master" to "v24.2.2"
   │   │   ├── restart system server on node 3 with binary version v24.2.2 (15)
   │   │   ├── restart system server on node 4 with binary version v24.2.2 (16)
   │   │   ├── run "run backup" (17)
   │   │   ├── restart system server on node 2 with binary version v24.2.2 (18)
   │   │   ├── run "test features" (19)
   │   │   └── restart system server on node 1 with binary version v24.2.2 (20)
   │   ├── upgrade nodes :1-4 from "v24.2.2" to "master"
   │   │   ├── restart system server on node 4 with binary version master (21)
   │   │   ├── run "run backup" (22)
   │   │   ├── restart system server on node 1 with binary version master (23)
   │   │   ├── restart system server on node 2 with binary version master (24)
   │   │   ├── run "test features" (25)
   │   │   └── restart system server on node 3 with binary version master (26)
   │   ├── allow upgrade to happen on system tenant by resetting `preserve_downgrade_option` (27)
   │   ├── run "test features" (28)
   │   └── wait for all nodes (:1-4) to acknowledge cluster version <current> on system tenant (29)
   └── upgrade tenant
      ├── upgrade nodes :1-4 from "v24.2.2" to "master"
      │   ├── restart mixed-version-tenant-2rzqh server on node 2 with binary version master (30)
      │   ├── run "test features" (31)
      │   ├── restart mixed-version-tenant-2rzqh server on node 4 with binary version master (32)
      │   ├── run "run backup" (33)
      │   ├── restart mixed-version-tenant-2rzqh server on node 3 with binary version master (34)
      │   └── restart mixed-version-tenant-2rzqh server on node 1 with binary version master (35)
      ├── downgrade nodes :1-4 from "master" to "v24.2.2"
      │   ├── restart mixed-version-tenant-2rzqh server on node 3 with binary version v24.2.2 (36)
      │   ├── restart mixed-version-tenant-2rzqh server on node 2 with binary version v24.2.2 (37)
      │   ├── restart mixed-version-tenant-2rzqh server on node 1 with binary version v24.2.2 (38)
      │   ├── run mixed-version hooks concurrently
      │   │   ├── run "run backup", after 100ms delay (39)
      │   │   └── run "test features", after 100ms delay (40)
      │   └── restart mixed-version-tenant-2rzqh server on node 4 with binary version v24.2.2 (41)
      ├── upgrade nodes :1-4 from "v24.2.2" to "master"
      │   ├── restart mixed-version-tenant-2rzqh server on node 2 with binary version master (42)
      │   ├── run "run backup" (43)
      │   ├── restart mixed-version-tenant-2rzqh server on node 1 with binary version master (44)
      │   ├── run "test features" (45)
      │   ├── restart mixed-version-tenant-2rzqh server on node 4 with binary version master (46)
      │   └── restart mixed-version-tenant-2rzqh server on node 3 with binary version master (47)
      ├── run following steps concurrently
      │   ├── set `version` to <current> on mixed-version-tenant-2rzqh tenant, after 0s delay (48)
      │   └── run "run backup", after 500ms delay (49)
      └── wait for all nodes (:1-4) to acknowledge cluster version <current> on mixed-version-tenant-2rzqh tenant (50)
```

##### Aside: delay in concurrent steps

In the plan above, we can see a delay is automatically added when running some steps concurrently (`after 100ms delay` on step 39, as an example). This delay is inserted automatically by the `mixedversion` framework to explore different orderings when multiple steps run concurrently. Tests should not have to worry about it and the delay is completely transparent to the function.

#### Reproducing failures

Needless to say, CockroachDB's behaviour in a roachtest is highly non-deterministic. In that sense, there is no guaranteed way to reproduce a roachtest failure, `mixedversion` or otherwise.

That said, there are measures we can take to make failures _more likely_ to be reproducible. When a [failure occurs](#understanding-the-test-failure), the `mixedversion` framework logs the _random seed_ used in that particular run. By re-running the test with the same seed, we guarantee that:

* the same test plan is generated (i.e., ordering of events, as performed by the test, is the same). Note that, at the moment, this is only guaranteed if `roachtest` is built on the same SHA as the one used when the failure occurred;
* random values used by user hooks are also the same. Note that this only leads to the same set of random decisions if user hooks are diligent about [using the `*rand.Rand` instance passed to them](#embrace-randomness).

In practice, we have observed that this goes a long way towards reproducing issues. To run a `mixedversion` test with a specific seed, set the `COCKROACH_RANDOM_SEED` environment variable accordingly, as illustrated below:

``` shell
$ COCKROACH_RANDOM_SEED=374411616843294270 roachtest run acceptance/version-upgrade
```

#### Using instrumented binaries

To help understand what is happening inside cockroach when a failure happens, it's common to run tests with instrumented binaries (e.g., including changes to increase log verbosity, enabling different profiling options, etc.) In order to use a different binary for what represents the "current" cockroach binary, use roachtest's `--cockroach` flag.

In addition, it is also possible to use custom binaries for _previous_ cockroach versions (for instance, if we are debugging a failure that happens during a test that exercises the `24.2.x` to `24.3.x` upgrade path, we might want to use a custom build of the `24.2.x` binary used in the test). In order to achieve that:

* Identify the patch release used in the test. For example, in the test plan [above](#understanding-the-test-failure), the test used the `24.2.2` release (see message at the top of the test plan: `Upgrades: v24.2.2 → master`);
* Build the modified binary, preferably by applying the desired patch on top of the patch release used in the test. Let's say you call this binary `cockroach-24.2.2-custom`.
* Invoke roachtest with the `versions-binary-override` flag.

The following command can be used as a reproduction attempt for the failure [above](#understanding-the-test-failure), using a custom build of the 24.2.2 release:

``` shell
$ COCKROACH_RANDOM_SEED=374411616843294270 roachtest run --versions-binary-override "24.2.2=./cockroach-22.2.10-custom" acceptance/version-upgrade
```

### Final Notes

* This is a high level document and does not include API documentation. The `mixedversion` package includes a lot of documentation in the form of source code comments, and that should be the source of truth when it comes to finding out what functionality is available and how to use it. Most of the public API is in the [`mixedversion.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/mixedversion.go) and [`helper.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachtestutil/mixedversion/helper.go) files.
* For a simple application of the `mixedversion` framework, check out the `acceptance/version-upgrade` roachtest. For a more complex example, see `backup-restore/mixed-version`.
* For any other questions, please reach out to `#test-eng`!
