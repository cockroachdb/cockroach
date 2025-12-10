// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type dataset interface {
	// init() initializes the dataset for use. It is called before any other
	// receivers for the dataset.
	init(ctx context.Context, c cluster.Cluster, l *logger.Logger) error

	// getTableName() returns the name of the table to import into.
	getTableName() string

	// getCreateTableStmt() returns the CREATE TABLE statement for the dataset.
	getCreateTableStmt() string

	// getDataURLs() returns a slice containing the URLs to import from.
	getDataURLs() []string

	// getFingerprint() returns a map containing the expected fingerprint for this
	// dataset, or 'nil' if the table has not been calibrated yet.
	getFingerprint() map[string]string
}

// staticDataset represents a statically created dataset stored in external
// storage. It's expected that each dataset type will fill in the struct with
// a custom init() function and then use the receivers on staticDataset to
// implement the rest of the dataset interface.
type staticDataset struct {
	tableName       string
	createTableStmt string
	dataURLs        []string
	fingerprint     map[string]string
}

func (sd *staticDataset) init(_ context.Context, _ cluster.Cluster, _ *logger.Logger) error {
	return nil
}
func (sd staticDataset) getTableName() string              { return sd.tableName }
func (sd staticDataset) getCreateTableStmt() string        { return sd.createTableStmt }
func (sd staticDataset) getDataURLs() []string             { return sd.dataURLs }
func (sd staticDataset) getFingerprint() map[string]string { return sd.fingerprint }

// tpchDataset represents a TPC-H dataset file stored on external storage.
type tpchDataset struct {
	staticDataset
}

// init() implements the dataset interface. We use scale factor 1 for local clusters and scale
// factor 100 for roachprod clusters.
func (tpch *tpchDataset) init(
	ctx context.Context, c cluster.Cluster, l *logger.Logger,
) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "initializing %s", tpch.tableName)
		}
	}()
	conn := c.Conn(ctx, l, 1)
	defer conn.Close()
	baseURL := "gs://cockroach-fixtures-us-east1/tpch-csv/"
	var scale string
	if c.IsLocal() {
		scale = "sf-1"
	} else {
		scale = "sf-100"
	}

	tpch.createTableStmt, err = readFileFromFixture(
		fmt.Sprintf("%s/schema/%s.sql?AUTH=implicit", baseURL, tpch.tableName), conn)
	if err != nil {
		return err
	}

	fingerprintURL := fmt.Sprintf("%s/%s/%s.fingerprint?AUTH=implicit", baseURL, scale, tpch.tableName)
	var fingerprint string
	fingerprint, err = readFileFromFixture(fingerprintURL, conn)
	if err != nil {
		// It's okay to not have fingerprints here because we may be doing a calibration run.
		if !strings.Contains(err.Error(), "gcs object does not exist") {
			return err
		}
	} else {
		err = json.Unmarshal([]byte(fingerprint), &tpch.fingerprint)
		if err != nil {
			return errors.Wrapf(err, "error unmarshalling expected table fingerprint: %s", fingerprint)
		}
	}

	tpch.dataURLs = make([]string, 0, 8)
	for i := 1; i <= 8; i++ {
		tpch.dataURLs = append(tpch.dataURLs,
			fmt.Sprintf("%s/%s/%s.tbl.%d?AUTH=implicit", baseURL, scale, tpch.tableName, i))
	}

	return nil
}

// newTPCHDataset() is a convenience function to quickly declare a TPC-H dataset by name.
func newTPCHDataset(name string) *tpchDataset {
	return &tpchDataset{
		staticDataset: staticDataset{
			tableName: name,
		},
	}
}

// datasets is the set of all known datasets to test with.
var datasets = map[string]dataset{
	"tpch/customer": newTPCHDataset("customer"),
	"tpch/lineitem": newTPCHDataset("lineitem"),
	"tpch/orders":   newTPCHDataset("orders"),
	"tpch/part":     newTPCHDataset("part"),
	"tpch/partsupp": newTPCHDataset("partsupp"),
	"tpch/supplier": newTPCHDataset("supplier"),
}

// readFileFromFixture() reads a URI by routing through the read_file() internal
// function to avoid authentication issues when reading from various clouds.
func readFileFromFixture(fixtureURI string, gatewayDB *gosql.DB) (string, error) {
	row := make([]byte, 0)
	err := gatewayDB.QueryRow(fmt.Sprintf(`SELECT crdb_internal.read_file('%s')`, fixtureURI)).Scan(&row)
	if err != nil {
		return "", err
	}
	return string(row), err
}

// The stringSource interface is a thin wrapper to allow tests to declare
// and build sets of datasets to test with.
type stringSource interface {
	Strings(rng *rand.Rand) []string
}

type One string

func (o One) Strings(_ *rand.Rand) []string { return []string{string(o)} }

type FromFunc func(rng *rand.Rand) []string

func (f FromFunc) Strings(rng *rand.Rand) []string { return f(rng) }

func allDatasets(_ *rand.Rand) []string {
	return slices.Collect(maps.Keys(datasets))
}

func nDatasets(rng *rand.Rand, n int) []string {
	allDatasets := allDatasets(rng)
	rng.Shuffle(len(allDatasets), func(i, j int) {
		allDatasets[i], allDatasets[j] = allDatasets[j], allDatasets[i]
	})
	return allDatasets[:n]
}

func anyThreeDatasets(rng *rand.Rand) []string {
	return nDatasets(rng, 3)
}

func anyDataset(rng *rand.Rand) []string {
	return nDatasets(rng, 1)
}

// importTestSpec represents a subtest within the import test.
type importTestSpec struct {
	// subtestName is the name to register for this test.
	subtestName string
	// nodes is a slice of cluster sizes to test with.
	nodes []int
	// if benchmark is set to true, register this test as a benchmark and
	// collect benchmarking artifacts.
	benchmark bool
	// manualOnly subtests are not registered to run nightly.
	manualOnly bool
	// calibrate indicates that it's not an error for table fingerprints to
	// be missing.
	calibrate bool
	// datasetNames is a list or generator of datasets that can be used
	// with this test.
	datasetNames stringSource

	// preTestHook is run after tables are created, but before the import starts.
	preTestHook func(context.Context, test.Test, cluster.Cluster, *rand.Rand)
	// importRunner is an alternate import runner.
	importRunner func(context.Context, test.Test, cluster.Cluster, *logger.Logger, *rand.Rand, dataset) error
}

var tests = []importTestSpec{
	// Generate fingerprints for datasets that don't have them. A calibrate run
	// will only generate fingerprints for datasets with no fingerprint at all. If
	// a dataset has a fingerprint that doesn't match the output of SHOW
	// FINGERPRINTS, first make sure this is not due to a product bug. If the
	// fingerprint for a dataset legitimately changes, the old fingerprint must be
	// removed before calibrate will generate a new one. Be aware that local tests
	// can only calibrate local datasets; for larger datasets, a roachprod cluster
	// is required.
	{
		subtestName:  "calibrate",
		nodes:        []int{4},
		manualOnly:   true,
		calibrate:    true,
		datasetNames: FromFunc(allDatasets),
	},
	// Small dataset for quickly iterating while developing this test.
	{
		subtestName:  "smoke",
		nodes:        []int{4},
		manualOnly:   true,
		datasetNames: One("tpch/supplier"),
	},
	// Basic test w/o injected failures.
	{
		subtestName:  "basic",
		nodes:        []int{4},
		datasetNames: FromFunc(anyDataset),
	},
	// Basic test w/benchmarking.
	{
		subtestName:  "benchmark",
		benchmark:    true,
		nodes:        []int{4},
		datasetNames: One("tpch/lineitem"),
	},
	// Basic test importing three datasets concurrently.
	{
		subtestName:  "concurrency",
		nodes:        []int{4},
		datasetNames: FromFunc(anyThreeDatasets),
	},
	// Test with a decommissioned node.
	{
		subtestName:  "decommissioned",
		nodes:        []int{4},
		datasetNames: FromFunc(anyDataset),
		preTestHook: func(ctx context.Context, t test.Test, c cluster.Cluster, _ *rand.Rand) {
			nodeToDecommission := 2
			t.Status(fmt.Sprintf("decommissioning node %d", nodeToDecommission))
			c.Run(ctx, option.WithNodes(c.Node(nodeToDecommission)),
				fmt.Sprintf(`./cockroach node decommission --self --wait=all --port={pgport:%d} --certs-dir=%s`,
					nodeToDecommission, install.CockroachNodeCertsDir))

			// Wait for a bit for node liveness leases to expire.
			time.Sleep(10 * time.Second)
		},
	},
	// Test job survival if a worker node is shutdown.
	{
		subtestName:  "nodeShutdown/worker",
		nodes:        []int{4},
		datasetNames: FromFunc(anyDataset),
		importRunner: func(ctx context.Context, t test.Test, c cluster.Cluster, l *logger.Logger, _ *rand.Rand, ds dataset) error {
			importConn := c.Conn(ctx, l, 2 /* gateway node */)
			defer importConn.Close()
			return executeNodeShutdown(ctx, t, c, defaultNodeShutdownConfig(c, 3 /* nodeToShutdown */),
				func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error) {
					return runAsyncImportJob(ctx, importConn, ds)
				})
		},
	},
	// Test job survival if the coordinator node is shutdown.
	{
		subtestName:  "nodeShutdown/coordinator",
		nodes:        []int{4},
		datasetNames: FromFunc(anyDataset),
		importRunner: func(ctx context.Context, t test.Test, c cluster.Cluster, l *logger.Logger, _ *rand.Rand, ds dataset) error {
			importConn := c.Conn(ctx, l, 2 /* gateway node */)
			defer importConn.Close()
			return executeNodeShutdown(ctx, t, c, defaultNodeShutdownConfig(c, 2 /* nodeToShutdown */),
				func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error) {
					return runAsyncImportJob(ctx, importConn, ds)
				})
		},
	},
	// Test cancellation of import jobs.
	{
		subtestName:  "cancellation",
		nodes:        []int{4},
		datasetNames: FromFunc(anyThreeDatasets),
		importRunner: importCancellationRunner,
	},
	// Test column families.
	{
		subtestName:  "colfam",
		nodes:        []int{4},
		datasetNames: FromFunc(anyDataset),
		preTestHook:  makeColumnFamilies,
	},
}

func registerImport(r registry.Registry) {
	// This may be excessively conservative. During a calibration run, lineitem
	// IMPORT took about four hours, with about another hour and a half to
	// fingerprint the imported table. See #68117.
	timeout := 10 * time.Hour
	for _, testSpec := range tests {
		suites := registry.Suites(registry.Nightly)
		if testSpec.manualOnly {
			suites = registry.ManualOnly
		}

		for _, distMerge := range []bool{false, true} {
			for _, numNodes := range testSpec.nodes {
				ts := testSpec
				numNodes := numNodes

				name := fmt.Sprintf("import/%s/distmerge=%v/nodes=%d", ts.subtestName, distMerge, numNodes)
				r.Add(registry.TestSpec{
					Name:              name,
					Owner:             registry.OwnerSQLQueries,
					Benchmark:         testSpec.benchmark,
					Timeout:           timeout,
					Cluster:           r.MakeClusterSpec(numNodes),
					CompatibleClouds:  registry.Clouds(spec.GCE, spec.Local),
					Suites:            suites,
					EncryptionSupport: registry.EncryptionMetamorphic,
					Leases:            registry.MetamorphicLeases,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runImportTest(ctx, t, c, ts, numNodes, timeout, distMerge)
					},
				})
			}
		}
	}
}

func runImportTest(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	testSpec importTestSpec,
	numNodes int,
	timeout time.Duration,
	useDistributedMerge bool,
) {
	rng, seed := randutil.NewTestRand()

	startOpts := roachtestutil.MaybeUseMemoryBudget(t, 50)
	startOpts.RoachprodOpts.ScheduleBackups = true
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings())

	datasetNames := testSpec.datasetNames.Strings(rng)
	t.Status(fmt.Sprintf("Starting import test '%s' using seed %d with datasets: %s",
		testSpec.subtestName, seed, strings.Join(datasetNames, ", ")))

	// Create and use a test database
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	_, err := conn.ExecContext(ctx, `CREATE DATABASE import_test`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `USE import_test`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		fmt.Sprintf(`SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = %v`, useDistributedMerge))
	require.NoError(t, err)

	// Initialize datasets and create tables.
	for _, name := range datasetNames {
		ds, ok := datasets[name]
		require.Truef(t, ok, "dataset '%s' not defined.", name)
		err = ds.init(ctx, c, t.L())
		require.NoError(t, err)
		if !testSpec.calibrate {
			require.NotNilf(t, ds.getFingerprint(),
				"dataset '%s' has no fingerprint. Run calibrate manually.", name)
		}
		_, err = conn.ExecContext(ctx, ds.getCreateTableStmt())
		require.NoError(t, err)
	}

	// If there's a pre-test hook, run it now.
	if testSpec.preTestHook != nil {
		t.Status("Running pre-test hook")
		testSpec.preTestHook(ctx, t, c, rng)
	}

	// For calibration runs, filter out datasets that have fingerprint files.
	if testSpec.calibrate {
		datasetNames = slices.DeleteFunc(datasetNames, func(n string) bool {
			return datasets[n].getFingerprint() != nil
		})
		t.Status(fmt.Sprintf("Calibrating datasets: %s", strings.Join(datasetNames, ", ")))
	}

	// If this is a test that does benchmarking, setup measurement and wait for nodes to be ready.
	tick := func() {}
	if testSpec.benchmark {
		t.Status("waiting for all nodes to be ready")
		exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
		var perfBuf *bytes.Buffer
		tick, perfBuf = initBulkJobPerfArtifacts(timeout, t, exporter)
		defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, c.Node(1), "")

		err = retry.ForDuration(30*time.Second, func() error {
			var nodes int
			err := conn.QueryRowContext(ctx,
				`SELECT count(*)
				   FROM crdb_internal.gossip_liveness
				  WHERE updated_at > now() - interval '8s'`).Scan(&nodes)
			// Don't retry on query error
			require.NoError(t, err)
			if numNodes != nodes {
				return errors.Errorf("expected %d nodes, got %d", numNodes, nodes)
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Start the disk usage logger for non-local tests.
	stopDiskUsageLogger := func() {}
	if !c.IsLocal() {
		dul := roachtestutil.NewDiskUsageLogger(t, c)
		t.Go(func(ctx context.Context, _ *logger.Logger) error {
			return dul.Runner(ctx)
		})
		var once sync.Once
		stopDiskUsageLogger = func() {
			once.Do(dul.Done)
		}
		defer stopDiskUsageLogger()
	}

	t.Status("test running")

	tick()
	m := t.NewGroup()
	for n, datasetName := range datasetNames {
		m.Go(func(ctx context.Context, l *logger.Logger) (err error) {
			ds := datasets[datasetName]
			defer func() {
				if err != nil {
					err = errors.Wrapf(err, "import %s", datasetName)
				}
			}()

			t.WorkerStatus("importing ", datasetName)
			defer t.WorkerStatus()

			// If this test has a custom import runner, use that, otherwise we just do a
			// synchronous import.
			if testSpec.importRunner != nil {
				return testSpec.importRunner(ctx, t, c, l, rng, ds)
			} else {
				importConn := c.Conn(ctx, l, (n%numNodes)+1)
				defer importConn.Close()
				return runSyncImportJob(ctx, importConn, ds)
			}
		})
	}
	m.Wait()
	tick()
	stopDiskUsageLogger()

	// Verify that we imported the correct data
	t.Status("validating imported data")
	m = t.NewGroup()
	for _, datasetName := range datasetNames {
		m.Go(func(ctx context.Context, l *logger.Logger) (err error) {
			ds := datasets[datasetName]
			defer func() {
				if err != nil {
					err = errors.Wrapf(err, "validate %s", datasetName)
				}
			}()

			t.WorkerStatus("validating ", datasetName)
			defer t.WorkerStatus()

			err = validateNoRowFragmentation(ctx, l, conn, ds.getTableName())
			if err != nil {
				return err
			}

			var rows *gosql.Rows
			rows, err = conn.Query(fmt.Sprintf(`SHOW FINGERPRINTS FROM TABLE import_test.%s`, ds.getTableName()))
			if err != nil {
				return err
			}

			fingerprint := make(map[string]string)
			for rows.Next() {
				var idxname, hash string
				err = rows.Scan(&idxname, &hash)
				if err != nil {
					return err
				}
				fingerprint[idxname] = hash
			}
			if err = rows.Err(); err != nil {
				return err
			}
			if ds.getFingerprint() == nil {
				jsonFingerprint, err := json.MarshalIndent(&fingerprint, "", "  ")
				if err != nil {
					return err
				}
				l.Printf("Found fingerprint for '%s':\n%s", datasetName, jsonFingerprint)
			} else if !maps.Equal(fingerprint, ds.getFingerprint()) {
				return errors.Errorf("found fingerprint %v but expected %v", fingerprint, ds.getFingerprint())
			} else {
				l.Printf("Fingerprint for '%s' matched.", datasetName)
			}
			return nil
		})
	}
	m.Wait()
}

// validateNoRowFragmentation verifies that IMPORT did not split rows with multiple
// column families across range boundaries. It queries the table schema and range
// boundaries to ensure that no range starts with a key that includes a column family suffix.
func validateNoRowFragmentation(
	ctx context.Context, l *logger.Logger, conn *gosql.DB, tableName string,
) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "%s", tableName)
		}
	}()

	var rows *gosql.Rows
	rows, err = conn.QueryContext(ctx,
		fmt.Sprintf(`SELECT index_name,
							count(*)
					   FROM [SHOW INDEXES FROM import_test.%s]
					  WHERE NOT storing
				   GROUP BY index_name`, tableName))
	if err != nil {
		return err
	}

	keyLens := make(map[string]int)
	for rows.Next() {
		var keyName string
		var keyLen int
		err = rows.Scan(&keyName, &keyLen)
		if err != nil {
			return err
		}
		keyLens[keyName] = keyLen
	}
	if err = rows.Err(); err != nil {
		return err
	}

	for keyName, keyLen := range keyLens {
		l.Printf("Checking key %s with %d key columns for split rows", keyName, keyLen)

		rows, err = conn.QueryContext(ctx, fmt.Sprintf(
			`SELECT start_key, end_key, range_id FROM [SHOW RANGES FROM INDEX import_test.%s@%s]`,
			tableName, keyName))
		if err != nil {
			return err
		}

		for rows.Next() {
			var startKey, endKey string
			var rangeID int64
			err = rows.Scan(&startKey, &endKey, &rangeID)
			if err != nil {
				return errors.Wrapf(err, "%s", keyName)
			}

			if err = checkKeyForFamilySuffix(startKey, keyLen); err != nil {
				return errors.Wrapf(err, "%s start key", keyName)
			}
			if err = checkKeyForFamilySuffix(endKey, keyLen); err != nil {
				return errors.Wrapf(err, "%s end key", keyName)
			}
		}
		if err = rows.Err(); err != nil {
			return errors.Wrapf(err, "%s", keyName)
		}
	}

	return nil
}

// checkKeyForFamilySuffix checks if a pretty-printed key from SHOW RANGES contains
// a column family suffix, which would indicate a mid-row split. maxAllowed is the
// maximum number of key segments expected for this index (before family suffix).
func checkKeyForFamilySuffix(prettyKey string, maxAllowed int) error {
	// Skip special boundary markers
	if strings.HasPrefix(prettyKey, "<before:") || strings.HasPrefix(prettyKey, "<after:") ||
		strings.Contains(prettyKey, "Min>") || strings.Contains(prettyKey, "Max>") {
		return nil
	}

	numKeyParts := len(strings.Split(strings.TrimPrefix(prettyKey, "…/"), "/"))

	if numKeyParts > maxAllowed {
		return errors.Newf("%s shows a mid-row split", prettyKey)
	}
	return nil
}

// importCancellationRunner() is the test runner for the import cancellation
// test. This test makes a number of attempts at importing a dataset, cancelling
// all but the last. Each attempt imports a random subset of files from the
// dataset in an effort to perturb tombstone errors.
func importCancellationRunner(
	ctx context.Context, t test.Test, c cluster.Cluster, l *logger.Logger, rng *rand.Rand, ds dataset,
) error {
	conn := c.Conn(ctx, l, 1)
	defer conn.Close()

	// Set a random GC TTL that is relatively short. We want to exercise GC
	// of MVCC range tombstones, and would like a mix of live MVCC Range
	// Tombstones and the Pebble RangeKeyUnset tombstones that clear them.
	ttl_stmt := fmt.Sprintf(`ALTER TABLE import_test.%s CONFIGURE ZONE USING gc.ttlseconds = $1`, ds.getTableName())
	_, err := conn.ExecContext(ctx, ttl_stmt, randutil.RandIntInRange(rng, 10*60 /* 10m */, 20*60 /* 20m */))
	if err != nil {
		return errors.Wrapf(err, "%s", ttl_stmt)
	}

	numAttempts := randutil.RandIntInRange(rng, 2, 5)
	finalAttempt := numAttempts - 1
	urlsToImport := ds.getDataURLs()
	for attempt := range numAttempts {
		if len(urlsToImport) == 0 {
			break
		}
		urls := urlsToImport

		// If not the last attempt, import a subset of the files available.
		// This will create MVCC range tombstones across separate regions of
		// the table's keyspace.
		if attempt != finalAttempt {
			rng.Shuffle(len(urls), func(i, j int) {
				urls[i], urls[j] = urls[j], urls[i]
			})
			urls = urls[:randutil.RandIntInRange(rng, 1, len(urls)+1)]
		}

		t.WorkerStatus(fmt.Sprintf("beginning attempt %d for %s using files: %s", attempt+1, ds.getTableName(),
			strings.Join(urls, ", ")))

		var jobID jobspb.JobID
		jobID, err = runRawAsyncImportJob(ctx, conn, ds.getTableName(), urls)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if attempt != finalAttempt {
			var timeout time.Duration
			// Local tests tend to finish before we cancel if we use the full range.
			if c.IsLocal() {
				timeout = time.Duration(randutil.RandIntInRange(rng, 5, 20)) * time.Second
			} else {
				timeout = time.Duration(randutil.RandIntInRange(rng, 10, 60*3)) * time.Second
			}
			select {
			case <-time.After(timeout):
				t.WorkerStatus(fmt.Sprintf("Cancelling import job for attempt %d/%d for table %s after %v.",
					attempt+1, numAttempts, ds.getTableName(), timeout))
				_, err = conn.ExecContext(ctx,
					`CANCEL JOBS (WITH x AS (SHOW JOBS)
						        SELECT job_id
								  FROM x
								 WHERE job_id = $1
								   AND status IN ('pending', 'running', 'retrying'))`, jobID)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}

		// Block until the job is complete. Afterwards, it either completed
		// succesfully before we cancelled it, or the cancellation has finished
		// reverting the keys it wrote prior to cancellation.
		var status, errorMsg string
		err = conn.QueryRowContext(
			ctx,
			`WITH x AS (SHOW JOBS WHEN COMPLETE (SELECT $1)) SELECT status, error FROM x`,
			jobID,
		).Scan(&status, &errorMsg)
		if err != nil {
			return err
		}
		t.WorkerStatus(fmt.Sprintf("Import job for attempt %d/%d for table %s (%d files) completed with status %s.",
			attempt+1, numAttempts, ds.getTableName(), len(urls), status))

		// If the IMPORT was successful (eg, our cancellation came in too late),
		// remove the files that succeeded so we don't try to import them again.
		// If this was the last attempt, this should remove all the remaining
		// files and `filesToImport` should be empty.
		if status == "succeeded" {
			t.L().PrintfCtx(ctx, "Removing files [%s] from consideration; completed", strings.Join(urls, ", "))
			urlsToImport = slices.DeleteFunc(urlsToImport, func(url string) bool {
				return slices.Contains(urls, url)
			})
		} else if status == "failed" {
			return errors.Newf("Job %s failed with error: %s\n", jobID, errorMsg)
		}
	}
	if len(urlsToImport) != 0 {
		return errors.Newf("Expected zero remaining %q files after final attempt, but %d remaining.",
			ds.getTableName(), len(urlsToImport))
	}

	// Restore GC TTLs so we don't interfere with post-import table validation.
	_, err = conn.ExecContext(ctx, ttl_stmt, 60*60*4 /* 4 hours */)
	return err
}

// makeColumnFamilies() is a pre-test hook that changes the tables
// in import_test to use column families. To do this, we iterate the
// tables in the database, reading the schema for each table, modifying
// the schema and then re-creating the table. Because CRDB does not
// allow altering a column's family, we simply drop and re-create the
// entire table.
func makeColumnFamilies(ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand) {
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Read table names
	rows, err := conn.QueryContext(ctx, `SELECT table_name FROM [SHOW TABLES FROM import_test]`)
	require.NoError(t, err)
	var tableNames []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tableNames = append(tableNames, name)
	}
	require.NoError(t, rows.Err())

	for _, tableName := range tableNames {
		var createStmt string
		err = conn.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE TABLE import_test.%s]`,
				tableName)).Scan(&createStmt)
		require.NoError(t, err)

		createStmt, changed := randgen.ApplyString(rng, createStmt, randgen.ColumnFamilyMutator)
		if !changed {
			continue
		}

		oldFullyQualifiedTableName := fmt.Sprintf("public.%s", tableName)
		newFullyQualifiedTableName := fmt.Sprintf("import_test.%s", tableName)
		createStmt = strings.Replace(createStmt, oldFullyQualifiedTableName, newFullyQualifiedTableName, 1)

		t.L().Printf("Using: %s", createStmt)
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE import_test.%s`, tableName))
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, createStmt)
		require.NoError(t, err)
	}
}

// runSyncImportJob() runs an import job and waits for it to complete.
func runSyncImportJob(ctx context.Context, conn *gosql.DB, ds dataset) error {
	importStmt := formatImportStmt(ds.getTableName(), ds.getDataURLs(), false)
	_, err := conn.ExecContext(ctx, importStmt)
	if err != nil {
		err = errors.Wrapf(err, "%s", importStmt)
	}
	return err
}

// runAsyncImportJob() runs an import job and returns the job id immediately.
func runAsyncImportJob(ctx context.Context, conn *gosql.DB, ds dataset) (jobspb.JobID, error) {
	return runRawAsyncImportJob(ctx, conn, ds.getTableName(), ds.getDataURLs())
}

// runRawAsyncImportJob() runs an import job using the table name and files provided.
func runRawAsyncImportJob(
	ctx context.Context, conn *gosql.DB, tableName string, urls []string,
) (jobspb.JobID, error) {
	importStmt := formatImportStmt(tableName, urls, true)

	var jobID jobspb.JobID
	err := conn.QueryRowContext(ctx, importStmt).Scan(&jobID)
	if err != nil {
		err = errors.Wrapf(err, "%s", importStmt)
	}

	return jobID, err
}

// formatImportStmt() takes a dataset and formats a SQL import statment for that
// dataset.
func formatImportStmt(tableName string, urls []string, detached bool) string {
	var stmt strings.Builder
	fmt.Fprintf(&stmt, `IMPORT INTO import_test.%s CSV DATA ('%s') WITH delimiter='|'`,
		tableName, strings.Join(urls, "', '"))

	if detached {
		stmt.WriteString(", detached")
	}

	return stmt.String()
}

func registerImportTPCC(r registry.Registry) {
	runImportTPCC := func(ctx context.Context, t test.Test, c cluster.Cluster, testName string,
		timeout time.Duration, warehouses int) {
		t.Status("starting csv servers")
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
		c.Run(ctx, option.WithNodes(c.All()), `./cockroach workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

		t.Status("running workload")
		m := c.NewDeprecatedMonitor(ctx)
		dul := roachtestutil.NewDiskUsageLogger(t, c)
		m.Go(dul.Runner)

		exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
		tick, perfBuf := initBulkJobPerfArtifacts(timeout, t, exporter)
		defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, c.Node(1), "")

		workloadStr := `./cockroach workload fixtures import tpcc --warehouses=%d --csv-server='http://localhost:8081' {pgurl:1}`
		m.Go(func(ctx context.Context) error {
			defer dul.Done()
			if c.Spec().Geo {
				// Increase the retry duration in the geo config to harden the
				// test.
				c.Run(ctx, option.WithNodes(c.Node(1)), `./cockroach sql -e "SET CLUSTER SETTING bulkio.import.retry_duration = '20m';" --url={pgurl:1}`)
			}
			cmd := fmt.Sprintf(workloadStr, warehouses)
			// Tick once before starting the import, and once after to capture the
			// total elapsed time. This is used by roachperf to compute and display
			// the average MB/sec per node.
			tick()
			c.Run(ctx, option.WithNodes(c.Node(1)), cmd)
			tick()
			return nil
		})
		m.Wait()
	}

	const warehouses = 1000
	for _, numNodes := range []int{4, 32} {
		testName := fmt.Sprintf("import/tpcc/warehouses=%d/nodes=%d", warehouses, numNodes)
		timeout := 5 * time.Hour
		r.Add(registry.TestSpec{
			Name:              testName,
			Owner:             registry.OwnerSQLQueries,
			Benchmark:         true,
			Cluster:           r.MakeClusterSpec(numNodes),
			CompatibleClouds:  registry.AllExceptAWS,
			Suites:            registry.Suites(registry.Nightly),
			Timeout:           timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			PostProcessPerfMetrics: func(test string, histograms *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
				metricName := fmt.Sprintf("%s_elapsed", test)
				totalElapsed := histograms.Elapsed

				gb := int64(1 << 30)
				mb := int64(1 << 20)
				dataSizeInMB := (56 * gb) / mb
				backupDuration := int64(totalElapsed / 1000)
				avgRatePerNode := roachtestutil.MetricPoint(float64(dataSizeInMB) / float64(int64(numNodes)*backupDuration))

				return roachtestutil.AggregatedPerfMetrics{
					{
						Name:           metricName,
						Value:          avgRatePerNode,
						Unit:           "MB/s/node",
						IsHigherBetter: false,
					},
				}, nil
			},
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runImportTPCC(ctx, t, c, testName, timeout, warehouses)
			},
		})
	}
	const geoWarehouses = 4000
	const geoZones = "europe-west2-b,europe-west4-b,asia-northeast1-b,us-west1-b"
	testName := fmt.Sprintf("import/tpcc/warehouses=%d/geo", geoWarehouses)
	r.Add(registry.TestSpec{
		Name:              testName,
		Owner:             registry.OwnerSQLQueries,
		Cluster:           r.MakeClusterSpec(8, spec.CPU(16), spec.Geo(), spec.GCEZones(geoZones)),
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly),
		Timeout:           5 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runImportTPCC(ctx, t, c, testName, 5*time.Hour, geoWarehouses)
		},
	})
}
