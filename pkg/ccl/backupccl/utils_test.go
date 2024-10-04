// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	singleNode                  = backuptestutils.SingleNode
	multiNode                   = backuptestutils.MultiNode
	backupRestoreDefaultRanges  = 10
	backupRestoreRowPayloadSize = 100
	localFoo                    = "nodelocal://1/foo"
)

// InitManualReplication calls tc.ToggleReplicateQueues(false).
//
// Note that the test harnesses that use this typically call
// tc.WaitForFullReplication before calling this method,
// so up-replication has usually already taken place.
var InitManualReplication = backuptestutils.InitManualReplication

func backupRestoreTestSetupWithParams(
	t testing.TB,
	clusterSize int,
	numAccounts int,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	return backuptestutils.StartBackupRestoreTestCluster(t, clusterSize,
		backuptestutils.WithInitFunc(init),
		backuptestutils.WithParams(params),
		backuptestutils.WithBank(numAccounts))
}

func backupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	// TODO (msbutler): The DefaultTestTenant should be disabled by the caller of this function
	return backupRestoreTestSetupWithParams(t, clusterSize, numAccounts, init,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TODOTestTenantDisabled,
			}})
}

func backupRestoreTestSetupEmpty(
	t testing.TB,
	clusterSize int,
	tempDir string,
	init func(*testcluster.TestCluster),
	params base.TestClusterArgs,
) (*testcluster.TestCluster, *sqlutils.SQLRunner, func()) {
	// TODO (msbutler): this should be disabled by callers of this function
	params.ServerArgs.DefaultTestTenant = base.TODOTestTenantDisabled
	tc, sqlDB, _, cleanup := backuptestutils.StartBackupRestoreTestCluster(t, clusterSize,
		backuptestutils.WithTempDir(tempDir),
		backuptestutils.WithInitFunc(init),
		backuptestutils.WithParams(params))
	return tc, sqlDB, cleanup
}

// getStatsQuery returns a SQL query that will return the properties of the
// statistics on a table that are expected to remain the same after being
// restored on a new cluster.
func getStatsQuery(tableName string) string {
	return fmt.Sprintf(`SELECT
	  statistics_name,
	  column_names,
	  row_count,
	  distinct_count,
	  null_count
	FROM [SHOW STATISTICS FOR TABLE %s]`, tableName)
}

// injectStats directly injects some arbitrary statistic into a given table for
// a specified column.
// See injectStatsWithRowCount.
func injectStats(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string, columnName string,
) [][]string {
	return injectStatsWithRowCount(t, sqlDB, tableName, columnName, 100 /* rowCount */)
}

// injectStatsWithRowCount directly injects some statistics specifying some row
// count for a column in the given table.
// N.B. This should be used in backup testing over CREATE STATISTICS since it
// ensures that the stats cache will be up to date during a subsequent BACKUP.
func injectStatsWithRowCount(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string, columnName string, rowCount int,
) [][]string {
	sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s INJECT STATISTICS '[
	{
		"columns": ["%s"],
		"created_at": "2018-01-01 1:00:00.00000+00:00",
		"row_count": %d,
		"distinct_count": %d
	}
	]'`, tableName, columnName, rowCount, rowCount))
	return sqlDB.QueryStr(t, getStatsQuery(tableName))
}

func makeInsecureHTTPServer(t *testing.T) (string, func()) {
	t.Helper()

	const badHeadResponse = "bad-head-response"

	tmp, dirCleanup := testutils.TempDir(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localfile := filepath.Join(tmp, filepath.Base(r.URL.Path))
		switch r.Method {
		case "PUT":
			f, err := os.Create(localfile)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			defer f.Close()
			if _, err := io.Copy(f, r.Body); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.WriteHeader(201)
		case "GET", "HEAD":
			if filepath.Base(localfile) == badHeadResponse {
				http.Error(w, "HEAD not implemented", 500)
				return
			}
			http.ServeFile(w, r, localfile)
		case "DELETE":
			if err := os.Remove(localfile); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.WriteHeader(204)
		default:
			http.Error(w, "unsupported method "+r.Method, 400)
		}
	}))

	cleanup := func() {
		srv.Close()
		dirCleanup()
	}

	t.Logf("Mock HTTP Storage %q", srv.URL)
	uri, err := url.Parse(srv.URL)
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	uri.Path = filepath.Join(uri.Path, "testing")
	return uri.String(), cleanup
}

// thresholdBlocker is a small wrapper around channels that are commonly used to
// block operations during testing.
// For example, it can be used in conjection with the RunBeforeBackfillChunk and
// BulkAdderFlushesEveryBatch cluster settings. The SQLSchemaChanger knob can be
// used to control the chunk size.
type thresholdBlocker struct {
	threshold        int
	reachedThreshold chan struct{}
	canProceed       chan struct{}
}

func (t thresholdBlocker) maybeBlock(count int) {
	if count == t.threshold {
		close(t.reachedThreshold)
		<-t.canProceed
	}
}

func (t thresholdBlocker) waitUntilBlocked() {
	<-t.reachedThreshold
}

func (t thresholdBlocker) allowToProceed() {
	close(t.canProceed)
}

func makeThresholdBlocker(threshold int) thresholdBlocker {
	return thresholdBlocker{
		threshold:        threshold,
		reachedThreshold: make(chan struct{}),
		canProceed:       make(chan struct{}),
	}
}

// getSpansFromManifest returns the spans that describe the data included in a
// given backup.
func getSpansFromManifest(ctx context.Context, t *testing.T, backupPath string) roachpb.Spans {
	backupManifestBytes, err := os.ReadFile(backupPath + "/" + backupbase.BackupManifestName)
	require.NoError(t, err)
	var backupManifest backuppb.BackupManifest
	decompressedBytes, err := backupinfo.DecompressData(ctx, nil, backupManifestBytes)
	require.NoError(t, err)
	require.NoError(t, protoutil.Unmarshal(decompressedBytes, &backupManifest))
	spans := make([]roachpb.Span, 0, len(backupManifest.Files))
	for _, file := range backupManifest.Files {
		spans = append(spans, file.Span)
	}
	mergedSpans, _ := roachpb.MergeSpans(&spans)
	return mergedSpans
}

func getKVCount(
	ctx context.Context, kvDB *kv.DB, codec keys.SQLCodec, dbName, tableName string,
) (int, error) {
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, dbName, tableName)
	tablePrefix := codec.TablePrefix(uint32(tableDesc.GetID()))
	tableEnd := tablePrefix.PrefixEnd()
	kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0)
	return len(kvs), err
}

// uriFmtStringAndArgs returns format strings like "$1" or "($1, $2, $3)" and
// an []interface{} of URIs for the BACKUP/RESTORE queries.
//
// Passing startIndex=i will start the fmt strings at $i+1. This can be useful
// when formatting different blocks of strings/args in the same query.
func uriFmtStringAndArgs(uris []string, startIndex int) (string, []interface{}) {
	urisForFormat := make([]interface{}, len(uris))
	var fmtString strings.Builder
	if len(uris) > 1 {
		fmtString.WriteString("(")
	}
	for i, uri := range uris {
		if i > 0 {
			fmtString.WriteString(", ")
		}
		fmtString.WriteString(fmt.Sprintf("$%d", startIndex+i+1))
		urisForFormat[i] = uri
	}
	if len(uris) > 1 {
		fmtString.WriteString(")")
	}
	return fmtString.String(), urisForFormat
}

// waitForTableSplit waits for the dbName.tableName range to split. This is
// often used by tests that rely on SpanConfig fields being applied to the table
// span.
func waitForTableSplit(t *testing.T, conn *gosql.DB, tableName, dbName string) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		count := 0
		if err := conn.QueryRow(
			fmt.Sprintf("SELECT count(*) FROM [SHOW RANGES FROM TABLE %s.%s]",
				tree.NameString(dbName), tree.NameString(tableName))).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return errors.New("waiting for table split")
		}
		return nil
	})
}

func getTableStartKey(t *testing.T, conn *gosql.DB, tableName, dbName string) roachpb.Key {
	t.Helper()
	row := conn.QueryRow(
		fmt.Sprintf(`SELECT crdb_internal.table_span('%s.%s'::regclass::oid::int)[1]`,
			tree.NameString(dbName), tree.NameString(tableName)))
	var startKey roachpb.Key
	require.NoError(t, row.Scan(&startKey))
	return startKey
}

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl = store.LookupReplica(roachpb.RKey(key))
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		return nil
	})
	return store, repl
}

func getStoreAndReplica(
	t *testing.T, tc *testcluster.TestCluster, conn *gosql.DB, tableName, dbName string,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	startKey := getTableStartKey(t, conn, tableName, dbName)

	// Okay great now we have a key and can go find replicas and stores and what not.
	r := tc.LookupRangeOrFatal(t, startKey)

	var l roachpb.Lease
	testutils.SucceedsSoon(t, func() error {
		var err error
		l, _, err = tc.FindRangeLease(r, nil)
		if err != nil {
			return err
		}
		if l.Replica.NodeID == 0 {
			return errors.New("range does not have a lease yet")
		}
		return nil
	})

	lhServer := tc.Server(int(l.Replica.NodeID) - 1)
	return getFirstStoreReplica(t, lhServer, startKey)
}

// waitForReplicaFieldToBeSet can be used to wait for the replica corresponding
// to `dbName.tableName` to have a field set on it.
func waitForReplicaFieldToBeSet(
	t *testing.T,
	tc *testcluster.TestCluster,
	conn *gosql.DB,
	tableName, dbName string,
	isReplicaFieldSet func(r *kvserver.Replica) (bool, error),
) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		_, r := getStoreAndReplica(t, tc, conn, tableName, dbName)
		if isSet, err := isReplicaFieldSet(r); !isSet {
			return err
		}
		return nil
	})
}

func thresholdFromTrace(t *testing.T, traceString string) hlc.Timestamp {
	t.Helper()
	thresholdRE := regexp.MustCompile(`(?s).*Threshold:(?P<threshold>[^\s]*)`)
	threshStr := string(thresholdRE.ExpandString(nil, "$threshold",
		traceString, thresholdRE.FindStringSubmatchIndex(traceString)))
	thresh, err := hlc.ParseTimestamp(threshStr)
	require.NoError(t, err)
	return thresh
}

func setAndWaitForSystemVisibleClusterSetting(
	t *testing.T,
	settingName settings.SettingName,
	systemTenantRunner *sqlutils.SQLRunner,
	tenantRunner *sqlutils.SQLRunner,
	tenantID roachpb.TenantID,
	val string,
) {
	t.Helper()
	systemTenantRunner.Exec(
		t,
		fmt.Sprintf(
			"ALTER TENANT [$1] SET CLUSTER SETTING %s = '%s'",
			settingName,
			val,
		),
		tenantID.ToUint64(),
	)

	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		tenantRunner.QueryRow(t,
			fmt.Sprintf(
				"SHOW CLUSTER SETTING %s", settingName,
			),
		).Scan(&currentVal)

		if currentVal != val {
			return errors.Newf("waiting for cluster setting to be set to %q", val)
		}
		return nil
	})
}

// runGCAndCheckTrace manually enqueues the replica with
// start_pretty=startPretty and runs `checkGCTrace` until it succeeds.
func runGCAndCheckTraceForSecondaryTenant(
	ctx context.Context,
	t *testing.T,
	tc *testcluster.TestCluster,
	runner *sqlutils.SQLRunner,
	skipShouldQueue bool,
	startPretty string,
	checkGCTrace func(traceStr string) error,
) {
	t.Helper()
	var startKey roachpb.Key
	testutils.SucceedsSoon(t, func() error {
		err := runner.DB.QueryRowContext(ctx, fmt.Sprintf(`
SELECT start_key FROM crdb_internal.ranges_no_leases
WHERE start_pretty LIKE '%s' ORDER BY start_key ASC`, startPretty)).Scan(&startKey)
		if err != nil {
			return errors.Wrap(err, "failed to query start_key")
		}
		return nil
	})

	r := tc.LookupRangeOrFatal(t, startKey)
	l, _, err := tc.FindRangeLease(r, nil)
	require.NoError(t, err)
	lhServer := tc.Server(int(l.Replica.NodeID) - 1)
	s, repl := getFirstStoreReplica(t, lhServer, startKey)
	testutils.SucceedsSoon(t, func() error {
		trace, _, err := s.Enqueue(ctx, "mvccGC", repl, skipShouldQueue, false /* async */)
		require.NoError(t, err)
		return checkGCTrace(trace.String())
	})
}

// runGCAndCheckTraceOnCluster manually enqueues the replica corresponding to
// `databaseName.tableName` in the mvccGC queue, and runs `checkGCTrace` until
// it succeeds.
func runGCAndCheckTraceOnCluster(
	ctx context.Context,
	t *testing.T,
	tc *testcluster.TestCluster,
	runner *sqlutils.SQLRunner,
	skipShouldQueue bool,
	databaseName, tableName string,
	checkGCTrace func(traceStr string) error,
) {
	t.Helper()
	var startKey roachpb.Key
	testutils.SucceedsSoon(t, func() error {
		err := runner.DB.QueryRowContext(ctx, fmt.Sprintf(`
SELECT raw_start_key
FROM [SHOW RANGES FROM TABLE %s.%s WITH KEYS]
ORDER BY raw_start_key ASC`, tree.NameString(databaseName), tree.NameString(tableName))).Scan(&startKey)
		if err != nil {
			return errors.Wrap(err, "failed to query start_key ")
		}
		return nil
	})
	r := tc.LookupRangeOrFatal(t, startKey)
	l, _, err := tc.FindRangeLease(r, nil)
	require.NoError(t, err)
	lhServer := tc.ServerConn(int(l.Replica.NodeID) - 1)
	lhSQLDB := sqlutils.MakeSQLRunner(lhServer)
	testutils.SucceedsSoon(t, func() error {
		trace := runGCWithTrace(t, lhSQLDB, skipShouldQueue, databaseName, tableName)
		return checkGCTrace(trace)
	})
}

func runGCWithTrace(
	t *testing.T, sqlDB *sqlutils.SQLRunner, skipShouldQueue bool, databaseName, tableName string,
) string {
	// Grab the range ID of the table and manually enqueue it in the mvccGC queue.
	var rangeID int
	sqlDB.QueryRow(t, fmt.Sprintf(`SELECT range_id FROM [SHOW RANGES FROM TABLE %s.%s]`,
		databaseName, tableName)).Scan(&rangeID)

	var trace string
	sqlDB.QueryRow(t, fmt.Sprintf(
		`SELECT crdb_internal.kv_enqueue_replica(%d, 'mvccGC', %t, true)`, rangeID, skipShouldQueue)).Scan(&trace)
	return trace
}

// upsertUntilBackpressure upserts data into a table until we see a
// `backpressure` error. This is usually used to build up a long chain of
// garbage revisions to make a range eligible for GC.
func upsertUntilBackpressure(
	t *testing.T, rRand *rand.Rand, conn *gosql.DB, database, table string,
) {
	t.Helper()
	for i := 1; i < 50; i++ {
		_, err := conn.Exec(fmt.Sprintf("UPSERT INTO %s.%s VALUES (1, $1)", database, table),
			randutil.RandBytes(rRand, 5<<20))
		if testutils.IsError(err, "backpressure") {
			return
		}
	}
	assert.Fail(t, "expected `backpressure` error")
}

// requireRecoveryEvent fetches all available log entries on disk after
// startTime, and requires that the first RecoveryEvent of recoveryType matches
// the expected event. requireRecoveryEvent will fail the test if the first
// event does not match what's expected or if no RecoveryEvents of recoveryType
// appear in the logs after a preset timeout.
func requireRecoveryEvent(
	t *testing.T,
	startTime int64,
	recoveryType eventpb.RecoveryEventType,
	expected eventpb.RecoveryEvent,
) {
	testutils.SucceedsSoon(t, func() error {
		log.FlushFiles()
		entries, err := log.FetchEntriesFromFiles(
			startTime,
			math.MaxInt64,
			1000,
			regexp.MustCompile(fmt.Sprintf(`"EventType":"recovery_event".*"RecoveryType":"%s"`, recoveryType)),
			log.WithMarkedSensitiveData,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(entries) == 0 {
			return errors.New("structured entry for recovery event not found in logs")
		}

		sort.Slice(entries, func(a int, b int) bool {
			return entries[a].Time < entries[b].Time
		})

		jsonPayload := []byte(entries[0].Message)
		var actual eventpb.RecoveryEvent
		if err := json.Unmarshal(jsonPayload, &actual); err != nil {
			t.Errorf("unmarshalling %q: %v", entries[0].Message, err)
		}

		// Exclude Job ID and timestamp from the comparison by clearing those values
		// on the actual event.
		actual.Timestamp = 0
		actual.JobID = 0

		require.Equal(t, expected, actual)
		return nil
	})
}

// Verify that during restore, if a restore span has too many files to fit in
// the memory budget with a single SST iterator, the restore processor should
// repeatedly open and process iterators for as many files as can fit within the
// budget until the span is finished.
//
//lint:ignore U1000 unused
func runTestRestoreMemoryMonitoring(t *testing.T, numSplits, numInc, restoreProcessorMaxFiles int) {
	skip.WithIssue(t, 119836, "this functionality was never enabled and will likely be removed rather than enabled")
	const splitSize = 10
	numAccounts := numSplits * splitSize
	var expectedNumFiles int
	var actualNumFiles int
	restoreProcessorKnobCount := atomic.Uint32{}
	args := base.TestServerArgs{
		DefaultTestTenant: base.TODOTestTenantDisabled,
		SQLMemoryPoolSize: 1 << 30, // Large enough for all mem limit settings.
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
					RunAfterProcessingRestoreSpanEntry: func(ctx context.Context, entry *execinfrapb.RestoreSpanEntry) error {
						// The total size of the backup files should be less than the target
						// SST size, thus should all fit in one import span.
						require.Equal(t, actualNumFiles, len(entry.Files))
						restoreProcessorKnobCount.Add(1)
						return nil
					},
				},
			},
		},
	}
	params := base.TestClusterArgs{ServerArgs: args}
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, params)
	defer cleanupFn()

	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.restore.sst_memory_limit.enabled=true")
	sqlDB.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.restore_node_concurrency=2")

	// Add some splits in the table, and set the target file size to be something
	// small so that we get one flushed file per split in the backup.
	sqlDB.Exec(t, "ALTER TABLE data.bank SPLIT AT SELECT generate_series($1::INT, $2, $3)", 0, numAccounts, splitSize)
	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.file_size = '1b'")
	sqlDB.Exec(t, "BACKUP data.bank INTO 'userfile:///backup'")

	// Take some incremental backups after mutating some rows. Take note of the
	// splits that have been changed as that determines the number of incremental
	// files that are created.
	var numIncFiles int
	for i := 0; i < numInc; i++ {
		incSplitsWithFile := make(map[int]bool)

		for n := 0; n < 100; n++ {
			id := rand.Intn(numAccounts)
			sqlDB.Exec(t, `UPDATE data.bank SET balance = balance + 1 WHERE id = $1`, id)
			split := id / splitSize
			incSplitsWithFile[split] = true
		}

		sqlDB.Exec(t, `BACKUP data.bank INTO latest IN 'userfile:///backup' WITH revision_history`)
		numIncFiles += len(incSplitsWithFile)
	}

	// Verify the file counts in the backup is at least what's expected. The
	// actual number can be more due to elastic CPU preempting export responses.
	expectedNumFiles += numSplits + numIncFiles
	sqlDB.QueryRow(t, "SELECT count(*) FROM [SHOW BACKUP FILES FROM latest IN 'userfile:///backup']").Scan(&actualNumFiles)
	require.GreaterOrEqual(t, actualNumFiles, expectedNumFiles)

	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.restore.per_processor_memory_limit = $1", restoreProcessorMaxFiles*sstReaderOverheadBytesPerFile)

	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.bank FROM latest IN 'userfile:///backup' WITH OPTIONS (into_db='data2')")

	// Assert that the restore processor is processing the same span multiple
	// times, and the count is based on what's expected from the memory budget.
	// The expected number is just the ceiling of actualNumFiles/restoreProcessorMaxFiles.
	require.Equal(t, (actualNumFiles-1)/restoreProcessorMaxFiles+1, int(restoreProcessorKnobCount.Load()))

	// Verify data in the restored table.
	expectedFingerprints := sqlDB.QueryStr(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data.bank")
	actualFingerprints := sqlDB.QueryStr(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data2.bank")
	require.Equal(t, expectedFingerprints, actualFingerprints)
}
