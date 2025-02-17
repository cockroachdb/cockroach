// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test schema changes are retried and complete properly when there's an error
// in the merge step. This also checks that a mutation checkpoint reduces the
// size of the span operated on during a retry.
func TestIndexBackfillMergeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "this test fails under duress")

	params, _ := createTestServerParamsAllowTenants()

	writesPopulated := false
	var writesFn func() error

	populateTempIndexWithWrites := func(sp roachpb.Span) error {
		if !writesPopulated {
			if err := writesFn(); err != nil {
				return err
			}
			writesPopulated = true
		}

		return nil
	}

	mergeSerializeCh := make(chan struct{}, 1)
	mergeChunk := 0
	var seenKey roachpb.Key
	checkStartingKey := func(key roachpb.Key) error {
		mergeChunk++
		if mergeChunk == 3 {
			// Fail right before merging the 3rd chunk.
			if rand.Intn(2) == 0 {
				return context.DeadlineExceeded
			} else {
				errAmbiguous := &kvpb.AmbiguousResultError{}
				return kvpb.NewError(errAmbiguous).GoError()
			}
		}

		if seenKey != nil && key != nil {
			// Check that starting span keys are never reevaluated.
			if seenKey.Compare(key) >= 0 {
				t.Errorf("reprocessing starting with key %v, already seen starting key %v", key, seenKey)
			}
		}

		seenKey = key
		return nil
	}

	const maxValue = 2000
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			WriteCheckpointInterval:          time.Nanosecond,
			AlwaysUpdateIndexBackfillDetails: true,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk:                     populateTempIndexWithWrites,
			BulkAdderFlushesEveryBatch:                 true,
			SerializeIndexBackfillCreationAndIngestion: make(chan struct{}, 1),
			IndexBackfillMergerTestingKnobs: &backfill.IndexBackfillMergerTestingKnobs{
				PushesProgressEveryChunk: true,
				RunBeforeScanChunk:       checkStartingKey,
				RunAfterScanChunk: func() {
					<-mergeSerializeCh
				},
				RunAfterMergeChunk: func() {
					mergeSerializeCh <- struct{}{}
				},
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		GCJob: &sql.GCJobTestingKnobs{
			SkipWaitingForMVCCGC: true,
		},
		KeyVisualizer: &keyvisualizer.TestingKnobs{SkipJobBootstrap: true},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	codec := s.ApplicationLayer().Codec()

	if _, err := sqlDB.Exec(`
SET use_declarative_schema_changer='off';
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// TODO(rui): use testing hook instead of cluster setting once this value for
	// the backfill merge is hooked up to testing hooks.
	if _, err := sqlDB.Exec(fmt.Sprintf(`SET CLUSTER SETTING bulkio.index_backfill.batch_size = %d;`, maxValue/5)); err != nil {
		t.Fatal(err)
	}

	writesFn = func() error {
		if _, err := sqlDB.Exec(fmt.Sprintf(`UPDATE t.test SET v = v + %d WHERE k >= 0`, 2*maxValue)); err != nil {
			return err
		}

		if _, err := sqlDB.Exec(fmt.Sprintf(`UPDATE t.test SET v = v - %d WHERE k >= 0`, 2*maxValue)); err != nil {
			return err
		}
		return nil
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	addIndexSchemaChange(t, sqlDB, kvDB, codec, maxValue, 2, func() {
		if _, err := sqlDB.Exec("SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS])"); err != nil {
			t.Fatal(err)
		}
	})
	require.True(t, mergeChunk > 3, fmt.Sprintf("mergeChunk: %d", mergeChunk))
}

func TestIndexBackfillFractionTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()

	const (
		rowCount  = 2000
		chunkSize = rowCount / 10
	)

	var jobID jobspb.JobID
	var sqlRunner *sqlutils.SQLRunner
	var kvDB *kv.DB
	var tc serverutils.TestClusterInterface

	split := func(tableDesc catalog.TableDescriptor, idx catalog.Index) {
		numSplits := 25
		var sps []serverutils.SplitPoint
		for i := 0; i < numSplits; i++ {
			sps = append(sps, serverutils.SplitPoint{TargetNodeIdx: 0, Vals: []interface{}{((rowCount * 2) / numSplits) * i}})
		}
		require.NoError(t, splitIndex(tc, tableDesc, idx, sps))
	}

	// Chunks are processed concurrently, so we need to synchronize access to
	// lastPercentage.
	var mu syncutil.Mutex
	var lastPercentage float32
	assertFractionBetween := func(op string, min float32, max float32) {
		mu.Lock()
		defer mu.Unlock()
		var fraction float32
		sqlRunner.QueryRow(t, "SELECT fraction_completed FROM [SHOW JOBS] WHERE job_id = $1", jobID).Scan(&fraction)
		t.Logf("fraction during %s: %f", op, fraction)
		assert.True(t, fraction >= min)
		assert.True(t, fraction <= max)
		assert.True(t, fraction >= lastPercentage)
		lastPercentage = fraction
	}

	var codec keys.SQLCodec
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
			RunBeforeResume: func(id jobspb.JobID) error {
				jobID = id
				tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "t", "public", "test")
				split(tableDesc, tableDesc.GetPrimaryIndex())
				return nil
			},
			RunBeforeTempIndexMerge: func() {
				for i := rowCount + 1; i < (rowCount*2)+1; i++ {
					sqlRunner.Exec(t, "INSERT INTO t.test VALUES ($1, $1)", i)
				}
				tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "t", "public", "test")
				tempIdx, err := findCorrespondingTemporaryIndex(tableDesc, "new_idx")
				require.NoError(t, err)
				split(tableDesc, tempIdx)
			},
			AlwaysUpdateIndexBackfillDetails:  true,
			AlwaysUpdateIndexBackfillProgress: true,
		},
		DistSQL: &execinfra.TestingKnobs{
			BulkAdderFlushesEveryBatch: true,
			RunAfterBackfillChunk:      func() { assertFractionBetween("backfill", 0.0, 0.60) },
			IndexBackfillMergerTestingKnobs: &backfill.IndexBackfillMergerTestingKnobs{
				PushesProgressEveryChunk: true,
				RunBeforeScanChunk: func(_ roachpb.Key) error {
					assertFractionBetween("merge", 0.60, 1.00)
					return nil
				},
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	tc = serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      params,
	})
	defer tc.Stopper().Stop(context.Background())
	kvDB = tc.Server(0).DB()
	codec = tc.Server(0).Codec()
	sqlDB := tc.ServerConn(0)
	_, err := sqlDB.Exec("SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer='off';")
	require.NoError(t, err)
	_, err = sqlDB.Exec("SET use_declarative_schema_changer='off';")
	require.NoError(t, err)
	sqlRunner = sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `CREATE DATABASE t; CREATE TABLE t.test (k INT PRIMARY KEY, v INT)`)
	sqlRunner.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING bulkio.index_backfill.batch_size = %d;`, chunkSize))
	require.NoError(t, sqltestutils.BulkInsertIntoTable(sqlDB, rowCount))
	sqlRunner.Exec(t, "CREATE INDEX new_idx ON t.test(v)")
}

// Test index backfill merges are not affected by various operations that run
// simultaneously.
func TestRaceWithIndexBackfillMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "TODO(ssd) test times outs under race")

	// protects mergeNotification, writesPopulated
	var mu syncutil.Mutex
	var mergeNotification chan struct{}
	writesPopulated := false

	const numNodes = 5
	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows and a
		// correspondingly smaller chunk size.
		chunkSize = 5
		maxValue = 200
	}

	params, _ := createTestServerParamsAllowTenants()
	initMergeNotification := func() chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		mergeNotification = make(chan struct{})
		return mergeNotification
	}

	notifyMerge := func() {
		mu.Lock()
		defer mu.Unlock()
		if mergeNotification != nil {
			// Close channel to notify that the backfill has started.
			close(mergeNotification)
			mergeNotification = nil
		}
	}

	var sqlWritesFn func() error
	var idxName string
	var splitTemporaryIndex func() error

	populateTempIndexWithWrites := func(_ roachpb.Span) error {
		mu.Lock()
		defer mu.Unlock()
		if !writesPopulated {
			if err := sqlWritesFn(); err != nil {
				return err
			}

			// Split the temporary index so that the merge is distributed.
			if err := splitTemporaryIndex(); err != nil {
				return err
			}

			writesPopulated = true
		}

		return nil
	}

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: populateTempIndexWithWrites,
			IndexBackfillMergerTestingKnobs: &backfill.IndexBackfillMergerTestingKnobs{
				RunBeforeScanChunk: func(key roachpb.Key) error {
					notifyMerge()
					return nil
				},
			},
		},
	}

	tc := serverutils.StartCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.Background())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	codec := tc.Server(0).ApplicationLayer().Codec()
	_, err := sqlDB.Exec("SET use_declarative_schema_changer='off'")
	require.NoError(t, err)
	_, err = sqlDB.Exec("SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer='off'")
	require.NoError(t, err)

	splitTemporaryIndex = func() error {
		tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "t", "public", "test")
		tempIdx, err := findCorrespondingTemporaryIndex(tableDesc, idxName)
		if err != nil {
			return err
		}

		var sps []serverutils.SplitPoint
		for i := 0; i < numNodes; i++ {
			sps = append(sps, serverutils.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue/numNodes*i + 5*maxValue}})
		}

		return splitIndex(tc, tableDesc, tempIdx, sps)
	}

	sqlWritesFn = func() error {
		if _, err := sqlDB.Exec(fmt.Sprintf(`UPDATE t.test SET v = v + %d WHERE k >= 0`, 5*maxValue)); err != nil {
			return err
		}

		if _, err := sqlDB.Exec(fmt.Sprintf(`UPDATE t.test SET v = v - %d WHERE k >= 0`, 5*maxValue)); err != nil {
			return err
		}

		return nil
	}

	// TODO(rui): use testing hook instead of cluster setting once this value for
	// the backfill merge is hooked up to testing hooks.
	if _, err := sqlDB.Exec(fmt.Sprintf(`SET CLUSTER SETTING bulkio.index_backfill.merge_batch_size = %d`, chunkSize)); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, x DECIMAL DEFAULT (DECIMAL '1.4'));
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// number of keys == 2 * number of rows; 1 column family and 1 index entry
	// for each row.
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, codec, 1, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Run some index schema changes with operations.
	idxName = "foo"
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		codec,
		"CREATE UNIQUE INDEX foo ON t.test (v)",
		maxValue,
		2,
		initMergeNotification(),
		true,
	)

	idxName = "bar"
	writesPopulated = false
	// Add STORING index (that will have non-nil values).
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		codec,
		"CREATE INDEX bar ON t.test(k) STORING (v)",
		maxValue,
		3,
		initMergeNotification(),
		true,
	)

	// Verify that the index foo over v is consistent/
	rows, err := sqlDB.Query(`SELECT v, x from t.test@foo ORDER BY v`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var val int
		var x float64
		if err := rows.Scan(&val, &x); err != nil {
			t.Fatalf("row %d scan failed: %s", count, err)
		}
		if count != val {
			t.Fatalf("wrong index contents: expected %d, found %d", count, val)
		}
		if x != 1.4 {
			t.Fatalf("wrong index contents: expected %f, found %f", 1.4, x)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	eCount := maxValue + 1
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}
}

func TestInvertedIndexMergeEveryStateWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "very slow")

	var chunkSize int64 = 1000
	var initialRows = 10000
	rowIdx := 0

	params, _ := createTestServerParamsAllowTenants()
	var writeMore func() error
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeDescTxn:  func(_ jobspb.JobID) error { return writeMore() },
			BackfillChunkSize: chunkSize,
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	rnd, _ := randutil.NewPseudoRand()
	var mu syncutil.Mutex
	writeMore = func() error {
		mu.Lock()
		defer mu.Unlock()

		start := rowIdx
		rowIdx += 20
		for i := 1; i <= 20; i++ {
			json, err := json.Random(20, rnd)
			require.NoError(t, err)
			_, err = sqlDB.Exec("UPSERT INTO t.test VALUES ($1, $2)", start+i, json.String())
			require.NoError(t, err)
		}
		return nil
	}

	if _, err := sqlDB.Exec(fmt.Sprintf(`SET CLUSTER SETTING bulkio.index_backfill.merge_batch_size = %d`, chunkSize)); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v JSONB);
`); err != nil {
		t.Fatal(err)
	}

	// Initial insert
	for i := 0; i < initialRows; i++ {
		json, err := json.Random(20, rnd)
		require.NoError(t, err)
		_, err = sqlDB.Exec("INSERT INTO t.test VALUES ($1, $2)", i, json.String())
		require.NoError(t, err)
	}

	_, err := sqlDB.Exec("CREATE INVERTED INDEX invidx ON t.test (v)")
	require.NoError(t, err)
}

// TestIndexBackfillMergeTxnRetry tests that the merge completes
// successfully even in the face of a transaction retry.
func TestIndexBackfillMergeTxnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var retryOnce sync.Once
	var (
		s       serverutils.TestServerInterface
		kvDB    *kv.DB
		sqlDB   *gosql.DB
		scratch roachpb.Key
	)
	const (
		maxValue               = 200
		additionalRowsForMerge = 10
	)

	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Ensure that the temp index has work to do.
			RunBeforeTempIndexMerge: func() {
				for i := 1; i <= additionalRowsForMerge; i++ {
					_, err := sqlDB.Exec("INSERT INTO t.test VALUES ($1, $1)", maxValue+i)
					require.NoError(t, err)
				}
			},
		},
		DistSQL: &execinfra.TestingKnobs{
			IndexBackfillMergerTestingKnobs: &backfill.IndexBackfillMergerTestingKnobs{
				RunDuringMergeTxn: func(ctx context.Context, txn *kv.Txn, startKey roachpb.Key, endKey roachpb.Key) error {
					var err error
					retryOnce.Do(func() {
						t.Logf("forcing txn retry for [%s, %s)] using %s", startKey, endKey, scratch)
						if _, err := txn.Get(ctx, scratch); err != nil {
							require.NoError(t, err)
						}
						require.NoError(t, kvDB.Put(ctx, scratch, "foo"))
						// This hits WriteTooOldError, but we swallow the error to test the txn failing after the entire txn closure has run.
						_ = txn.Put(ctx, scratch, "bar")

					})
					return err
				},
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		GCJob: &sql.GCJobTestingKnobs{
			SkipWaitingForMVCCGC: true,
		},
		KeyVisualizer: &keyvisualizer.TestingKnobs{SkipJobBootstrap: true},
	}

	s, sqlDB, kvDB = serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	codec := s.ApplicationLayer().Codec()
	var err error
	scratch, err = s.ScratchRange()
	require.NoError(t, err)

	if _, err := sqlDB.Exec(`
SET use_declarative_schema_changer='off';
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(fmt.Sprintf(`SET CLUSTER SETTING bulkio.index_backfill.batch_size = %d;`, maxValue/5)); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}
	addIndexSchemaChange(t, sqlDB, kvDB, codec, maxValue+additionalRowsForMerge, 2, func() {
		if _, err := sqlDB.Exec("SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS])"); err != nil {
			t.Fatal(err)
		}
	})
}

func findCorrespondingTemporaryIndex(
	tableDesc catalog.TableDescriptor, idxName string,
) (catalog.Index, error) {
	idx := -1
	for i, mut := range tableDesc.AllMutations() {
		if mut.AsIndex() != nil && mut.AsIndex().GetName() == idxName {
			idx = i
		}
	}

	if idx == -1 {
		return nil, errors.Errorf("could not find an index mutation with name %s", idxName)
	}

	if idx+1 < len(tableDesc.AllMutations()) {
		tempIdxMut := tableDesc.AllMutations()[idx+1]
		if tempIdxMut.AsIndex() != nil && tempIdxMut.AsIndex().IndexDesc().UseDeletePreservingEncoding {
			return tempIdxMut.AsIndex(), nil
		}
	}

	return nil, errors.Errorf("could not find temporary index mutation for index %s", idxName)
}

type rangeAndKT struct {
	Range roachpb.RangeDescriptor
	KT    serverutils.KeyAndTargets
}

func splitIndex(
	tc serverutils.TestClusterInterface,
	desc catalog.TableDescriptor,
	index catalog.Index,
	sps []serverutils.SplitPoint,
) error {
	if tc.ReplicationMode() != base.ReplicationManual {
		return errors.Errorf("splitIndex called on a test cluster that was not in manual replication mode")
	}

	rkts := make(map[roachpb.RangeID]rangeAndKT)
	for _, sp := range sps {

		pik, err := randgen.TestingMakeSecondaryIndexKey(
			desc, index, tc.Server(0).Codec(), sp.Vals...)
		if err != nil {
			return err
		}

		rangeDesc, err := tc.LookupRange(pik)
		if err != nil {
			return err
		}

		li, _, err := tc.FindRangeLeaseEx(ctx, rangeDesc, nil)
		if err != nil {
			return err
		}
		holder := li.CurrentOrProspective().Replica

		_, rightRange, err := tc.Server(int(holder.NodeID) - 1).SplitRange(pik)
		if err != nil {
			return err
		}

		rightRangeStartKey := rightRange.StartKey.AsRawKey()
		target := tc.Target(sp.TargetNodeIdx)

		rkts[rightRange.RangeID] = rangeAndKT{
			rightRange,
			serverutils.KeyAndTargets{StartKey: rightRangeStartKey, Targets: []roachpb.ReplicationTarget{target}}}
	}

	var kts []serverutils.KeyAndTargets
	for _, rkt := range rkts {
		kts = append(kts, rkt.KT)
	}
	var descs []roachpb.RangeDescriptor
	for _, kt := range kts {
		desc, err := tc.AddVoters(kt.StartKey, kt.Targets...)
		if err != nil {
			if testutils.IsError(err, "trying to add a voter to a store that already has a VOTER_FULL") {
				desc, err = tc.LookupRange(kt.StartKey)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}

		descs = append(descs, desc)
	}

	for _, desc := range descs {
		rkt, ok := rkts[desc.RangeID]
		if !ok {
			continue
		}

		for _, target := range rkt.KT.Targets {
			if err := retry.WithMaxAttempts(context.Background(), retry.Options{
				MaxBackoff: time.Millisecond,
			}, 10, func() error {
				return tc.TransferRangeLease(desc, target)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestIndexMergeEveryChunkWrite tests the case where the workload
// writes sequentially into the key space faster than the merger and
// write.
func TestIndexMergeEveryChunkWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	chunkSize := 10
	rowsPerWrite := 20
	rowIdx := 0
	mergeSerializeCh := make(chan struct{}, 1)

	params, _ := createTestServerParamsAllowTenants()
	var writeMore func() error
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			IndexBackfillMergerTestingKnobs: &backfill.IndexBackfillMergerTestingKnobs{
				RunBeforeScanChunk: func(key roachpb.Key) error {
					return writeMore()
				},
				RunAfterScanChunk: func() {
					<-mergeSerializeCh
				},
				RunAfterMergeChunk: func() {
					mergeSerializeCh <- struct{}{}
				},
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	var mu syncutil.Mutex
	writeMore = func() error {
		mu.Lock()
		defer mu.Unlock()
		start := rowIdx
		rowIdx += rowsPerWrite
		for i := 1; i <= rowsPerWrite; i++ {
			if _, err := sqlDB.Exec("UPSERT INTO t.test VALUES ($1, $2)", start+i, start+i); err != nil {
				return err
			}
		}
		return nil
	}

	if _, err := sqlDB.Exec(fmt.Sprintf(`SET CLUSTER SETTING bulkio.index_backfill.merge_batch_size = %d`, chunkSize)); err != nil {
		t.Fatal(err)
	}

	_, err := sqlDB.Exec(`CREATE DATABASE t; CREATE TABLE t.test (k INT PRIMARY KEY, v int);`)
	require.NoError(t, err)
	require.NoError(t, writeMore(), "initial insert")
	_, err = sqlDB.Exec("CREATE INDEX ON t.test (v)")
	require.NoError(t, err)
}

// TestIndexMergeWithSplitsAndDistSQL tests the case where there's an index
// merge that occurs and that merge is distributed, and the temp index has been
// split a number of times and scattered over the nodes. The hazard we're
// working to reproduce is a situation whereby the temp index being results in
// a situation where the successor key of the last key of the range is the
// range boundary.
func TestIndexMergeWithSplitsAndDistSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var tc *testcluster.TestCluster
	const createStmts = `
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v int);`
	const numRanges = 10
	const rowsToWrite = 1000
	var called bool
	writeToAndSplitIndex := func(mi *scop.MergeIndex) error {
		s := tc.Server(0)
		db := s.DB()
		if _, err := tc.ServerConn(0).Exec(
			"UPSERT INTO t.test SELECT i, i FROM generate_series($1, $2) as t(i)",
			0, rowsToWrite,
		); err != nil {
			return err
		}
		indexPrefix := s.Codec().IndexPrefix(uint32(mi.TableID), uint32(mi.TemporaryIndexID))
		rows, err := db.Scan(ctx, indexPrefix, indexPrefix.PrefixEnd(), 0)
		if err != nil {
			return err
		}
		perm := rand.Perm(rowsToWrite)
		splitPoints := perm[:numRanges]
		// Sometimes split at the next key and sometimes split at a row itself.
		// The only known hazard was when the split is at the next key, but we
		// may as well make this test core more situations.
		splitPoint := func(i int) roachpb.Key {
			if i%2 == 0 {
				return rows[i].Key
			}
			return rows[i].Key.Next()
		}
		for _, i := range splitPoints {
			if err := db.AdminSplit(ctx, splitPoint(i), hlc.MaxTimestamp); err != nil {
				return err
			}
		}

		for _, i := range splitPoints {
			if _, err := db.AdminScatter(ctx, splitPoint(i), 0); err != nil {
				return err
			}
		}
		called = true
		return nil
	}

	// Wait for the beginning of the Merge step of the schema change.
	// Write data to the temp index and split it at the hazardous
	// points.
	params, _ := createTestServerParamsAllowTenants()
	params.Knobs = base.TestingKnobs{
		SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				getMergeIndex := func(ops []scop.Op) *scop.MergeIndex {
					for _, op := range ops {
						if mi, ok := op.(*scop.MergeIndex); ok {
							return mi
						}
					}
					return nil
				}
				if op := getMergeIndex(p.Stages[stageIdx].EdgeOps); op != nil {
					return writeToAndSplitIndex(op)
				}
				return nil
			},
		},
	}

	ctx := context.Background()
	tc = testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer tc.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, createStmts)
	tdb.Exec(t, "CREATE INDEX ON t.test (v)")
	require.True(t, called)
}
