// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test schema changes are retried and complete properly when there's an error
// in the merge step. This also checks that a mutation checkpoint reduces the
// size of the span operated on during a retry.
func TestIndexBackfillMergeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "TODO(ssd) test times outs under race")

	params, _ := tests.CreateTestServerParams()

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

	mergeChunk := 0
	var seenKey roachpb.Key
	checkStartingKey := func(key roachpb.Key) error {
		mergeChunk++
		if mergeChunk == 3 {
			// Fail right before merging the 3rd chunk.
			if rand.Intn(2) == 0 {
				return context.DeadlineExceeded
			} else {
				errAmbiguous := &roachpb.AmbiguousResultError{}
				return roachpb.NewError(errAmbiguous).GoError()
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
				RunBeforeMergeChunk:      checkStartingKey,
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
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

	addIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2, func() {
		if _, err := sqlDB.Exec("SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS])"); err != nil {
			t.Fatal(err)
		}
	})
	require.True(t, mergeChunk > 3, fmt.Sprintf("mergeChunk: %d", mergeChunk))
}

// Test index backfill merges are not affected by various operations that run
// simultaneously.
func TestRaceWithIndexBackfillMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "TODO(ssd) test times outs under race")

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

	params, _ := tests.CreateTestServerParams()
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
				RunBeforeMergeChunk: func(key roachpb.Key) error {
					notifyMerge()
					return nil
				},
			},
		},
	}

	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.Background())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	splitTemporaryIndex = func() error {
		tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "public", "test")
		tempIdx, err := findCorrespondingTemporaryIndex(tableDesc, idxName)
		if err != nil {
			return err
		}

		var sps []sql.SplitPoint
		for i := 0; i < numNodes; i++ {
			sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue/numNodes*i + 5*maxValue}})
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
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
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
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count != val {
			t.Errorf("e = %d, v = %d", count, val)
		}
		if x != 1.4 {
			t.Errorf("e = %f, v = %f", 1.4, x)
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

	var chunkSize int64 = 1000
	var initialRows = 10000
	rowIdx := 0

	params, _ := tests.CreateTestServerParams()
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

	params, _ := tests.CreateTestServerParams()
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
	}

	s, sqlDB, kvDB = serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	var err error
	scratch, err = s.ScratchRange()
	require.NoError(t, err)

	if _, err := sqlDB.Exec(`
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
	addIndexSchemaChange(t, sqlDB, kvDB, maxValue+additionalRowsForMerge, 2, func() {
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
	sps []sql.SplitPoint,
) error {
	if tc.ReplicationMode() != base.ReplicationManual {
		return errors.Errorf("splitIndex called on a test cluster that was not in manual replication mode")
	}

	rkts := make(map[roachpb.RangeID]rangeAndKT)
	for _, sp := range sps {

		pik, err := randgen.TestingMakeSecondaryIndexKey(desc, index, keys.SystemSQLCodec, sp.Vals...)
		if err != nil {
			return err
		}

		rangeDesc, err := tc.LookupRange(pik)
		if err != nil {
			return err
		}

		holder, err := tc.FindRangeLeaseHolder(rangeDesc, nil)
		if err != nil {
			return err
		}

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
			if err := tc.TransferRangeLease(desc, target); err != nil {
				return err
			}
		}
	}
	return nil
}
