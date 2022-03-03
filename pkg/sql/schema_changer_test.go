// Copyright 2015 The Cockroach Authors.
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
	"database/sql/driver"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestSchemaChangeProcess adds mutations manually to a table descriptor and
// ensures that RunStateMachineBeforeBackfill processes the mutation.
// TODO (lucy): This is the only test that creates its own schema changer and
// calls methods on it. Now that every schema changer "belongs" to a single
// instance of a job resumer there's less of a reason to test this way. Should
// this test still even exist?
func TestSchemaChangeProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	var instance = base.SQLInstanceID(2)
	stopper := stop.NewStopper()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	rf, err := rangefeed.NewFactory(stopper, kvDB, execCfg.Settings, nil /* knobs */)
	require.NoError(t, err)
	leaseMgr := lease.NewLeaseManager(
		s.AmbientCtx(),
		execCfg.NodeID,
		execCfg.DB,
		execCfg.Clock,
		execCfg.InternalExecutor,
		execCfg.Settings,
		execCfg.Codec,
		lease.ManagerTestingKnobs{},
		stopper,
		rf,
	)
	jobRegistry := s.JobRegistry().(*jobs.Registry)
	defer stopper.Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo(v));
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	tableID := descpb.ID(sqlutils.QueryTableID(t, sqlDB, "t", "public", "test"))

	changer := sql.NewSchemaChangerForTesting(
		tableID, 0, instance, kvDB, leaseMgr, jobRegistry, &execCfg, cluster.MakeTestingClusterSettings())

	// Read table descriptor for version.
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	expectedVersion := tableDesc.Version
	ctx := context.Background()

	// Check that RunStateMachineBeforeBackfill doesn't do anything
	// if there are no mutations queued.
	if err := changer.RunStateMachineBeforeBackfill(ctx); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Check that RunStateMachineBeforeBackfill functions properly.
	expectedVersion = tableDesc.Version
	// Make a copy of the index for use in a mutation.
	index := tableDesc.PublicNonPrimaryIndexes()[0].IndexDescDeepCopy()
	index.Name = "bar"
	index.ID = tableDesc.NextIndexID
	tableDesc.NextIndexID++
	changer = sql.NewSchemaChangerForTesting(
		tableID, tableDesc.NextMutationID, instance, kvDB, leaseMgr, jobRegistry,
		&execCfg, cluster.MakeTestingClusterSettings(),
	)
	tableDesc.TableDesc().Mutations = append(tableDesc.TableDesc().Mutations, descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: &index},
		Direction:   descpb.DescriptorMutation_ADD,
		State:       descpb.DescriptorMutation_DELETE_ONLY,
		MutationID:  tableDesc.NextMutationID,
	})
	tableDesc.NextMutationID++

	// Run state machine in both directions.
	for _, direction := range []descpb.DescriptorMutation_Direction{
		descpb.DescriptorMutation_ADD, descpb.DescriptorMutation_DROP,
	} {
		tableDesc.Mutations[0].Direction = direction
		expectedVersion++
		if err := kvDB.Put(
			ctx,
			catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
			tableDesc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
		// The expected end state.
		expectedState := descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
		if direction == descpb.DescriptorMutation_DROP {
			expectedState = descpb.DescriptorMutation_DELETE_ONLY
		}
		// Run two times to ensure idempotency of operations.
		for i := 0; i < 2; i++ {
			if err := changer.RunStateMachineBeforeBackfill(ctx); err != nil {
				t.Fatal(err)
			}

			tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(
				kvDB, keys.SystemSQLCodec, "t", "test")
			newVersion = tableDesc.Version
			if newVersion != expectedVersion {
				t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
			}
			state := tableDesc.Mutations[0].State
			if state != expectedState {
				t.Fatalf("bad state; e = %d, v = %d", expectedState, state)
			}
		}
	}
	// RunStateMachineBeforeBackfill() doesn't complete the schema change.
	tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "t", "test")
	if len(tableDesc.Mutations) == 0 {
		t.Fatalf("table expected to have an outstanding schema change: %v", tableDesc)
	}
}

// TODO (lucy): In the current state of the code it doesn't make sense to try to
// test the "async" path separately. This test doesn't have any special
// settings. Should it even still exist?
func TestAsyncSchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()
	// Disable synchronous schema change execution so the asynchronous schema
	// changer executes all schema changes.
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "t", "test")

	// A long running schema change operation runs through
	// a state machine that increments the version by 6.
	expectedVersion := tableDesc.Version + 6

	// Run some schema change
	if _, err := sqlDB.Exec(`
CREATE INDEX foo ON t.test (v)
`); err != nil {
		t.Fatal(err)
	}

	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}

	// Wait until index is created.
	for r := retry.Start(retryOpts); r.Next(); {
		tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", "test")
		if len(tableDesc.PublicNonPrimaryIndexes()) == 1 {
			break
		}
	}

	// Ensure that the indexes have been created.
	mTest := makeMutationTest(t, kvDB, sqlDB, tableDesc)
	indexQuery := `SELECT v FROM t.test@foo`
	mTest.CheckQueryResults(t, indexQuery, [][]string{{"b"}, {"d"}})

	// Ensure that the version has been incremented.
	tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "t", "test")
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Apply a schema change that only sets the UpVersion bit.
	expectedVersion = newVersion + 1

	mTest.Exec(t, `ALTER INDEX t.test@foo RENAME TO ufo`)

	for r := retry.Start(retryOpts); r.Next(); {
		// Ensure that the version gets incremented.
		tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", "test")
		name := tableDesc.PublicNonPrimaryIndexes()[0].GetName()
		if name != "ufo" {
			t.Fatalf("bad index name %s", name)
		}
		newVersion = tableDesc.Version
		if newVersion == expectedVersion {
			break
		}
	}

	// Run many schema changes simultaneously and check
	// that they all get executed.
	count := 5
	for i := 0; i < count; i++ {
		mTest.Exec(t, fmt.Sprintf(`CREATE INDEX foo%d ON t.test (v)`, i))
	}
	// Wait until indexes are created.
	for r := retry.Start(retryOpts); r.Next(); {
		tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", "test")
		if len(tableDesc.PublicNonPrimaryIndexes()) == count+1 {
			break
		}
	}
	for i := 0; i < count; i++ {
		indexQuery := fmt.Sprintf(`SELECT v FROM t.test@foo%d`, i)
		mTest.CheckQueryResults(t, indexQuery, [][]string{{"b"}, {"d"}})
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// Run a particular schema change and run some OLTP operations in parallel, as
// soon as the schema change starts executing its backfill.
func runSchemaChangeWithOperations(
	t *testing.T,
	sqlDB *gosql.DB,
	kvDB *kv.DB,
	schemaChange string,
	maxValue int,
	keyMultiple int,
	backfillNotification chan struct{},
	useUpsert bool,
) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		start := timeutil.Now()
		// Start schema change that eventually runs a backfill.
		if _, err := sqlDB.Exec(schemaChange); err != nil {
			t.Error(err)
		}
		t.Logf("schema change %s took %v", schemaChange, timeutil.Since(start))
		wg.Done()
	}()

	// Wait until the schema change backfill starts.
	<-backfillNotification

	// Run a variety of operations during the backfill.
	ctx := context.Background()

	// Update some rows.
	var updatedKeys []int
	for i := 0; i < 10; i++ {
		k := rand.Intn(maxValue)
		v := maxValue + i + 1
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, v, k); err != nil {
			t.Error(err)
		}
		updatedKeys = append(updatedKeys, k)
	}

	// Reupdate updated values back to what they were before.
	for _, k := range updatedKeys {
		if rand.Float32() < 0.5 || !useUpsert {
			if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, maxValue-k, k); err != nil {
				t.Error(err)
			}
		} else {
			if _, err := sqlDB.Exec(`UPSERT INTO t.test (k,v) VALUES ($1, $2)`, k, maxValue-k); err != nil {
				t.Error(err)
			}
		}
	}

	// Delete some rows.
	deleteStartKey := rand.Intn(maxValue - 10)
	for i := 0; i < 10; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, deleteStartKey+i); err != nil {
			t.Error(err)
		}
	}
	// Reinsert deleted rows.
	for i := 0; i < 10; i++ {
		k := deleteStartKey + i
		if rand.Float32() < 0.5 || !useUpsert {
			if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, k, maxValue-k); err != nil {
				t.Error(err)
			}
		} else {
			if _, err := sqlDB.Exec(`UPSERT INTO t.test VALUES($1, $2)`, k, maxValue-k); err != nil {
				t.Error(err)
			}
		}
	}

	// Insert some new rows.
	numInserts := 10
	for i := 0; i < numInserts; i++ {
		k := maxValue + i + 1
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $1)`, k); err != nil {
			t.Error(err)
		}
	}

	wg.Wait() // for schema change to complete.

	// Verify the number of keys left behind in the table to
	// validate schema change operations. We wait for any SCHEMA
	// CHANGE GC jobs to complete to ensure our key count doesn't
	// include keys from a temporary index.
	if _, err := sqlDB.Exec(`SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC')`); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		return sqltestutils.CheckTableKeyCount(ctx, kvDB, keyMultiple, maxValue+numInserts)
	})
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Delete the rows inserted.
	for i := 0; i < numInserts; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, maxValue+i+1); err != nil {
			t.Error(err)
		}
	}
}

func TestRollbackOfAddingTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Protects shouldError.
	var mu syncutil.Mutex
	shouldError := true

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeQueryBackfill: func() error {
				mu.Lock()
				defer mu.Unlock()
				if shouldError {
					shouldError = false
					return jobs.MarkAsPermanentJobError(errors.New("boom"))
				}
				return nil
			},
		},
	}

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE DATABASE d`)
	require.NoError(t, err)

	// This view creation will fail and eventually rollback.
	_, err = sqlDB.Exec(`CREATE MATERIALIZED VIEW d.v AS SELECT 1`)
	require.EqualError(t, err, "pq: boom")

	// Get the view descriptor we just created and verify that it's in the
	// dropping state. We're unable to access the descriptor via the usual means
	// because catalog.FilterDescriptorState filters out tables in the ADD state,
	// and once we move the table to the DROP state we also remove the namespace
	// entry. So we just get the most recent descriptor.
	var descBytes []byte
	row := sqlDB.QueryRow(`SELECT descriptor FROM system.descriptor ORDER BY id DESC LIMIT 1`)
	require.NoError(t, row.Scan(&descBytes))
	var desc descpb.Descriptor
	require.NoError(t, protoutil.Unmarshal(descBytes, &desc))
	//nolint:descriptormarshal
	viewDesc := desc.GetTable()
	require.Equal(t, "v", viewDesc.GetName(), "read a different descriptor than expected")
	require.Equal(t, descpb.DescriptorState_DROP, viewDesc.GetState())

	// The view should be cleaned up after the failure, so we should be able
	// to create a new view with the same name.
	_, err = sqlDB.Exec(`CREATE MATERIALIZED VIEW d.v AS SELECT 1`)
	require.NoError(t, err)
}

func TestUniqueViolationsAreCaught(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	readyToMerge := make(chan struct{})
	startMerge := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeTempIndexMerge: func() {
				close(readyToMerge)
				<-startMerge
			},
		},
	}
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec(`CREATE DATABASE t;
CREATE TABLE t.test (pk INT PRIMARY KEY, v INT);
INSERT INTO t.test VALUES (1,1), (2,2), (3,3)
`)
	require.NoError(t, err)
	grp := ctxgroup.WithContext(context.Background())
	grp.GoCtx(func(ctx context.Context) error {
		_, err := sqlDB.Exec(`CREATE UNIQUE INDEX ON t.test (v)`)
		return err
	})

	<-readyToMerge
	// This conflicts with the new index but doesn't conflict with
	// the online indexes. It should produce a failure on
	// validation.
	_, err = sqlDB.Exec(`INSERT INTO t.test VALUES (4, 1), (5, 2)`)
	require.NoError(t, err)

	close(startMerge)
	err = grp.Wait()
	require.Error(t, err)
}

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestRaceWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// protects backfillNotification
	var mu syncutil.Mutex
	var backfillNotification chan struct{}

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
	initBackfillNotification := func() chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		backfillNotification = make(chan struct{})
		return backfillNotification
	}
	notifyBackfill := func() {
		mu.Lock()
		defer mu.Unlock()
		if backfillNotification != nil {
			// Close channel to notify that the backfill has started.
			close(backfillNotification)
			backfillNotification = nil
		}
	}

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		// Disable GC job.
		// GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { select {} }},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				notifyBackfill()
				return nil
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

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
CREATE UNIQUE INDEX vidx ON t.test (v);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.Background()

	// number of keys == 2 * number of rows; 1 column family and 1 index entry
	// for each row.
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Run some schema changes with operations.

	// Add column with a check constraint.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4') CHECK (x >= 0)",
		maxValue,
		2,
		initBackfillNotification(),
		true,
	)

	// Drop column.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test DROP pi",
		maxValue,
		2,
		initBackfillNotification(),
		true,
	)

	// Add index.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"CREATE UNIQUE INDEX foo ON t.test (v)",
		maxValue,
		3,
		initBackfillNotification(),
		true,
	)

	// Add STORING index (that will have non-nil values).
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"CREATE INDEX bar ON t.test(k) STORING (v)",
		maxValue,
		4,
		initBackfillNotification(),
		true,
	)

	// Verify that the index foo over v is consistent, and that column x has
	// been backfilled properly.
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

// Test that a table drop in the middle of a backfill works properly.
// The backfill will terminate in the middle, and the drop will
// successfully complete without deleting the data.
func TestDropWhileBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// protects backfillNotification
	var mu syncutil.Mutex
	backfillNotification := make(chan struct{})

	var partialBackfillDone atomic.Value
	partialBackfillDone.Store(false)
	const numNodes, chunkSize = 5, 100
	maxValue := 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		// We expect this to also reduce the memory footprint of the test.
		maxValue = 200
	}
	params, _ := tests.CreateTestServerParams()
	notifyBackfill := func() {
		mu.Lock()
		defer mu.Unlock()
		if backfillNotification != nil {
			// Close channel to notify that the backfill has started.
			close(backfillNotification)
			backfillNotification = nil
		}
	}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if partialBackfillDone.Load().(bool) {
					notifyBackfill()
					// Returning DeadlineExceeded will result in the
					// schema change being retried.
					return context.DeadlineExceeded
				}
				partialBackfillDone.Store(true)
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
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

	if _, err := sqlDB.Exec(`
SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off';
SET use_declarative_schema_changer = 'off';
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
CREATE UNIQUE INDEX vidx ON t.test (v);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.Background()

	// number of keys == 2 * number of rows; 1 column family and 1 index entry
	// for each row.
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	notification := backfillNotification
	// Run the schema change in a separate goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Start schema change that eventually runs a partial backfill.
		if _, err := sqlDB.Exec("CREATE UNIQUE INDEX bar ON t.test (v)"); err != nil && !testutils.IsError(err, "descriptor is being dropped") {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until the schema change backfill is partially complete.
	<-notification

	if _, err := sqlDB.Exec("DROP TABLE t.test"); err != nil {
		t.Fatal(err)
	}

	// Wait until the schema change is done.
	wg.Wait()

	// Ensure that the table data hasn't been deleted.
	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 2 * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
	// Check that the table descriptor exists so we know the data will
	// eventually be deleted.
	tbDescKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID())
	if gr, err := kvDB.Get(ctx, tbDescKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatalf("table descriptor doesn't exist after table is dropped: %q", tbDescKey)
	}
}

// Test that a schema change on encountering a permanent backfill error
// on a remote node terminates properly and returns the database to a
// proper state.
func TestBackfillErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes, chunkSize, maxValue = 5, 100, 4000
	params, _ := tests.CreateTestServerParams()

	blockGC := make(chan struct{})
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { <-blockGC; return nil }},
	}

	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.Background())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Update v column on some rows to be the same so that the future
	// UNIQUE index we create on it fails.
	//
	// Pick a set of random rows because if we pick a deterministic set
	// we can't be sure they will end up on a remote node. We want this
	// test to fail if an error is not reported correctly on a local or
	// remote node and the randomness allows us to test both.
	const numUpdatedRows = 10
	for i := 0; i < numUpdatedRows; i++ {
		k := rand.Intn(maxValue - numUpdatedRows)
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, 1, k); err != nil {
			t.Error(err)
		}
	}

	// Split the table into multiple ranges.
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.Background()

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	CREATE UNIQUE INDEX vidx ON t.test (v);
	`); !testutils.IsError(err, `violates unique constraint "vidx"`) {
		t.Fatalf("got err=%s", err)
	}

	// Index backfill errors at a non-deterministic chunk and the garbage
	// keys remain because the async schema changer for the rollback stays
	// disabled in order to assert the next errors. Therefore we do not check
	// the keycount from this operation and just check that the next failed
	// operations do not add more.
	keyCount, err := sqltestutils.GetTableKeyCount(ctx, kvDB)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	  ALTER TABLE t.test ADD COLUMN p DECIMAL NOT NULL DEFAULT (DECIMAL '1-3');
	  `); !testutils.IsError(err, `could not parse "1-3" as type decimal`) {
		t.Fatalf("got err=%s", err)
	}

	if err := sqltestutils.CheckTableKeyCountExact(ctx, kvDB, keyCount); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	ALTER TABLE t.test ADD COLUMN p DECIMAL NOT NULL;
	`); !testutils.IsError(err, `null value in column \"p\" violates not-null constraint`) {
		t.Fatalf("got err=%s", err)
	}

	if err := sqltestutils.CheckTableKeyCountExact(ctx, kvDB, keyCount); err != nil {
		t.Fatal(err)
	}
	close(blockGC)
}

// Test aborting a schema change backfill transaction and check that the
// backfill is completed correctly. The backfill transaction is aborted at a
// time when it thinks it has processed all the rows of the table. Later,
// before the transaction is retried, the table is populated with more rows
// that a backfill chunk, requiring the backfill to forget that it is at the
// end of its processing and needs to continue on to process two more chunks
// of data.
func TestAbortSchemaChangeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var backfillNotification, commandsDone chan struct{}
	var dontAbortBackfill uint32
	params, _ := tests.CreateTestServerParams()
	const maxValue = 100
	backfillCount := int64(0)
	retriedBackfill := int64(0)
	var retriedSpan roachpb.Span

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// TODO (lucy): Stress this test. This test used to require fast GC, but
			// it passes without it.
			BackfillChunkSize: maxValue,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				switch atomic.LoadInt64(&backfillCount) {
				case 0:
					// Keep track of the span provided with the first backfill
					// attempt.
					retriedSpan = sp
				case 1:
					// Ensure that the second backfill attempt provides the
					// same span as the first.
					if sp.EqualValue(retriedSpan) {
						atomic.AddInt64(&retriedBackfill, 1)
					}
				}
				return nil
			},
			RunAfterBackfillChunk: func() {
				atomic.AddInt64(&backfillCount, 1)
				if atomic.SwapUint32(&dontAbortBackfill, 1) == 1 {
					return
				}
				// Close channel to notify that the backfill has been
				// completed but hasn't yet committed.
				close(backfillNotification)
				// Receive signal that the commands that push the backfill
				// transaction have completed; The backfill will attempt
				// to commit and will abort.
				<-commandsDone
			},
			BulkAdderFlushesEveryBatch: true,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	// Add a zone config for the table.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// Bulk insert enough rows to exceed the chunk size.
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, i)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	// The two drop cases (column and index) do not need to be tested here
	// because the INSERT down below will not insert an entry for a dropped
	// column or index, however, it's still nice to have the column drop
	// just in case INSERT gets messed up. The writes will never abort a
	// drop index because it uses ClearRange, so it is not tested.
	testCases := []struct {
		sql string
		// Each schema change adds/drops a schema element that affects the
		// number of keys representing a table row.
		expectedNumKeysPerRow int
	}{
		{"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4') CHECK (x >= 0)", 1},
		{"ALTER TABLE t.test DROP x", 1},
		{"CREATE UNIQUE INDEX foo ON t.test (v)", 2},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			// Delete two rows so that the table size is smaller than a backfill
			// chunk. The two values will be added later to make the table larger
			// than a backfill chunk after the schema change backfill is aborted.
			for i := 0; i < 2; i++ {
				if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, i); err != nil {
					t.Fatal(err)
				}
			}

			backfillNotification = make(chan struct{})
			commandsDone = make(chan struct{})
			atomic.StoreUint32(&dontAbortBackfill, 0)
			// Run the column schema change in a separate goroutine.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				// Start schema change that eventually runs a backfill.
				if _, err := sqlDB.Exec(testCase.sql); err != nil {
					t.Error(err)
				}

				wg.Done()
			}()

			// Wait until the schema change backfill has finished writing its
			// intents.
			<-backfillNotification

			// Delete a row that will push the backfill transaction.
			if _, err := sqlDB.Exec(`
BEGIN TRANSACTION PRIORITY HIGH;
DELETE FROM t.test WHERE k = 2;
COMMIT;
			`); err != nil {
				t.Fatal(err)
			}

			// Add missing rows so that the table exceeds the size of a
			// backfill chunk.
			for i := 0; i < 3; i++ {
				if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, i, i); err != nil {
					t.Fatal(err)
				}
			}

			// Release backfill so that it can try to commit and in the
			// process discover that it was aborted.
			close(commandsDone)

			wg.Wait() // for schema change to complete

			ctx := context.Background()

			// Verify the number of keys left behind in the table to validate
			// schema change operations. We expect this to fail until garbage
			// collection on the temporary index completes.
			testutils.SucceedsSoon(t, func() error {
				return sqltestutils.CheckTableKeyCount(ctx, kvDB, testCase.expectedNumKeysPerRow, maxValue)
			})

			if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Add an index and check that it succeeds.
func addIndexSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, maxValue int, numKeysPerRow int, waitFn func(),
) {
	if _, err := sqlDB.Exec("CREATE UNIQUE INDEX foo ON t.test (v)"); err != nil {
		t.Fatal(err)
	}

	// The schema change succeeded. Verify that the index foo over v is
	// consistent.
	rows, err := sqlDB.Query(`SELECT v from t.test@foo`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var val int
		if err := rows.Scan(&val); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count != val {
			t.Errorf("e = %d, v = %d", count, val)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := maxValue + 1; eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	ctx := context.Background()

	if waitFn != nil {
		waitFn()
	}

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}
}

// Add a column with a check constraint and check that it succeeds.
func addColumnSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4') CHECK (x >= 0)"); err != nil {
		t.Fatal(err)
	}
	rows, err := sqlDB.Query(`SELECT x from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for ; rows.Next(); count++ {
		var val float64
		if err := rows.Scan(&val); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if e := 1.4; e != val {
			t.Errorf("e = %f, v = %f", e, val)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := maxValue + 1; eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	ctx := context.Background()

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}
}

// Drop a column and check that it succeeds.
func dropColumnSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("ALTER TABLE t.test DROP x"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}

}

// Drop an index and check that it succeeds.
func dropIndexSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("DROP INDEX t.test@foo CASCADE"); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(context.Background(), kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}
}

// TestDropColumn tests that dropped columns properly drop their Table's CHECK constraints,
// or an error occurs if a CHECK constraint is being added on it.
func TestDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
  k INT PRIMARY KEY,
  v INT CONSTRAINT check_v CHECK (v >= 0),
  a INT DEFAULT 0 CONSTRAINT check_av CHECK (a <= v),
  b INT DEFAULT 100 CONSTRAINT check_ab CHECK (b > a)
);
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if len(tableDesc.GetChecks()) != 3 {
		t.Fatalf("Expected 3 checks but got %d ", len(tableDesc.GetChecks()))
	}

	if _, err := sqlDB.Exec("ALTER TABLE t.test DROP v"); err != nil {
		t.Fatal(err)
	}

	// Re-read table descriptor.
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	// Only check_ab should remain
	if len(tableDesc.GetChecks()) != 1 {
		checkExprs := make([]string, 0)
		for i := range tableDesc.GetChecks() {
			checkExprs = append(checkExprs, tableDesc.GetChecks()[i].Expr)
		}
		t.Fatalf("Expected 1 check but got %d with CHECK expr %s ", len(tableDesc.GetChecks()), strings.Join(checkExprs, ", "))
	}

	if tableDesc.GetChecks()[0].Name != "check_ab" {
		t.Fatalf("Only check_ab should remain, got: %s ", tableDesc.GetChecks()[0].Name)
	}

	// Test that a constraint being added prevents the column from being dropped.
	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec(`ALTER TABLE t.test ADD CONSTRAINT check_bk CHECK (b >= k)`); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec(`ALTER TABLE t.test DROP b`); !testutils.IsError(err,
		"pq: unimplemented: constraint \"check_bk\" in the middle of being added, try again later") {
		t.Fatalf("err = %+v", err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Test schema changes are retried and complete properly. This also checks
// that a mutation checkpoint reduces the number of chunks operated on during
// a retry.
func TestSchemaChangeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()

	currChunk := 0
	seenSpan := roachpb.Span{}
	checkSpan := func(sp roachpb.Span) error {
		currChunk++
		// Fail somewhere in the middle.
		if currChunk == 3 {
			if rand.Intn(2) == 0 {
				return context.DeadlineExceeded
			} else {
				errAmbiguous := &roachpb.AmbiguousResultError{}
				return roachpb.NewError(errAmbiguous).GoError()
			}
		}
		if sp.Key != nil && seenSpan.Key != nil {
			// Check that the keys are never reevaluated
			if seenSpan.Key.Compare(sp.Key) >= 0 {
				t.Errorf("reprocessing span %s, already seen span %s", sp, seenSpan)
			}
			if !seenSpan.EndKey.Equal(sp.EndKey) {
				t.Errorf("different EndKey: span %s, already seen span %s", sp, seenSpan)
			}
		}
		seenSpan = sp
		return nil
	}

	const maxValue = 2000
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			WriteCheckpointInterval:          time.Nanosecond,
			AlwaysUpdateIndexBackfillDetails: true,
			BackfillChunkSize:                maxValue / 5,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk:                     checkSpan,
			BulkAdderFlushesEveryBatch:                 true,
			SerializeIndexBackfillCreationAndIngestion: make(chan struct{}, 1),
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

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	addIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2, nil)

	currChunk = 0
	seenSpan = roachpb.Span{}
	addColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2)
}

// Test schema changes are retried and complete properly when the table
// version changes. This also checks that a mutation checkpoint reduces
// the number of chunks operated on during a retry.
func TestSchemaChangeRetryOnVersionChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	var upTableVersion func()
	const maxValue = 2000
	currChunk := 0
	var numBackfills uint32
	seenSpan := roachpb.Span{}
	unblockGC := make(chan struct{})
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				atomic.AddUint32(&numBackfills, 1)
				return nil
			},
			WriteCheckpointInterval:          time.Nanosecond,
			BackfillChunkSize:                maxValue / 10,
			AlwaysUpdateIndexBackfillDetails: true,
		},
		// Block GC Job during the test. The index we add
		// creates a GC job to clean up the temporary index
		// used during backfill. If that GC job runs, it will
		// bump the table version causing an extra backfill
		// that our assertions don't account for.
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error {
			<-unblockGC
			t.Log("gc unblocked")
			return nil
		}},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				currChunk++
				// Fail somewhere in the middle.
				if currChunk == 3 {
					// Publish a new version of the table.
					upTableVersion()

					// TODO(adityamaru): Previously, the index backfiller would
					// periodically redo the DistSQL flow setup after checkpointing its
					// progress. This would mean that it would notice a changed descriptor
					// version and retry the backfill. Since, the new index backfiller
					// does not repeat this DistSQL setup step unless retried, we must
					// force a retry.
					errAmbiguous := &roachpb.AmbiguousResultError{}
					return roachpb.NewError(errAmbiguous).GoError()
				}
				if seenSpan.Key != nil {
					if !seenSpan.EndKey.Equal(sp.EndKey) {
						t.Errorf("different EndKey: span %s, already seen span %s", sp, seenSpan)
					}
				}
				seenSpan = sp
				return nil
			},
			BulkAdderFlushesEveryBatch:                 true,
			SerializeIndexBackfillCreationAndIngestion: make(chan struct{}, 1),
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
	defer func() {
		t.Log("unblocking GC")
		close(unblockGC)
	}()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	id := tableDesc.GetID()
	ctx := context.Background()

	upTableVersion = func() {
		leaseMgr := s.LeaseManager().(*lease.Manager)
		var version descpb.DescriptorVersion

		// Use a timeout shorter than the lease duration to ensure that we aren't
		// just waiting for the lease to expire.
		timeoutCtx, cancel := context.WithTimeout(ctx, base.DefaultDescriptorLeaseDuration/2)
		defer cancel()
		if err := sql.TestingDescsTxn(timeoutCtx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
			tbl, err := col.Direct().MustGetTableDescByID(ctx, txn, tableDesc.GetID())
			if err != nil {
				return err
			}
			table := tabledesc.NewBuilder(tbl.TableDesc()).BuildExistingMutableTable()
			if err != nil {
				return err
			}
			table.MaybeIncrementVersion()
			ba := txn.NewBatch()
			if err := col.WriteDescToBatch(ctx, false /* kvTrace */, table, ba); err != nil {
				return err
			}
			version = table.GetVersion()

			// Here we don't want to actually wait for the backfill to drop its lease.
			// To avoid that, we hack the machinery which tries oh so hard to make it
			// impossible to avoid, by calling the ReleaseAll method on the
			// collection to reset its state. In practice, this machinery exists only
			// for the lower-level usages in the connExecutor and probably ought not
			// to exist on the object passed to descs.Txn, but, we have it, and it's
			// effective, so, let's use it.
			defer col.ReleaseAll(ctx)
			return txn.Run(ctx, ba)
		}); err != nil {
			t.Error(err)
		}

		// Grab a lease at the latest version so that we are confident
		// that all future leases will be taken at the latest version.
		table, err := leaseMgr.TestingAcquireAndAssertMinVersion(timeoutCtx, s.Clock().Now(), id, version)
		if err != nil {
			t.Error(err)
		}
		table.Release(timeoutCtx)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	addIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2, nil)
	if num := atomic.SwapUint32(&numBackfills, 0); num != 2 {
		t.Fatalf("expected %d backfills, but saw %d", 2, num)
	}

	currChunk = 0
	seenSpan = roachpb.Span{}
	addColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)
	if num := atomic.SwapUint32(&numBackfills, 0); num != 2 {
		t.Fatalf("expected %d backfills, but saw %d", 2, num)
	}

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)
	if num := atomic.SwapUint32(&numBackfills, 0); num != 2 {
		t.Fatalf("expected %d backfills, but saw %d", 2, num)
	}
}

// Test schema change purge failure doesn't leave DB in a bad state.
func TestSchemaChangePurgeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 51796)
	// TODO (lucy): This test needs more complicated schema changer knobs than
	// currently implemented. Previously this test disabled the async schema
	// changer so that we don't retry the cleanup of the failed schema change
	// until a certain point in the test.
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	var enableAsyncSchemaChanges uint32
	var attempts int32
	// attempt 1: write the first chunk of the index.
	// attempt 2: write the second chunk and hit a unique constraint
	// violation; purge the schema change.
	// attempt 3: return an error while purging the schema change.
	var expectedAttempts int32 = 3
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				// Return a deadline exceeded error during the third attempt
				// which attempts to clean up the schema change.
				if atomic.AddInt32(&attempts, 1) == expectedAttempts {
					// Disable the async schema changer for assertions.
					atomic.StoreUint32(&enableAsyncSchemaChanges, 0)
					return context.DeadlineExceeded
				}
				return nil
			},
			BulkAdderFlushesEveryBatch: true,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	const maxValue = chunkSize + 1
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Add a row with a duplicate value=0 which is the same
	// value as for the key maxValue.
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test VALUES ($1, $2)`, maxValue+1, 0,
	); err != nil {
		t.Fatal(err)
	}

	// A schema change that violates integrity constraints.
	if _, err := sqlDB.Exec(
		"CREATE UNIQUE INDEX foo ON t.test (v)",
	); !testutils.IsError(err, `violates unique constraint "foo"`) {
		t.Fatal(err)
	}

	// The index doesn't exist
	if _, err := sqlDB.Query(
		`SELECT v from t.test@foo`,
	); !testutils.IsError(err, "index .* not found") {
		t.Fatal(err)
	}

	// Allow async schema change purge to attempt backfill and error.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	// deal with schema change knob
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// The deadline exceeded error in the schema change purge results in no
	// retry attempts of the purge.
	testutils.SucceedsSoon(t, func() error {
		if read := atomic.LoadInt32(&attempts); read != expectedAttempts {
			return errors.Errorf("%d retries, despite allowing only (schema change + reverse) = %d", read, expectedAttempts)
		}
		return nil
	})

	// There is still some garbage index data that needs to be purged. All the
	// rows from k = 0 to k = chunkSize - 1 have index values.
	numGarbageValues := chunkSize

	ctx := context.Background()

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue+1+numGarbageValues); err != nil {
		t.Fatal(err)
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Enable async schema change processing to ensure that it cleans up the
	// above garbage left behind.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	// No garbage left behind.
	testutils.SucceedsSoon(t, func() error {
		numGarbageValues = 0
		return sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue+1+numGarbageValues)
	})

	// A new attempt cleans up a chunk of data.
	if attempts != expectedAttempts+1 {
		t.Fatalf("%d chunk ops, despite allowing only (schema change + reverse) = %d", attempts, expectedAttempts)
	}
}

// Test schema change failure after a backfill checkpoint has been written
// doesn't leave the DB in a bad state.
func TestSchemaChangeFailureAfterCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer gcjob.SetSmallMaxGCIntervalForTest()()
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	attempts := 0
	// attempt 1: write two chunks of the column.
	// attempt 2: writing the third chunk returns a permanent failure
	// purge the schema change.
	expectedAttempts := 3
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
			// Aggressively checkpoint, so that a schema change
			// failure happens after a checkpoint has been written.
			WriteCheckpointInterval: time.Nanosecond,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Return a deadline exceeded error during the third attempt
				// which attempts to clean up the schema change.
				if attempts == expectedAttempts {
					return errors.New("permanent failure")
				}
				return nil
			},
		},
		// Disable backfill migrations so it doesn't interfere with the
		// backfill in this test.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	const maxValue = 4*chunkSize + 1
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(context.Background(), kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	// A schema change that fails.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD column d INT DEFAULT 0 CREATE FAMILY F3, ADD CHECK (d >= 0)`); !testutils.IsError(err, `permanent failure`) {
		t.Fatalf("err = %s", err)
	}

	// No garbage left behind.
	if err := sqltestutils.CheckTableKeyCount(context.Background(), kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	// A schema change that fails after the first mutation has completed. The
	// column is backfilled and the index backfill fails requiring the column
	// backfill to be rolled back.
	//
	// The schema changer may detect a unique constraint violation in 2 ways.
	//  1. If a duplicate key is processed in the same BulkAdder batch, a
	//     duplicate key error is returned.
	//
	//  2. If the number of index entries we added does not equal the table
	//     row count, we detect we've violated the constraint.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test ADD column e INT DEFAULT 0 UNIQUE CREATE FAMILY F4, ADD CHECK (e >= 0)`,
	); !testutils.IsError(err, ` violates unique constraint`) {
		t.Fatalf("err = %s", err)
	}

	// No garbage left behind, after the rollback has been GC'ed.
	testutils.SucceedsSoon(t, func() error {
		return sqltestutils.CheckTableKeyCount(context.Background(), kvDB, 1, maxValue)
	})

	// Check that constraints are cleaned up on the latest version of the
	// descriptor.
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if checks := tableDesc.AllActiveAndInactiveChecks(); len(checks) > 0 {
		t.Fatalf("found checks %+v", checks)
	}
}

// TestSchemaChangeReverseMutations tests that schema changes get reversed
// correctly when one of them violates a constraint.
func TestSchemaChangeReverseMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO (lucy): This test needs more complicated schema changer knobs than
	// currently implemented. What this test should be doing is starting a schema
	// change, and, before it hits a violation that requires a rollback, starting
	// another schema change that depends on the successful completion of the
	// first. At the end, all the dependent schema change jobs should have been
	// rolled back. Right now we're just testing that schema changes fail
	// correctly when run sequentially, which is not as interesting. The previous
	// comments are kept around to describe the intended results of the test, but
	// they don't reflect what's happening now.
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	// Disable synchronous schema change processing so that the mutations get
	// processed asynchronously.
	var enableAsyncSchemaChanges uint32
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	// Create a k-v table.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT8);
`); err != nil {
		t.Fatal(err)
	}

	// Add some data
	const maxValue = chunkSize + 1
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	testCases := []struct {
		sql       string
		errString string
	}{
		// Create a column that is not NULL. This schema change doesn't return an
		// error only because we've turned off the synchronous execution path; it
		// will eventually fail when run by the asynchronous path.
		{`ALTER TABLE t.public.test ADD COLUMN a INT8 UNIQUE DEFAULT 0, ADD COLUMN c INT8`,
			"violates unique constraint"},
		// Add an index over a column that will be purged. This index will
		// eventually not get added. The column aa will also be dropped as
		// a result.
		{`ALTER TABLE t.public.test ADD COLUMN aa INT8, ADD CONSTRAINT foo UNIQUE (a)`,
			"column \"a\" does not exist"},

		// The purge of column 'a' doesn't influence these schema changes.

		// Drop column 'v' moves along just fine.
		{`ALTER TABLE t.public.test DROP COLUMN v`,
			""},
		// Add unique column 'b' moves along creating column b and the index on
		// it.
		{`ALTER TABLE t.public.test ADD COLUMN b INT8 UNIQUE`,
			""},
		// #27033: Add a column followed by an index on the column.
		{`ALTER TABLE t.public.test ADD COLUMN d STRING NOT NULL DEFAULT 'something'`,
			""},

		{`CREATE INDEX ON t.public.test (d)`,
			""},

		// Add an index over a column 'c' that will be purged. This index will
		// eventually not get added. The column bb will also be dropped as
		// a result.
		{`ALTER TABLE t.public.test ADD COLUMN bb INT8, ADD CONSTRAINT bar UNIQUE (never_existed)`,
			"column \"never_existed\" does not exist"},
		// Cascading of purges. column 'c' -> column 'bb' -> constraint 'idx_bb'.
		{`ALTER TABLE t.public.test ADD CONSTRAINT idx_bb UNIQUE (bb)`,
			"column \"bb\" does not exist"},
	}

	for _, tc := range testCases {
		_, err := sqlDB.Exec(tc.sql)
		if tc.errString == "" {
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
		} else {
			if err == nil {
				t.Fatalf("%s: expected error", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.errString) {
				t.Fatalf("%s: %v", tc.sql, err)
			}
		}
	}

	// Enable async schema change processing for purged schema changes.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	expectedCols := []string{"k", "b", "d"}
	// Wait until all the mutations have been processed.
	testutils.SucceedsSoon(t, func() error {
		tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		if len(tableDesc.AllMutations()) > 0 {
			return errors.Errorf("%d mutations remaining", len(tableDesc.AllMutations()))
		}
		return nil
	})

	// Verify that t.public.test has the expected data. Read the table data while
	// ensuring that the correct table lease is in use.
	rows, err := sqlDB.Query(`SELECT * from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that sql is using the correct table lease.
	if len(cols) != len(expectedCols) {
		t.Fatalf("incorrect columns: %v, expected: %v", cols, expectedCols)
	}
	if cols[0] != expectedCols[0] || cols[1] != expectedCols[1] {
		t.Fatalf("incorrect columns: %v", cols)
	}

	// rows contains the data; verify that it's the right data.
	vals := make([]interface{}, len(expectedCols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	var count int64
	for ; rows.Next(); count++ {
		if err := rows.Scan(vals...); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		for j, v := range vals {
			switch j {
			case 0:
				if val := *v.(*interface{}); val != nil {
					switch k := val.(type) {
					case int64:
						if count != k {
							t.Errorf("k = %d, expected %d", k, count)
						}

					default:
						t.Errorf("error input of type %T", k)
					}
				} else {
					t.Error("received NULL value for column 'k'")
				}

			case 1:
				if val := *v.(*interface{}); val != nil {
					t.Error("received non NULL value for column 'b'")
				}

			case 2:
				if val := *v.(*interface{}); val == nil {
					t.Error("received NULL value for column 'd'")
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := int64(maxValue + 1); eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Check that the index on b eventually goes live even though a schema
	// change in front of it in the queue got purged.
	rows, err = sqlDB.Query(`SELECT * from t.test@test_b_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count = 0
	for ; rows.Next(); count++ {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := int64(maxValue + 1); eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Check that the index on c gets purged.
	if _, err := sqlDB.Query(`SELECT * from t.test@foo`); err == nil {
		t.Fatal("SELECT over index 'foo' works")
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Add immediate GC TTL to allow index creation purge to complete.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	testutils.SucceedsSoon(t, func() error {
		// Check that the number of k-v pairs is accurate.
		return sqltestutils.CheckTableKeyCount(ctx, kvDB, 3, maxValue)
	})

	// State of jobs table
	skip.WithIssue(t, 51796, "TODO(pbardea): The following fails due to causes seemingly unrelated to GC")
	runner := sqlutils.SQLRunner{DB: sqlDB}
	// TODO (lucy): This test API should use an offset starting from the
	// most recent job instead.
	const migrationJobOffset = 0
	for i, tc := range testCases {
		status := jobs.StatusSucceeded
		if tc.errString != "" {
			status = jobs.StatusFailed
		}
		if err := jobutils.VerifySystemJob(t, &runner, migrationJobOffset+i, jobspb.TypeSchemaChange, status, jobs.Record{
			Username:    security.RootUserName(),
			Description: tc.sql,
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	jobRolledBack := 0
	jobID := jobutils.GetJobID(t, &runner, jobRolledBack)

	// Roll back job.
	if err := jobutils.VerifySystemJob(t, &runner, len(testCases), jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUserName(),
		Description: fmt.Sprintf("ROLL BACK JOB %d: %s", jobID, testCases[jobRolledBack].sql),
		DescriptorIDs: descpb.IDs{
			tableDesc.GetID(),
		},
	}); err != nil {
		t.Fatal(err)
	}

}

// This test checks backward compatibility with old data that contains
// sentinel kv pairs at the start of each table row. Cockroachdb used
// to write table rows with sentinel values in the past. When a new column
// is added to such a table with the new column included in the same
// column family as the primary key columns, the sentinel kv pairs
// start representing this new column. This test checks that the sentinel
// values represent NULL column values, and that an UPDATE to such
// a column works correctly.
func TestParseSentinelValueWithNewColumnInSentinelFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
	k INT PRIMARY KEY,
	FAMILY F1 (k)
);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if tableDesc.GetFamilies()[0].DefaultColumnID != 0 {
		t.Fatalf("default column id not set properly: %s", tableDesc)
	}

	// Add some data.
	const maxValue = 10
	inserts := make([]string, maxValue+1)
	for i := range inserts {
		inserts[i] = fmt.Sprintf(`(%d)`, i)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Convert table data created by the above INSERT into sentinel
	// values. This is done to make the table appear like it were
	// written in the past when cockroachdb used to write sentinel
	// values for each table row.
	startKey := keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
	kvs, err := kvDB.Scan(
		ctx,
		startKey,
		startKey.PrefixEnd(),
		maxValue+1)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvs {
		value := roachpb.MakeValueFromBytes(nil)
		if err := kvDB.Put(ctx, kv.Key, &value); err != nil {
			t.Fatal(err)
		}
	}

	// Add a new column that gets added to column family 0,
	// updating DefaultColumnID.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN v INT FAMILY F1`); err != nil {
		t.Fatal(err)
	}
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if tableDesc.GetFamilies()[0].DefaultColumnID != 2 {
		t.Fatalf("default column id not set properly: %s", tableDesc)
	}

	// Update one of the rows.
	const setKey = 5
	const setVal = maxValue - setKey
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, setVal, setKey); err != nil {
		t.Fatal(err)
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// The table contains the one updated value and remaining NULL values.
	rows, err := sqlDB.Query(`SELECT v from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	const eCount = maxValue + 1
	count := 0
	for ; rows.Next(); count++ {
		var val *int
		if err := rows.Scan(&val); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count == setKey {
			if val != nil {
				if setVal != *val {
					t.Errorf("value = %d, expected %d", *val, setVal)
				}
			} else {
				t.Error("received nil value for column 'v'")
			}
		} else if val != nil {
			t.Error("received non NULL value for column 'v'")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}
}

// This test checks whether a column can be added using the name of a column that has just been dropped.
func TestAddColumnDuringColumnDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Close channel to notify that the schema change has
					// been queued and the backfill has started.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT PRIMARY KEY NOT NULL,
    v INT NOT NULL
);
`); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.BulkInsertIntoTable(sqlDB, 1000); err != nil {
		t.Fatal(err)
	}
	// Run the column schema change in a separate goroutine.
	notification := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP column v;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notification
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD column v INT DEFAULT 0;`); !testutils.IsError(err, `column "v" being dropped, try again later`) {
		t.Fatal(err)
	}

	close(continueBackfillNotification)
	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// Test a DROP failure on a unique column. The rollback
// process might not be able to reconstruct the index and thus
// purges the rollback. For now this is considered acceptable.
func TestSchemaUniqueColumnDropFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	waitUntilRevert := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	attempts := 0
	// DROP UNIQUE COLUMN is executed in two steps: drop index and drop column.
	// Dropping the index happens in a separate mutation job from the drop column
	// which does not perform backfilling (like adding indexes and add/drop
	// columns) and completes successfully. However, note that the testing knob
	// hooks are still run as if they were backfill attempts. The index truncation
	// happens as an asynchronous change after the index descriptor is removed,
	// and will be run after the GC TTL is passed and there are no pending
	// synchronous mutations. Therefore, the first two backfill attempts are from
	// the column drop. This part of the change errors during backfilling the
	// second chunk.
	const expectedColumnBackfillAttempts = 2
	const maxValue = (expectedColumnBackfillAttempts/2+1)*chunkSize + 1
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
			// Aggressively checkpoint, so that a schema change
			// failure happens after a checkpoint has been written.
			WriteCheckpointInterval: time.Nanosecond,
			RunBeforeOnFailOrCancel: func(jobID jobspb.JobID) error {
				waitUntilRevert <- struct{}{}
				<-waitUntilRevert
				return nil
			},
		},
		// Disable GC job.
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { select {} }},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Return a deadline exceeded error while dropping
				// the column after the index has been dropped.
				if attempts == expectedColumnBackfillAttempts {
					return errors.New("permanent failure")
				}
				return nil
			},
		},
		// Disable backfill migrations so it doesn't interfere with the
		// backfill in this test.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT UNIQUE DEFAULT 23 CREATE FAMILY F3);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(context.Background(), kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}

	// A schema change that fails.
	go func() {
		// This query stays blocked until the end of the test.
		_, _ = sqlDB.Exec(`ALTER TABLE t.test DROP column v`)
	}()

	// Wait until the job is reverted.
	<-waitUntilRevert

	// The index is not regenerated.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if len(tableDesc.PublicNonPrimaryIndexes()) > 0 {
		t.Fatalf("indexes %+v", tableDesc.PublicNonPrimaryIndexes())
	}

	// Unfortunately this is the same failure present when an index drop
	// fails, so the rollback never completes and leaves orphaned kvs.
	// TODO(erik): Ignore errors or individually drop indexes in
	// DELETE_AND_WRITE_ONLY which failed during the creation backfill
	// as a rollback from a drop.
	if e := 1; e != len(tableDesc.PublicColumns()) {
		t.Fatalf("e = %d, v = %d, columns = %+v", e, len(tableDesc.PublicColumns()), tableDesc.PublicColumns())
	} else if tableDesc.PublicColumns()[0].GetName() != "k" {
		t.Fatalf("columns %+v", tableDesc.PublicColumns())
	} else if len(tableDesc.AllMutations()) != 2 {
		t.Fatalf("mutations %+v", tableDesc.AllMutations())
	}

	close(waitUntilRevert)
}

// TestDropIndexNoRevert tests that failed DROP INDEX requests are not
// reverted.
func TestDropIndexNoRevert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec(`
CREATE TABLE t (pk INT PRIMARY KEY, b int, c int);
CREATE INDEX index_to_drop ON t (b);
INSERT INTO t VALUES (1, 1, 1), (2, 2, 1);
`)
	require.NoError(t, err)

	txn, err := sqlDB.Begin()
	require.NoError(t, err)

	_, err = txn.Exec("DROP INDEX index_to_drop")
	require.NoError(t, err)

	// This CREATE INDEX should fail on commit because of a
	// duplicate key violation.
	_, err = txn.Exec("CREATE UNIQUE INDEX ON t (c)")
	require.NoError(t, err)

	err = txn.Commit()
	require.Error(t, err)

	// Index is dropped even though the commit failed.
	r := sqlDB.QueryRow("SELECT DISTINCT(index_name) FROM [SHOW INDEXES FROM t] WHERE index_name = $1", "index_to_drop")
	var n string
	require.EqualError(t, r.Scan(&n), "sql: no rows in result set")
}

// TestOldRevertedDropIndexesAreIgnored tests previously reverted DROP
// INDEX mutations are no longer respected. That is, we continue to
// DROP the index when the job is resumed.
func TestOldRevertedDropIndexesAreIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	var (
		server serverutils.TestServerInterface
		sqlDB  *gosql.DB
		kvDB   *kv.DB
	)
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobspb.JobID) error {
				mut := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
				for _, m := range mut.AllMutations() {
					if m.Adding() && m.AsIndex() != nil {
						// Make this schema change addition look like a rollback from a failed DROP
						mut.Mutations[m.MutationOrdinal()].Rollback = true
					}
				}
				require.NoError(t, kvDB.Put(
					context.Background(),
					catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, mut.GetID()),
					mut.DescriptorProto(),
				))
				return nil
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	server, sqlDB, kvDB = serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec("CREATE DATABASE test; CREATE TABLE test.t (pk INT PRIMARY KEY, b int)")
	require.NoError(t, err)

	// This create index is mutated above to look like it was the
	// result of a rollback.
	_, err = sqlDB.Exec("CREATE INDEX pretend_drop_revert ON test.t (b)")
	require.NoError(t, err)

	// Index should never get added because the revert should be
	// dropped.
	r := sqlDB.QueryRow("SELECT DISTINCT(index_name) FROM [SHOW INDEXES FROM test.t] WHERE index_name = $1", "pretend_drop_revert")
	var n string
	require.EqualError(t, r.Scan(&n), "sql: no rows in result set")

}

// TestVisibilityDuringPrimaryKeyChange tests visibility of different indexes
// during the primary key change process.
func TestVisibilityDuringPrimaryKeyChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePrimaryKeySwap: func() {
				// Notify the tester that the primary key swap is about to happen.
				swapNotification <- struct{}{}
				// Wait for the tester to finish before continuing the swap.
				<-waitBeforeContinuing
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY, y INT NOT NULL, z INT, INDEX i (z));
INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
`); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (y)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-swapNotification

	row := sqlDB.QueryRow("SHOW CREATE TABLE t.test")
	var scanName, create string
	if err := row.Scan(&scanName, &create); err != nil {
		t.Fatal(err)
	}
	expected := `CREATE TABLE public.test (
	x INT8 NOT NULL,
	y INT8 NOT NULL,
	z INT8 NULL,
	CONSTRAINT test_pkey PRIMARY KEY (x ASC),
	INDEX i (z ASC)
)`
	if create != expected {
		t.Fatalf("expected %s, found %s", expected, create)
	}

	// Let the schema change process continue.
	waitBeforeContinuing <- struct{}{}
	// Wait for the primary key swap to happen.
	wg.Wait()

	row = sqlDB.QueryRow("SHOW CREATE TABLE t.test")
	if err := row.Scan(&scanName, &create); err != nil {
		t.Fatal(err)
	}
	expected = `CREATE TABLE public.test (
	x INT8 NOT NULL,
	y INT8 NOT NULL,
	z INT8 NULL,
	CONSTRAINT test_pkey PRIMARY KEY (y ASC),
	UNIQUE INDEX test_x_key (x ASC),
	INDEX i (z ASC)
)`
	if create != expected {
		t.Fatalf("expected %s, found %s", expected, create)
	}
}

// TestPrimaryKeyChangeWithPrecedingIndexCreation tests that a primary key change
// successfully rewrites indexes that are being created while the primary key change starts.
func TestPrimaryKeyChangeWithPrecedingIndexCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
		chunkSize = 5
	}

	// Protects backfillNotification.
	var mu syncutil.Mutex
	var backfillNotification, continueNotification chan struct{}
	// We have to have initBackfillNotification return the new
	// channel rather than having later users read the original
	// backfillNotification to make the race detector happy.
	initBackfillNotification := func() (chan struct{}, chan struct{}) {
		mu.Lock()
		defer mu.Unlock()
		backfillNotification = make(chan struct{})
		continueNotification = make(chan struct{})
		return backfillNotification, continueNotification
	}
	notifyBackfill := func() {
		mu.Lock()
		defer mu.Unlock()
		if backfillNotification != nil {
			close(backfillNotification)
			backfillNotification = nil
		}
		if continueNotification != nil {
			<-continueNotification
		}
	}
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(_ roachpb.Span) error {
				notifyBackfill()
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}

	t.Run("create-index-before", func(t *testing.T) {
		skip.WithIssue(t, 45510, "unskip when finished")
		if _, err := sqlDB.Exec(`CREATE TABLE t.test (k INT NOT NULL, v INT)`); err != nil {
			t.Fatal(err)
		}
		if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
			t.Fatal(err)
		}

		backfillNotif, _ := initBackfillNotification()
		var wg sync.WaitGroup
		wg.Add(1)
		// Create an index on the table that will need to get rewritten.
		go func() {
			if _, err := sqlDB.Exec(`CREATE INDEX i ON t.test (v)`); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()

		// Wait until the create index mutation has progressed before starting the alter primary key.
		<-backfillNotif

		if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)`); err != nil {
			t.Fatal(err)
		}

		wg.Wait()

		// There should be 4 k/v pairs per row:
		// * the original rowid index.
		// * the old index on v.
		// * the new primary key on k.
		// * the new index for v with k as the unique column.
		testutils.SucceedsSoon(t, func() error {
			return sqltestutils.CheckTableKeyCount(ctx, kvDB, 4, maxValue)
		})
	})

	// Repeat the prior process but with a primary key change before.
	t.Run("pk-change-before", func(t *testing.T) {
		var wg sync.WaitGroup
		if _, err := sqlDB.Exec(`
DROP TABLE IF EXISTS t.test;
CREATE TABLE t.test (k INT NOT NULL, v INT, v2 INT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		backfillNotif, continueNotif := initBackfillNotification()
		// Can't use sqltestutils.BulkInsertIntoTable here because that only works with 2 columns.
		inserts := make([]string, maxValue+1)
		for i := 0; i < maxValue+1; i++ {
			inserts[i] = fmt.Sprintf(`(%d, %d, %d)`, i, maxValue-i, i)
		}
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		// Alter the primary key of the table.
		go func() {
			if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (v2)`); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()

		<-backfillNotif

		// This must be rejected, because there is a primary key change already in progress.
		_, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)`)
		if !testutils.IsError(err, "pq: unimplemented: table test is currently undergoing a schema change") {
			t.Errorf("expected to concurrent primary key change to error, but got %+v", err)
		}

		// After the expected error, let the backfill continue.
		close(continueNotif)

		wg.Wait()

		// After the first primary key change is done, the follow up primary key change should succeed.
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)`); err != nil {
			t.Fatal(err)
		}

		// There should be 4 k/v pairs per row:
		// * the original rowid index.
		// * the new primary index on v2.
		// * the new primary index on k.
		// * the rewritten demoted index on v2.
		testutils.SucceedsSoon(t, func() error {
			return sqltestutils.CheckTableKeyCount(ctx, kvDB, 4, maxValue)
		})
	})
}

// TestSchemaChangeWhileExecutingPrimaryKeyChange tests that other schema
// changes cannot be queued while a primary key change is executing.
func TestSchemaChangeWhileExecutingPrimaryKeyChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	backfillNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(_ roachpb.Span) error {
				backfillNotification <- struct{}{}
				<-waitBeforeContinuing
				return nil
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT NOT NULL, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableID := descpb.ID(sqlutils.QueryTableID(t, sqlDB, "t", "public", "test"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-backfillNotification

	// Test that trying different schema changes results an error.
	_, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN z INT`)
	expected := fmt.Sprintf(`pq: relation "test" \(%d\): unimplemented: cannot perform a schema change operation while a primary key change is in progress`, tableID)
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected to find error %s but found %+v", expected, err)
	}

	_, err = sqlDB.Exec(`CREATE INDEX ON t.test(v)`)
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected to find error %s but found %+v", expected, err)
	}

	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}

// TestPrimaryKeyChangeWithOperations ensures that different operations succeed
// while a primary key change is happening.
func TestPrimaryKeyChangeWithOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
		chunkSize = 5
	}

	// protects backfillNotification
	var mu syncutil.Mutex
	backfillNotification := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	initBackfillNotification := func() chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		backfillNotification = make(chan struct{})
		return backfillNotification
	}
	notifyBackfill := func() {
		mu.Lock()
		defer mu.Unlock()
		if backfillNotification != nil {
			// Close channel to notify that the backfill has started.
			close(backfillNotification)
			backfillNotification = nil
		}
	}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				notifyBackfill()
				return nil
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT NOT NULL, v INT);
`); err != nil {
		t.Fatal(err)
	}
	// GC the old indexes to be dropped after the PK change immediately.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)",
		maxValue,
		1,
		initBackfillNotification(),
		// We don't let runSchemaChangeWithOperations use UPSERT statements, because
		// way in which runSchemaChangeWithOperations uses them assumes that k is already
		// the primary key of the table, leading to some errors during the UPSERT
		// conflict handling. Since we are changing the primary key of the table to
		// be using k as the primary key, we disable the UPSERT statements in the test.
		false,
	)

	// runSchemaChangeWithOperations only performs some simple
	// operations on the kv table. We want to also run some
	// more operations against a table with more columns.
	// We separate the columns into multiple different families
	// in order to test different cases of reads, writes and
	// deletions operating on different sets of families.
	if _, err := sqlDB.Exec(`
DROP TABLE t.test;
CREATE TABLE t.test (
	x INT PRIMARY KEY, y INT NOT NULL, z INT, a INT, b INT,
	c INT, d INT, FAMILY (x), FAMILY (y), FAMILY (z),
	FAMILY (a, b), FAMILY (c), FAMILY (d)
);
`); err != nil {
		t.Fatal(err)
	}
	// Insert into the table.
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(
			"(%d, %d, %d, %d, %d, %d, %d)",
			i, i, i, i, i, i, i,
		)
	}
	if _, err := sqlDB.Exec(
		fmt.Sprintf(`INSERT INTO t.test VALUES %s`, strings.Join(inserts, ","))); err != nil {
		t.Fatal(err)
	}

	notification := initBackfillNotification()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`
ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (y)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until the backfill starts.
	<-notification

	// Update some rows.
	rowsUpdated := make(map[int]struct{})
	for i := 0; i < 10; i++ {
		// Update a row that hasn't been updated yet.
		for {
			k := rand.Intn(maxValue)
			if _, ok := rowsUpdated[k]; !ok {
				rowsUpdated[k] = struct{}{}
				break
			}
		}
	}
	for k := range rowsUpdated {
		if _, err := sqlDB.Exec(`
UPDATE t.test SET z = NULL, a = $1, b = NULL, c = NULL, d = $1 WHERE y = $2`, 2*k, k); err != nil {
			t.Fatal(err)
		}
	}

	// Delete some rows.
	rowsDeleted := make(map[int]struct{})
	for i := 0; i < 10; i++ {
		// Delete a row that hasn't been updated.
		for {
			k := rand.Intn(maxValue)
			_, updated := rowsUpdated[k]
			_, deleted := rowsDeleted[k]
			if !updated && !deleted {
				rowsDeleted[k] = struct{}{}
				break
			}
		}
	}
	for k := range rowsDeleted {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE x = $1`, k); err != nil {
			t.Fatal(err)
		}
	}

	// Insert some rows.
	inserts = make([]string, 10)
	for i := 0; i < 10; i++ {
		val := i + maxValue + 1
		inserts[i] = fmt.Sprintf(
			"(%d, %d, %d, %d, %d, %d, %d)",
			val, val, val, val, val, val, val,
		)
	}
	if _, err := sqlDB.Exec(
		fmt.Sprintf(`INSERT INTO t.test VALUES %s`, strings.Join(inserts, ","))); err != nil {
		t.Fatal(err)
	}

	// Wait for the pk change to complete.
	wg.Wait()

	// Ensure that the count of rows is correct along both indexes.
	var count int
	row := sqlDB.QueryRow(`SELECT count(*) FROM t.test@test_pkey`)
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != maxValue+1 {
		t.Fatalf("expected %d rows, found %d", maxValue+1, count)
	}
	row = sqlDB.QueryRow(`SELECT count(x) FROM t.test@test_x_key`)
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != maxValue+1 {
		t.Fatalf("expected %d rows, found %d", maxValue+1, count)
	}

	// Verify that we cannot find our deleted rows.
	for k := range rowsDeleted {
		row := sqlDB.QueryRow(`SELECT count(*) FROM t.test WHERE x = $1`, k)
		if err := row.Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("expected %d rows, found %d", 0, count)
		}
	}

	// Verify that we can find our inserted rows.
	for i := 0; i < 10; i++ {
		val := i + maxValue + 1
		row := sqlDB.QueryRow(`SELECT * FROM t.test WHERE y = $1`, val)
		var x, y, z, a, b, c, d int
		if err := row.Scan(&x, &y, &z, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		for i, v := range []int{x, y, z, a, b, c, d} {
			if v != val {
				t.Fatalf("expected to find %d for column %d, but found %d", val, i, v)
			}
		}
	}

	// Verify that our updated rows have indeed been updated.
	for k := range rowsUpdated {
		row := sqlDB.QueryRow(`SELECT * FROM t.test WHERE y = $1`, k)
		var (
			x, y, a, d int
			z, b, c    gosql.NullInt64
		)
		if err := row.Scan(&x, &y, &z, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, k, x)
		require.Equal(t, k, y)
		require.Equal(t, 2*k, a)
		require.Equal(t, 2*k, d)
		for _, v := range []gosql.NullInt64{z, b, c} {
			if v.Valid {
				t.Fatalf("expected NULL but found %d", v.Int64)
			}
		}
	}
}

// TestPrimaryKeyChangeInTxn tests running a primary key
// change on a table created in the same transaction.
func TestPrimaryKeyChangeInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
BEGIN;
CREATE TABLE t.test (x INT PRIMARY KEY, y INT NOT NULL, z INT, INDEX (z));
ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (y);
COMMIT;
`); err != nil {
		t.Fatal(err)
	}
	// Ensure that t.test doesn't have any pending mutations
	// after the primary key change.
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if len(desc.AllMutations()) != 0 {
		t.Fatalf("expected to find 0 mutations, but found %d", len(desc.AllMutations()))
	}
}

// TestPrimaryKeyChangeKVOps tests sequences of k/v operations
// on the new primary index while it is staged as a special
// secondary index. We cannot test this in a standard logic
// test because we only have control over stopping the backfill
// process in a unit test like this. This test is essentially
// a heavy-weight poor man's logic tests, but there doesn't
// seem to be a better way to achieve what is needed here.
func TestPrimaryKeyChangeKVOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	backfillNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(_ roachpb.Span) error {
				backfillNotification <- struct{}{}
				<-waitBeforeContinuing
				return nil
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	defer close(waitBeforeContinuing)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
	x INT PRIMARY KEY,
	y INT NOT NULL,
	z INT,
	a INT,
	b INT,
	c INT,
	FAMILY (x), FAMILY (y), FAMILY (z, a), FAMILY (b), FAMILY (c)
)
`); err != nil {
		t.Fatal(err)
	}

	tableID := descpb.ID(sqlutils.QueryTableID(t, sqlDB, "t", "public", "test"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (y)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait for the temporary indexes for the new primary indexes
	// to move to the DELETE_AND_WRITE_ONLY state, which happens
	// right before backfilling of the index begins.
	<-backfillNotification

	scanToArray := func(rows *gosql.Rows) []string {
		var found []string
		for rows.Next() {
			var message string
			if err := rows.Scan(&message); err != nil {
				t.Fatal(err)
			}
			found = append(found, message)
		}
		return found
	}

	// Test that we only insert the necessary k/v's.
	rows, err := sqlDB.Query(fmt.Sprintf(`
	SET TRACING=on,kv,results;
	INSERT INTO t.test VALUES (1, 2, 3, NULL, NULL, 6);
	SET TRACING=off;
	SELECT message FROM [SHOW KV TRACE FOR SESSION] WHERE
		message LIKE '%%Put /Table/%d%%' ORDER BY message;`, tableID))
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		// The first CPut's are to the primary index.
		fmt.Sprintf("CPut /Table/%d/1/1/0 -> /TUPLE/", tableID),
		// TODO (rohany): this k/v is spurious and should be removed
		//  when #45343 is fixed.
		fmt.Sprintf("CPut /Table/%d/1/1/1/1 -> /INT/2", tableID),
		fmt.Sprintf("CPut /Table/%d/1/1/2/1 -> /TUPLE/3:3:Int/3", tableID),
		fmt.Sprintf("CPut /Table/%d/1/1/4/1 -> /INT/6", tableID),
		// Temporary index that exists during the
		// backfill. This should have the same number of Puts
		// as there are CPuts above.
		fmt.Sprintf("Put /Table/%d/3/2/0 -> /BYTES/0x0a030a1302", tableID),
		// TODO (rohany): this k/v is spurious and should be removed
		//  when #45343 is fixed.
		fmt.Sprintf("Put /Table/%d/3/2/1/1 -> /BYTES/0x0a020104", tableID),
		fmt.Sprintf("Put /Table/%d/3/2/2/1 -> /BYTES/0x0a030a3306", tableID),
		fmt.Sprintf("Put /Table/%d/3/2/4/1 -> /BYTES/0x0a02010c", tableID),

		// ALTER PRIMARY KEY makes an additional unique index
		// based on the old primary key.
		fmt.Sprintf("Put /Table/%d/5/1/0 -> /BYTES/0x0a02038a", tableID),

		// Indexes 2 and 4 which are currently being added
		// should have no writes because they are in the
		// BACKFILLING state at this point.
	}
	require.Equal(t, expected, scanToArray(rows))

	// Test that we remove all families when deleting.
	rows, err = sqlDB.Query(fmt.Sprintf(`
	SET TRACING=on, kv, results;
	DELETE FROM t.test WHERE y = 2;
	SET TRACING=off;
	SELECT message FROM [SHOW KV TRACE FOR SESSION]
        WHERE
		message LIKE 'Del /Table/%[1]d%%' OR
                message LIKE 'Put (delete) /Table/%[1]d%%'
        ORDER BY message;`, tableID))
	if err != nil {
		t.Fatal(err)
	}

	expected = []string{
		// Primary index should see this delete.
		fmt.Sprintf("Del /Table/%d/1/1/0", tableID),
		fmt.Sprintf("Del /Table/%d/1/1/1/1", tableID),
		fmt.Sprintf("Del /Table/%d/1/1/2/1", tableID),
		fmt.Sprintf("Del /Table/%d/1/1/3/1", tableID),
		fmt.Sprintf("Del /Table/%d/1/1/4/1", tableID),

		// The temporary indexes are delete-preserving -- they
		// should see the delete and issue Puts.
		fmt.Sprintf("Put (delete) /Table/%d/3/2/0", tableID),
		fmt.Sprintf("Put (delete) /Table/%d/3/2/1/1", tableID),
		fmt.Sprintf("Put (delete) /Table/%d/3/2/2/1", tableID),
		fmt.Sprintf("Put (delete) /Table/%d/3/2/3/1", tableID),
		fmt.Sprintf("Put (delete) /Table/%d/3/2/4/1", tableID),
		fmt.Sprintf("Put (delete) /Table/%d/5/1/0", tableID),
	}
	require.Equal(t, expected, scanToArray(rows))

	// Test that we update all families when the key changes.
	rows, err = sqlDB.Query(fmt.Sprintf(`
	INSERT INTO t.test VALUES (1, 2, 3, NULL, NULL, 6);
	SET TRACING=on, kv, results;
	UPDATE t.test SET y = 3 WHERE y = 2;
	SET TRACING=off;
	SELECT message FROM [SHOW KV TRACE FOR SESSION] WHERE
		message LIKE 'Put /Table/%[1]d/%%' OR
		message LIKE 'Del /Table/%[1]d/%%' OR
		message LIKE 'CPut /Table/%[1]d/%%';`, tableID))
	if err != nil {
		t.Fatal(err)
	}

	expected = []string{
		// The primary index should see the update
		fmt.Sprintf("Put /Table/%d/1/1/1/1 -> /INT/3", tableID),
		// The temporary index for the newly added index sees
		// a Put in all families.
		fmt.Sprintf("Put /Table/%d/3/3/0 -> /BYTES/0x0a030a1302", tableID),
		fmt.Sprintf("Put /Table/%d/3/3/1/1 -> /BYTES/0x0a020106", tableID),
		fmt.Sprintf("Put /Table/%d/3/3/2/1 -> /BYTES/0x0a030a3306", tableID),
		fmt.Sprintf("Put /Table/%d/3/3/4/1 -> /BYTES/0x0a02010c", tableID),
		// TODO(ssd): double-check that this trace makes
		// sense.
		fmt.Sprintf("Put /Table/%d/5/1/0 -> /BYTES/0x0a02038b", tableID),
	}
	require.Equal(t, expected, scanToArray(rows))

	// Test that we only update necessary families when the key doesn't change.
	rows, err = sqlDB.Query(fmt.Sprintf(`
	SET TRACING=on, kv, results;
	UPDATE t.test SET z = NULL, b = 5, c = NULL WHERE y = 3;
	SET TRACING=off;
	SELECT message FROM [SHOW KV TRACE FOR SESSION] WHERE
		message LIKE 'Put /Table/%[1]d/%%' OR
		message LIKE 'Del /Table/%[1]d/%%' OR
		message LIKE 'CPut /Table/%[1]d/2%%';`, tableID))
	if err != nil {
		t.Fatal(err)
	}

	expected = []string{

		fmt.Sprintf("Del /Table/%d/1/1/2/1", tableID),
		fmt.Sprintf("Put /Table/%d/1/1/3/1 -> /INT/5", tableID),
		fmt.Sprintf("Del /Table/%d/1/1/4/1", tableID),

		// TODO(ssd): double-check that this trace makes
		// sense.
		fmt.Sprintf("Put /Table/%d/3/3/3/1 -> /BYTES/0x0a02010a", tableID),
	}
	require.Equal(t, expected, scanToArray(rows))

	waitBeforeContinuing <- struct{}{}

	wg.Wait()
}

// TestPrimaryKeyIndexRewritesGetRemoved ensures that the old versions of
// indexes that are being rewritten eventually get cleaned up and removed.
func TestPrimaryKeyIndexRewritesGetRemoved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT NOT NULL, w INT, INDEX i (w));
INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (v);
`); err != nil {
		t.Fatal(err)
	}

	// Wait for the async schema changer to run.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		// We expect to have 3 (one for each row) * 3 (new primary key, old primary key and i rewritten).
		return sqltestutils.CheckTableKeyCountExact(ctx, kvDB, 9)
	})
}

func TestPrimaryKeyChangeWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
		chunkSize = 5
	}

	ctx := context.Background()
	var db *gosql.DB
	shouldCancel := true
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if !shouldCancel {
					return nil
				}
				if _, err := db.Exec(`CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_type = 'SCHEMA CHANGE' AND
						status = $1 AND
						description NOT LIKE 'ROLL BACK%'
				)`, jobs.StatusRunning); err != nil {
					t.Error(err)
				}
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	db = sqlDB
	defer s.Stopper().Stop(ctx)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT NOT NULL, v INT);
`); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// This will fail, so we don't want to check the error.
	_, _ = sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)`)

	// Ensure that the mutations corresponding to the primary key change are cleaned up and
	// that the job did not succeed even though it was canceled.
	testutils.SucceedsSoon(t, func() error {
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		if len(tableDesc.AllMutations()) != 0 {
			return errors.Errorf("expected 0 mutations after cancellation, found %d", len(tableDesc.AllMutations()))
		}
		if tableDesc.GetPrimaryIndex().NumKeyColumns() != 1 || tableDesc.GetPrimaryIndex().GetKeyColumnName(0) != "rowid" {
			return errors.Errorf("expected primary key change to not succeed after cancellation")
		}
		return nil
	})

	// Stop any further attempts at cancellation, so the GC jobs don't fail.
	shouldCancel = false
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if _, err := sqltestutils.AddImmediateGCZoneConfig(db, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}
	// Ensure that the writes from the partial new indexes are cleaned up.
	testutils.SucceedsSoon(t, func() error {
		return sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue)
	})
}

// TestPrimaryKeyDropIndexNotCancelable tests that the job to drop indexes after
// a primary key change is not cancelable.
func TestPrimaryKeyDropIndexNotCancelable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var db *gosql.DB
	shouldAttemptCancel := true
	hasAttemptedCancel := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		GCJob: &sql.GCJobTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				if !shouldAttemptCancel {
					return nil
				}
				_, err := db.Exec(`CANCEL JOB ($1)`, jobID)
				require.Regexp(t, "not cancelable", err)
				shouldAttemptCancel = false
				close(hasAttemptedCancel)
				return nil
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	db = sqlDB
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT NOT NULL, v INT);
`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (k)`)
	require.NoError(t, err)

	// Wait until the testing knob has notified that canceling the job has been
	// attempted before continuing.
	<-hasAttemptedCancel

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, 1, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Description:   "CLEANUP JOB for 'ALTER TABLE t.public.test ALTER PRIMARY KEY USING COLUMNS (k)'",
			Username:      security.RootUserName(),
			DescriptorIDs: descpb.IDs{tableDesc.GetID()},
		})
	})
}

// TestMultiplePrimaryKeyChanges ensures that we can run many primary key
// changes back to back. We cannot run this in a logic test because we need to
// set a low job registry adopt interval, so that each successive schema change
// can run immediately without waiting too long for a retry due to being second
// in line after the mutation to drop indexes for the previous primary key
// change.
func TestMultiplePrimaryKeyChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT NOT NULL, y INT NOT NULL, z INT NOT NULL, w int, INDEX i (w));
INSERT INTO t.test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);
`); err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"x", "y", "z"} {
		query := fmt.Sprintf("ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (%s)", col)
		if _, err := sqlDB.Exec(query); err != nil {
			t.Fatal(err)
		}
		rows, err := sqlDB.Query("SELECT * FROM t.test")
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; rows.Next(); i++ {
			var x, y, z, w int
			if err := rows.Scan(&x, &y, &z, &w); err != nil {
				t.Fatal(err)
			}
			if !(x == i && y == i && z == i && w == i) {
				t.Errorf("expected all columns to be %d, but found (%d, %d, %d, %d)", i, x, y, z, w)
			}
		}
	}
}

func TestGrantRevokeWhileIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	backfillNotification := make(chan bool)

	backfillCompleteNotification := make(chan bool)
	continueSchemaChangeNotification := make(chan bool)

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if backfillNotification != nil {
					// Close channel to notify that the schema change has
					// been queued and the backfill has started.
					close(backfillNotification)
					backfillNotification = nil
					<-continueSchemaChangeNotification
				}
				return nil
			},
			BulkAdderFlushesEveryBatch: true,
		},
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunAfterIndexBackfill: func() {
				if backfillCompleteNotification != nil {
					// Close channel to notify that the schema change
					// backfill is complete and not finalized.
					close(backfillCompleteNotification)
					backfillCompleteNotification = nil
					<-continueSchemaChangeNotification
				}
			},
		},

		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	server, db, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
CREATE USER foo;
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT8 NOT NULL,
    v INT8,
    length INT8 NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (k),
    FAMILY "primary" (k, v, length)
);
INSERT INTO t.test (k, v, length) VALUES (0, 1, 1);
INSERT INTO t.test (k, v, length) VALUES (1, 2, 1);
INSERT INTO t.test (k, v, length) VALUES (2, 3, 1);
`)

	// Run the index backfill schema change in a separate goroutine.
	notification := backfillNotification
	doneNotification := backfillCompleteNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.Exec(t, `CREATE UNIQUE INDEX v_idx ON t.test (v)`)
		wg.Done()
	}()

	// Wait until the first mutation has processed through the state machine
	// and has started backfilling.
	<-notification

	sqlDB.Exec(t, `GRANT ALL ON TABLE t.test TO foo`)
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT privilege_type FROM [SHOW GRANTS ON TABLE t.test] WHERE grantee='%s'`, "foo"), [][]string{{"ALL"}})
	sqlDB.Exec(t, `REVOKE ALL ON TABLE t.test FROM foo`)
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM [SHOW GRANTS ON TABLE t.test] WHERE grantee='%s'`, "foo"), [][]string{{"0"}})

	continueSchemaChangeNotification <- true

	<-doneNotification

	// The index should have been backfilled at this point.
	expectedErr := "duplicate key value violates unique constraint \"v_idx\""
	sqlDB.ExpectErr(t, expectedErr, `INSERT INTO t.test(k, v, length) VALUES (5, 1, 8)`)

	close(continueSchemaChangeNotification)
	wg.Wait()

	// Verify that the grants on the table are still as expected after the
	// backfill has completed.
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM [SHOW GRANTS ON TABLE t.test] WHERE grantee='%s'`, "foo"), [][]string{{"0"}})
}

// Test CRUD operations can read NULL values for NOT NULL columns
// in the middle of a column backfill.
func TestCRUDWhileColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	backfillNotification := make(chan bool)

	backfillCompleteNotification := make(chan bool)
	continueSchemaChangeNotification := make(chan bool)

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if backfillNotification != nil {
					// Close channel to notify that the schema change has
					// been queued and the backfill has started.
					close(backfillNotification)
					backfillNotification = nil
					<-continueSchemaChangeNotification
				}
				return nil
			},
			RunAfterBackfillChunk: func() {
				if backfillCompleteNotification != nil {
					// Close channel to notify that the schema change
					// backfill is complete and not finalized.
					close(backfillCompleteNotification)
					backfillCompleteNotification = nil
					<-continueSchemaChangeNotification
				}
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT8 NOT NULL,
    v INT8,
    length INT8 NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (k),
    INDEX v_idx (v),
    FAMILY "primary" (k, v, length)
);
INSERT INTO t.test (k, v, length) VALUES (0, 1, 1);
INSERT INTO t.test (k, v, length) VALUES (1, 2, 1);
INSERT INTO t.test (k, v, length) VALUES (2, 3, 1);
`); err != nil {
		t.Fatal(err)
	}

	// Run the column schema change in a separate goroutine.
	notification := backfillNotification
	doneNotification := backfillCompleteNotification
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD id INT8 NOT NULL DEFAULT 2, ADD u INT8 NOT NULL AS (v+1) STORED;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until the first mutation has processed through the state machine
	// and has started backfilling.
	<-notification

	go func() {
		// Create a column that uses the above column in an expression.
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD z INT8 AS (k + id) STORED;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until both mutations are queued up.
	testutils.SucceedsSoon(t, func() error {
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		if l := len(tableDesc.AllMutations()); l != 3 {
			return errors.Errorf("number of mutations = %d", l)
		}
		return nil
	})

	// UPDATE the row using the secondary index.
	if _, err := sqlDB.Exec(`UPDATE t.test SET length = 27000 WHERE v = 1`); err != nil {
		t.Error(err)
	}

	// UPDATE the row using the primary index.
	if _, err := sqlDB.Exec(`UPDATE t.test SET length = 27001 WHERE k = 1`); err != nil {
		t.Error(err)
	}

	// Use UPSERT instead of UPDATE.
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, length) VALUES (2, 3, 27000)`); err != nil {
		t.Error(err)
	}

	// UPSERT inserts a new row.
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, length) VALUES (3, 4, 27000)`); err != nil {
		t.Error(err)
	}

	// INSERT inserts a new row.
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, length) VALUES (4, 5, 270)`); err != nil {
		t.Error(err)
	}

	// DELETE.
	if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 1`); err != nil {
		t.Error(err)
	}

	// DELETE using the secondary index.
	if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE v = 4`); err != nil {
		t.Error(err)
	}

	// Ensure that the newly added column cannot be supplied with any values.
	if _, err := sqlDB.Exec(`UPDATE t.test SET id = 27000 WHERE k = 2`); !testutils.IsError(err,
		`column "id" does not exist`) && !testutils.IsError(err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET id = NULL WHERE k = 2`); !testutils.IsError(err,
		`column "id" does not exist`) && !testutils.IsError(err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, id) VALUES (2, 3, 234)`); !testutils.IsError(
		err, `column "id" does not exist`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, id) VALUES (2, 3, NULL)`); !testutils.IsError(
		err, `column "id" does not exist`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, id) VALUES (4, 5, 270)`); !testutils.IsError(
		err, `column "id" does not exist`) && !testutils.IsError(
		err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, id) VALUES (4, 5, NULL)`); !testutils.IsError(
		err, `column "id" does not exist`) && !testutils.IsError(
		err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}

	// Use column in an expression.
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test (k, v, length) VALUES (2, 1, 3) ON CONFLICT (k) DO UPDATE SET (k, v, length) = (id + 1, 1, 3)`,
	); err != nil {
		t.Error(err)
	}

	// SHOW CREATE TABLE doesn't show new columns.
	row := sqlDB.QueryRow(`SHOW CREATE TABLE t.test`)
	var scanName, create string
	if err := row.Scan(&scanName, &create); err != nil {
		t.Fatal(err)
	}
	if scanName != `t.public.test` {
		t.Fatalf("expected table name %s, got %s", `test`, scanName)
	}
	expect := `CREATE TABLE public.test (
	k INT8 NOT NULL,
	v INT8 NULL,
	length INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (k ASC),
	INDEX v_idx (v ASC)
)`
	if create != expect {
		t.Fatalf("got: %s\nexpected: %s", create, expect)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if l := len(tableDesc.AllMutations()); l != 3 {
		t.Fatalf("number of mutations = %d", l)
	}

	continueSchemaChangeNotification <- true

	<-doneNotification

	expectedErr := "\"u\" violates not-null constraint"
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, length) VALUES (5, NULL, 8)`); !testutils.IsError(err, expectedErr) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`UPDATE t.test SET v = NULL WHERE k = 0`); !testutils.IsError(err, expectedErr) {
		t.Fatal(err)
	}

	close(continueSchemaChangeNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
	// Check data!
	rows, err := sqlDB.Query(`SELECT k, v, length, id, u, z FROM t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	expected := [][]int{
		{0, 1, 27000, 2, 2, 2},
		{3, 1, 3, 2, 2, 5},
		{4, 5, 270, 2, 6, 6},
	}
	count := 0
	for ; rows.Next(); count++ {
		var i1, i2, i3, i4, i5, i6 *int
		if err := rows.Scan(&i1, &i2, &i3, &i4, &i5, &i6); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		row := fmt.Sprintf("%d %d %d %d %d %d", *i1, *i2, *i3, *i4, *i5, *i6)
		exp := expected[count]
		expRow := fmt.Sprintf("%d %d %d %d %d %d", exp[0], exp[1], exp[2], exp[3], exp[4], exp[5])
		if row != expRow {
			t.Errorf("expected %q but read %q", expRow, row)
		}
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	} else if count != 3 {
		t.Errorf("expected 3 rows but read %d", count)
	}
}

// Test that a schema change backfill that completes on a
// backfill chunk boundary works correctly. A backfill is done
// by scanning a table in chunks and backfilling the schema
// element for each chunk. Normally the last chunk is smaller
// than the other chunks (configured chunk size), but it can
// sometimes be equal in size. This test deliberately runs a
// schema change where the last chunk size is equal to the
// configured chunk size.
func TestBackfillCompletesOnChunkBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numNodes = 5
	const chunkSize = 100
	// The number of rows in the table is a multiple of chunkSize.
	// [0...maxValue], so that the backfill processing ends on
	// a chunk boundary.
	const maxValue = 3*chunkSize - 1
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
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

	if _, err := sqlDB.Exec(`
 CREATE DATABASE t;
 CREATE TABLE t.test (k INT8 PRIMARY KEY, v INT8, pi DECIMAL DEFAULT (DECIMAL '3.14'));
 CREATE UNIQUE INDEX vidx ON t.test (v);
 `); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	// Run some schema changes.
	testCases := []struct {
		sql           string
		numKeysPerRow int
	}{
		{sql: "ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')", numKeysPerRow: 2},
		{sql: "ALTER TABLE t.test DROP pi", numKeysPerRow: 2},
		{sql: "CREATE UNIQUE INDEX foo ON t.test (v)", numKeysPerRow: 3},
		{sql: "DROP INDEX t.test@vidx CASCADE", numKeysPerRow: 3},
	}

	for _, tc := range testCases {
		t.Run(tc.sql, func(t *testing.T) {
			// Start schema change that eventually runs a backfill.
			if _, err := sqlDB.Exec(tc.sql); err != nil {
				t.Error(err)
			}

			ctx := context.Background()

			// Verify the number of keys left behind in the table to
			// validate schema change operations.
			if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, tc.numKeysPerRow, maxValue); err != nil {
				t.Fatal(err)
			}

			if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSchemaChangeInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name        string
		firstStmt   string
		secondStmt  string
		expectedErr string
	}{
		// schema change followed by another statement works.
		{
			name:        `createindex-insert`,
			firstStmt:   `CREATE INDEX foo ON t.kv (v)`,
			secondStmt:  `INSERT INTO t.kv VALUES ('c', 'd')`,
			expectedErr: ``,
		},
		// CREATE TABLE followed by INSERT works.
		{
			name:        `createtable-insert`,
			firstStmt:   `CREATE TABLE t.origin (k CHAR PRIMARY KEY, v CHAR);`,
			secondStmt:  `INSERT INTO t.origin VALUES ('c', 'd')`,
			expectedErr: ``},
		// Support multiple schema changes for ORMs: #15269
		// Support insert into another table after schema changes: #15297
		{
			name:        `multiple-schema-change`,
			firstStmt:   `CREATE TABLE t.orm1 (k CHAR PRIMARY KEY, v CHAR); CREATE TABLE t.orm2 (k CHAR PRIMARY KEY, v CHAR);`,
			secondStmt:  `CREATE INDEX foo ON t.orm1 (v); CREATE INDEX foo ON t.orm2 (v); INSERT INTO t.origin VALUES ('e', 'f')`,
			expectedErr: ``,
		},
		// schema change at the end of a transaction that has written.
		{
			name:       `insert-create`,
			firstStmt:  `INSERT INTO t.kv VALUES ('e', 'f')`,
			secondStmt: `CREATE INDEX foo2 ON t.kv (v)`,
		},
		// schema change at the end of a read only transaction.
		{
			name:        `select-create`,
			firstStmt:   `SELECT * FROM t.kv`,
			secondStmt:  `CREATE INDEX bar ON t.kv (v)`,
			expectedErr: ``,
		},
		{
			name:        `index-on-add-col`,
			firstStmt:   `ALTER TABLE t.kv ADD i INT`,
			secondStmt:  `CREATE INDEX foobar ON t.kv (i)`,
			expectedErr: ``,
		},
		{
			name:        `check-on-add-col`,
			firstStmt:   `ALTER TABLE t.kv ADD j INT`,
			secondStmt:  `ALTER TABLE t.kv ADD CONSTRAINT ck_j CHECK (j >= 0)`,
			expectedErr: ``,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tx, err := sqlDB.Begin()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := tx.Exec(testCase.firstStmt); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Exec(testCase.secondStmt)

			if testCase.expectedErr != "" {
				// Can't commit after ALTER errored, so we ROLLBACK.
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Fatal(rollbackErr)
				}

				if !testutils.IsError(err, testCase.expectedErr) {
					t.Fatalf("different error than expected: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}

				if err := sqlutils.RunScrub(sqlDB, "t", "kv"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSecondaryIndexWithOldStoringEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX i (a) STORING (b),
  UNIQUE INDEX u (a) STORING (b)
);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "d", "t")
	// Verify that this descriptor uses the new STORING encoding. Overwrite it
	// with one that uses the old encoding.
	for _, index := range tableDesc.PublicNonPrimaryIndexes() {
		if index.NumKeySuffixColumns() != 1 {
			t.Fatalf("KeySuffixColumnIDs not set properly: %s", tableDesc)
		}
		if index.NumSecondaryStoredColumns() != 1 {
			t.Fatalf("StoreColumnIDs not set properly: %s", tableDesc)
		}
		newIndexDesc := index.IndexDescDeepCopy()
		newIndexDesc.KeySuffixColumnIDs = append(newIndexDesc.KeySuffixColumnIDs, newIndexDesc.StoreColumnIDs...)
		newIndexDesc.StoreColumnIDs = nil
		tableDesc.SetPublicNonPrimaryIndex(index.Ordinal(), newIndexDesc)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (11, 1, 2);`); err != nil {
		t.Fatal(err)
	}
	// Force another ID allocation to ensure that the old encoding persists.
	if _, err := sqlDB.Exec(`ALTER TABLE d.t ADD COLUMN c INT;`); err != nil {
		t.Fatal(err)
	}
	// Ensure that the decoder sees the old encoding.
	for indexName, expExplainRow := range map[string]string{
		"i": "fetched: /t/i/1/11/2 -> <undecoded>",
		"u": "fetched: /t/u/1 -> /11/2",
	} {
		t.Run("index scan", func(t *testing.T) {
			if _, err := sqlDB.Exec(fmt.Sprintf(`SET tracing = on,kv; SELECT k, a, b FROM d.t@%s; SET tracing = off`, indexName)); err != nil {
				t.Fatal(err)
			}

			rows, err := sqlDB.Query(
				`SELECT message FROM [SHOW KV TRACE FOR SESSION] ` +
					`WHERE message LIKE 'fetched:%'`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for ; rows.Next(); count++ {
				var msg string
				if err := rows.Scan(&msg); err != nil {
					t.Errorf("row %d scan failed: %s", count, err)
					continue
				}
				if msg != expExplainRow {
					t.Errorf("expected %q but read %q", expExplainRow, msg)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			} else if count != 1 {
				t.Errorf("expected one row but read %d", count)
			}
		})
		t.Run("data scan", func(t *testing.T) {
			rows, err := sqlDB.Query(fmt.Sprintf(`SELECT k, a, b FROM d.t@%s;`, indexName))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for ; rows.Next(); count++ {
				var i1, i2, i3 *int
				if err := rows.Scan(&i1, &i2, &i3); err != nil {
					t.Errorf("row %d scan failed: %s", count, err)
					continue
				}
				row := fmt.Sprintf("%d %d %d", *i1, *i2, *i3)
				const expRow = "11 1 2"
				if row != expRow {
					t.Errorf("expected %q but read %q", expRow, row)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			} else if count != 1 {
				t.Errorf("expected one row but read %d", count)
			}

			if err := sqlutils.RunScrub(sqlDB, "d", "t"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Test that a backfill is executed with an EvalContext generated on the
// gateway. We assert that by checking that the same timestamp is used by all
// the backfilled columns.
func TestSchemaChangeEvalContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numNodes = 3
	const chunkSize = 200
	const maxValue = 5000
	params, _ := tests.CreateTestServerParams()
	// Disable asynchronous schema change execution.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
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

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	testCases := []struct {
		sql    string
		column string
	}{
		{"ALTER TABLE t.test ADD COLUMN x TIMESTAMP DEFAULT current_timestamp;", "x"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {

			if _, err := sqlDB.Exec(testCase.sql); err != nil {
				t.Fatal(err)
			}

			rows, err := sqlDB.Query(fmt.Sprintf(`SELECT DISTINCT %s from t.test`, testCase.column))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("read the wrong number of rows: e = %d, v = %d", 1, count)
			}

		})
	}
}

// Tests that a schema change that is queued behind another schema change
// is executed through the synchronous execution path properly even if it
// gets to run before the first schema change.
func TestSchemaChangeCompletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 51796)
	// TODO (lucy): This test needs more complicated schema changer knobs than is
	// currently implemented.
	params, _ := tests.CreateTestServerParams()
	var notifySchemaChange chan struct{}
	var restartSchemaChange chan struct{}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Add some data
	const maxValue = 200
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Do not execute the first schema change so that the second schema
	// change gets queued up behind it. The second schema change will be
	// given the green signal to execute before the first one.
	var wg sync.WaitGroup
	wg.Add(2)
	notifySchemaChange = make(chan struct{})
	restartSchemaChange = make(chan struct{})
	restart := restartSchemaChange
	go func() {
		if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX foo ON t.test (v)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notifySchemaChange

	notifySchemaChange = make(chan struct{})
	restartSchemaChange = make(chan struct{})
	go func() {
		if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX bar ON t.test (v)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notifySchemaChange
	// Allow second schema change to execute.
	close(restartSchemaChange)

	// Allow first schema change to execute after a bit.
	time.Sleep(time.Millisecond)
	close(restart)

	// Check that both schema changes have completed.
	wg.Wait()
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 3, maxValue); err != nil {
		t.Fatal(err)
	}

	// The notify schema change channel must be nil-ed out, or else
	// running scrub will cause it to trigger again on an already closed
	// channel when we run another statement.
	notifySchemaChange = nil
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// Test that a table TRUNCATE leaves the database in the correct state
// for the asynchronous schema changer to eventually execute it.
func TestTruncateInternals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const maxValue = 2000
	params, _ := tests.CreateTestServerParams()
	// Disable schema changes.
	blockGC := make(chan struct{})
	params.Knobs = base.TestingKnobs{
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { <-blockGC; return nil }},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Add a zone config.
	cfg := zonepb.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.GetID(), buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("TRUNCATE TABLE t.test"); err != nil {
		t.Error(err)
	}

	// Check that SQL thinks the table is empty.
	row := sqlDB.QueryRow("SELECT count(*) FROM t.test")
	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if tableDesc.Adding() {
		t.Fatalf("bad state = %s", tableDesc.GetState())
	}
	if err := zoneExists(sqlDB, &cfg, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// Ensure that the table data hasn't been deleted.
	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := maxValue + 1; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	close(blockGC)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChangeGC, jobs.StatusRunning, jobs.Record{
			Description:   "GC for TRUNCATE TABLE t.public.test",
			Username:      security.RootUserName(),
			DescriptorIDs: descpb.IDs{tableDesc.GetID()},
		})
	})
}

// Test that a table truncation completes properly.
func TestTruncateCompletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const maxValue = 2000

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	params, _ := tests.CreateTestServerParams()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.pi (d DECIMAL PRIMARY KEY);
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL REFERENCES t.pi (d) DEFAULT (DECIMAL '3.14'));
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`INSERT INTO t.pi VALUES (3.14)`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Add a zone config.
	var cfg zonepb.ZoneConfig
	cfg, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID())
	if err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("TRUNCATE TABLE t.test"); err != nil {
		t.Error(err)
	}

	// Check that SQL thinks the table is empty.
	row := sqlDB.QueryRow("SELECT count(*) FROM t.test")
	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	row = sqlDB.QueryRow("SELECT count(*) FROM t.test")
	require.NoError(t, row.Scan(&count))
	require.Equal(t, maxValue+1, count)

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Ensure that the FK property still holds.
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test VALUES ($1 , $2, $3)`, maxValue+2, maxValue+2, 3.15,
	); !testutils.IsError(err, "foreign key violation|violates foreign key") {
		t.Fatalf("err = %v", err)
	}

	// Get the table descriptor after the truncation.
	newTableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if newTableDesc.Adding() {
		t.Fatalf("bad state = %s", newTableDesc.GetState())
	}
	if err := zoneExists(sqlDB, &cfg, newTableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// Ensure that the table data has been deleted.
	tablePrefix := keys.SystemSQLCodec.IndexPrefix(uint32(tableDesc.GetID()), uint32(tableDesc.GetPrimaryIndexID()))
	tableEnd := tablePrefix.PrefixEnd()
	testutils.SucceedsSoon(t, func() error {
		if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
			t.Fatal(err)
		} else if e := 0; len(kvs) != e {
			return errors.Errorf("expected %d key value pairs, but got %d", e, len(kvs))
		}
		return nil
	})

	fkTableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "pi")
	tablePrefix = keys.SystemSQLCodec.TablePrefix(uint32(fkTableDesc.GetID()))
	tableEnd = tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 1; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Ensure that the job is marked as succeeded.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)

	// TODO (lucy): This test API should use an offset starting from the
	// most recent job instead.
	schemaChangeJobOffset := 0
	if err := jobutils.VerifySystemJob(t, sqlRun, schemaChangeJobOffset+2, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUserName(),
		Description: "TRUNCATE TABLE t.public.test",
		DescriptorIDs: descpb.IDs{
			tableDesc.GetID(),
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that, when DDL statements are run in a transaction, their errors are
// received as the results of the commit statement.
func TestSchemaChangeErrorOnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
INSERT INTO t.test (k, v) VALUES (1, 99), (2, 99);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// This schema change is invalid because of the duplicate v, but its error is
	// only reported later.
	if _, err := tx.Exec("ALTER TABLE t.test ADD CONSTRAINT v_unique UNIQUE (v)"); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); !testutils.IsError(
		err, `unique constraint "v_unique"`,
	) {
		t.Fatalf(`expected 'unique constraint "v_unique"', got %+v`, err)
	}
}

// TestIndexBackfillAfterGC verifies that if a GC is done after an index
// backfill has started, it will move past the error and complete.
func TestIndexBackfillAfterGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var tc serverutils.TestClusterInterface
	ctx := context.Background()
	var gcAt hlc.Timestamp
	runGC := func(sp roachpb.Span) error {
		if tc == nil {
			return nil
		}
		gcAt = tc.Server(0).Clock().Now()
		gcr := roachpb.GCRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(sp),
			Threshold:     gcAt,
		}
		_, err := kv.SendWrapped(ctx, tc.Server(0).DistSenderI().(*kvcoord.DistSender), &gcr)
		if err != nil {
			panic(err)
		}
		return nil
	}

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if fn := runGC; fn != nil {
					runGC = nil
					return fn(sp)
				}
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	tc = serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(context.Background())
	db := tc.ServerConn(0)
	kvDB := tc.Server(0).DB()
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE t`)
	sqlDB.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'))`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES (1, 1)`)
	if _, err := db.Exec(`CREATE UNIQUE INDEX index_created_in_test ON t.test (v)`); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(context.Background(), kvDB, 2, 0); err != nil {
		t.Fatal(err)
	}

	got := sqlDB.QueryStr(t, `
		SELECT p->'schemaChange'->'writeTimestamp'->>'wallTime' < $1, jsonb_pretty(p)
		FROM (SELECT crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', payload) AS p FROM system.jobs)
		WHERE p->>'description' LIKE 'CREATE UNIQUE INDEX index_created_in_test%'`,
		gcAt.WallTime,
	)[0]
	if got[0] != "true" {
		t.Fatalf("expected write-ts < gc time. details: %s", got[1])
	}
}

// TestAddComputedColumn verifies that while a column backfill is happening
// for a computed column, INSERTs and UPDATEs for that column are correct.
func TestAddComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var db *gosql.DB
	done := false
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if db == nil || done {
					return nil
				}
				done = true
				if _, err := db.Exec(`INSERT INTO t.test VALUES (10)`); err != nil {
					panic(err)
				}
				if _, err := db.Exec(`UPDATE t.test SET a = a + 1 WHERE a < 10`); err != nil {
					panic(err)
				}
				return nil
			},
		},
	}

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(context.Background())
	db = tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE t`)
	sqlDB.Exec(t, `CREATE TABLE t.test (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES (1)`)
	sqlDB.Exec(t, `ALTER TABLE t.test ADD COLUMN b INT AS (a + 5) STORED`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM t.test ORDER BY a`, [][]string{{"2", "7"}, {"10", "15"}})
}

// TestNoBackfillForVirtualColumn verifies that adding or dropping a virtual
// column does not involve a backfill.
func TestNoBackfillForVirtualColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sawBackfill := false
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				sawBackfill = true
				return nil
			},
		},
	}
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(context.Background())
	db := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE t`)
	sqlDB.Exec(t, `CREATE TABLE t.test (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES (1), (2), (3)`)

	sawBackfill = false
	sqlDB.Exec(t, `ALTER TABLE t.test ADD COLUMN b INT AS (a + 5) VIRTUAL`)
	if sawBackfill {
		t.Fatal("saw backfill when adding virtual column")
	}

	sqlDB.Exec(t, `ALTER TABLE t.test DROP COLUMN b`)
	if sawBackfill {
		t.Fatal("saw backfill when dropping virtual column")
	}
}

func TestSchemaChangeAfterCreateInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// The schema change below can occasionally take more than
	// 5 seconds and gets pushed by the closed timestamp mechanism
	// in the timestamp cache. Setting the closed timestamp
	// target duration to a higher value.
	// TODO(vivek): Remove the need to do this by removing the use of
	// txn.CommitTimestamp() in schema changes.
	if _, err := sqlDB.Exec(`
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '20s'
`); err != nil {
		t.Fatal(err)
	}

	// A large enough value that the backfills run as part of the
	// schema change run in many chunks.
	var maxValue = 4001
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`CREATE TABLE t.testing (k INT PRIMARY KEY, v INT, INDEX foo(v), CONSTRAINT ck_k CHECK (k >= 0));`); err != nil {
		t.Fatal(err)
	}

	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, maxValue-i)
	}

	if _, err := tx.Exec(`INSERT INTO t.testing VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`ALTER TABLE t.testing RENAME TO t.test`); err != nil {
		t.Fatal(err)
	}

	// Run schema changes that execute Column, Check and Index backfills.
	if _, err := tx.Exec(`
ALTER TABLE t.test ADD COLUMN c INT AS (v + 4) STORED, ADD COLUMN d INT DEFAULT 23, ADD CONSTRAINT bar UNIQUE (c), DROP CONSTRAINT ck_k, ADD CONSTRAINT ck_c CHECK (c >= 4)
`); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`DROP INDEX t.test@foo`); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(context.Background(), kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Verify that the index bar over is consistent, and that columns c, d
	// have been backfilled properly.
	rows, err := sqlDB.Query(`SELECT c, d from t.test@bar ORDER BY c`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var c int
		var d int
		if err := rows.Scan(&c, &d); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count+4 != c {
			t.Errorf("e = %d, v = %d", count+4, c)
		}
		if d != 23 {
			t.Errorf("e = %d, v = %d", 23, d)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	eCount := maxValue + 1
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Constraint ck_k dropped, ck_c public.
	if _, err := sqlDB.Exec(fmt.Sprintf("INSERT INTO t.test (k, v) VALUES (-1, %d)", maxValue+10)); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("INSERT INTO t.test (k, v) VALUES (%d, -1)", maxValue+10)
	if _, err := sqlDB.Exec(q); !testutils.IsError(err,
		`failed to satisfy CHECK constraint \(c >= 4:::INT8\)`) {
		t.Fatalf("err = %+v", err)
	}

	// The descriptor version hasn't changed.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if tableDesc.GetVersion() != 1 {
		t.Fatalf("invalid version = %d", tableDesc.GetVersion())
	}
}

// TestCancelSchemaChange tests that a CANCEL JOB run midway through column
// and index backfills is canceled.
func TestCancelSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 51796)

	const (
		numNodes = 3
		maxValue = 100
	)

	var sqlDB *sqlutils.SQLRunner
	var db *gosql.DB
	params, _ := tests.CreateTestServerParams()
	doCancel := false
	var enableAsyncSchemaChanges uint32
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			WriteCheckpointInterval: time.Nanosecond, // checkpoint after every chunk.
			BackfillChunkSize:       10,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if !doCancel {
					return nil
				}
				if _, err := db.Exec(`CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_type = 'SCHEMA CHANGE' AND
						status = $1 AND
						description NOT LIKE 'ROLL BACK%'
				)`, jobs.StatusRunning); err != nil {
					panic(err)
				}
				return nil
			},
		},
	}

	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      params,
	})
	defer tc.Stopper().Stop(context.Background())
	db = tc.ServerConn(0)
	kvDB := tc.Server(0).DB()
	sqlDB = sqlutils.MakeSQLRunner(db)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, db)()

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
	`)

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	// Split the table into multiple ranges.
	var sps []sql.SplitPoint
	const numSplits = numNodes * 2
	for i := 1; i <= numSplits-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i % numNodes, Vals: []interface{}{maxValue / numSplits * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.Background()
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		sql string
		// Set to true if this schema change is to be canceled.
		cancel bool
		// Set to true if the rollback returns in a running, waiting status.
		isGC bool
	}{
		{`ALTER TABLE t.public.test ADD COLUMN x DECIMAL DEFAULT 1.4::DECIMAL CREATE FAMILY f2, ADD CHECK (x >= 0)`,
			true, false},
		{`CREATE INDEX foo ON t.public.test (v)`,
			true, true},
		{`ALTER TABLE t.public.test ADD COLUMN x DECIMAL DEFAULT 1.2::DECIMAL CREATE FAMILY f3, ADD CHECK (x >= 0)`,
			false, false},
		{`CREATE INDEX foo ON t.public.test (v)`,
			false, true},
	}

	idx := 0
	for _, tc := range testCases {
		doCancel = tc.cancel
		if doCancel {
			if _, err := db.Exec(tc.sql); !testutils.IsError(err, "job canceled") {
				t.Fatalf("unexpected %v", err)
			}
			if err := jobutils.VerifySystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, jobs.StatusCanceled, jobs.Record{
				Username:    security.RootUserName(),
				Description: tc.sql,
				DescriptorIDs: descpb.IDs{
					tableDesc.GetID(),
				},
			}); err != nil {
				t.Fatal(err)
			}
			jobID := jobutils.GetJobID(t, sqlDB, idx)
			idx++
			jobRecord := jobs.Record{
				Username:    security.RootUserName(),
				Description: fmt.Sprintf("ROLL BACK JOB %d: %s", jobID, tc.sql),
				DescriptorIDs: descpb.IDs{
					tableDesc.GetID(),
				},
			}
			var err error
			if tc.isGC {
				err = jobutils.VerifyRunningSystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, sql.RunningStatusWaitingGC, jobRecord)
			} else {
				err = jobutils.VerifySystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobRecord)
			}
			if err != nil {
				t.Fatal(err)
			}
		} else {
			sqlDB.Exec(t, tc.sql)
			if err := jobutils.VerifySystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUserName(),
				Description: tc.sql,
				DescriptorIDs: descpb.IDs{
					tableDesc.GetID(),
				},
			}); err != nil {
				t.Fatal(err)
			}
		}
		idx++
	}

	// Verify that the index foo over v is consistent, and that column x has
	// been backfilled properly.
	rows, err := db.Query(`SELECT v, x from t.test@foo`)
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
		if x != 1.2 {
			t.Errorf("e = %f, v = %f", 1.2, x)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	eCount := maxValue + 1
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Verify that the data from the canceled CREATE INDEX is cleaned up.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)
	// TODO (lucy): when this test is no longer canceled, have it correctly handle doing GC immediately
	if _, err := sqltestutils.AddImmediateGCZoneConfig(db, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		return sqltestutils.CheckTableKeyCount(ctx, kvDB, 3, maxValue)
	})

	// Check that constraints are cleaned up.
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if checks := tableDesc.AllActiveAndInactiveChecks(); len(checks) != 1 {
		t.Fatalf("expected 1 check, found %+v", checks)
	}
}

// This test checks that when a transaction containing schema changes
// needs to be retried it gets retried internal to cockroach. This test
// currently fails because a schema change transaction is not retried.
func TestSchemaChangeRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numNodes = 3

	params, _ := tests.CreateTestServerParams()

	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.Background())
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
 CREATE DATABASE t;
 CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
 `); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// The timestamp of the transaction is guaranteed to be fixed after
	// this statement.
	if _, err := tx.Exec(`
		CREATE TABLE t.another (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
		`); err != nil {
		t.Fatal(err)
	}

	otherSQLDB := tc.ServerConn(1)

	// Read schema on another node that picks a later timestamp.
	rows, err := otherSQLDB.Query("SELECT * FROM t.test")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	if _, err := tx.Exec(`
		CREATE UNIQUE INDEX vidx ON t.test (v);
		`); err != nil {
		t.Fatal(err)
	}

	// The transaction should get pushed and commit without an error.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestCancelSchemaChangeContext tests that a canceled context on
// the session with a schema change after the schema change transaction
// has committed will not indefinitely retry executing the post schema
// execution transactions using a canceled context. The schema
// change will give up and ultimately be executed to completion through
// the asynchronous schema changer.
func TestCancelSchemaChangeContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const maxValue = 100
	notifyBackfill := make(chan struct{})
	cancelSessionDone := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if notify := notifyBackfill; notify != nil {
					notifyBackfill = nil
					close(notify)
					<-cancelSessionDone
				}
				return nil
			},
			// TODO (lucy): We need an OnError knob so we can verify that we got a
			// context cancellation error, but it should be for jobs, not for schema
			// changes. For now, don't try to intercept the error. It's sufficient to
			// test that the schema change terminates.
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	`)

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	notification := notifyBackfill

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		// When using db.Exec(), CANCEL SESSION below will result in the
		// database client retrying the request on another connection.
		// Use a connection here so when the session gets canceled; a
		// connection failure is returned.
		// TODO(vivek): It's likely we need to vendor lib/pq#422 and check
		// that this is unnecessary.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Error(err)
		}
		if _, err := conn.ExecContext(
			ctx, `CREATE INDEX foo ON t.public.test (v)`); !errors.Is(err, driver.ErrBadConn) {
			t.Errorf("unexpected err = %+v", err)
		}
	}()

	<-notification

	if _, err := db.Exec(`
CANCEL SESSIONS (SELECT session_id FROM [SHOW SESSIONS] WHERE last_active_query LIKE 'CREATE INDEX%')
`); err != nil {
		t.Error(err)
	}

	close(cancelSessionDone)

	wg.Wait()
}

func TestSchemaChangeGRPCError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const maxValue = 100
	params, _ := tests.CreateTestServerParams()
	seenNodeUnavailable := false
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if !seenNodeUnavailable {
					seenNodeUnavailable = true
					return errors.Errorf("node unavailable")
				}
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	`)

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`CREATE INDEX foo ON t.public.test (v)`); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
}

// TestBlockedSchemaChange tests whether a schema change that
// has no data backfill processing will be blocked by a schema
// change that is holding the schema change lease while backfill
// processing.
func TestBlockedSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const maxValue = 100
	notifyBackfill := make(chan struct{})
	tableRenameDone := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if notify := notifyBackfill; notify != nil {
					notifyBackfill = nil
					close(notify)
					<-tableRenameDone
				}
				return nil
			},
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	`)

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := sqltestutils.CheckTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	notification := notifyBackfill

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := db.Exec(`CREATE INDEX foo ON t.public.test (v)`); err != nil {
			t.Error(err)
		}
	}()

	<-notification

	if _, err := db.Exec(`ALTER TABLE t.test RENAME TO t.newtest`); err != nil {
		t.Fatal(err)
	}

	close(tableRenameDone)

	if _, err := db.Query(`SELECT x from t.test`); !testutils.IsError(err, `relation "t.test" does not exist`) {
		t.Fatalf("err = %+v", err)
	}

	wg.Wait()
}

// Tests index backfill validation step by purposely deleting an index
// value during the index backfill and checking that the validation
// fails.
func TestIndexBackfillValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	const maxValue = 1000
	backfillCount := int64(0)
	var db *kv.DB
	var tableDesc catalog.TableDescriptor
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: maxValue / 5,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunAfterBackfillChunk: func() {
				count := atomic.AddInt64(&backfillCount, 1)
				if count == 2 {
					// drop an index value before validation.
					key := keys.SystemSQLCodec.IndexPrefix(uint32(tableDesc.GetID()), uint32(tableDesc.GetNextIndexID()))
					kv, err := db.Scan(context.Background(), key, key.PrefixEnd(), 1)
					if err != nil {
						t.Error(err)
					}
					if err := db.Del(context.Background(), kv[0].Key); err != nil {
						t.Error(err)
					}
				}
			},
			BulkAdderFlushesEveryBatch: true,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	db = kvDB
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Bulk insert enough rows to exceed the chunk size.
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, i)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	// Start schema change that eventually runs a backfill.
	if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX foo ON t.test (v)`); !testutils.IsError(
		err, "duplicate key value violates unique constraint \"foo\"",
	) {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if len(tableDesc.PublicNonPrimaryIndexes()) > 0 || len(tableDesc.AllMutations()) > 0 {
		t.Fatalf("descriptor broken %d, %d", len(tableDesc.PublicNonPrimaryIndexes()), len(tableDesc.AllMutations()))
	}
}

// Tests inverted index backfill validation step by purposely deleting an index
// value during the index backfill and checking that the validation fails.
func TestInvertedIndexBackfillValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	const maxValue = 1000
	backfillCount := int64(0)
	var db *kv.DB
	var tableDesc catalog.TableDescriptor
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: maxValue / 5,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunAfterBackfillChunk: func() {
				count := atomic.AddInt64(&backfillCount, 1)
				if count == 2 {
					// drop an index value before validation.
					key := keys.SystemSQLCodec.IndexPrefix(uint32(tableDesc.GetID()), uint32(tableDesc.GetNextIndexID()))
					kv, err := db.Scan(context.Background(), key, key.PrefixEnd(), 1)
					if err != nil {
						t.Error(err)
					}
					if err := db.Del(context.Background(), kv[0].Key); err != nil {
						t.Error(err)
					}
				}
			},
			BulkAdderFlushesEveryBatch: true,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	db = kvDB
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v JSON);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	// Insert enough rows to exceed the chunk size.
	for i := 0; i < maxValue+1; i++ {
		jsonVal, err := json.Random(20, r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2)`, i, jsonVal.String()); err != nil {
			t.Fatal(err)
		}
	}

	// Start schema change that eventually runs a backfill.
	if _, err := sqlDB.Exec(`CREATE INVERTED INDEX foo ON t.test (v)`); !testutils.IsError(
		err, "validation of index foo failed",
	) {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	if len(tableDesc.PublicNonPrimaryIndexes()) > 0 || len(tableDesc.AllMutations()) > 0 {
		t.Fatalf("descriptor broken %d, %d", len(tableDesc.PublicNonPrimaryIndexes()), len(tableDesc.AllMutations()))
	}
}

// Test multiple index backfills (for forward and inverted indexes) from the
// same transaction.
func TestMultipleIndexBackfills(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	const maxValue = 1000
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: maxValue / 5,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (a INT, b INT, c JSON, d JSON);
`); err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	// Insert enough rows to exceed the chunk size.
	for i := 0; i < maxValue+1; i++ {
		jsonVal1, err := json.Random(20, r)
		if err != nil {
			t.Fatal(err)
		}
		jsonVal2, err := json.Random(20, r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2, $3, $4)`, i, i, jsonVal1.String(), jsonVal2.String()); err != nil {
			t.Fatal(err)
		}
	}

	// Start schema changes.
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INDEX idx_a ON t.test (a)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INDEX idx_b ON t.test (b)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INVERTED INDEX idx_c ON t.test (c)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INVERTED INDEX idx_d ON t.test (d)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateStatsAfterSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = time.Microsecond

	server, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(context.Background())
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)

	sqlRun.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlRun.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v CHAR, w CHAR);`)

	sqlRun.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`)

	// Add an index.
	sqlRun.Exec(t, `CREATE INDEX foo ON t.test (w)`)

	// Verify that statistics have been created for the new index (note that
	// column w is ordered before column v, since index columns are added first).
	sqlRun.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE t.test]`,
		[][]string{
			{"__auto__", "{k}", "0", "0", "0"},
			{"__auto__", "{w}", "0", "0", "0"},
			{"__auto__", "{v}", "0", "0", "0"},
		})

	// Add a column.
	sqlRun.Exec(t, `ALTER TABLE t.test ADD COLUMN x INT`)

	// Verify that statistics have been created for the new column.
	sqlRun.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE t.test] ORDER BY column_names::STRING`,
		[][]string{
			{"__auto__", "{k}", "0", "0", "0"},
			{"__auto__", "{k}", "0", "0", "0"},
			{"__auto__", "{v}", "0", "0", "0"},
			{"__auto__", "{v}", "0", "0", "0"},
			{"__auto__", "{w}", "0", "0", "0"},
			{"__auto__", "{w}", "0", "0", "0"},
			{"__auto__", "{x}", "0", "0", "0"},
		})
}

func TestTableValidityWhileAddingFK(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.child (a INT PRIMARY KEY, b INT, INDEX (b));
CREATE TABLE t.parent (a INT PRIMARY KEY);
`); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.child ADD FOREIGN KEY (b) REFERENCES t.child (a), ADD FOREIGN KEY (a) REFERENCES t.parent (a)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.child`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.parent`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.child`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.parent`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "child"); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "parent"); err != nil {
		t.Fatal(err)
	}
}

func TestTableValidityWhileAddingUniqueConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.tab (a INT PRIMARY KEY, b INT, c INT);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`SET experimental_enable_unique_without_index_constraints = true`); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.tab ADD UNIQUE WITHOUT INDEX (b), ADD UNIQUE WITHOUT INDEX (b, c)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.tab`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.tab`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "tab"); err != nil {
		t.Fatal(err)
	}
}

// TestWritesWithChecksBeforeDefaultColumnBackfill tests that when a check on a
// column being added references a different public column, writes to the public
// column ignore the constraint before the backfill for the non-public column
// begins. See #35258.
func TestWritesWithChecksBeforeDefaultColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT PRIMARY KEY NOT NULL,
    v INT NOT NULL
);
`); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.BulkInsertIntoTable(sqlDB, 1000); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN a INT DEFAULT 0, ADD CHECK (a < v AND a IS NOT NULL)`)
		if !testutils.IsError(err, `validation of CHECK "\(a < v\) AND \(a IS NOT NULL\)" failed on row: k=1003, v=-1003, a=0`) {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1001, 1001)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 100 WHERE v < 100`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1002, 1002)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 200 WHERE v < 200`); err != nil {
		t.Fatal(err)
	}
	// Final insert violates the constraint
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1003, -1003)`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// TestWritesWithChecksBeforeComputedColumnBackfill tests that when a check on a
// column being added references a different public column, writes to the public
// column ignore the constraint before the backfill for the non-public column
// begins. See #35258.
func TestWritesWithChecksBeforeComputedColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT PRIMARY KEY NOT NULL,
    v INT NOT NULL
);
`); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.BulkInsertIntoTable(sqlDB, 1000); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN a INT AS (v - 1) STORED, ADD CHECK (a < v AND a > -1000 AND a IS NOT NULL)`)
		if !testutils.IsError(err, `validation of CHECK "\(\(a < v\) AND \(a > \(-1000\):::INT8\)\) AND \(a IS NOT NULL\)" failed on row: k=1003, v=-1003, a=-1004`) {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1001, 1001)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 100 WHERE v < 100`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1002, 1002)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 200 WHERE v < 200`); err != nil {
		t.Fatal(err)
	}
	// Final insert violates the constraint
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1003, -1003)`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestIntentRaceWithIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var readyToBackfill, canStartBackfill, backfillProgressing chan struct{}

	const numNodes = 1
	var maxValue = 2000

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: 100,
			RunBeforeBackfill: func() error {
				select {
				case <-readyToBackfill:
				default:
					close(readyToBackfill)
					<-canStartBackfill
				}
				return nil
			},
		},
		DistSQL: &execinfra.TestingKnobs{
			RunAfterBackfillChunk: func() {
				select {
				case <-backfillProgressing:
				default:
					close(backfillProgressing)
				}
			},
			BulkAdderFlushesEveryBatch: true,
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

	ctx, cancel := context.WithCancel(context.Background())

	readyToBackfill = make(chan struct{})
	canStartBackfill = make(chan struct{})
	backfillProgressing = make(chan struct{})

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	var sps []sql.SplitPoint
	for i := 1; i < numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / 2}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	bg := ctxgroup.WithContext(ctx)
	bg.Go(func() error {
		if _, err := sqlDB.ExecContext(ctx, "CREATE UNIQUE INDEX ON t.test(v)"); err != nil {
			cancel()
			return err
		}
		return nil
	})

	// Wait until the schema change backfill starts.
	select {
	case <-readyToBackfill:
	case <-ctx.Done():
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE t.test SET v = $2 WHERE k = $1`, maxValue-1, maxValue-1); err != nil {
		t.Error(err)
	}
	if _, err := tx.Exec(`DELETE FROM t.test WHERE k = $1`, maxValue-1); err != nil {
		t.Error(err)
	}

	close(canStartBackfill)

	bg.Go(func() error {
		// We need to give the schema change time in which it could progress and end
		// up writing between our intent and its write before we rollback and the
		// intent is cleaned up. At the same time, we need to rollback so that a
		// correct schema change -- which waits for any intents -- will eventually
		// proceed and not block the test forever.
		time.Sleep(50 * time.Millisecond)
		return tx.Rollback()
	})

	select {
	case <-backfillProgressing:
	case <-ctx.Done():
	}

	rows, err := sqlDB.Query(`
	SELECT t.range_id, t.start_key_pretty, t.status, t.detail
	FROM
	crdb_internal.check_consistency(false, '', '') as t
	WHERE t.status NOT IN ('RANGE_CONSISTENT', 'RANGE_INDETERMINATE', 'RANGE_CONSISTENT_STATS_ESTIMATED')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if err := rows.Scan(&rangeID, &prettyKey, &status, &detail); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("r%d (%s) is inconsistent: %s %s", rangeID, prettyKey, status, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if err := bg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaChangeJobRunningStatusValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	var runBeforeConstraintValidation func() error
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeConstraintValidation: func(constraints []catalog.ConstraintToUpdate) error {
				return runBeforeConstraintValidation()
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
INSERT INTO t.test (k, v) VALUES (1, 99), (2, 100);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	runBeforeConstraintValidation = func() error {
		// TODO (lucy): Maybe this test API should use an offset starting
		// from the most recent job instead.
		return jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusValidation, jobs.Record{
			Username:    security.RootUserName(),
			Description: "ALTER TABLE t.public.test ADD COLUMN a INT8 AS (v - 1) STORED, ADD CHECK ((a < v) AND (a IS NOT NULL))",
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		})
	}

	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test ADD COLUMN a INT AS (v - 1) STORED, ADD CHECK (a < v AND a IS NOT NULL)`,
	); err != nil {
		t.Fatal(err)
	}
}

// TestFKReferencesAddedOnlyOnceOnRetry verifies that if ALTER TABLE ADD FOREIGN
// KEY is retried, both the FK reference and backreference (on another table)
// are only added once. This is addressed by #38377.
func TestFKReferencesAddedOnlyOnceOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	var runBeforeConstraintValidation func() error
	errorReturned := false
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeConstraintValidation: func(constraints []catalog.ConstraintToUpdate) error {
				return runBeforeConstraintValidation()
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE TABLE t.test2 (k INT, INDEX (k));
`); err != nil {
		t.Fatal(err)
	}

	// After FK forward references and backreferences are installed, and before
	// the validation query is run, return an error so that the schema change
	// has to be retried. The error is only returned on the first try.
	runBeforeConstraintValidation = func() error {
		if !errorReturned {
			errorReturned = true
			return context.DeadlineExceeded

		}
		return nil
	}
	if _, err := sqlDB.Exec(`
ALTER TABLE t.test2 ADD FOREIGN KEY (k) REFERENCES t.test;
`); err != nil {
		t.Fatal(err)
	}

	// Table descriptor validation failures, resulting from, e.g., broken or
	// duplicated backreferences, are returned by SHOW CONSTRAINTS.
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.test`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.test2`); err != nil {
		t.Fatal(err)
	}
}

// TestMultipleRevert starts a schema change then cancels it. After the canceled
// job, after reversing the mutations the job is set up to throw an error so
// that mutations are attempted to be reverted again. The mutation shouldn't be
// attempted to be reversed twice.
func TestMultipleRevert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	shouldBlockBackfill := true
	ranCancelCommand := false
	shouldRetryAfterReversingMutations := true

	params, _ := tests.CreateTestServerParams()
	var db *gosql.DB
	params.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if !shouldBlockBackfill {
					return nil
				}
				if !ranCancelCommand {
					// Cancel the job the first time it tried to backfill.
					if _, err := db.Exec(`CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_type = 'SCHEMA CHANGE' AND
						status = $1 AND
						description NOT LIKE 'ROLL BACK%'
				)`, jobs.StatusRunning); err != nil {
						t.Error(err)
					}
					ranCancelCommand = true
				}
				// Keep returning a retryable error until the job was actually canceled.
				return jobs.MarkAsRetryJobError(errors.New("retry until cancel"))
			},
			RunBeforeOnFailOrCancel: func(_ jobspb.JobID) error {
				// Allow the backfill to proceed normally once the job was actually
				// canceled.
				shouldBlockBackfill = false
				return nil
			},
			RunAfterMutationReversal: func(_ jobspb.JobID) error {
				// Throw one retryable error right after mutations were reversed so that
				// the mutation gets attempted to be reversed again.
				if !shouldRetryAfterReversingMutations {
					return nil
				}
				shouldRetryAfterReversingMutations = false
				// After cancelation, the job should get one more retryable error.
				return jobs.MarkAsRetryJobError(errors.New("retry once after cancel"))
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	db = sqlDB
	defer s.Stopper().Stop(context.Background())

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	// Create a k-v table and kick off a schema change that should get rolled
	// back.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT8);
INSERT INTO t.test VALUES (1, 2);
ALTER TABLE t.public.test DROP COLUMN v;
`); err == nil {
		t.Fatal("expected job to be canceled")
	} else if !strings.Contains(err.Error(), "job canceled by user") {
		t.Fatal(err)
	}

	// Ensure that the schema change was rolled back.
	runner := sqlutils.MakeSQLRunner(sqlDB)
	rows := runner.QueryStr(t, "SELECT * FROM t.test")
	require.Equal(t, [][]string{
		{"1", "2"},
	}, rows)

	// Validate the job cancellation metrics.
	rows = runner.QueryStr(t, "SELECT * FROM crdb_internal.feature_usage WHERE feature_name LIKE 'job.%.canceled'")
	if len(rows) != 1 ||
		len(rows[0]) != 2 ||
		rows[0][0] != "job.schema_change.canceled" {
		require.Failf(t, "Unexpected result set", "Rows: %s", rows)
	} else if val, err := strconv.ParseInt(rows[0][1], 10, 32); err != nil || val < 0 {
		require.Failf(t, "Invalid integer or value", "Error: %s Val: %d", err, val)
	}
}

// TestRetriableErrorDuringRollback tests that a retriable error while rolling
// back a schema change causes the rollback to retry and succeed.
func TestRetriableErrorDuringRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	runTest := func(params base.TestServerArgs) {
		s, sqlDB, kvDB := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		// Disable strict GC TTL enforcement because we're going to shove a zero-value
		// TTL into the system with AddImmediateGCZoneConfig.
		defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

		_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT8);
INSERT INTO t.test VALUES (1, 2), (2, 2);
`)
		require.NoError(t, err)
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		// Add a zone config for the table.
		_, err = sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID())
		require.NoError(t, err)

		// Try to create a unique index which won't be valid and will need a rollback.
		_, err = sqlDB.Exec(`
CREATE UNIQUE INDEX i ON t.test(v);
`)
		require.Regexp(t, `violates unique constraint "i"`, err.Error())
		// Verify that the index was cleaned up.
		testutils.SucceedsSoon(t, func() error {
			return sqltestutils.CheckTableKeyCountExact(ctx, kvDB, 2)
		})
		var permanentErrors int
		require.NoError(t, sqlDB.QueryRow(`
SELECT value
  FROM crdb_internal.node_metrics
 WHERE name = 'sql.schema_changer.permanent_errors';
`).Scan(&permanentErrors))
		require.Equal(t, 1, permanentErrors)
		var userErrors int
		require.NoError(t, sqlDB.QueryRow(`
SELECT usage_count
  FROM crdb_internal.feature_usage
 WHERE feature_name = 'sql.schema_changer.errors.constraint_violation';
`).Scan(&userErrors))
		require.GreaterOrEqual(t, userErrors, 1)
	}

	t.Run("error-before-backfill", func(t *testing.T) {
		onFailOrCancelStarted := false
		injectedError := false
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeOnFailOrCancel: func(_ jobspb.JobID) error {
					onFailOrCancelStarted = true
					return nil
				},
				RunBeforeBackfill: func() error {
					// The first time through the backfiller in OnFailOrCancel, return a
					// retriable error.
					if !onFailOrCancelStarted || injectedError {
						return nil
					}
					injectedError = true
					// Return an artificial context canceled error.
					return context.Canceled
				},
			},
			// Decrease the adopt loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}
		runTest(params)
	})

	t.Run("error-before-reversing-mutations", func(t *testing.T) {
		onFailOrCancelStarted := false
		injectedError := false
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeOnFailOrCancel: func(_ jobspb.JobID) error {
					onFailOrCancelStarted = true
					return nil
				},
				RunBeforeMutationReversal: func(_ jobspb.JobID) error {
					// The first time through reversing mutations, return a retriable
					// error.
					if !onFailOrCancelStarted || injectedError {
						return nil
					}
					injectedError = true
					// Return an artificial context canceled error.
					return context.Canceled
				},
			},
			// Decrease the adopt loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}
		runTest(params)
	})
}

// TestDropTableWhileSchemaChangeReverting tests that schema changes in the
// reverting state end up as failed when the table is dropped.
func TestDropTableWhileSchemaChangeReverting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Closed when we enter the RunBeforeOnFailOrCancel knob, at which point the
	// job is in the reverting state.
	beforeOnFailOrCancelNotification := make(chan struct{})
	// Closed when we're ready to continue with the schema change (rollback).
	continueNotification := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeOnFailOrCancel: func(_ jobspb.JobID) error {
				close(beforeOnFailOrCancelNotification)
				<-continueNotification
				// Return a retry error, so that we can be sure to test the path where
				// the job is marked as failed by the DROP TABLE instead of running to
				// completion and ending up in the failed state on its own.
				return jobs.MarkAsRetryJobError(errors.New("injected retry error"))
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT8);
INSERT INTO t.test VALUES (1, 2), (2, 2);
`)
	require.NoError(t, err)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		// Try to create a unique index which won't be valid and will need a rollback.
		_, err := sqlDB.Exec(`CREATE UNIQUE INDEX i ON t.test(v);`)
		assert.Regexp(t, "violates unique constraint", err)
		return nil
	})

	<-beforeOnFailOrCancelNotification

	_, err = sqlDB.Exec(`DROP TABLE t.test;`)
	require.NoError(t, err)

	close(continueNotification)
	require.NoError(t, g.Wait())

	var status jobs.Status
	var jobError string
	require.NoError(t, sqlDB.QueryRow(`
SELECT status, error FROM crdb_internal.jobs WHERE description LIKE '%CREATE UNIQUE INDEX%'
`).Scan(&status, &jobError))
	require.Equal(t, jobs.StatusFailed, status)
	require.Regexp(t, "violates unique constraint", jobError)
}

// TestRetryOnAllErrorsWhenReverting tests that a permanent error while rolling
// back a schema change causes the job to revert, and that the appropriate error
// is displayed in the jobs table.
func TestRetryOnAllErrorsWhenReverting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	runTest := func(t *testing.T, params base.TestServerArgs, gcJobRecord bool) {
		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT8);
INSERT INTO t.test VALUES (1, 2), (2, 2);
`)
		require.NoError(t, err)

		// Try to create a unique index which won't be valid and will need a rollback.
		_, err = sqlDB.Exec(`
CREATE UNIQUE INDEX i ON t.test(v);
`)
		require.Regexp(t, `violates unique constraint "i"`, err.Error())

		var jobID jobspb.JobID
		var jobErr string
		row := sqlDB.QueryRow("SELECT job_id, error FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE'")
		require.NoError(t, row.Scan(&jobID, &jobErr))
		require.Regexp(t, `violates unique constraint "i"`, jobErr)

		if gcJobRecord {
			_, err := sqlDB.Exec(`DELETE FROM system.jobs WHERE id = $1`, jobID)
			require.NoError(t, err)
		}

		// Test that dropping the table is still possible.
		_, err = sqlDB.Exec(`DROP TABLE t.test`)
		require.NoError(t, err)
	}

	t.Run("error-before-backfill", func(t *testing.T) {
		onFailOrCancelStarted := false
		injectedError := false
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeOnFailOrCancel: func(_ jobspb.JobID) error {
					onFailOrCancelStarted = true
					return nil
				},
				RunBeforeBackfill: func() error {
					// The first time through the backfiller in OnFailOrCancel, return a
					// permanent error.
					if !onFailOrCancelStarted || injectedError {
						return nil
					}
					injectedError = true
					// Any error not on the allowlist of retriable errors is considered permanent.
					return errors.New("permanent error")
				},
			},
			// Decrease the adopt loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}
		// Don't GC the job record after the schema change, so we can test dropping
		// the table with a failed mutation job.
		runTest(t, params, false /* gcJobRecord */)
	})

	t.Run("error-before-reversing-mutations", func(t *testing.T) {
		onFailOrCancelStarted := false
		injectedError := false
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeOnFailOrCancel: func(_ jobspb.JobID) error {
					onFailOrCancelStarted = true
					return nil
				},
				RunBeforeBackfill: func() error {
					// The first time through reversing mutations, return a permanent
					// error.
					if !onFailOrCancelStarted || injectedError {
						return nil
					}
					injectedError = true
					// Any error not on the allowlist of retriable errors is considered permanent.
					return errors.New("permanent error")
				},
			},
			// Decrease the adopt loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}
		// GC the job record after the schema change, so we can test dropping the
		// table with a nonexistent mutation job.
		runTest(t, params, true /* gcJobRecord */)
	})
}

// TestPartialIndexBackfill tests that backfilling a partial index adds the
// correct number of entries to the index.
func TestPartialIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, a INT, b INT);
INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);
CREATE INDEX i ON t.test (a) WHERE b > 2
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	index, err := tableDesc.FindIndexWithName("i")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Collect all the keys in the partial index.
	span := tableDesc.IndexSpan(keys.SystemSQLCodec, index.GetID())
	keys, err := kvDB.Scan(ctx, span.Key, span.EndKey, 0)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	numKeys := len(keys)
	expectedNumKeys := 2
	if numKeys != expectedNumKeys {
		t.Errorf("partial index contains an incorrect number of keys: expected %d, but found %d", expectedNumKeys, numKeys)
	}
}

// TestAddingTableResolution tests that table names cannot be resolved in the
// adding state.
func TestAddingTableResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		// Don't run the schema change to take the table out of the adding state.
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool { return true },
		},
	}

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)

	// Materialized views are in the adding state until successfully backfilled.
	// In this test it gets stuck in the adding state.
	sqlRun.Exec(t, `CREATE MATERIALIZED VIEW foo AS SELECT random(), generate_series FROM generate_series(1, 10);`)

	sqlRun.ExpectErr(t, `pq: materialized view "foo" is being added`, `SELECT * FROM foo`)
	sqlRun.ExpectErr(t, `pq: materialized view "foo" is being added`, `ALTER MATERIALIZED VIEW foo RENAME TO bar`)
	// Regression test for #52829.
	sqlRun.ExpectErr(t, `pq: materialized view "foo" is being added`, `SHOW CREATE foo`)
}

// TestFailureToMarkCanceledReversalLeadsToCanceledStatus is a regression test
// to ensure that when the job registry fails to mark a job as canceled but
// after the mutation has been removed, that the OnFailOrCancel hook of the
// schema change returns a nil error. In particular, this deals with the case
// where the mutation corresponding to the job no longer exists on the
// descriptor.
func TestFailureToMarkCanceledReversalLeadsToCanceledStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	canProceed := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	jobCancellationsToFail := struct {
		syncutil.Mutex
		jobs map[jobspb.JobID]struct{}
	}{
		jobs: make(map[jobspb.JobID]struct{}),
	}
	withJobsToFail := func(f func(m map[jobspb.JobID]struct{})) {
		jobCancellationsToFail.Lock()
		defer jobCancellationsToFail.Unlock()
		f(jobCancellationsToFail.jobs)
	}
	jobKnobs := jobs.NewTestingKnobsWithShortIntervals()
	jobKnobs.BeforeUpdate = func(orig, updated jobs.JobMetadata) (err error) {
		withJobsToFail(func(m map[jobspb.JobID]struct{}) {
			if _, ok := m[orig.ID]; ok && updated.Status == jobs.StatusCanceled {
				delete(m, orig.ID)
				err = errors.Errorf("boom")
			}
		})
		return err
	}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				<-canProceed
				return nil
			},
		},
		JobsTestingKnobs: jobKnobs,
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (i INT PRIMARY KEY, j INT)`)
	var schemaChangeWaitGroup sync.WaitGroup
	var jobsErrGroup errgroup.Group
	const numIndexes = 2                       // number of indexes to add
	jobIDs := make([]jobspb.JobID, numIndexes) // job IDs for the index additions
	for i := 0; i < numIndexes; i++ {
		idxName := "t_" + strconv.Itoa(i) + "_idx"
		schemaChangeWaitGroup.Add(1)
		i := i
		go func() {
			defer schemaChangeWaitGroup.Done()
			_, err := sqlDB.Exec("CREATE INDEX " + idxName + " ON db.t (j)")
			assert.Regexp(t, "job canceled by user", err)
		}()
		jobsErrGroup.Go(func() error {
			return testutils.SucceedsSoonError(func() error {
				return sqlDB.QueryRow(`
SELECT job_id FROM crdb_internal.jobs
 WHERE description LIKE '%` + idxName + `%'`).Scan(&jobIDs[i])
			})
		})
	}
	require.NoError(t, jobsErrGroup.Wait())
	withJobsToFail(func(m map[jobspb.JobID]struct{}) {
		for _, id := range jobIDs {
			m[id] = struct{}{}
		}
	})
	for _, id := range jobIDs {
		tdb.Exec(t, "CANCEL JOB $1", id)
	}
	close(canProceed)
	schemaChangeWaitGroup.Wait()

	// Verify that all the jobs reached the expected terminal state.
	// Do this after the above change to ensure that all canceled states have
	// been reached.
	for _, id := range jobIDs {
		var status jobs.Status
		tdb.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", id).
			Scan(&status)
		require.Equal(t, jobs.StatusCanceled, status)
	}
	withJobsToFail(func(m map[jobspb.JobID]struct{}) {
		require.Len(t, m, 0)
	})

	// Validate the job cancellation metrics.
	rows := tdb.QueryStr(t, "SELECT * FROM crdb_internal.feature_usage WHERE feature_name LIKE 'job.%.canceled'")
	if len(rows) != 1 ||
		len(rows[0]) != 2 ||
		rows[0][0] != "job.schema_change.canceled" {
		require.Failf(t, "Unexpected result set", "Rows: %s", rows)
	} else if val, err := strconv.ParseInt(rows[0][1], 10, 32); err != nil || val < 2 {
		require.Failf(t, "Invalid integer or value", "Error: %s Val: %d", err, val)
	}
}

// TestCancelMultipleQueued tests that canceling schema changes when there are
// multiple queued schema changes works as expected.
func TestCancelMultipleQueued(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	canProceed := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				<-canProceed
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (i INT PRIMARY KEY, j INT)`)
	var schemaChangeWaitGroup sync.WaitGroup
	var jobsErrGroup errgroup.Group
	const numIndexes = 10                      // number of indexes to add
	jobIDs := make([]jobspb.JobID, numIndexes) // job IDs for the index additions
	shouldCancel := make([]bool, numIndexes)
	for i := 0; i < numIndexes; i++ {
		idxName := "t_" + strconv.Itoa(i) + "_idx"
		schemaChangeWaitGroup.Add(1)
		i := i
		shouldCancel[i] = rand.Float64() < .5
		go func() {
			defer schemaChangeWaitGroup.Done()
			_, err := sqlDB.Exec("CREATE INDEX " + idxName + " ON db.t (j)")
			if shouldCancel[i] {
				assert.Regexp(t, "job canceled by user", err)
			} else {
				assert.NoError(t, err)
			}
		}()
		jobsErrGroup.Go(func() error {
			return testutils.SucceedsSoonError(func() error {
				return sqlDB.QueryRow(`
SELECT job_id FROM crdb_internal.jobs
 WHERE description LIKE '%` + idxName + `%'`).Scan(&jobIDs[i])
			})
		})
	}
	require.NoError(t, jobsErrGroup.Wait())
	for i, id := range jobIDs {
		if shouldCancel[i] {
			tdb.Exec(t, "CANCEL JOB $1", id)
		}
	}
	close(canProceed)
	schemaChangeWaitGroup.Wait()

	// Verify that after all of the canceled jobs have been canceled and all of
	// the other indexes which were not canceled have completed, that we can
	// perform another schema change. This ensures that there are no orphaned
	// mutations.
	tdb.Exec(t, "CREATE INDEX foo ON db.t (j)")

	// Verify that all the jobs reached the expected terminal state.
	// Do this after the above change to ensure that all canceled states have
	// been reached.
	for i, id := range jobIDs {
		var status jobs.Status
		tdb.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", id).
			Scan(&status)
		if shouldCancel[i] {
			require.Equal(t, jobs.StatusCanceled, status)
		} else {
			require.Equal(t, jobs.StatusSucceeded, status)
		}

		if shouldCancel[i] {
			tdb.Exec(t, "CANCEL JOB $1", id)
		}
	}

	// Validate the job cancellation metrics.
	rows := tdb.QueryStr(t, "SELECT * FROM crdb_internal.feature_usage WHERE feature_name LIKE 'job.%.canceled'")
	if len(rows) != 1 ||
		len(rows[0]) != 2 ||
		rows[0][0] != "job.schema_change.canceled" {
		require.Failf(t, "Unexpected result set", "Rows: %s", rows)
	} else if val, err := strconv.ParseInt(rows[0][1], 10, 32); err != nil || val < 2 {
		require.Failf(t, "Invalid integer or value", "Error: %s Val: %d", err, val)
	}
}

// TestRollbackForeignKeyAddition tests that rolling back a schema change to add
// a foreign key before the backreference on the other table has been installed
// works correctly (#57596).
func TestRollbackForeignKeyAddition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Track whether we've attempted the backfill already, since there's a second
	// backfill during the schema change rollback.
	attemptedBackfill := false
	// Closed when we enter the RunBeforeBackfill knob (which is before
	// backreferences for foreign keys are added).
	beforeBackfillNotification := make(chan struct{})
	// Closed when we're ready to continue with the schema change.
	continueNotification := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if !attemptedBackfill {
					attemptedBackfill = true
					close(beforeBackfillNotification)
					<-continueNotification
				}
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
	tdb.Exec(t, `CREATE TABLE db.t2 (a INT)`)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := sqlDB.ExecContext(ctx, `ALTER TABLE db.t2 ADD FOREIGN KEY (a) REFERENCES db.t`)
		require.Regexp(t, "job canceled by user", err)
		return nil
	})

	<-beforeBackfillNotification

	var jobID jobspb.JobID

	// We filter by descriptor_ids because there's a bug where we create an extra
	// no-op job for the referenced table (#57624).
	require.NoError(t, sqlDB.QueryRow(`
SELECT job_id FROM crdb_internal.jobs WHERE description LIKE '%ALTER TABLE%'
AND descriptor_ids[1] = 'db.t2'::regclass::int`,
	).Scan(&jobID))
	tdb.Exec(t, "CANCEL JOB $1", jobID)

	close(continueNotification)
	require.NoError(t, g.Wait())

	var status jobs.Status
	var error string
	tdb.QueryRow(t, "SELECT status, error FROM crdb_internal.jobs WHERE job_id = $1", jobID).
		Scan(&status, &error)
	require.Equal(t, status, jobs.StatusCanceled)
	require.Equal(t, error, "job canceled by user")
}

// TestRevertingJobsOnDatabasesAndSchemas tests that schema change jobs on
// databases and schemas return an error from the OnFailOrCancel hook. It also
// tests that such jobs are not cancelable. Regression test for #59415.
func TestRevertingJobsOnDatabasesAndSchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name       string
		setupStmts string
		scStmt     string
		jobRegex   string
	}{
		{
			name:       "drop schema",
			setupStmts: `CREATE DATABASE db_drop_schema; CREATE SCHEMA db_drop_schema.sc;`,
			scStmt:     `DROP SCHEMA db_drop_schema.sc`,
			jobRegex:   `^DROP SCHEMA db_drop_schema.sc$`,
		},
		{
			name:       "rename schema",
			setupStmts: `CREATE DATABASE db_rename_schema; CREATE SCHEMA db_rename_schema.sc;`,
			scStmt:     `ALTER SCHEMA db_rename_schema.sc RENAME TO new_name`,
			jobRegex:   `^ALTER SCHEMA db_rename_schema.sc RENAME TO new_name$`,
		},
		{
			name:       "grant on schema",
			setupStmts: `CREATE DATABASE db_grant_on_schema; CREATE SCHEMA db_grant_on_schema.sc;`,
			scStmt:     `GRANT ALL ON SCHEMA db_grant_on_schema.sc TO PUBLIC`,
			jobRegex:   `updating privileges for schema`,
		},
		{
			name:       "rename database",
			setupStmts: `CREATE DATABASE db_rename;`,
			scStmt:     `ALTER DATABASE db_rename RENAME TO db_new_name`,
			jobRegex:   `^ALTER DATABASE db_rename RENAME TO db_new_name$`,
		},
		{
			name:       "grant on database",
			setupStmts: `CREATE DATABASE db_grant`,
			scStmt:     `GRANT ALL ON DATABASE db_grant TO PUBLIC`,
			jobRegex:   `updating privileges for database`,
		},
	}

	ctx := context.Background()

	t.Run("failed due to injected error", func(t *testing.T) {
		var injectedError bool
		var s serverutils.TestServerInterface
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					scJob, err := s.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
					if err != nil {
						return err
					}
					pl := scJob.Payload()
					// This is a hacky way to only inject errors in the rename/drop/grant jobs.
					if strings.Contains(pl.Description, "updating parent database") {
						return nil
					}
					for _, s := range []string{"DROP", "RENAME", "updating privileges"} {
						if strings.Contains(pl.Description, s) {
							if !injectedError {
								injectedError = true
								// Return a non-permanent error. The job will be retried in
								// running state as the job is non-cancelable.
								return errors.New("injected error")
							} else {
								// Return a permanent error to transition to reverting.
								return jobs.MarkAsPermanentJobError(errors.New("injected permanent error"))
							}
						}
					}
					return nil
				},
			},
			// Decrease the adopt-loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}

		var db *gosql.DB
		s, db, _ = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET use_declarative_schema_changer = 'off'`)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				injectedError = false
				sqlDB.Exec(t, tc.setupStmts)

				go func() {
					// This transaction will not return until the server is shutdown. Therefore,
					// we run it in a separate goroutine and don't check the returned error.
					_, _ = db.Exec(tc.scStmt)
				}()
				// Verify that the job is in retry state while reverting.
				const query = `SELECT num_runs > 3 FROM crdb_internal.jobs WHERE status = '` + string(jobs.StatusReverting) + `' AND description ~ '%s'`
				sqlDB.CheckQueryResultsRetry(t, fmt.Sprintf(query, tc.jobRegex), [][]string{{"true"}})
			})
		}
	})

	t.Run("canceling not allowed", func(t *testing.T) {
		var state = struct {
			mu    syncutil.Mutex
			jobID jobspb.JobID
			// Closed in the RunBeforeResume testing knob.
			beforeResumeNotification chan struct{}
			// Closed when we're ready to resume the schema change.
			continueNotification chan struct{}
		}{}
		initNotification := func() (chan struct{}, chan struct{}) {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.beforeResumeNotification = make(chan struct{})
			state.continueNotification = make(chan struct{})
			return state.beforeResumeNotification, state.continueNotification
		}
		notifyBeforeResume := func(jobID jobspb.JobID) {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.jobID = jobID
			if state.beforeResumeNotification != nil {
				close(state.beforeResumeNotification)
				state.beforeResumeNotification = nil
			}
			if state.continueNotification != nil {
				<-state.continueNotification
			}
		}

		var s serverutils.TestServerInterface
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					scJob, err := s.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
					if err != nil {
						return err
					}
					pl := scJob.Payload()
					// This is a hacky way to only block in the rename/drop/grant jobs.
					if strings.Contains(pl.Description, "updating parent database") {
						return nil
					}
					for _, s := range []string{"DROP", "RENAME", "updating privileges"} {
						if strings.Contains(pl.Description, s) {
							notifyBeforeResume(jobID)
						}
					}
					return nil
				},
			},
			// Decrease the adopt-loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}
		var db *gosql.DB
		s, db, _ = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET use_declarative_schema_changer = 'off'`)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				beforeResumeNotification, continueNotification := initNotification()
				sqlDB.Exec(t, tc.setupStmts)

				g := ctxgroup.WithContext(ctx)
				g.GoCtx(func(ctx context.Context) error {
					_, err := db.ExecContext(ctx, tc.scStmt)
					assert.NoError(t, err)
					return nil
				})

				<-beforeResumeNotification
				sqlDB.ExpectErr(t, "not cancelable", "CANCEL JOB $1", state.jobID)

				close(continueNotification)
				require.NoError(t, g.Wait())
			})
		}
	})
}

// TestDropColumnAfterMutations tests the impact of a drop column
// after an existing mutation on the column.
func TestDropColumnAfterMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var jobControlMu syncutil.Mutex
	var delayJobList []string
	var delayJobChannels []chan struct{}
	delayNotify := make(chan struct{})
	jobIDs := make([]jobspb.JobID, 2)

	proceedBeforeBackfill := make(chan error)
	params, _ := tests.CreateTestServerParams()

	var s serverutils.TestServerInterface
	params.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				lockHeld := true
				jobControlMu.Lock()
				scJob, err := s.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
				if err != nil {
					return err
				}
				pl := scJob.Payload()
				// Check if we are blocking the correct job.
				for idx, s := range delayJobList {
					if strings.Contains(pl.Description, s) {
						jobIDs[idx] = jobID
						delayNotify <- struct{}{}
						channel := delayJobChannels[idx]
						jobControlMu.Unlock()
						lockHeld = false
						<-channel
						break
					}
				}
				if lockHeld {
					jobControlMu.Unlock()
				}
				return nil
			},
			RunAfterBackfill: func(jobID jobspb.JobID) error {
				return <-proceedBeforeBackfill
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn1 := sqlutils.MakeSQLRunner(sqlDB)
	conn2 := sqlutils.MakeSQLRunner(sqlDB)
	var schemaChangeWaitGroup sync.WaitGroup

	conn1.Exec(t, `
CREATE TABLE t (i INT8 PRIMARY KEY, j INT8);
INSERT INTO t VALUES (1, 1);
`)

	// Test 1: with concurrent drop and mutations.
	t.Run("basic-concurrent-drop-mutations", func(t *testing.T) {
		jobControlMu.Lock()
		delayJobList = []string{"ALTER TABLE defaultdb.public.t ADD COLUMN k INT8 NOT NULL UNIQUE DEFAULT 42",
			"ALTER TABLE defaultdb.public.t DROP COLUMN j"}
		delayJobChannels = []chan struct{}{make(chan struct{}), make(chan struct{})}
		jobControlMu.Unlock()

		schemaChangeWaitGroup.Add(1)
		go func() {
			defer schemaChangeWaitGroup.Done()
			_, err := conn2.DB.ExecContext(ctx,
				`
BEGIN;
ALTER TABLE t ALTER COLUMN j SET NOT NULL;
ALTER TABLE t ADD COLUMN k INT8 DEFAULT 42 NOT NULL UNIQUE;
COMMIT;
`)
			if err != nil {
				t.Error(err)
			}
		}()
		<-delayNotify
		schemaChangeWaitGroup.Add(1)
		go func() {
			defer schemaChangeWaitGroup.Done()
			_, err := conn2.DB.ExecContext(ctx,
				`
SET sql_safe_updates = false;
BEGIN;
ALTER TABLE t DROP COLUMN j;
ALTER TABLE t ALTER COLUMN k SET NOT NULL;
COMMIT;
`)
			if err != nil {
				t.Error(err)
			}
		}()
		<-delayNotify

		// Allow jobs to proceed once both are concurrent.
		delayJobChannels[0] <- struct{}{}
		// Allow both backfill jobs to proceed.
		proceedBeforeBackfill <- nil
		// Allow the second job to proceed.
		delayJobChannels[1] <- struct{}{}
		// Second job will also do backfill next.
		proceedBeforeBackfill <- nil

		schemaChangeWaitGroup.Wait()
		close(delayJobChannels[0])
		close(delayJobChannels[1])
	})

	// Test 2: with concurrent drop and mutations, where
	// the drop column will be failed intentionally.
	t.Run("failed-concurrent-drop-mutations", func(t *testing.T) {
		jobControlMu.Lock()
		delayJobList = []string{"ALTER TABLE defaultdb.public.t ALTER COLUMN j SET NOT NULL",
			"ALTER TABLE defaultdb.public.t DROP COLUMN j"}
		delayJobChannels = []chan struct{}{make(chan struct{}), make(chan struct{})}
		jobControlMu.Unlock()

		conn2.Exec(t, `
	   DROP TABLE t;
	   CREATE TABLE t (i INT8 PRIMARY KEY, j INT8);
	   INSERT INTO t VALUES (1, 1);
	   `)
		go func() {
			// This transaction will not complete. Therefore, we don't check the returned error.
			_, _ = conn2.DB.ExecContext(context.Background(),
				`
	   SET application_name='failed-concurrent-drop-mutations';
	   BEGIN;
	   ALTER TABLE t ALTER COLUMN j SET NOT NULL;
	   ALTER TABLE t ADD COLUMN k INT8 DEFAULT 42 NOT NULL UNIQUE;
	   COMMIT;
	   `)
		}()

		// Wait for the alter to get submitted first.
		<-delayNotify

		go func() {
			// This transaction will not complete. Therefore, we don't check the returned error.
			_, _ = conn1.DB.ExecContext(context.Background(),
				`
	   SET application_name='failed-concurrent-drop-mutations';
	   SET sql_safe_updates = false;
	   BEGIN;
	   ALTER TABLE t DROP COLUMN j;
	   ALTER TABLE t ALTER COLUMN k SET NOT NULL;
	   ALTER TABLE t ALTER COLUMN k SET DEFAULT 421;
	   ALTER TABLE t ADD COLUMN o INT8 DEFAULT 42 NOT NULL UNIQUE;
	   COMMIT;
	   `)
		}()
		<-delayNotify

		// Allow the first operation relying on
		// the dropped column to resume.
		delayJobChannels[0] <- struct{}{}
		// Allow internal backfill processing to resume
		proceedBeforeBackfill <- nil
		// Allow the second job to proceed
		delayJobChannels[1] <- struct{}{}
		// Second job will also do backfill next
		proceedBeforeBackfill <- errors.Newf("Bogus error for drop column transaction")

		// We expect the first job to be stuck in running state.
		// At this point, first job is now waiting for second to complete. However, first job
		// is in an infinite-loop due to retries while reverting. Therefore, we don't wait
		// for the jobs to complete. Instead, we validate their current status before completing
		// the test.

		// Second job should be in reverting state and retrying.
		conn1.CheckQueryResultsRetry(t,
			fmt.Sprintf("SELECT num_runs > 3 FROM system.jobs WHERE id = %d AND status = '%s'", jobIDs[1], jobs.StatusReverting),
			[][]string{{"true"}},
		)
		// First job should be in running state.
		conn1.CheckQueryResults(t, fmt.Sprintf("SELECT status from system.jobs WHERE id = %d", jobIDs[0]), [][]string{{string(jobs.StatusRunning)}})
		// Both jobs should be stuck in COMMIT, waiting for jobs to complete.
		conn1.CheckQueryResults(t,
			"SELECT count(*) FROM [SHOW SESSIONS] WHERE last_active_query LIKE '%COMMIT%' AND session_id != (SELECT * FROM [SHOW session_id]) "+
				"AND application_name='failed-concurrent-drop-mutations'",
			[][]string{{"2"}},
		)

		close(delayJobChannels[0])
		close(delayJobChannels[1])
	})

	// Test 3: with concurrent drop and mutations where an insert will
	// cause the backfill operation to fail.
	t.Run("concurrent-drop-mutations-insert-fail", func(t *testing.T) {
		conn1.Exec(t, `SET use_declarative_schema_changer = 'off'`)
		conn2.Exec(t, `SET use_declarative_schema_changer = 'off'`)

		jobControlMu.Lock()
		delayJobList = []string{"ALTER TABLE defaultdb.public.t ALTER COLUMN j SET NOT NULL",
			"ALTER TABLE defaultdb.public.t DROP COLUMN j"}
		delayJobChannels = []chan struct{}{make(chan struct{}), make(chan struct{})}
		jobControlMu.Unlock()

		conn2.Exec(t, `
	   DROP TABLE t;
	   CREATE TABLE t (i INT8 PRIMARY KEY, j INT8);
	   INSERT INTO t VALUES (1, 1);
	   `)

		go func() {
			// Two possibilities exist based on timing, either the following transaction
			// will fail during backfill or the dependent one with the drop will fail.

			// This transaction will not complete. Therefore, we don't check the returned error.
			_, _ = conn2.DB.ExecContext(context.Background(),
				`
	   SET application_name='concurrent-drop-mutations-insert-fail';
	   BEGIN;
	   ALTER TABLE t ALTER COLUMN j SET NOT NULL;
	   ALTER TABLE t ADD COLUMN k INT8 DEFAULT 42 NOT NULL;
	   COMMIT;
	   	   `)
		}()
		<-delayNotify

		go func() {
			// This transaction will not complete. Therefore, we don't check the returned error.
			_, _ = conn1.DB.ExecContext(context.Background(),
				`
	   SET application_name='concurrent-drop-mutations-insert-fail';
	   SET sql_safe_updates = false;
	   BEGIN;
	   ALTER TABLE t DROP COLUMN j;
	   ALTER TABLE t ALTER COLUMN k SET NOT NULL;
	   ALTER TABLE t ALTER COLUMN k SET DEFAULT 421;
	   ALTER TABLE t ADD COLUMN o INT8 DEFAULT 42 NOT NULL UNIQUE;
	   INSERT INTO t VALUES (2);
	   COMMIT;
	   	   `)
		}()
		<-delayNotify

		// Allow jobs to proceed once both are concurrent.
		// Allow the first operation relying on
		// the dropped column to resume.
		delayJobChannels[0] <- struct{}{}
		// Allow internal backfill processing to resume
		proceedBeforeBackfill <- nil
		// Allow the second job to proceed
		delayJobChannels[1] <- struct{}{}

		var revertingJobID jobspb.JobID
		testutils.SucceedsSoon(t, func() error {
			// In this test, depending on the concurrent execution schedule, one of the jobs
			// will be stuck in running state while the other job will be stuck in reverting state.
			// Here we check that this statement is valid. Moreover, we also get the ID of the
			// reverting job to ensure that it is retrying. Furthermore, we prevent the running
			// job to backfill to validate this test's correctness assumptions.
			firstJobID := jobIDs[0]
			secondJobID := jobIDs[1]
			res := conn1.QueryStr(t, "SELECT status from system.jobs where id in ($1, $2)", firstJobID, secondJobID)
			require.Len(t, res, 2)
			firstJobStatus := jobs.Status(res[0][0])
			secondJobStatus := jobs.Status(res[1][0])
			if firstJobStatus == jobs.StatusRunning && secondJobStatus == jobs.StatusReverting {
				revertingJobID = secondJobID
				return nil
			} else if firstJobStatus == jobs.StatusReverting && secondJobStatus == jobs.StatusRunning {
				revertingJobID = firstJobID
				return nil
			}
			return errors.New("one job should be running while the other job should be reverting")
		})
		// Ensure that the reverting job is retrying.
		conn1.CheckQueryResultsRetry(t,
			fmt.Sprintf("SELECT num_runs > 3 FROM system.jobs WHERE id = %d", revertingJobID),
			[][]string{{"true"}},
		)
		// Both jobs should be stuck in COMMIT, waiting for jobs to complete.
		conn1.CheckQueryResults(t,
			"SELECT count(*) FROM [SHOW SESSIONS] WHERE last_active_query LIKE '%COMMIT%' AND session_id != (SELECT * FROM [SHOW session_id]) "+
				"AND application_name='concurrent-drop-mutations-insert-fail'",
			[][]string{{"2"}},
		)

		close(delayJobChannels[0])
		close(delayJobChannels[1])
	})

	close(delayNotify)
	close(proceedBeforeBackfill)
}

// TestCheckConstraintDropAndColumn tests for Issue #61749 which uncovered
// that checks would be incorrectly activated if a drop column occurred, even
// if they weren't fully validated.
func TestCheckConstraintDropAndColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var jobControlMu syncutil.Mutex
	var delayJobList []string
	var delayJobChannels []chan struct{}
	delayNotify := make(chan struct{})
	routineResults := make(chan error)

	params, _ := tests.CreateTestServerParams()
	var s serverutils.TestServerInterface
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				lockHeld := true
				jobControlMu.Lock()
				scJob, err := s.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
				if err != nil {
					return err
				}
				pl := scJob.Payload()
				// Check if we are blocking the correct job
				for idx, s := range delayJobList {
					if strings.Contains(pl.Description, s) {
						delayNotify <- struct{}{}
						channel := delayJobChannels[idx]
						jobControlMu.Unlock()
						lockHeld = false
						<-channel
						break
					}
				}
				if lockHeld {
					jobControlMu.Unlock()
				}
				return nil
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn1 := sqlutils.MakeSQLRunner(sqlDB)
	conn2 := sqlutils.MakeSQLRunner(sqlDB)

	conn1.Exec(t, `
CREATE TABLE t (i INT8 PRIMARY KEY, j INT8);
INSERT INTO t VALUES (1, 1);
`)

	// Issue #61749 uncovered that checks would be incorrectly
	// activated if a drop column occurred, even if they weren't
	// fully validate.
	t.Run("drop-column-and-check-constraint", func(t *testing.T) {
		jobControlMu.Lock()
		delayJobList = []string{"ALTER TABLE defaultdb.public.t ADD CHECK (i > 0)",
			"ALTER TABLE defaultdb.public.t DROP COLUMN j"}
		delayJobChannels = []chan struct{}{make(chan struct{}), make(chan struct{})}
		jobControlMu.Unlock()

		go func() {
			_, err := conn2.DB.ExecContext(ctx,
				`
ALTER TABLE t ADD CHECK (i > 0);
`)
			routineResults <- errors.Wrap(err, "alter table add check")
		}()
		<-delayNotify

		go func() {
			_, err := conn2.DB.ExecContext(ctx,
				`
SET sql_safe_updates = false;
BEGIN;
ALTER TABLE t DROP COLUMN j;
INSERT INTO t VALUES(-5);
DELETE FROM t WHERE i=-5;
COMMIT;
`)
			routineResults <- errors.Wrap(err, "alter table drop column")
		}()
		<-delayNotify

		// Allow jobs in expected order.
		delayJobChannels[0] <- struct{}{}
		delayJobChannels[1] <- struct{}{}
		close(delayJobChannels[0])
		close(delayJobChannels[1])
		// Check for the results from the routines
		for range delayJobChannels {
			require.NoError(t, <-routineResults)
		}
	})

}

// Ensures that errors coming from hlc due to clocks being out of sync are not
// treated as permanent failures.
func TestClockSyncErrorsAreNotPermanent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var tc serverutils.TestClusterInterface
	ctx := context.Background()
	var updatedClock int64 // updated with atomics
	tc = testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					RunBeforeBackfillChunk: func(sp roachpb.Span) error {
						if atomic.AddInt64(&updatedClock, 1) > 1 {
							return nil
						}
						clock := tc.Server(0).Clock()
						now := clock.Now()
						farInTheFuture := now.Add(time.Hour.Nanoseconds(), 0)

						return clock.UpdateAndCheckMaxOffset(ctx, farInTheFuture.UnsafeToClockTimestamp())
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE TABLE t (i INT PRIMARY KEY)`)
	// Before the commit which added this test, the below command would fail
	// due to a permanent error.
	tdb.Exec(t, `ALTER TABLE t ADD COLUMN j INT NOT NULL DEFAULT 42`)
}

// TestJobsWithoutMutationsAreCancelable validates that the jobs, which are created
// when a schema-change does not have mutations, are cancelable.
func TestJobsWithoutMutationsAreCancelable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var registry *jobs.Registry
	var scJobID jobspb.JobID
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				job, err := registry.LoadJob(ctx, jobID)
				assert.NoError(t, err)
				pl := job.Payload()
				// Validate that the job is cancelable and has an invalid mutation ID.
				assert.False(t, pl.Noncancelable)
				assert.Equal(t, pl.GetSchemaChange().TableMutationID, descpb.InvalidMutationID)
				scJobID = jobID
				return nil
			},
		}},
	})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	registry = s.JobRegistry().(*jobs.Registry)

	// This query results in a schema-change job that doesn't have mutations.
	tdb.Exec(t, "CREATE TABLE t (x PRIMARY KEY) AS VALUES (1)")
	var id jobspb.JobID
	tdb.QueryRow(t,
		`SELECT job_id FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE'`,
	).Scan(&id)
	require.Equal(t, scJobID, id)
}

func TestShardColumnConstraintSkipValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	constraintsToValidate := make(chan []catalog.ConstraintToUpdate, 1)
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeConstraintValidation: func(constraints []catalog.ConstraintToUpdate) error {
				constraintsToValidate <- constraints
				return nil
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test(a INT PRIMARY KEY, b INT NOT NULL);
INSERT INTO t.test VALUES (1, 2);
`,
	)

	// Make sure non-shard column constraint is validated.
	tdb.Exec(t, `ALTER TABLE t.test ADD CONSTRAINT check_b_positive CHECK (b > 0);`)
	require.Len(t, <-constraintsToValidate, 1)

	// Make sure shard column constraint is not validated.
	tdb.Exec(t, `
CREATE INDEX ON t.test (b) USING HASH WITH (bucket_count=8);
`,
	)
	require.Len(t, constraintsToValidate, 0)
}

func TestHashShardedIndexRangePreSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	getShardedIndexRanges := func(tableDesc *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) ([]kv.KeyValue, error) {
		indexSpan := tableDesc.IndexSpan(codec, descpb.IndexID(2))
		ranges, err := kvDB.Scan(
			ctx,
			keys.RangeMetaKey(keys.MustAddr(indexSpan.Key)),
			keys.RangeMetaKey(keys.MustAddr(indexSpan.EndKey)),

			100,
		)
		if err != nil {
			return nil, err
		}
		return ranges, nil
	}

	var runBeforePreSplitting func(tbl *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) error
	var runAfterPreSplitting func(tbl *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) error
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeHashShardedIndexRangePreSplit: func(tbl *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) error {
				return runBeforePreSplitting(tbl, kvDB, codec)
			},
			RunAfterHashShardedIndexRangePreSplit: func(tbl *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) error {
				return runAfterPreSplitting(tbl, kvDB, codec)
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test_split(a INT PRIMARY KEY, b INT NOT NULL);
`,
	)

	runBeforePreSplitting = func(tableDesc *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) error {
		ranges, err := getShardedIndexRanges(tableDesc, kvDB, codec)
		if err != nil {
			return err
		}
		if len(ranges) != 0 {
			return errors.Newf("expected 0 ranges but found %d", len(ranges))
		}
		return nil
	}

	runAfterPreSplitting = func(tableDesc *tabledesc.Mutable, kvDB *kv.DB, codec keys.SQLCodec) error {
		ranges, err := getShardedIndexRanges(tableDesc, kvDB, codec)
		if err != nil {
			return err
		}
		if len(ranges) != 8 {
			return errors.Newf("expected 8 ranges but found %d", len(ranges))
		}
		return nil
	}

	tdb.Exec(t, `
CREATE INDEX idx_test_split_b ON t.test_split (b) USING HASH WITH (bucket_count=8);
`)
}

func TestTTLAutomaticColumnSchemaChangeFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var shouldFail bool
	failFunc := func() error {
		if shouldFail {
			shouldFail = false
			return errors.AssertionFailedf("fail!")
		}
		return nil
	}

	const (
		createNonTTLTable = `CREATE DATABASE t;
	CREATE TABLE t.test (id TEXT PRIMARY KEY);`
		expectNonTTLTable = `CREATE TABLE public.test (
	id STRING NOT NULL,
	CONSTRAINT test_pkey PRIMARY KEY (id ASC)
)`

		createTTLTable = `CREATE DATABASE t;
CREATE TABLE t.test (id TEXT PRIMARY KEY) WITH (ttl_expire_after = '10 hours');`
		expectTTLTable = `CREATE TABLE public.test (
	id STRING NOT NULL,
	crdb_internal_expiration TIMESTAMPTZ NOT VISIBLE NOT NULL DEFAULT current_timestamp():::TIMESTAMPTZ + '10:00:00':::INTERVAL ON UPDATE current_timestamp():::TIMESTAMPTZ + '10:00:00':::INTERVAL,
	CONSTRAINT test_pkey PRIMARY KEY (id ASC)
) WITH (ttl = 'on', ttl_automatic_column = 'on', ttl_expire_after = '10:00:00':::INTERVAL)`
	)

	testCases := []struct {
		desc                    string
		setup                   string
		schemaChange            string
		knobs                   *sql.SchemaChangerTestingKnobs
		expectedShowCreateTable string
		expectSchedule          bool
	}{
		{
			desc:         "error during ALTER TABLE ... SET (ttl_expire_after ...) during add mutation",
			setup:        createNonTTLTable,
			schemaChange: `ALTER TABLE t.test SET (ttl_expire_after = '10 hours')`,
			knobs: &sql.SchemaChangerTestingKnobs{
				RunBeforeBackfill: failFunc,
			},
			expectedShowCreateTable: expectNonTTLTable,
			expectSchedule:          false,
		},
		{
			desc:         "error during ALTER TABLE ... SET (ttl_expire_after ...) during modify row-level-ttl mutation",
			setup:        createNonTTLTable,
			schemaChange: `ALTER TABLE t.test SET (ttl_expire_after = '10 hours')`,
			knobs: &sql.SchemaChangerTestingKnobs{
				RunBeforeModifyRowLevelTTL: failFunc,
			},
			expectedShowCreateTable: expectNonTTLTable,
			expectSchedule:          false,
		},
		{
			desc:         "error during ALTER TABLE ... RESET (ttl) during delete column mutation",
			setup:        createTTLTable,
			schemaChange: `ALTER TABLE t.test RESET (ttl)`,
			knobs: &sql.SchemaChangerTestingKnobs{
				RunBeforeBackfill: failFunc,
			},
			expectedShowCreateTable: expectTTLTable,
			expectSchedule:          true,
		},
		{
			desc:         "error during ALTER TABLE ... SET (ttl_expire_after ...) during modify row-level-ttl mutation",
			setup:        createTTLTable,
			schemaChange: `ALTER TABLE t.test RESET (ttl)`,
			knobs: &sql.SchemaChangerTestingKnobs{
				RunBeforeModifyRowLevelTTL: failFunc,
			},
			expectedShowCreateTable: expectTTLTable,
			expectSchedule:          true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			params, _ := tests.CreateTestServerParams()
			params.Knobs.SQLSchemaChanger = tc.knobs
			s, sqlDB, kvDB := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)

			_, err := sqlDB.Exec(tc.setup)
			require.NoError(t, err)

			shouldFail = true
			defer func() {
				shouldFail = false
			}()
			_, err = sqlDB.Exec(tc.schemaChange)
			require.Error(t, err)

			// Ensure CREATE TABLE is the same.
			var actualSchema string
			require.NoError(t, sqlDB.QueryRow(`SELECT create_statement FROM [SHOW CREATE TABLE t.test]`).Scan(&actualSchema))
			require.Equal(t, tc.expectedShowCreateTable, actualSchema)

			// Ensure the schedule is still there.
			desc := desctestutils.TestingGetPublicTableDescriptor(
				kvDB,
				keys.SystemSQLCodec,
				"t",
				"test",
			)
			if tc.expectSchedule {
				require.NotNil(t, desc.GetRowLevelTTL())
				require.Greater(t, desc.GetRowLevelTTL().ScheduleID, int64(0))

				// Ensure there is only one schedule and that it belongs to the table.
				var numSchedules int
				require.NoError(t, sqlDB.QueryRow(
					`SELECT count(1) FROM [SHOW SCHEDULES] WHERE label LIKE $1`,
					fmt.Sprintf("row-level-ttl-%d", desc.GetRowLevelTTL().ScheduleID),
				).Scan(&numSchedules))
				require.Equal(t, 0, numSchedules)
				require.NoError(t, sqlDB.QueryRow(`SELECT count(1) FROM [SHOW SCHEDULES] WHERE label LIKE 'row-level-ttl-%'`).Scan(&numSchedules))
				require.Equal(t, 1, numSchedules)
			} else {
				require.Nil(t, desc.GetRowLevelTTL())

				// Ensure there are no schedules.
				var numSchedules int
				require.NoError(t, sqlDB.QueryRow(`SELECT count(1) FROM [SHOW SCHEDULES] WHERE label LIKE 'row-level-ttl-%'`).Scan(&numSchedules))
				require.Equal(t, 0, numSchedules)
			}
		})
	}
}

func TestSchemaChangeWhileAddingOrDroppingTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc                    string
		setup                   string
		successfulChange        string
		conflictingSchemaChange string
		expected                func(uint32) string
	}{
		{
			desc: `during adding TTL`,
			setup: `
CREATE DATABASE t;
CREATE TABLE t.test (x INT);`,
			successfulChange:        `ALTER TABLE t.test SET (ttl_expire_after = '10 minutes')`,
			conflictingSchemaChange: `ALTER TABLE t.test ADD COLUMN y int`,
			expected: func(tableID uint32) string {
				return fmt.Sprintf(`pq: relation "test" \(%d\): cannot perform a schema change operation while a TTL change is in progress`, tableID)
			},
		},
		{
			desc: `during dropping TTL`,
			setup: `
CREATE DATABASE t;
CREATE TABLE t.test (x INT) WITH (ttl_expire_after = '10 minutes');`,
			successfulChange:        `ALTER TABLE t.test RESET (ttl)`,
			conflictingSchemaChange: `ALTER TABLE t.test ADD COLUMN y int`,
			expected: func(tableID uint32) string {
				return fmt.Sprintf(`pq: relation "test" \(%d\): cannot perform a schema change operation while a TTL change is in progress`, tableID)
			},
		},

		{
			desc: `TTL change whilst adding column`,
			setup: `
CREATE DATABASE t;
CREATE TABLE t.test (x INT);`,
			successfulChange:        `ALTER TABLE t.test ADD COLUMN y int`,
			conflictingSchemaChange: `ALTER TABLE t.test SET (ttl_expire_after = '10 minutes')`,
			expected: func(tableID uint32) string {
				return `pq: cannot modify TTL settings while another schema change on the table is being processed`
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			params, _ := tests.CreateTestServerParams()
			childJobStartNotification := make(chan struct{})
			waitBeforeContinuing := make(chan struct{})
			var doOnce sync.Once
			waitFunc := func() error {
				doOnce.Do(func() {
					childJobStartNotification <- struct{}{}
					<-waitBeforeContinuing
				})
				return nil
			}
			params.Knobs = base.TestingKnobs{
				SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
					RunBeforeBackfill:          waitFunc,
					RunBeforeModifyRowLevelTTL: waitFunc,
				},
			}

			s, db, _ := serverutils.StartServer(t, params)
			sqlDB := sqlutils.MakeSQLRunner(db)
			ctx := context.Background()
			defer s.Stopper().Stop(ctx)

			sqlDB.Exec(t, tc.setup)

			tableID := sqlutils.QueryTableID(t, db, "t", "public", "test")

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				sqlDB.Exec(t, tc.successfulChange)
				wg.Done()
			}()

			<-childJobStartNotification

			expected := tc.expected(tableID)
			sqlDB.ExpectErr(t, expected, tc.conflictingSchemaChange)

			waitBeforeContinuing <- struct{}{}
			wg.Wait()
		})
	}
}

func TestMixedAddIndexStyleFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	params.Knobs.Server = &server.TestingKnobs{
		DisableAutomaticVersionUpgrade: make(chan struct{}),
		BinaryVersionOverride:          clusterversion.ByKey(clusterversion.MVCCIndexBackfiller - 1),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT)")
	require.NoError(t, err)

	txn, err := sqlDB.Begin()
	require.NoError(t, err)
	_, err = txn.Exec("CREATE INDEX ON t (b)")
	require.NoError(t, err)

	waitOnce := &sync.Once{}
	wait := make(chan struct{})
	s.ClusterSettings().Version.SetOnChange(func(_ context.Context, newVersion clusterversion.ClusterVersion) {
		if newVersion.IsActive(clusterversion.MVCCIndexBackfiller) {
			waitOnce.Do(func() { close(wait) })
		}
	})
	close(params.Knobs.Server.(*server.TestingKnobs).DisableAutomaticVersionUpgrade)
	t.Log("waiting for version change")
	<-wait
	_, err = txn.Exec("CREATE INDEX ON t (c)")
	require.NoError(t, err)

	err = txn.Commit()
	require.Error(t, err, "expected 1 temporary indexes, but found 2; schema change may have been constructed during cluster version upgrade")
}

func TestAddIndexResumeAfterSettingFlippedFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	changeSetting := make(chan struct{})
	wait := make(chan struct{})
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		RunBeforeResume: func(jobID jobspb.JobID) error {
			close(changeSetting)
			<-wait
			return nil
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	errC := make(chan error)

	go func() {
		_, err := sqlDB.Exec("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT)")
		require.NoError(t, err)
		_, err = sqlDB.Exec("CREATE INDEX ON t (b)")
		errC <- err
	}()

	<-changeSetting
	_, err := sqlDB.Exec("SET CLUSTER SETTING sql.mvcc_compliant_index_creation.enabled = false")
	require.NoError(t, err)
	close(wait)

	require.Error(t, <-errC, "schema change requires MVCC-compliant backfiller, but MVCC-compliant backfiller is not supported")
}

func TestPauseBeforeRandomDescTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type testCase struct {
		name      string
		setupSQL  string
		changeSQL string
		verify    func(t *testing.T, sqlRunner *sqlutils.SQLRunner)
	}

	// We run the schema change twice. First, to find out how many
	// sc.txn calls there are, and then a second time that pauses
	// a random one. By finding the count of txns, we make sure
	// that we have an equal probability of pausing after each
	// transaction.
	getTxnCount := func(t *testing.T, tc testCase) int {
		var (
			count       int32 // accessed atomically
			shouldCount int32 // accessed atomically
		)
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeDescTxn: func(_ jobspb.JobID) error {
					if atomic.LoadInt32(&shouldCount) == 1 {
						atomic.AddInt32(&count, 1)
					}
					return nil
				},
			},
		}
		s, sqlDB, _ := serverutils.StartServer(t, params)
		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		defer s.Stopper().Stop(ctx)

		sqlRunner.Exec(t, tc.setupSQL)
		atomic.StoreInt32(&shouldCount, 1)
		sqlRunner.Exec(t, tc.changeSQL)
		return int(atomic.LoadInt32(&count))
	}

	runWithPauseAt := func(t *testing.T, tc testCase, pauseAt int) {
		var (
			count       int32 // accessed atomically
			shouldPause int32 // accessed atomically
			jobID       jobspb.JobID
		)

		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(id jobspb.JobID) error {
					jobID = id
					return nil
				},
				RunBeforeDescTxn: func(_ jobspb.JobID) error {
					if atomic.LoadInt32(&shouldPause) == 0 {
						return nil
					}
					current := int(atomic.AddInt32(&count, 1))
					if current == pauseAt {
						atomic.StoreInt32(&shouldPause, 0)
						return jobs.MarkPauseRequestError(errors.Newf("paused sc.txn call %d", current))
					}
					return nil
				},
			},
		}
		s, sqlDB, _ := serverutils.StartServer(t, params)
		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		defer s.Stopper().Stop(ctx)

		sqlRunner.Exec(t, tc.setupSQL)
		atomic.StoreInt32(&shouldPause, 1)
		sqlRunner.ExpectErr(t, ".*paused sc.txn call.*", tc.changeSQL)
		sqlRunner.Exec(t, "RESUME JOB $1", jobID)

		row := sqlRunner.QueryRow(t, "SELECT status FROM [SHOW JOB WHEN COMPLETE $1]", jobID)
		var status string
		row.Scan(&status)
		require.Equal(t, "succeeded", status)
		tc.verify(t, sqlRunner)
	}

	rnd, _ := randutil.NewTestRand()
	for _, tc := range []testCase{
		{
			name: "create index",
			setupSQL: `
CREATE TABLE t (pk INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);
`,
			changeSQL: "CREATE INDEX on t (b)",
			verify: func(t *testing.T, sqlRunner *sqlutils.SQLRunner) {
				rows := sqlutils.MatrixToStr(sqlRunner.QueryStr(t, "SELECT * FROM t@t_b_idx"))
				require.Equal(t, "1, 1\n2, 2\n3, 3\n", rows)
			},
		},
	} {
		txnCount := getTxnCount(t, tc)

		const testAll = false
		if testAll {
			for i := 1; i <= txnCount; i++ {
				t.Run(fmt.Sprintf("%s_pause_at_txn_%d", tc.name, i), func(t *testing.T) {
					runWithPauseAt(t, tc, i)
				})
			}
		} else {
			pauseAt := rnd.Intn(txnCount) + 1
			t.Run(fmt.Sprintf("%s_pause_at_txn_%d", tc.name, pauseAt), func(t *testing.T) {
				runWithPauseAt(t, tc, pauseAt)

			})
		}
	}
}

func TestTruncateWithIndexAdditionAtEveryStateTransition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var (
		s     serverutils.TestTenantInterface
		sqlDB *gosql.DB
		kvDB  *kv.DB

		jobID jobspb.JobID
		count int32
	)
	rowCount := 10
	writeSomeRows := func() error {
		for i := 0; i < rowCount; i++ {
			_, err := sqlDB.Exec("INSERT INTO t.test VALUES ($1, $1)", i)
			if err != nil {
				return err
			}
		}
		return nil
	}
	rng, _ := randutil.NewPseudoRand()
	truncateAtLim := 14
	truncateAt := rng.Intn(truncateAtLim) + 1
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(id jobspb.JobID) error {
				if jobID == 0 {
					jobID = id
				}
				return nil
			},
			RunBeforeDescTxn: func(id jobspb.JobID) error {
				if jobID == id {
					current := int(atomic.AddInt32(&count, 1))
					if current == truncateAt {
						t.Logf("running TRUNCATE at txn %d", current)
						if err := writeSomeRows(); err != nil {
							return err
						}

						_, err := sqlDB.Exec("TRUNCATE t.test")
						if err != nil {
							return err
						}
						truncateAt = -1
						// Write more rows so that there is something to truncate the next time.
						return writeSomeRows()
					}
				}
				return nil
			},
		},
	}
	s, sqlDB, kvDB = serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	_, err := sqlDB.Exec("CREATE DATABASE t; CREATE TABLE t.test (pk INT PRIMARY KEY, v INT)")
	require.NoError(t, err)

	_, err = sqlDB.Exec("CREATE INDEX ON t.test(v)")
	require.NoError(t, err)

	txnCount := int(atomic.LoadInt32(&count))
	require.True(t, txnCount <= truncateAtLim, "schema change ran %d which is larger than the hardcoded limit %d", txnCount, truncateAtLim)

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		return sqltestutils.CheckTableKeyCountExact(ctx, kvDB, 2*rowCount)
	})
	indexes := tableDesc.ActiveIndexes()
	require.Equal(t, 2, len(indexes))
	require.Equal(t, descpb.IndexID(4), indexes[0].GetID())
	require.Equal(t, descpb.IndexID(5), indexes[1].GetID())
}
