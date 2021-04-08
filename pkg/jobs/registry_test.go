// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func FakePHS(opName string, user security.SQLUsername) (interface{}, func()) {
	return nil, func() {}
}

func TestRegistryCancelation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	// Not using the server.DefaultHistogramWindowInterval constant because
	// of a dep cycle.
	const histogramWindowInterval = 60 * time.Second

	const nodeCount = 1
	nodeLiveness := NewFakeNodeLiveness(nodeCount)

	var db *kv.DB
	// Insulate this test from wall time.
	mClock := hlc.NewManualClock(hlc.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		roachpb.Version{Major: 20, Minor: 1},
		roachpb.Version{Major: 20, Minor: 1},
		true)
	sqlStorage := slstorage.NewStorage(stopper, clock, db, nil, settings)
	sqlInstance := slinstance.NewSQLInstance(stopper, clock, sqlStorage, settings)
	registry := MakeRegistry(
		log.AmbientContext{},
		stopper,
		clock,
		optionalnodeliveness.MakeContainer(nodeLiveness),
		db,
		nil, /* ex */
		base.TestingIDContainer,
		sqlInstance,
		settings,
		histogramWindowInterval,
		FakePHS,
		"",
		nil, /* knobs */
	)

	const cancelInterval = time.Nanosecond
	const adoptInterval = time.Duration(math.MaxInt64)
	if err := registry.Start(ctx, stopper, cancelInterval, adoptInterval); err != nil {
		t.Fatal(err)
	}

	wait := func() {
		// Every turn of the registry's liveness poll loop will generate exactly one
		// call to nodeLiveness.Self. Only after we've witnessed two calls can we be
		// sure that the first turn of the registry's loop has completed.
		//
		// Waiting for only the first call to nodeLiveness.Self is racy, as we'd
		// perform our assertions concurrently with the registry loop's observation
		// of our injected liveness failure, if any.
		<-nodeLiveness.SelfCalledCh
		<-nodeLiveness.SelfCalledCh
	}

	cancelCount := 0
	didRegister := false
	jobID := jobspb.JobID(1)
	const nodeID = roachpb.NodeID(1)

	register := func() {
		didRegister = true
		jobID++
		if err := registry.deprecatedRegister(jobID, func() { cancelCount++ }); err != nil {
			t.Fatal(err)
		}
	}
	unregister := func() {
		registry.unregister(jobID)
		didRegister = false
	}
	expectCancel := func(expect bool) {
		t.Helper()

		wait()
		var e int
		if expect {
			e = 1
		}
		if a := cancelCount; e != a {
			t.Errorf("expected cancelCount of %d, but got %d", e, a)
		}
	}
	check := func(fn func()) {
		fn()
		if didRegister {
			unregister()
			wait()
		}
		cancelCount = 0
	}
	// inWindow slews the expiration time of the node's expiration.
	inWindow := func(in bool) {
		nanos := -defaultLeniencySetting.Nanoseconds()
		if in {
			nanos = nanos / 2
		} else {
			nanos = nanos * 2
		}
		nodeLiveness.FakeSetExpiration(nodeID, clock.Now().Add(nanos, 0))
	}

	// Jobs that complete while the node is live should be canceled once.
	check(func() {
		register()
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs that are in-progress when the liveness epoch is incremented
	// should not be canceled.
	check(func() {
		register()
		nodeLiveness.FakeIncrementEpoch(nodeID)
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs started in the new epoch that complete while the new epoch is live
	// should be canceled once.
	check(func() {
		register()
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs **alive** within the leniency period should not be canceled.
	check(func() {
		register()
		inWindow(true)
		expectCancel(false)
		unregister()
		expectCancel(true)
	})

	// Jobs **started** within the leniency period should not be canceled.
	check(func() {
		inWindow(true)
		register()
		expectCancel(false)
	})

	// Jobs **alive** outside of the leniency period should be canceled.
	check(func() {
		register()
		inWindow(false)
		expectCancel(true)
	})

	// Jobs **started** outside of the leniency period should be canceled.
	check(func() {
		inWindow(false)
		register()
		expectCancel(true)
	})
}

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func writeColumnMutation(
	t *testing.T,
	kvDB *kv.DB,
	tableDesc *tabledesc.Mutable,
	column string,
	m descpb.DescriptorMutation,
) {
	col, err := tableDesc.FindColumnWithName(tree.Name(column))
	if err != nil {
		t.Fatal(err)
	}
	for i := range tableDesc.Columns {
		if col.GetID() == tableDesc.Columns[i].ID {
			// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
			// to ColumnDescriptors may unexpectedly change.
			tableDesc.Columns = append(tableDesc.Columns[:i:i], tableDesc.Columns[i+1:]...)
			break
		}
	}
	m.Descriptor_ = &descpb.DescriptorMutation_Column{Column: col.ColumnDesc()}
	writeMutation(t, kvDB, tableDesc, m)
}

// writeMutation writes the mutation to the table descriptor.
func writeMutation(
	t *testing.T, kvDB *kv.DB, tableDesc *tabledesc.Mutable, m descpb.DescriptorMutation,
) {
	tableDesc.Mutations = append(tableDesc.Mutations, m)
	tableDesc.Version++
	if err := catalog.ValidateSelf(tableDesc); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

func writeGCMutation(
	t *testing.T,
	kvDB *kv.DB,
	tableDesc *tabledesc.Mutable,
	m descpb.TableDescriptor_GCDescriptorMutation,
) {
	tableDesc.GCMutations = append(tableDesc.GCMutations, m)
	tableDesc.Version++
	if err := catalog.ValidateSelf(tableDesc); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

func clearColumnMutationAndDropJob(
	t *testing.T, kvDB *kv.DB, tableName, colName string, mutOptions mutationOptions,
) {
	tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "t", tableName)
	if mutOptions.hasMutation {
		// Since we are removing the mutation, we must add it back to the columns so
		// that the descriptor passes verification.
		col, err := tableDesc.FindColumnWithName(tree.Name(colName))
		if err != nil {
			t.Fatal(err)
		}
		tableDesc.Columns = append(tableDesc.Columns, *col.ColumnDesc())
		tableDesc.Mutations = nil
	}
	tableDesc.GCMutations = nil
	tableDesc.DropJobID = 0

	tableDesc.Version++
	if err := catalog.ValidateSelf(tableDesc); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

type mutationOptions struct {
	// Set if the desc should have any mutations of any sort.
	hasMutation bool
	// Set if the mutation being inserted is a GCMutation.
	hasGCMutation bool
	// Set if the desc should have a job that is dropping it.
	hasDropJob bool
}

func (m mutationOptions) string() string {
	return fmt.Sprintf("hasMutation=%s_hasGCMutation=%s_hasDropJob=%s",
		strconv.FormatBool(m.hasMutation), strconv.FormatBool(m.hasGCMutation),
		strconv.FormatBool(m.hasDropJob))
}

func TestRegistryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := timeutil.Now()
	earlier := ts.Add(-1 * time.Hour)
	muchEarlier := ts.Add(-2 * time.Hour)

	setDropJob := func(dbName, tableName string) {
		desc := catalogkv.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, dbName, tableName)
		desc.DropJobID = 123
		if err := kvDB.Put(
			context.Background(),
			catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
			desc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
	}

	constructTableName := func(prefix string, mutOptions mutationOptions) string {
		return fmt.Sprintf("%s_%s", prefix, mutOptions.string())
	}

	writeJob := func(name string, created, finished time.Time, status Status, mutOptions mutationOptions) string {
		tableName := constructTableName(name, mutOptions)
		if _, err := sqlDB.Exec(fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS t; 
CREATE TABLE t."%s" (k VARCHAR PRIMARY KEY DEFAULT 'default', v VARCHAR,i VARCHAR NOT NULL DEFAULT 'i');
INSERT INTO t."%s" VALUES('a', 'foo');
`, tableName, tableName)); err != nil {
			t.Fatal(err)
		}
		tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", tableName)
		if mutOptions.hasDropJob {
			setDropJob("t", tableName)
		}
		if mutOptions.hasMutation {
			writeColumnMutation(t, kvDB, tableDesc, "i", descpb.DescriptorMutation{State: descpb.
				DescriptorMutation_DELETE_AND_WRITE_ONLY, Direction: descpb.DescriptorMutation_DROP})
		}
		if mutOptions.hasGCMutation {
			writeGCMutation(t, kvDB, tableDesc, descpb.TableDescriptor_GCDescriptorMutation{})
		}

		payload, err := protoutil.Marshal(&jobspb.Payload{
			Description: name,
			Lease:       &jobspb.Lease{NodeID: 1, Epoch: 1},
			// register a mutation on the table so that jobs that reference
			// the table are not considered orphaned
			DescriptorIDs: []descpb.ID{
				tableDesc.GetID(),
				descpb.InvalidID, // invalid id to test handling of missing descriptors.
			},
			Details:        jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{}),
			StartedMicros:  timeutil.ToUnixMicros(created),
			FinishedMicros: timeutil.ToUnixMicros(finished),
		})
		if err != nil {
			t.Fatal(err)
		}
		progress, err := protoutil.Marshal(&jobspb.Progress{
			Details: jobspb.WrapProgressDetails(jobspb.SchemaChangeProgress{}),
		})
		if err != nil {
			t.Fatal(err)
		}

		var id jobspb.JobID
		db.QueryRow(t,
			`INSERT INTO system.jobs (status, payload, progress, created) VALUES ($1, $2, $3, $4) RETURNING id`,
			status, payload, progress, created).Scan(&id)
		return strconv.Itoa(int(id))
	}

	// Test the descriptor when any of the following are set.
	// 1. Mutations
	// 2. GC Mutations
	// 3. A drop job
	for _, hasMutation := range []bool{true, false} {
		for _, hasGCMutation := range []bool{true, false} {
			for _, hasDropJob := range []bool{true, false} {
				if !hasMutation && !hasGCMutation && !hasDropJob {
					continue
				}
				mutOptions := mutationOptions{
					hasMutation:   hasMutation,
					hasGCMutation: hasGCMutation,
					hasDropJob:    hasDropJob,
				}
				oldRunningJob := writeJob("old_running", muchEarlier, time.Time{}, StatusRunning, mutOptions)
				oldSucceededJob := writeJob("old_succeeded", muchEarlier, muchEarlier.Add(time.Minute), StatusSucceeded, mutOptions)
				oldFailedJob := writeJob("old_failed", muchEarlier, muchEarlier.Add(time.Minute),
					StatusFailed, mutOptions)
				oldRevertFailedJob := writeJob("old_revert_failed", muchEarlier, muchEarlier.Add(time.Minute),
					StatusRevertFailed, mutOptions)
				oldCanceledJob := writeJob("old_canceled", muchEarlier, muchEarlier.Add(time.Minute),
					StatusCanceled, mutOptions)
				newRunningJob := writeJob("new_running", earlier, earlier.Add(time.Minute), StatusRunning,
					mutOptions)
				newSucceededJob := writeJob("new_succeeded", earlier, earlier.Add(time.Minute), StatusSucceeded, mutOptions)
				newFailedJob := writeJob("new_failed", earlier, earlier.Add(time.Minute), StatusFailed, mutOptions)
				newRevertFailedJob := writeJob("new_revert_failed", earlier, earlier.Add(time.Minute), StatusRevertFailed, mutOptions)
				newCanceledJob := writeJob("new_canceled", earlier, earlier.Add(time.Minute),
					StatusCanceled, mutOptions)

				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldSucceededJob}, {oldFailedJob}, {oldRevertFailedJob}, {oldCanceledJob},
					{newRunningJob}, {newSucceededJob}, {newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newSucceededJob},
					{newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newRevertFailedJob}})

				// force the running jobs to become orphaned and ensure they're cleaned up.
				oldRunningTableName := constructTableName("old_running", mutOptions)
				newRunningTableName := constructTableName("new_running", mutOptions)
				clearColumnMutationAndDropJob(t, kvDB, oldRunningTableName, "i", mutOptions)
				clearColumnMutationAndDropJob(t, kvDB, newRunningTableName, "i", mutOptions)
				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRevertFailedJob}, {newRevertFailedJob},
				})

				// Delete the revert failed jobs for the next run of the test.
				_, err := sqlDB.Exec(`DELETE FROM system.jobs WHERE id = $1 OR id = $2`,
					oldRevertFailedJob, newRevertFailedJob)
				require.NoError(t, err)
			}
		}
	}
}

func TestRegistryGCPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(ctx)

	for i := 0; i < 2*cleanupPageSize+1; i++ {
		payload, err := protoutil.Marshal(&jobspb.Payload{})
		require.NoError(t, err)
		db.Exec(t,
			`INSERT INTO system.jobs (status, created, payload) VALUES ($1, $2, $3)`,
			StatusCanceled, timeutil.Now().Add(-time.Hour), payload)
	}

	ts := timeutil.Now()
	require.NoError(t, s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(-10*time.Minute)))
	var count int
	db.QueryRow(t, `SELECT count(1) FROM system.jobs`).Scan(&count)
	require.Zero(t, count)
}
