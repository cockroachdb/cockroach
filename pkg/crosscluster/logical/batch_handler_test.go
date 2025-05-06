// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/stretchr/testify/require"
)

// EventBuilder helps construct StreamEvent_KV events for testing.
type EventBuilder struct {
	t         *testing.T
	tableDesc catalog.TableDescriptor
	colMap    catalog.TableColMap
	codec     keys.SQLCodec
}

// newEventBuilder creates a new EventBuilder for the given table descriptor.
func newKvEventBuilder(t *testing.T, desc *descpb.TableDescriptor) *EventBuilder {
	tableDesc := tabledesc.NewBuilder(desc).BuildImmutableTable()
	var colMap catalog.TableColMap
	for i, col := range tableDesc.PublicColumns() {
		colMap.Set(col.GetID(), i)
	}
	return &EventBuilder{
		t:         t,
		tableDesc: tableDesc,
		colMap:    colMap,
		codec:     keys.SystemSQLCodec,
	}
}

func (b *EventBuilder) encodeRow(timestamp hlc.Timestamp, row tree.Datums) roachpb.KeyValue {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		false,
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)
	kv := roachpb.KeyValue{
		Key:   indexEntries[0].Key,
		Value: indexEntries[0].Value,
	}
	kv.Value.Timestamp = timestamp
	kv.Value.InitChecksum(kv.Key)
	return kv
}

// insertEvent creates an insert event for the given row at the specified timestamp.
func (b *EventBuilder) insertEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	event := streampb.StreamEvent_KV{
		KeyValue: b.encodeRow(time, row),
	}
	return event
}

// updateEvent creates an update event for the given row and previous values at the specified timestamp.
func (b *EventBuilder) updateEvent(
	time hlc.Timestamp, row tree.Datums, prevValue tree.Datums,
) streampb.StreamEvent_KV {
	kv := b.encodeRow(time, row)
	kvPrev := b.encodeRow(time, prevValue)
	return streampb.StreamEvent_KV{
		KeyValue:  kv,
		PrevValue: kvPrev.Value,
	}
}

// deleteEvent creates a delete event for the given row at the specified timestamp.
func (b *EventBuilder) deleteEvent(
	time hlc.Timestamp, prevValue tree.Datums,
) streampb.StreamEvent_KV {
	kv := b.encodeRow(time, prevValue)
	return streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   kv.Key,
			Value: roachpb.Value{Timestamp: time},
		},
		PrevValue: kv.Value,
	}
}

// newKvBatchHandler creates a new batch handler for testing.
func newKvBatchHandler(
	t *testing.T, s serverutils.ApplicationLayerInterface, tableName string,
) (BatchHandler, catalog.TableDescriptor) {
	ctx := context.Background()
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(tableName))
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	handler, err := newKVRowProcessor(
		ctx,
		&execinfra.ServerConfig{
			DB:           s.InternalDB().(descs.DB),
			LeaseManager: s.LeaseManager(),
			Settings:     s.ClusterSettings(),
		},
		&eval.Context{
			Codec:            s.Codec(),
			Settings:         s.ClusterSettings(),
			SessionDataStack: sessiondata.NewStack(sd),
		},
		execinfrapb.LogicalReplicationWriterSpec{},
		map[descpb.ID]sqlProcessorTableConfig{
			desc.GetID(): {
				srcDesc: desc,
			},
		},
	)
	require.NoError(t, err)
	return handler, desc
}

// newSqlBatchHandler creates a new SQL-based batch handler for testing.
func newSqlBatchHandler(
	t *testing.T, s serverutils.ApplicationLayerInterface, tableName string,
) (BatchHandler, catalog.TableDescriptor) {
	ctx := context.Background()
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(tableName))
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	codec := s.Codec()
	handler, err := makeSQLProcessor(
		ctx,
		s.ClusterSettings(),
		map[descpb.ID]sqlProcessorTableConfig{
			desc.GetID(): {
				srcDesc: desc,
			},
		},
		0, // jobID
		s.InternalDB().(descs.DB),
		s.InternalDB().(isql.DB).Executor(isql.WithSessionData(sd)),
		sd,
		execinfrapb.LogicalReplicationWriterSpec{
			Mode: jobspb.LogicalReplicationDetails_Immediate,
		},
		codec,
		s.LeaseManager().(*lease.Manager),
	)
	require.NoError(t, err)
	return handler, desc
}

type batchHandlerFactory func(t *testing.T, s serverutils.ApplicationLayerInterface, tableName string) (BatchHandler, catalog.TableDescriptor)

// addAndRemoveColumn is a metamorphic test option that randomly decides whether
// to add and remove a column to trigger a rebuild of the primary key.
var addAndRemoveColumn = metamorphic.ConstantWithTestBool("batch-handler-exhaustive-rebuild-primary-index", false)

// uniqueConstraint is a metamorphic test option that randomly decides whether
// to add a unique constraint to the table.
var uniqueConstraint = metamorphic.ConstantWithTestBool("batch-handler-exhaustive-unique-constraint", false)

func TestBatchHandlerExhaustiveKV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testBatchHandlerExhaustive(t, newKvBatchHandler)
}

func TestBatchHandlerExhaustiveSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testBatchHandlerExhaustive(t, newSqlBatchHandler)
}

func TestBatchHandlerExhaustiveCrud(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testBatchHandlerExhaustive(t, newCrudBatchHandler)
}

func testBatchHandlerExhaustive(t *testing.T, factory batchHandlerFactory) {
	t.Helper()

	// This test is an "exhaustive" test of the batch handler. It tries to test
	// cross product of every possible (replication event type, local value,
	// previous value, lww win).
	//
	// This doesn't test
	// 1. Batches containing multiple rows or multiple events for the same row.
	// 2. Events that are supposed to be DLQ'd, like unique constraint
	// violations or foreign key violations.

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// TODO(jeffswenson): improve this test so that it uses a random schema.

	// Create a test table with an index
	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			value STRING
		)
	`)

	if uniqueConstraint {
		runner.Exec(t, `ALTER TABLE test_table ADD CONSTRAINT unique_value UNIQUE (value)`)
	}
	if addAndRemoveColumn {
		runner.Exec(t, `ALTER TABLE test_table ADD COLUMN temp_col INT`)
		runner.Exec(t, `ALTER TABLE test_table DROP COLUMN temp_col`)
	}

	handler, desc := factory(t, s, "test_table")
	defer handler.ReleaseLeases(ctx)

	// TODO(jeffswenson): test the other handler types.
	eventBuilder := newKvEventBuilder(t, desc.TableDesc())

	type previousValue string
	const (
		previousValuePresent previousValue = "present"
		previousValueAbsent  previousValue = "absent"
	)
	previousValues := []previousValue{
		previousValuePresent,
		previousValueAbsent,
	}

	type localValue string
	const (
		// The local value is the same as the previous value supplied by the
		// event.
		localValueMatch localValue = "previous-value"
		// The local value is different from the previous value supplied by the
		// event.
		localValueMismatch localValue = "different-value"
		// The local value never existed in the database.
		localValueNull localValue = "null-value"
		// The local value is a tombstone.
		localValueTombstone localValue = "tombstone-value"
		// The local value is the same as the replication value supplied by the
		// event.
		localValueReplay localValue = "replay-value"
	)
	localValues := []localValue{
		localValueMatch,
		localValueMismatch,
		localValueNull,
		localValueTombstone,
		localValueReplay,
	}

	type replicationType string
	const (
		replicationTypeUpdate replicationType = "update"
		replicationTypeInsert replicationType = "insert"
		replicationTypeDelete replicationType = "delete"
	)
	replicationTypes := []replicationType{
		replicationTypeUpdate,
		replicationTypeInsert,
		replicationTypeDelete,
	}

	start := s.Clock().Now()

	type testCase struct {
		id              int
		previousValue   previousValue
		localValue      localValue
		replicationType replicationType
		winLww          bool
	}
	testCaseName := func(tc testCase) string {
		return fmt.Sprintf("replication-type=%s:previous-value=%s:local-value=%s:win-lww=%t", tc.replicationType, tc.previousValue, tc.localValue, tc.winLww)
	}
	getPreviousValue := func(tc testCase) string {
		if tc.previousValue == previousValuePresent {
			return fmt.Sprintf("previous-value-%s", testCaseName(tc))
		}
		return ""
	}
	getEventValue := func(tc testCase) string {
		if tc.replicationType == replicationTypeDelete {
			return ""
		}
		return fmt.Sprintf("new-value-%s", testCaseName(tc))
	}
	getEvent := func(tc testCase) streampb.StreamEvent_KV {
		origin := start
		if tc.winLww {
			origin = s.Clock().Now()
		}
		switch tc.replicationType {
		case replicationTypeUpdate:
			return eventBuilder.updateEvent(
				origin,
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getEventValue(tc))},
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getPreviousValue(tc))},
			)
		case replicationTypeInsert:
			return eventBuilder.insertEvent(
				origin,
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getEventValue(tc))},
			)
		case replicationTypeDelete:
			return eventBuilder.deleteEvent(
				origin,
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getPreviousValue(tc))},
			)
		default:
			panic(fmt.Sprintf("unknown replication type: %s", tc.replicationType))
		}
	}
	getLocalValue := func(tc testCase) string {
		switch tc.localValue {
		case localValueMatch:
			return getPreviousValue(tc)
		case localValueMismatch:
			return fmt.Sprintf("value-mismatch-%d", tc.id)
		case localValueReplay:
			return getEventValue(tc)
		}
		return ""
	}

	// Construct the Cartesian product of all the possible test cases.
	var testCases []testCase
	id := rand.Intn(10000) // Start at a non-zero value to avoid bugs that are papered over by ordered ids.
	for _, previousValue := range previousValues {
		for _, localValue := range localValues {
			for _, replicationType := range replicationTypes {
				for _, winLww := range []bool{true, false} {

					if !winLww {
						if localValue == localValueNull {
							// If there is no tombstone or local row, then its
							// impossible to lose lww. So skip the test case.
							continue
						}
						if localValue == localValueMatch && previousValue == previousValueAbsent {
							// If the local value matches the previous value,
							// but the replication event does not have a
							// previous value, then it's impossible to lose lww.
							// So skip the test case.
							continue
						}
					}

					testCases = append(testCases, testCase{
						id:              id,
						previousValue:   previousValue,
						localValue:      localValue,
						replicationType: replicationType,
						winLww:          winLww,
					})
					id++
				}
			}
		}
	}

	// Initialize the database with the specified local values.
	for _, tc := range testCases {
		switch tc.localValue {
		case localValueNull:
			// Do nothing
		case localValueTombstone:
			runner.Exec(t, fmt.Sprintf(`INSERT INTO test_table (id, value) VALUES (%d, 'old-value-%d')`, tc.id, rand.Int()))
			runner.Exec(t, fmt.Sprintf(`DELETE FROM test_table WHERE id = %d`, tc.id))
		default:
			value := getLocalValue(tc)
			if value == "" {
				continue
			}
			runner.Exec(t, fmt.Sprintf(`INSERT INTO test_table (id, value) VALUES (%d, '%s')`, tc.id, value))
		}
	}

	// TODO(jeffswenson): metamorphically enable batching
	for _, tc := range testCases {
		_, err := handler.HandleBatch(ctx, []streampb.StreamEvent_KV{getEvent(tc)})
		require.NoError(t, err)
	}

	found := map[string]string{}
	results := runner.QueryStr(t, `SELECT id, value FROM test_table`)
	for _, row := range results {
		found[row[0]] = row[1]
	}

	for _, tc := range testCases {
		id := fmt.Sprintf("%d", tc.id)
		var expected string
		switch {
		case tc.winLww && tc.replicationType != replicationTypeDelete:
			expected = getEventValue(tc)
		case !tc.winLww:
			expected = getLocalValue(tc)
		}
		caseDesc := testCaseName(tc) // Get test case description for assertion messages
		if expected == "" {
			require.NotContains(t, found, id, "expected row %s to be deleted. Test case: %s", id, caseDesc)
		} else {
			require.Contains(t, found, id, "expected row %s to exist, but it was not found. Test case: %s", id, caseDesc)
			require.Equal(t, expected, found[id], "mismatch for row %s. Test case: %s", id, caseDesc)
		}
	}

	// TODO(jeffswenson): find a way to validate that all indexes are in a valid
	// state.
}
