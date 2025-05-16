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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
func newKvEventBuilder(t *testing.T, desc *descpb.TableDescriptor, codec keys.SQLCodec) *EventBuilder {
	tableDesc := tabledesc.NewBuilder(desc).BuildImmutableTable()
	var colMap catalog.TableColMap
	for i, col := range tableDesc.PublicColumns() {
		colMap.Set(col.GetID(), i)
	}
	return &EventBuilder{
		t:         t,
		tableDesc: tableDesc,
		colMap:    colMap,
		codec:     codec,
	}
}

// insertEvent creates an insert event for the given row at the specified timestamp.
func (b *EventBuilder) insertEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)

	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   indexEntries[0].Key,
			Value: indexEntries[0].Value,
		},
	}
	event.KeyValue.Value.Timestamp = time
	event.KeyValue.Value.InitChecksum(event.KeyValue.Key)
	return event
}

// updateEvent creates an update event for the given row and previous values at the specified timestamp.
func (b *EventBuilder) updateEvent(time hlc.Timestamp, row tree.Datums, prev tree.Datums) streampb.StreamEvent_KV {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)

	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   indexEntries[0].Key,
			Value: indexEntries[0].Value,
		},
	}
	event.KeyValue.Value.Timestamp = time

	prevEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		prev,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, prevEntries, 1)
	event.PrevValue = prevEntries[0].Value

	return event
}

// deleteEvent creates a delete event for the given row at the specified timestamp.
func (b *EventBuilder) deleteEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)

	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key: indexEntries[0].Key,
		},
	}
	event.KeyValue.Value.Timestamp = time
	return event
}

// newBatchHandler creates a new batch handler for testing.
func newBatchHandler(
	t *testing.T,
	s serverutils.ApplicationLayerInterface,
	tableName string,
) (*kvRowProcessor, catalog.TableDescriptor) {
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

func TestBatchHandlerReplayUniqueConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{ServerArgs: base.TestServerArgs{}})
	defer tc.Stopper().Stop(ctx)

	srv := tc.Server(0)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Create a test table with an index
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name STRING,
			INDEX idx_name (name),
			UNIQUE (name)
		)
	`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.dist_sender.concurrency_limit = 0`)

	// Add and remove a column to create a new primary key index that comes after the name index
	sqlDB.Exec(t, `ALTER TABLE test_table ADD COLUMN temp_col INT`)
	sqlDB.Exec(t, `ALTER TABLE test_table DROP COLUMN temp_col`)

	// Construct a kv batch handler to write to the table
	handler, desc := newBatchHandler(t, s, "test_table")
	defer handler.ReleaseLeases(ctx)

	clock := s.Clock()

	eventBuilder := newKvEventBuilder(t, desc.TableDesc(), s.Codec())
	events := []streampb.StreamEvent_KV{
		eventBuilder.updateEvent(clock.Now(), []tree.Datum{
			tree.NewDInt(1),
			tree.NewDString("test"),
		}, []tree.Datum{
			tree.NewDInt(1),
			tree.DNull,
		}),
	}
	for i := 0; i < 3; i++ {
		_, err := handler.HandleBatch(ctx, events)
		require.NoError(t, err)
		handler.ReleaseLeases(ctx)
	}

	sqlDB.CheckQueryResults(t, `SELECT id, name FROM test_table`, [][]string{
		{"1", "test"},
	})
}

func TestBatchHandlerExhaustive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is an "exhaustive" test of the batch handler. It tries to test
	// cross product of every possible (replication event type, local value,
	// previous value, lww win).
	//
	// Some things that aren't tested:
	// 1. Batches containing multiple events of the same row.
	// 2. The optimistic path. Since the batch is guaranteed to fail the first
	//    apply attempt, it always takes the read refresh path.

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create a test table with an index
	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			value STRING
		)
	`)

	handler, desc := newBatchHandler(t, s, "test_table")
	defer handler.ReleaseLeases(ctx)

	eventBuilder := newKvEventBuilder(t, desc.TableDesc(), s.Codec())

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
	)
	localValues := []localValue{
		localValueMatch,
		localValueMismatch,
		localValueNull,
		localValueTombstone,
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
	getNewValue := func(tc testCase) string {
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
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getNewValue(tc))},
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getPreviousValue(tc))},
			)
		case replicationTypeInsert:
			return eventBuilder.insertEvent(
				origin,
				[]tree.Datum{tree.NewDInt(tree.DInt(tc.id)), tree.NewDString(getNewValue(tc))},
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
		}
		return ""
	}

	// Construct the Cartesian product of all the possible test cases.
	var testCases []testCase
	id := 1337 // Start at a non-zero value to avoid bugs that are papered over by ordered ids.
	for _, previousValue := range previousValues {
		for _, localValue := range localValues {
			for _, replicationType := range replicationTypes {
				for _, winLww := range []bool{true, false} {

					if !winLww {
						// If there is no tombstone or local row, then its
						// impossible to lose lww. So skip the test case.
						if localValue == localValueNull {
							continue
						}
						if localValue == localValueMatch && previousValue == previousValueAbsent {
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
		case localValueMatch, localValueMismatch:
			value := getLocalValue(tc)
			if value == "" {
				continue
			}
			runner.Exec(t, fmt.Sprintf(`INSERT INTO test_table (id, value) VALUES (%d, '%s')`, tc.id, value))
		case localValueNull:
			// Do nothing
		case localValueTombstone:
			runner.Exec(t, fmt.Sprintf(`INSERT INTO test_table (id, value) VALUES (%d, 'old-value-%d')`, tc.id, rand.Int()))
			runner.Exec(t, fmt.Sprintf(`DELETE FROM test_table WHERE id = %d`, tc.id))
		}
	}

	batch := make([]streampb.StreamEvent_KV, 0, len(testCases))
	for _, tc := range testCases {
		batch = append(batch, getEvent(tc))
	}
	_, err := handler.HandleBatch(ctx, batch)
	require.NoError(t, err)

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
			expected = getNewValue(tc)
		case !tc.winLww:
			expected = getLocalValue(tc)
		}
		if expected == "" {
			require.NotContains(t, found, id, "expected row %s to be deleted", id)
		} else {
			require.Equal(t, expected, found[id], "expected row %s to be %s", id, expected)
		}
	}
}
