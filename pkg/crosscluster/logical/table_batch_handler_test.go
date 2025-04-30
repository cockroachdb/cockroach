// Copyright 2025 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type eventBuilder struct {
	descID descpb.ID
}

func newEventBuilder(
	t *testing.T, runner *sqlutils.SQLRunner, s serverutils.ApplicationLayerInterface,
) *eventBuilder {
	runner.Exec(t, `
		CREATE TABLE test_table (
			id STRING PRIMARY KEY,
			value STRING
		);
	`)

	// Get the actual table descriptor
	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", "test_table")

	return &eventBuilder{
		descID: desc.GetID(),
	}
}

func (e *eventBuilder) makeInsert(id string, value string, timestamp hlc.Timestamp) decodedEvent {
	return decodedEvent{
		dstDescID:       e.descID,
		isDelete:        false,
		originTimestamp: timestamp,
		row:             tree.Datums{tree.NewDString(id), tree.NewDString(value)},
	}
}

func (e *eventBuilder) makeUpdate(
	id string, old, new string, timestamp hlc.Timestamp,
) decodedEvent {
	return decodedEvent{
		dstDescID:       e.descID,
		isDelete:        false,
		originTimestamp: timestamp,
		row:             tree.Datums{tree.NewDString(id), tree.NewDString(new)},
		prevRow:         tree.Datums{tree.NewDString(id), tree.NewDString(old)},
	}
}

func (e *eventBuilder) makeDelete(id string, value string, timestamp hlc.Timestamp) decodedEvent {
	return decodedEvent{
		dstDescID:       e.descID,
		isDelete:        true,
		originTimestamp: timestamp,
		row:             tree.Datums{tree.NewDString(id), tree.DNull},
		prevRow:         tree.Datums{tree.NewDString(id), tree.NewDString(value)},
	}
}

func newTestTableHandler(
	t *testing.T, descID descpb.ID, s serverutils.ApplicationLayerInterface,
) *tableHandler {
	sd := sql.NewInternalSessionData(context.Background(), s.ClusterSettings(), "" /* opName */)
	handler, err := newTableHandler(context.Background(), descID, s.InternalDB().(descs.DB), s.Codec(), sd, 1337, s.LeaseManager().(*lease.Manager), s.ClusterSettings())
	require.NoError(t, err)
	return handler
}

func TestTableHandlerFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)
	eb := newEventBuilder(t, runner, s)
	handler := newTestTableHandler(t, eb.descID, s)

	// Apply a batch with inserts to popluate some data
	stats, err := handler.handleDecodedBatch(ctx, []decodedEvent{
		eb.makeInsert("1", "foo", s.Clock().Now()),
		eb.makeInsert("2", "bar", s.Clock().Now()),
	})
	require.NoError(t, err)
	require.Equal(t, tableBatchStats{inserts: 2}, stats)
	runner.CheckQueryResults(t, `SELECT id, value FROM test_table`, [][]string{
		{"1", "foo"},
		{"2", "bar"},
	})

	// Apply a batch with updates and deletes for the previously written rows
	stats, err = handler.handleDecodedBatch(ctx, []decodedEvent{
		eb.makeUpdate("1", "foo", "foo-update", s.Clock().Now()),
		eb.makeDelete("2", "bar", s.Clock().Now()),
	})
	require.NoError(t, err)
	require.Equal(t, tableBatchStats{updates: 1, deletes: 1}, stats)
	runner.CheckQueryResults(t, `SELECT id, value FROM test_table`, [][]string{
		{"1", "foo-update"},
	})

}

func TestTableHandlerSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	eb := newEventBuilder(t, runner, s)

	handler := newTestTableHandler(t, eb.descID, s)

	runner.Exec(t, `
		INSERT INTO test_table (id, value) VALUES 
		('a', 'alpha'), ('b', 'beta'), ('c', 'gamma');
	`)

	stats, err := handler.handleDecodedBatch(ctx, []decodedEvent{
		eb.makeInsert("a", "alpha-update", s.Clock().Now()),
		eb.makeUpdate("b", "wrong", "beta-update", s.Clock().Now()),
		eb.makeDelete("c", "wrong", s.Clock().Now()),
		// TODO(jeffswenson): add tombstone case
		eb.makeDelete("d", "never-existed", s.Clock().Now()),
		eb.makeUpdate("e", "never-existed", "omega-insert", s.Clock().Now()),
	})
	require.NoError(t, err)
	require.Equal(t,
		tableBatchStats{inserts: 1, updates: 2, deletes: 1, tombstoneUpdates: 1, refreshedRows: 5},
		stats)

	runner.CheckQueryResults(t, `SELECT id, value FROM test_table`, [][]string{
		{"a", "alpha-update"}, // a was processed as an update
		{"b", "beta-update"},  // b was processed as an update
		// c is deleted, so it should not be in the result
		// d has an update tombstone
		{"e", "omega-insert"}, // e was processed as an insert
	})
}

func TestTableHandlerDuplicateBatchEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	eb := newEventBuilder(t, runner, s)
	handler := newTestTableHandler(t, eb.descID, s)

	before := s.Clock().Now()
	after := s.Clock().Now()

	// TODO(jeffswenson): think about the different weird grouped batches that
	// could exist. Is it sensitive to order? What happens when an update chain
	// hits a refresh?
	_, err := handler.handleDecodedBatch(ctx, []decodedEvent{
		eb.makeUpdate("a", "wrong-value", "insert-followed-by-delete", before),
		eb.makeDelete("a", "insert-followed-by-delete", after),
	})
	// The update causes a refresh and then after the refresh, the delete ends up
	// taking the update tombstone path because there is no local row, but the
	// cput fails because it observes the insert/delete.
	require.ErrorContains(t, err, "unexpected value")
}

func TestTableHandlerExhaustive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is an "exhaustive" test of the table handler. It tries to test
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

	eb := newEventBuilder(t, runner, s)

	handler := newTestTableHandler(t, eb.descID, s)

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
	getDecodedEvent := func(tc testCase) decodedEvent {
		origin := start
		if tc.winLww {
			origin = s.Clock().Now()
		}
		switch tc.replicationType {
		case replicationTypeUpdate:
			return eb.makeUpdate(fmt.Sprintf("%d", tc.id), getPreviousValue(tc), getNewValue(tc), origin)
		case replicationTypeInsert:
			return eb.makeInsert(fmt.Sprintf("%d", tc.id), getNewValue(tc), origin)
		case replicationTypeDelete:
			return eb.makeDelete(fmt.Sprintf("%d", tc.id), getPreviousValue(tc), origin)
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
			runner.Exec(t, fmt.Sprintf(`INSERT INTO test_table (id, value) VALUES ('%d', '%s')`, tc.id, value))
		case localValueNull:
			// Do nothing
		case localValueTombstone:
			runner.Exec(t, fmt.Sprintf(`INSERT INTO test_table (id, value) VALUES ('%d', 'old-value-%d')`, tc.id, rand.Int()))
			runner.Exec(t, fmt.Sprintf(`DELETE FROM test_table WHERE id = '%d'`, tc.id))
		}
	}

	batch := make([]decodedEvent, 0, len(testCases))
	for _, tc := range testCases {
		batch = append(batch, getDecodedEvent(tc))
	}
	_, err := handler.handleDecodedBatch(ctx, batch)
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
