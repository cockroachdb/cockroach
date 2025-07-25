// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func newTestDecoder(
	t *testing.T, s serverutils.ApplicationLayerInterface, tableName string,
) (*eventDecoder, *EventBuilder) {
	ctx := context.Background()
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(tableName))

	decoder, err := newEventDecoder(ctx, s.InternalDB().(descs.DB), s.ClusterSettings(), map[descpb.ID]sqlProcessorTableConfig{
		desc.GetID(): {
			srcDesc: desc,
		},
	})
	require.NoError(t, err)

	eb := newKvEventBuilder(t, desc.TableDesc())
	return decoder, eb
}

func TestEventDecoder_Deduplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, `
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            value STRING
        )
    `)

	decoder, eb := newTestDecoder(t, s, "test_table")

	times := []hlc.Timestamp{}
	for i := 0; i < 8; i++ {
		times = append(times, s.Clock().Now())
	}

	batch := []streampb.StreamEvent_KV{
		eb.updateEvent(times[1],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v1")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("original")},
		),
		eb.updateEvent(times[2],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v2")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v1")},
		),
		eb.updateEvent(times[3],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v3_final")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v2")},
		),
		eb.updateEvent(times[4],
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("single_value")},
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("single_prev")},
		),
		eb.insertEvent(times[5],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("inserted")},
		),
		eb.updateEvent(times[6],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("updated")},
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("inserted")},
		),
		eb.deleteEvent(times[7],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("updated")},
		),
	}

	rand.Shuffle(len(batch), func(i, j int) {
		batch[i], batch[j] = batch[j], batch[i]
	})

	events, err := decoder.decodeAndCoalesceEvents(ctx, batch, jobspb.LogicalReplicationDetails_DiscardNothing)
	require.NoError(t, err)

	require.Len(t, events, 3)

	// Row 1: Multiple updates coalesced
	require.False(t, events[0].isDelete)
	require.Equal(t, times[3], events[0].originTimestamp)
	require.Equal(t, tree.NewDString("v3_final"), events[0].row[1])
	require.Equal(t, tree.NewDString("original"), events[0].prevRow[1])

	// Row 2: Single update unchanged
	require.False(t, events[1].isDelete)
	require.Equal(t, times[4], events[1].originTimestamp)
	require.Equal(t, tree.NewDString("single_value"), events[1].row[1])
	require.Equal(t, tree.NewDString("single_prev"), events[1].prevRow[1])

	// Row 3: Insert -> Update -> Delete coalesced
	require.True(t, events[2].isDelete)
	require.Equal(t, times[7], events[2].originTimestamp)
	require.Equal(t, tree.DNull, events[2].prevRow[1])
}

func TestEventDecoder_DeduplicationWithDiscardDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, `
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            value STRING
        )
    `)

	decoder, eb := newTestDecoder(t, s, "test_table")

	times := []hlc.Timestamp{}
	for i := 0; i < 8; i++ {
		times = append(times, s.Clock().Now())
	}

	batch := []streampb.StreamEvent_KV{
		// Single delete coalesces to nothing
		eb.deleteEvent(times[1],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("original")},
		),

		// Update followed by delete coalesces to the update
		eb.updateEvent(times[2],
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("v2")},
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("v1")},
		),
		eb.deleteEvent(times[3],
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("v2")},
		),

		// Delete -> Insert -> Delete coalesces to the insert
		eb.deleteEvent(times[4],
			// NOTE: this could be used as the prev value, but since we discard rows before
			// coalescing, the insert's NULL prev value will be used.
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("prev_value")},
		),
		eb.insertEvent(times[5],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("inserted")},
		),
		eb.deleteEvent(times[6],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("second_dropped_delete")},
		),
	}

	rand.Shuffle(len(batch), func(i, j int) {
		batch[i], batch[j] = batch[j], batch[i]
	})

	events, err := decoder.decodeAndCoalesceEvents(ctx, batch, jobspb.LogicalReplicationDetails_DiscardAllDeletes)
	require.NoError(t, err)

	require.Len(t, events, 2)
	for _, e := range events {
		require.False(t, e.isDelete, "expected no deletes when discarding deletes")
	}

	// Row 3: Insert -> Update -> Delete coalesced
	require.Equal(t, times[2], events[0].originTimestamp)
	require.Equal(t, tree.NewDString("v2"), events[0].row[1])
	require.Equal(t, tree.NewDString("v1"), events[0].prevRow[1])

	// Row 2: Coalesces to insert
	require.Equal(t, times[5], events[1].originTimestamp)
	require.Equal(t, tree.NewDString("inserted"), events[1].row[1])
	require.Equal(t, tree.DNull, events[1].prevRow[1])
}
