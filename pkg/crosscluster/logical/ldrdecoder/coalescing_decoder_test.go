// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func newTestDecoder(
	t *testing.T, s serverutils.ApplicationLayerInterface, tableName string,
) (*CoalescingDecoder, *EventBuilder) {
	ctx := context.Background()
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(tableName))

	decoder, err := NewCoalescingDecoder(ctx, s.InternalDB().(descs.DB), s.ClusterSettings(), []TableMapping{{
		SourceDescriptor: desc,
		DestID:           desc.GetID(),
	}})
	require.NoError(t, err)

	eb := NewTestEventBuilder(t, desc.TableDesc())
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
		eb.UpdateEvent(times[1],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v1")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("original")},
		),
		eb.UpdateEvent(times[2],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v2")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v1")},
		),
		eb.UpdateEvent(times[3],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v3_final")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("v2")},
		),
		eb.UpdateEvent(times[4],
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("single_value")},
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("single_prev")},
		),
		eb.InsertEvent(times[5],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("inserted")},
		),
		eb.UpdateEvent(times[6],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("updated")},
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("inserted")},
		),
		eb.DeleteEvent(times[7],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("updated")},
		),
	}

	rand.Shuffle(len(batch), func(i, j int) {
		batch[i], batch[j] = batch[j], batch[i]
	})

	events, err := decoder.DecodeAndCoalesceEvents(ctx, batch, jobspb.LogicalReplicationDetails_DiscardNothing)
	require.NoError(t, err)

	require.Len(t, events, 3)

	// Row 1: Multiple updates coalesced
	require.True(t, events[0].IsUpdateRow())
	require.Equal(t, times[3], events[0].RowTimestamp)
	require.Equal(t, tree.NewDString("v3_final"), events[0].Row[1])
	require.Equal(t, tree.NewDString("original"), events[0].PrevRow[1])

	// Row 2: Single update unchanged
	require.True(t, events[1].IsUpdateRow())
	require.Equal(t, times[4], events[1].RowTimestamp)
	require.Equal(t, tree.NewDString("single_value"), events[1].Row[1])
	require.Equal(t, tree.NewDString("single_prev"), events[1].PrevRow[1])

	// Row 3: Insert -> Update -> Delete coalesced to a delete with no prev value
	// because the original insert had no prev value.
	require.True(t, events[2].IsTombstoneUpdate())
	require.Equal(t, times[7], events[2].RowTimestamp)
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
		eb.DeleteEvent(times[1],
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("original")},
		),

		// Update followed by delete coalesces to the update
		eb.UpdateEvent(times[2],
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("v2")},
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("v1")},
		),
		eb.DeleteEvent(times[3],
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("v2")},
		),

		// Delete -> Insert -> Delete coalesces to the insert
		eb.DeleteEvent(times[4],
			// NOTE: this could be used as the prev value, but since we discard rows before
			// coalescing, the insert's NULL prev value will be used.
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("prev_value")},
		),
		eb.InsertEvent(times[5],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("inserted")},
		),
		eb.DeleteEvent(times[6],
			[]tree.Datum{tree.NewDInt(tree.DInt(3)), tree.NewDString("second_dropped_delete")},
		),
	}

	rand.Shuffle(len(batch), func(i, j int) {
		batch[i], batch[j] = batch[j], batch[i]
	})

	events, err := decoder.DecodeAndCoalesceEvents(ctx, batch, jobspb.LogicalReplicationDetails_DiscardAllDeletes)
	require.NoError(t, err)

	require.Len(t, events, 2)
	for _, e := range events {
		require.False(t, e.IsDeleteRow() || e.IsTombstoneUpdate(), "expected no deletes when discarding deletes")
	}

	// Row 2: Update -> Delete coalesced to an update.
	require.True(t, events[0].IsUpdateRow())
	require.Equal(t, times[2], events[0].RowTimestamp)
	require.Equal(t, tree.NewDString("v2"), events[0].Row[1])
	require.Equal(t, tree.NewDString("v1"), events[0].PrevRow[1])

	// Row 3: Delete -> Insert -> Delete coalesced to insert (no prev value).
	require.True(t, events[1].IsInsertRow())
	require.Equal(t, times[5], events[1].RowTimestamp)
	require.Equal(t, tree.NewDString("inserted"), events[1].Row[1])
}

func TestEventDecoder_UserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(jeffswenson): it would be nice if we could implement this test using
	// the randgen package, but randgen appears to be missing a utililty that
	// lets us create a random table random dependent UDTs.

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	server := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	for _, db := range []string{"srcdb", "dstdb"} {
		runner.Exec(t, "CREATE DATABASE "+db)
		runner.Exec(t, "USE "+db)
		runner.Exec(t, "CREATE TYPE status_enum AS ENUM ('active', 'inactive')")
		runner.Exec(t, "CREATE TYPE metadata_type AS (key TEXT, value INT)")
		runner.Exec(t, `CREATE TABLE user_types (
			id INT PRIMARY KEY,
			status status_enum,
			tags TEXT[],
			metadata metadata_type
		)`)
	}

	srcDesc := cdctest.GetHydratedTableDescriptor(t, server.ExecutorConfig(), "srcdb", "public", "user_types")
	dstDesc := cdctest.GetHydratedTableDescriptor(t, server.ExecutorConfig(), "dstdb", "public", "user_types")

	decoder, err := NewCoalescingDecoder(ctx, server.InternalDB().(descs.DB), server.ClusterSettings(), []TableMapping{{
		SourceDescriptor: srcDesc,
		DestID:           dstDesc.GetID(),
	}})
	require.NoError(t, err)

	eb := NewTestEventBuilder(t, srcDesc.TableDesc())

	// Build test row with user-defined types
	enumType := catalog.FindColumnByName(srcDesc, "status").GetType()
	arrayType := catalog.FindColumnByName(srcDesc, "tags").GetType()
	tupleType := catalog.FindColumnByName(srcDesc, "metadata").GetType()
	testRow := tree.Datums{
		tree.NewDInt(1),
		&tree.DEnum{
			EnumTyp:     enumType,
			PhysicalRep: enumType.TypeMeta.EnumData.PhysicalRepresentations[0], // Use correct physical rep
			LogicalRep:  enumType.TypeMeta.EnumData.LogicalRepresentations[0],  // Use correct logical rep
		},
		randgen.RandDatum(rng, arrayType, false).(*tree.DArray),
		randgen.RandDatum(rng, tupleType, false).(*tree.DTuple),
	}

	insertEvent := eb.InsertEvent(server.Clock().Now(), testRow)
	events, err := decoder.DecodeAndCoalesceEvents(ctx, []streampb.StreamEvent_KV{insertEvent}, jobspb.LogicalReplicationDetails_DiscardNothing)
	require.NoError(t, err)
	require.Len(t, events, 1)

	row := events[0].Row
	require.Len(t, row, 4)

	// Verify enum type mapping
	enum := row[1].(*tree.DEnum)
	dstEnumType := catalog.FindColumnByName(dstDesc, "status").GetType()
	require.Equal(t, dstEnumType, enum.EnumTyp)

	// Verify array type mapping
	array := row[2].(*tree.DArray)
	dstArrayType := catalog.FindColumnByName(dstDesc, "tags").GetType()
	require.Equal(t, dstArrayType, array.ParamTyp)

	// Verify tuple type mapping
	tuple := row[3].(*tree.DTuple)
	dstTupleType := catalog.FindColumnByName(dstDesc, "metadata").GetType()
	require.Equal(t, dstTupleType, tuple.ResolvedType())
}
