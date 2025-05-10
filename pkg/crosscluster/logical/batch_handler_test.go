// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
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
	"github.com/stretchr/testify/require"
)

func createEvent(t *testing.T, desc *descpb.TableDescriptor, isInsert bool, time hlc.Timestamp, row tree.Datums, prev tree.Datums) streampb.StreamEvent_KV {
	// Create a catalog.TableDescriptor from the descpb.TableDescriptor
	tableDesc := tabledesc.NewBuilder(desc).BuildImmutableTable()

	// Create a column map for the primary key columns
	var colMap catalog.TableColMap
	for i, col := range tableDesc.PublicColumns() {
		colMap.Set(col.GetID(), i)
	}

	// Encode the primary key values
	indexEntries, err := rowenc.EncodePrimaryIndex(
		keys.SystemSQLCodec,
		tableDesc,
		tableDesc.GetPrimaryIndex(),
		colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(t, err)
	require.Len(t, indexEntries, 1)

	// Create the KV event
	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   indexEntries[0].Key,
			Value: indexEntries[0].Value,
		},
	}
	event.KeyValue.Value.Timestamp = time

	// Set the event type based on isInsert
	if isInsert {
		event.KeyValue.Value.InitChecksum(event.KeyValue.Key)
	} else {
		// For deletes, we need to encode the previous values
		if prev != nil {
			prevEntries, err := rowenc.EncodePrimaryIndex(
				keys.SystemSQLCodec,
				tableDesc,
				tableDesc.GetPrimaryIndex(),
				colMap,
				prev,
				true, // includeEmpty
			)
			require.NoError(t, err)
			require.Len(t, prevEntries, 1)
			event.PrevValue = prevEntries[0].Value
		}
	}

	return event
}

func TestBatchHandlerReplayUniqueConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create a test table with an index
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name STRING,
			INDEX idx_name (name),
			UNIQUE (name)
		)
	`)

	// Add and remove a column to create a new primjkary key index that comes after the name index.
	sqlDB.Exec(t, `ALTER TABLE test_table ADD COLUMN temp_col INT`)
	sqlDB.Exec(t, `ALTER TABLE test_table DROP COLUMN temp_col`)

	// Construct a kv batch handler to write to the table
	desc := cdctest.GetHydratedTableDescriptor(t, s.ApplicationLayer().ExecutorConfig(), "test_table")
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

	clock := s.Clock()

	require.NoError(t, err)
	defer handler.ReleaseLeases(ctx)
	events := []streampb.StreamEvent_KV{
		createEvent(t, desc.TableDesc(), true, clock.Now(), []tree.Datum{
			tree.NewDInt(1),
			tree.NewDString("test"),
		}, nil),
	}
	for i := 0; i < 2; i++ {
		_, err := handler.HandleBatch(ctx, events)
		require.NoError(t, err)
		handler.ReleaseLeases(ctx)
	}

	sqlDB.CheckQueryResults(t, `SELECT id, name FROM test_table`, [][]string{
		{"1", "test"},
	})
}
