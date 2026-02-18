// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTxnDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `CREATE TABLE a (id INT PRIMARY KEY, value STRING)`)
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `CREATE TABLE b (id INT PRIMARY KEY, data INT)`)

	descA := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name("a"))
	descB := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name("b"))

	decoder, err := NewTxnDecoder(ctx, s.InternalDB().(descs.DB), s.ClusterSettings(), []TableMapping{
		{SourceDescriptor: descA, DestID: descA.GetID()},
		{SourceDescriptor: descB, DestID: descB.GetID()},
	})
	require.NoError(t, err)

	ebA := NewTestEventBuilder(t, descA.TableDesc())
	ebB := NewTestEventBuilder(t, descB.TableDesc())
	txnTime := s.Clock().Now()

	// Build a transaction with events from both tables.
	events := []streampb.StreamEvent_KV{
		ebA.InsertEvent(txnTime, tree.Datums{tree.NewDInt(1), tree.NewDString("inserted")}),
		ebA.DeleteEvent(txnTime, tree.Datums{tree.NewDInt(2), tree.NewDString("deleted")}),
		ebB.UpdateEvent(txnTime,
			tree.Datums{tree.NewDInt(10), tree.NewDInt(100)},
			tree.Datums{tree.NewDInt(10), tree.NewDInt(50)}),
		ebB.TombstoneEvent(txnTime, tree.Datums{tree.NewDInt(20), tree.NewDInt(0)}),
	}

	txn, err := decoder.DecodeTxn(ctx, events)
	require.NoError(t, err)
	require.Equal(t, txnTime, txn.Timestamp)
	require.Len(t, txn.WriteSet, 4)

	writeSetChecks := []struct {
		name            string
		isInsert        bool
		isDelete        bool
		isUpdate        bool
		isTombstone     bool
		expectedRow     tree.Datums
		expectedPrevRow tree.Datums
	}{{
		name:        "insert on table a",
		isInsert:    true,
		expectedRow: tree.Datums{tree.NewDInt(1), tree.NewDString("inserted")},
	}, {
		name:            "delete on table a",
		isDelete:        true,
		expectedPrevRow: tree.Datums{tree.NewDInt(2), tree.NewDString("deleted")},
	}, {
		name:            "update on table b",
		isUpdate:        true,
		expectedRow:     tree.Datums{tree.NewDInt(10), tree.NewDInt(100)},
		expectedPrevRow: tree.Datums{tree.NewDInt(10), tree.NewDInt(50)},
	}, {
		name:        "tombstone on table b",
		isTombstone: true,
	}}

	for i, tc := range writeSetChecks {
		t.Run(tc.name, func(t *testing.T) {
			row := txn.WriteSet[i]
			require.Equal(t, tc.isInsert, row.IsInsertRow())
			require.Equal(t, tc.isDelete, row.IsDeleteRow())
			require.Equal(t, tc.isUpdate, row.IsUpdateRow())
			require.Equal(t, tc.isTombstone, row.IsTombstoneUpdate())
			if tc.expectedRow != nil {
				require.Equal(t, tc.expectedRow, row.Row)
			}
			if tc.expectedPrevRow != nil {
				require.Equal(t, tc.expectedPrevRow, row.PrevRow)
			}
		})
	}

}

func TestTxnDecoderRejectsTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `CREATE TABLE a (id INT PRIMARY KEY, value STRING)`)

	descA := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name("a"))

	decoder, err := NewTxnDecoder(ctx, s.InternalDB().(descs.DB), s.ClusterSettings(), []TableMapping{
		{SourceDescriptor: descA, DestID: descA.GetID()},
	})
	require.NoError(t, err)

	ebA := NewTestEventBuilder(t, descA.TableDesc())
	txnTime := s.Clock().Now()

	badEvents := []streampb.StreamEvent_KV{
		ebA.InsertEvent(txnTime, tree.Datums{tree.NewDInt(1), tree.NewDString("a")}),
		ebA.InsertEvent(txnTime.Add(1, 0), tree.Datums{tree.NewDInt(2), tree.NewDString("b")}),
	}
	_, err = decoder.DecodeTxn(ctx, badEvents)
	require.ErrorContains(t, err, "inconsistent timestamps")
}
