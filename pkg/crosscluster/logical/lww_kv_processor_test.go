// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestKVWriterUpdateEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(sqlDB)

	tableWithNullableColumns := `CREATE TABLE %s (pk INT PRIMARY KEY, payload1 STRING NULL, payload2 STRING NULL)`

	tableNumber := 0
	createTable := func(t *testing.T, schema string) string {
		tableName := fmt.Sprintf("tab%d", tableNumber)
		runner.Exec(t, fmt.Sprintf(schema, tableName))
		tableNumber++
		return tableName
	}

	type encoderFn func(datums ...interface{}) roachpb.KeyValue

	setup := func(t *testing.T, schema string) (string, BatchHandler, encoderFn) {
		tableName := createTable(t, schema)
		desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableName)
		sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
		rp, err := newKVRowProcessor(ctx,
			&execinfra.ServerConfig{
				DB:           s.InternalDB().(descs.DB),
				LeaseManager: s.LeaseManager(),
				Settings:     s.ClusterSettings(),
			}, &eval.Context{
				Codec:    s.Codec(),
				Settings: s.ClusterSettings(),
			}, sd, execinfrapb.LogicalReplicationWriterSpec{}, map[descpb.ID]sqlProcessorTableConfig{
				desc.GetID(): {
					srcDesc: desc,
				},
			})
		require.NoError(t, err)
		return tableName, rp, func(datums ...interface{}) roachpb.KeyValue {
			kv := replicationtestutils.EncodeKV(t, s.Codec(), desc, datums...)
			kv.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			return kv
		}
	}

	insertRow := func(rp BatchHandler, keyValue roachpb.KeyValue, prevValue roachpb.Value) error {
		_, err := rp.HandleBatch(ctx, []streampb.StreamEvent_KV{{KeyValue: keyValue, PrevValue: prevValue}})
		return err
	}

	rng, _ := randutil.NewTestRand()
	useRandomPrevValue := rng.Float32() < 0.5
	t.Logf("using random previous values = %v", useRandomPrevValue)

	maybeRandomPrevValue := func(expectedPrev roachpb.KeyValue, encoder encoderFn) roachpb.KeyValue {
		if useRandomPrevValue {
			return encoder(1, fmt.Sprintf("rand-%d", rng.Int63()), fmt.Sprintf("rand-%d", rng.Int63()))
		}
		return expectedPrev
	}

	t.Run("one-NULL-to-not-NULL", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)

		runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, NULL)", tableNameDst), 1, "not null")

		keyValue1 := encoder(1, "not null", "not null")
		keyValue2 := maybeRandomPrevValue(encoder(1, "not null", tree.DNull), encoder)
		require.NoError(t, insertRow(rp, keyValue1, keyValue2.Value))
		expectedRows := [][]string{
			{"1", "not null", "not null"},
		}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
	t.Run("one-not-NULL-to-NULL", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)

		runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", tableNameDst), 1, "not null", "not null")

		keyValue1 := encoder(1, "not null", tree.DNull)
		keyValue2 := maybeRandomPrevValue(encoder(1, "not null", "not null"), encoder)
		require.NoError(t, insertRow(rp, keyValue1, keyValue2.Value))
		expectedRows := [][]string{
			{"1", "not null", "NULL"},
		}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
	t.Run("all-NULL-to-not-NULL", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)

		runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, NULL, NULL)", tableNameDst), 1)

		keyValue1 := encoder(1, "not null", "not null")
		keyValue2 := maybeRandomPrevValue(encoder(1, tree.DNull, tree.DNull), encoder)
		require.NoError(t, insertRow(rp, keyValue1, keyValue2.Value))
		expectedRows := [][]string{
			{"1", "not null", "not null"},
		}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
	t.Run("all-not-NULL-to-NULL", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)

		runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", tableNameDst), 1, "not null", "not null")

		keyValue1 := encoder(1, tree.DNull, tree.DNull)
		keyValue2 := maybeRandomPrevValue(encoder(1, "not null", "not null"), encoder)
		require.NoError(t, insertRow(rp, keyValue1, keyValue2.Value))
		expectedRows := [][]string{
			{"1", "NULL", "NULL"},
		}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
	t.Run("deleted-one-NULL", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)
		runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, NULL)", tableNameDst), 1, "not null")

		keyValue1 := encoder(1, "not null", tree.DNull)
		keyValue1.Value.RawBytes = nil
		keyValue2 := maybeRandomPrevValue(encoder(1, "not null", tree.DNull), encoder)
		require.NoError(t, insertRow(rp, keyValue1, keyValue2.Value))

		expectedRows := [][]string{}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
	t.Run("deleted-all-NULL", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)
		runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, NULL, NULL)", tableNameDst), 1)

		keyValue1 := maybeRandomPrevValue(encoder(1, tree.DNull, tree.DNull), encoder)
		keyValue1.Value.RawBytes = nil
		keyValue2 := encoder(1, tree.DNull, tree.DNull)
		require.NoError(t, insertRow(rp, keyValue1, keyValue2.Value))

		expectedRows := [][]string{}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
	t.Run("phantom-delete", func(t *testing.T) {
		tableNameDst, rp, encoder := setup(t, tableWithNullableColumns)

		keyValue1 := encoder(1, tree.DNull, tree.DNull)
		keyValue1.Value.RawBytes = nil
		require.NoError(t, insertRow(rp, keyValue1, keyValue1.Value))

		expectedRows := [][]string{}
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
	})
}
