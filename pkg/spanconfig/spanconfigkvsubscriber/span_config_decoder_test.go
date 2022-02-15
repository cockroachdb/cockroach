// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestSpanConfigDecoder verifies that we can decode rows stored in the
// system.span_configurations table.
func TestSpanConfigDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	const dummyTableName = "dummy_span_configurations"
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyTableName))

	var dummyTableID uint32
	tdb.QueryRow(t, fmt.Sprintf(
		`SELECT table_id FROM crdb_internal.tables WHERE name = '%s'`, dummyTableName),
	).Scan(&dummyTableID)

	getCount := func() int {
		q := tdb.Query(t, fmt.Sprintf(`SELECT count(*) FROM %s`, dummyTableName))
		q.Next()
		var c int
		require.Nil(t, q.Scan(&c))
		require.Nil(t, q.Close())
		return c
	}
	initialCount := getCount()

	key := tc.ScratchRange(t)
	rng := tc.GetFirstStoreFromServer(t, 0).LookupReplica(keys.MustAddr(key))
	span := rng.Desc().RSpan().AsRawSpanWithNoLocals()
	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}

	buf, err := protoutil.Marshal(&conf)
	require.NoError(t, err)
	tdb.Exec(t, fmt.Sprintf(`UPSERT INTO %s (start_key, end_key, config) VALUES ($1, $2, $3)`,
		dummyTableName), span.Key, span.EndKey, buf)
	require.Equal(t, initialCount+1, getCount())

	k := keys.SystemSQLCodec.IndexPrefix(dummyTableID, keys.SpanConfigurationsTablePrimaryKeyIndexID)
	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Len(t, rows, initialCount+1)

	last := rows[len(rows)-1]
	got, err := spanconfigkvsubscriber.TestingDecoderFn()(
		roachpb.KeyValue{
			Key:   last.Key,
			Value: *last.Value,
		},
	)
	require.NoError(t, err)
	require.Truef(t, span.Equal(got.Target.GetSpan()),
		"expected span=%s, got span=%s", span, got.Target.GetSpan())
	require.Truef(t, conf.Equal(got.Config),
		"expected config=%s, got config=%s", conf, got.Config)
}

func BenchmarkSpanConfigDecoder(b *testing.B) {
	defer log.Scope(b).Close(b)

	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{UseDatabase: "bench"})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	const dummyTableName = "dummy_span_configurations"
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(b, `CREATE DATABASE bench`)
	tdb.Exec(b, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyTableName))

	var dummyTableID uint32
	tdb.QueryRow(b, fmt.Sprintf(
		`SELECT table_id from crdb_internal.tables WHERE name = '%s'`, dummyTableName),
	).Scan(&dummyTableID)

	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}
	buf, err := protoutil.Marshal(&conf)
	require.NoError(b, err)

	tdb.Exec(b, fmt.Sprintf(`UPSERT INTO %s (start_key, end_key, config) VALUES ($1, $2, $3)`,
		dummyTableName), roachpb.Key("a"), roachpb.Key("b"), buf)

	k := keys.SystemSQLCodec.IndexPrefix(dummyTableID, keys.SpanConfigurationsTablePrimaryKeyIndexID)
	rows, err := s.DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(b, err)
	last := rows[len(rows)-1]
	decoderFn := spanconfigkvsubscriber.TestingDecoderFn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decoderFn(roachpb.KeyValue{
			Key:   last.Key,
			Value: *last.Value,
		})
	}
}
