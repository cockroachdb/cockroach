// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc_test

import (
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colenc"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

var lineitemSchema string = `CREATE TABLE lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      DECIMAL(15,2) NOT NULL,
	l_extendedprice DECIMAL(15,2) NOT NULL,
	l_discount      DECIMAL(15,2) NOT NULL,
	l_tax           DECIMAL(15,2) NOT NULL,
	l_returnflag    CHAR(1) NOT NULL,
	l_linestatus    CHAR(1) NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  CHAR(25) NOT NULL,
	l_shipmode      CHAR(10) NOT NULL,
	l_comment       VARCHAR(44) NOT NULL,
	l_dummy         CHAR,
	PRIMARY KEY     (l_orderkey, l_linenumber),
	INDEX l_ok      (l_orderkey ASC),
	INDEX l_pk      (l_partkey ASC),
	INDEX l_sk      (l_suppkey ASC),
	INDEX l_sd      (l_shipdate ASC),
	INDEX l_cd      (l_commitdate ASC),
	INDEX l_rd      (l_receiptdate ASC),
	INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
	INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC))`

var lineitemTypes = []*types.T{
	types.Int,
	types.Int,
	types.Int,
	types.Int,
	types.Decimal,
	types.Decimal,
	types.Decimal,
	types.Decimal,
	types.String,
	types.String,
	types.Date,
	types.Date,
	types.Date,
	types.String,
	types.String,
	types.String,
	types.String,
}

func BenchmarkTCPHLineItem(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	srv, _, kvdb := serverutils.StartServer(b, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	url, cleanup := pgurlutils.PGUrl(b, s.AdvSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, lineitemSchema)
	require.NoError(b, err)
	// Make benchmark stable by using a constant seed.
	rng := randutil.NewTestRandWithSeed(0)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	factory := coldataext.NewExtendedColumnFactory(evalCtx)
	numRows := 1000
	cb := coldata.NewMemBatchWithCapacity(lineitemTypes, numRows, factory)
	for i, t := range lineitemTypes {
		vec := cb.ColVec(i)
		for row := 0; row < numRows; row++ {
			switch t.Family() {
			case types.IntFamily:
				vec.Int64()[row] = int64(randutil.RandIntInRange(rng, 0, 10000))
			case types.DecimalFamily:
				d := randgen.RandDatum(rng, t, false)
				vec.Decimal().Set(row, d.(*tree.DDecimal).Decimal)
			case types.StringFamily:
				l := randutil.RandIntInRange(rng, 10, 20)
				vec.Bytes().Set(row, []byte(randutil.RandString(rng, l, "asdf")))
			case types.DateFamily:
				d := randgen.RandDatum(rng, t, false)
				vec.Int64()[row] = d.(*tree.DDate).UnixEpochDaysWithOrig()
			}
		}
	}
	cb.SetLength(numRows)
	desc := desctestutils.TestingGetTableDescriptor(kvdb, s.Codec(), "defaultdb", "public", "lineitem")
	enc := colenc.MakeEncoder(s.Codec(), desc, &st.SV, cb, desc.PublicColumns(),
		nil /*metrics*/, nil /*partialIndexMap*/, func() error { return nil })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = enc.PrepareBatch(ctx, &noopPutter{}, 0, cb.Length())
	}
	b.StopTimer()
	require.NoError(b, err)
}

type noopPutter struct{}

func (n *noopPutter) CPut(key, value interface{}, expValue []byte) {}
func (n *noopPutter) CPutWithOriginTimestamp(
	key, value interface{}, expValue []byte, ts hlc.Timestamp, shouldWinTie bool,
) {
}
func (n *noopPutter) Put(key, value interface{})                                {}
func (n *noopPutter) PutMustAcquireExclusiveLock(key, value interface{})        {}
func (n *noopPutter) Del(key ...interface{})                                    {}
func (n *noopPutter) CPutBytesEmpty(kys []roachpb.Key, values [][]byte)         {}
func (n *noopPutter) CPutValuesEmpty(kys []roachpb.Key, values []roachpb.Value) {}
func (n *noopPutter) CPutTuplesEmpty(kys []roachpb.Key, values [][]byte)        {}
func (n *noopPutter) PutBytes(kys []roachpb.Key, values [][]byte)               {}
func (n *noopPutter) PutTuples(kys []roachpb.Key, values [][]byte)              {}
