// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package copy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// BenchmarkCopyFrom measures copy performance against a TestServer.
func BenchmarkCopyFrom(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(b, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, lineitemSchema)
	require.NoError(b, err)

	err = conn.Exec(ctx, "SET copy_from_atomic_enabled = false")
	require.NoError(b, err)

	// send data in 5 batches of 10k rows
	const ROWS = sql.CopyBatchRowSizeDefault * 20
	datalen := 0
	var rows []string
	for i := 0; i < ROWS; i++ {
		row := fmt.Sprintf(csvData, i)
		rows = append(rows, row)
		datalen += len(row)
	}
	rowsize := datalen / ROWS
	for _, batchSizeFactor := range []float64{.5, 1, 2, 4} {
		batchSize := int(batchSizeFactor * sql.CopyBatchRowSizeDefault)
		b.Run(fmt.Sprintf("%d", batchSize), func(b *testing.B) {
			actualRows := rows[:batchSize]
			for i := 0; i < b.N; i++ {
				pprof.Do(ctx, pprof.Labels("run", "copy"), func(ctx context.Context) {
					numrows, err := conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(strings.Join(actualRows, "\n")), "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';")
					require.NoError(b, err)
					require.Equal(b, int(numrows), len(actualRows))
				})
				b.StopTimer()
				err = conn.Exec(ctx, "TRUNCATE TABLE lineitem")
				require.NoError(b, err)
				b.StartTimer()
			}
			b.SetBytes(int64(len(actualRows) * rowsize))
		})
	}
}

// TODO(cucaroach): get the rand utilities and ParseAndRequire to be friends
// STRINGS don't roundtrip well, need to figure out proper escaping
// INET doesn't round trip: ERROR: could not parse "70e5:112:5114:7da5:1" as inet. invalid IP (SQLSTATE 22P02)
// DECIMAL(15,2) don't round trip, get too big number errors.
const lineitemSchemaMunged string = `CREATE TABLE lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      INT8 NOT NULL,
	l_extendedprice FLOAT NOT NULL,
	l_discount      FLOAT NOT NULL,
	l_tax           FLOAT NOT NULL,
	l_returnflag    TIMESTAMPTZ NOT NULL,
	l_linestatus    TIMESTAMPTZ NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  INTERVAL NOT NULL,
	l_shipmode      UUID NOT NULL,
	l_comment       UUID NOT NULL,
	PRIMARY KEY     (l_orderkey, l_linenumber),
	INDEX l_ok      (l_orderkey ASC),
	INDEX l_pk      (l_partkey ASC),
	INDEX l_sk      (l_suppkey ASC),
	INDEX l_sd      (l_shipdate ASC),
	INDEX l_cd      (l_commitdate ASC),
	INDEX l_rd      (l_receiptdate ASC),
	INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
	INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
)`

// Perform a copy
func BenchmarkLargeCopy(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, _, kvdb := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(b, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, lineitemSchemaMunged)
	require.NoError(b, err)

	desc := desctestutils.TestingGetTableDescriptor(kvdb, keys.SystemSQLCodec, "defaultdb", "public", "lineitem")
	require.NotNil(b, desc, "Failed to lookup descriptor")

	err = conn.Exec(ctx, "SET copy_from_atomic_enabled = false")
	require.NoError(b, err)

	rng := rand.New(rand.NewSource(0))
	rows := 10000
	numrows, err := conn.GetDriverConn().CopyFrom(ctx,
		&copyReader{rng: rng, cols: desc.PublicColumns(), rows: rows},
		"COPY lineitem FROM STDIN WITH CSV;")
	require.NoError(b, err)
	require.Equal(b, int(numrows), rows)
}

type copyReader struct {
	cols          []catalog.Column
	rng           *rand.Rand
	rows          int
	count         int
	generatedRows io.Reader
}

var _ io.Reader = &copyReader{}

func (c *copyReader) Read(b []byte) (n int, err error) {
	if c.generatedRows == nil {
		c.generateRows()
	}
	n, err = c.generatedRows.Read(b)
	if err == io.EOF && c.count < c.rows {
		c.generateRows()
		n, err = c.generatedRows.Read(b)
	}
	return
}

func (c *copyReader) generateRows() {
	numRows := min(1000, c.rows-c.count)
	sb := strings.Builder{}
	for i := 0; i < numRows; i++ {
		row := make([]string, len(c.cols))
		for j, col := range c.cols {
			t := col.GetType()
			var ds string
			if j == 0 {
				// Special handling for ID field
				ds = strconv.Itoa(c.count + i)
			} else {
				d := randgen.RandDatum(c.rng, t, col.IsNullable())
				ds = tree.AsStringWithFlags(d, tree.FmtBareStrings)
				// Empty string is treated as null
				if len(ds) == 0 && !col.IsNullable() {
					ds = "a"
				}
				switch t.Family() {
				case types.CollatedStringFamily:
					// For collated strings, we just want the raw contents in COPY.
					ds = d.(*tree.DCollatedString).Contents
				case types.FloatFamily:
					ds = strings.TrimSuffix(ds, ".0")
				}
				switch t.Family() {
				case types.BytesFamily,
					types.DateFamily,
					types.IntervalFamily,
					types.INetFamily,
					types.StringFamily,
					types.TimestampFamily,
					types.TimestampTZFamily,
					types.UuidFamily,
					types.CollatedStringFamily:
					var b bytes.Buffer
					if err := sql.EncodeCopy(&b, encoding.UnsafeConvertStringToBytes(ds), ','); err != nil {
						panic(err)
					}
					ds = b.String()
				}
			}
			row[j] = ds
		}
		r := strings.Join(row, ",")
		sb.WriteString(r)
		sb.WriteString("\n")
	}
	c.count += numRows
	c.generatedRows = strings.NewReader(sb.String())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
