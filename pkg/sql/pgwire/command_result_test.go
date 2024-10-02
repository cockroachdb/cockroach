// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type fakeConn struct {
	net.Conn
}

func (conn *fakeConn) Read(b []byte) (n int, err error) {
	return len(b), nil
}

func (conn *fakeConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (conn *fakeConn) RemoteAddr() net.Addr {
	return nil
}

func TestTruncateCommandResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestServer()
	const maxBufferSize = 30
	const cmdPos = sql.CmdPos(1)
	c := s.newConn(ctx, cancel, &fakeConn{}, sql.SessionArgs{ConnResultsBufferSize: maxBufferSize}, timeutil.Now())
	stmt, err := parser.ParseOne("select 1")
	require.NoError(t, err)
	cr := c.newCommandResult(
		sql.DontNeedRowDesc,
		cmdPos,
		stmt.AST,
		[]pgwirebase.FormatCode{0},
		sessiondatapb.DataConversionConfig{},
		time.UTC,
		0,    /* limit */
		"",   /* portalName */
		true, /* implicitTxn */
		sql.PortalPausabilityDisabled,
	)
	r := cr.(*commandResult)
	r.SetColumns(ctx, []colinfo.ResultColumn{{Name: "a", Typ: types.Int}})

	// Each row has a 4 byte header and 8 byte value.
	const expectedBytesPerRow = 12
	// Sanity check to make sure test constants are correct. We want to make sure
	// 2 rows fit in the buffer, but 3 do not.
	require.Greater(t, maxBufferSize, 2*expectedBytesPerRow)
	require.Less(t, maxBufferSize, 3*expectedBytesPerRow)

	err = r.AddRow(ctx, []tree.Datum{tree.NewDInt(1)})
	require.NoError(t, err)

	// Verify that we can truncate at the end of the buffer, and it's a no-op.
	bufferedLen := r.BufferedResultsLen()
	require.Equal(t, expectedBytesPerRow, bufferedLen)
	truncated := r.TruncateBufferedResults(bufferedLen)
	require.True(t, truncated)
	truncated = r.TruncateBufferedResults(bufferedLen + 1)
	require.True(t, truncated)

	// The previous truncates should not have actually done anything.
	bufferedLen = r.BufferedResultsLen()
	require.Equal(t, expectedBytesPerRow, bufferedLen)

	// Truncate to the beginning of the buffer.
	truncated = r.TruncateBufferedResults(0)
	require.True(t, truncated)

	// Now the buffer should be empty.
	bufferedLen = r.BufferedResultsLen()
	require.Equal(t, 0, bufferedLen)

	err = r.AddRow(ctx, []tree.Datum{tree.NewDInt(1)})
	require.NoError(t, err)
	err = r.AddRow(ctx, []tree.Datum{tree.NewDInt(1)})
	require.NoError(t, err)

	// If we add a row that causes the buffer to be flushed, then truncating
	// should not be allowed.
	bufferedLen = r.BufferedResultsLen()
	require.Equal(t, 2*expectedBytesPerRow, bufferedLen)
	err = r.AddRow(ctx, []tree.Datum{tree.NewDInt(1)})
	require.NoError(t, err)
	truncated = r.TruncateBufferedResults(bufferedLen)
	require.False(t, truncated)
	require.Equal(t, cmdPos, r.conn.writerState.fi.lastFlushed)

	// The buffer should be empty now, since it was flushed.
	bufferedLen = r.BufferedResultsLen()
	require.Equal(t, 0, bufferedLen)
}
