// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/jackc/pgproto3/v2"
)

type fakeConn struct {
	pgwirebase.BufferedReader
	rd *bufio.Reader
}

var _ io.Reader = &fakeConn{}
var _ pgwirebase.BufferedReader = &fakeConn{}

// Rd is part of the pgwirebase.Conn interface and returns a reader to be used
// to consume bytes from the connection.
func (c *fakeConn) Rd() pgwirebase.BufferedReader {
	return c
}

// Read is part of io.Reader interface.
func (c *fakeConn) Read(p []byte) (n int, err error) {
	return c.rd.Read(p)
}

// ReadString is part of pgwirebase.BufferedReader interface.
func (c *fakeConn) ReadString(delim byte) (string, error) {
	return c.rd.ReadString(delim)
}

// ReadByte is part of pgwirebase.BufferedReader interface.
func (c *fakeConn) ReadByte() (byte, error) {
	return c.rd.ReadByte()
}

// BeginCopyIn is part of the pgwirebase.Conn interface. Not needed for shim
// purposes.
func (c *fakeConn) BeginCopyIn(
	ctx context.Context, columns []colinfo.ResultColumn, format pgwirebase.FormatCode,
) error {
	return nil
}

// SendCommandComplete is part of the pgwirebase.Conn interface. Not needed for shim
// purposes.
func (c *fakeConn) SendCommandComplete(tag []byte) error {
	return nil
}

// RunCopyFrom exposes copy functionality for the logictest "copy" command, it's
// test-only code but not in test package because logictest isn't in a test package.
func RunCopyFrom(
	ctx context.Context,
	s serverutils.TestServerInterface,
	db string,
	txn *kv.Txn,
	copySQL string,
	data []string,
	copyBatchRowSizeOverride int,
	atomic bool,
) (int, error) {
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	dsp := execCfg.DistSQLPlanner
	stmt, err := parser.ParseOne(copySQL)
	if err != nil {
		return -1, err
	}

	// TODO(cucaroach): test open transaction and implicit txn, this will require
	// a real client side/over the wire copy implementation logictest can use.
	txnOpt := copyTxnOpt{txn: txn}
	txnOpt.resetPlanner = func(ctx context.Context, p *planner, txn *kv.Txn, txnTS time.Time, stmtTS time.Time) {
		p.cancelChecker.Reset(ctx)
		p.optPlanningCtx.init(p)
		p.resetPlanner(ctx, txn, stmtTS, p.sessionDataMutatorIterator.sds.Top())
	}
	p, cleanup := newInternalPlanner("copytest",
		txn,
		username.RootUserName(),
		&MemoryMetrics{},
		&execCfg,
		sessiondatapb.SessionData{
			Database: db,
		},
	)
	// TODO(cucaroach): I believe newInternalPlanner should do this but doing it
	// there causes lots of session diffs and test failures and is risky.
	if err := p.resetAllSessionVars(ctx); err != nil {
		return -1, err
	}
	defer cleanup()

	p.SessionData().CopyFromAtomicEnabled = atomic

	// Write what the client side would write into a buffer and then make it the conn's data.
	var buf []byte
	for _, d := range data {
		b := make([]byte, 0, len(d)+10)
		cd := pgproto3.CopyData{Data: []byte(d)}
		b = cd.Encode(b)
		buf = append(buf, b...)
	}

	done := pgproto3.CopyDone{}
	buf = done.Encode(buf)

	conn := &fakeConn{
		rd: bufio.NewReader(bytes.NewReader(buf)),
	}
	rows := 0
	mon := execinfra.NewTestMemMonitor(ctx, execCfg.Settings)
	c, err := newCopyMachine(ctx, conn, stmt.AST.(*tree.CopyFrom), p, txnOpt, mon,
		func(ctx context.Context, p *planner, res RestrictedCommandResult) error {
			err := dsp.ExecLocalAll(ctx, execCfg, p, res)
			if err != nil {
				return err
			}
			rows += res.RowsAffected()
			return nil
		},
	)
	if err != nil {
		return -1, err
	}
	if copyBatchRowSizeOverride != 0 {
		c.copyBatchRowSize = copyBatchRowSizeOverride
	}

	if err := c.run(ctx); err != nil {
		return -1, err
	}

	return rows, nil
}
