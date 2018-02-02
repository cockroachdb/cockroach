// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgwire

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Test the conn struct: check that it marshalls the correct commands to the
// stmtBuf.
//
// This test is weird because it aims to be a "unit test" for the conn with
// minimal dependencies, but it needs a producer speaking the pgwire protocol
// on the other end of the connection. We use the pgx Postgres driver for this.
// We're going to simulate a client sending various commands to the server. We
// don't have proper execution of those commands in this test, so we synthesize
// responses.
//
// This test depends on recognizing the queries sent by pgx when it opens a
// connection. If that set of queries changes, this test will probably fail
// complaining that the stmtBuf has an unexpected entry in it.
func TestConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The test server is used only incidentally by this test: this is not the
	// server that the client will connect to; we just use it on the side to
	// execute some metadata queries that pgx sends whenever it opens a
	// connection.
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true, UseDatabase: "test"})
	defer s.Stopper().Stop(context.TODO())

	// Start a pgwire "server".
	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverAddr := ln.Addr()

	var g errgroup.Group
	ctx := context.TODO()

	var clientWG sync.WaitGroup
	clientWG.Add(1)

	g.Go(func() error {
		return client(ctx, serverAddr, &clientWG)
	})

	// Wait for the client to connect and perform the handshake.
	conn, err := waitForClientConn(ln)
	if err != nil {
		t.Fatal(err)
	}

	// Run the conn's loop in the background - it will push commands to the
	// buffer.
	g.Go(func() error {
		return conn.serve(ctx, func() bool { return false } /* draining */)
	})

	if err := processPgxStartup(t, ctx, s, conn); err != nil {
		t.Fatal(err)
	}

	// Now we'll expect to receive the commands corresponding to the operations in
	// client().
	rd := sql.MakeStmtBufReader(conn.stmtBuf)
	expectExecStmt(ctx, t, "SELECT 1", &rd, conn, queryStringComplete)
	expectExecStmt(ctx, t, "SELECT 2", &rd, conn, queryStringComplete)
	expectPrepareStmt(ctx, t, "p1", "SELECT 'p1'", &rd, conn)
	expectDescribeStmt(ctx, t, "p1", pgwirebase.PrepareStatement, &rd, conn)
	expectBindStmt(ctx, t, "p1", &rd, conn)
	expectExecPortal(ctx, t, "", &rd, conn)
	// Check that a query string with multiple queries sent using the simple
	// protocol is broken up.
	expectExecStmt(ctx, t, "SELECT 4", &rd, conn, queryStringIncomplete)
	expectExecStmt(ctx, t, "SELECT 5", &rd, conn, queryStringIncomplete)
	expectExecStmt(ctx, t, "SELECT 6", &rd, conn, queryStringComplete)

	// Check that the batching works like the client intended.

	// pgx wraps batchs in transactions.
	expectExecStmt(ctx, t, "BEGIN TRANSACTION", &rd, conn, queryStringComplete)
	expectPrepareStmt(ctx, t, "", "SELECT 7", &rd, conn)
	expectBindStmt(ctx, t, "", &rd, conn)
	expectDescribeStmt(ctx, t, "", pgwirebase.PreparePortal, &rd, conn)
	expectExecPortal(ctx, t, "", &rd, conn)
	expectPrepareStmt(ctx, t, "", "SELECT 8", &rd, conn)
	// Now we'll send an error, in the middle of this batch. pgx will stop waiting
	// for results for commands in the batch. We'll then test that seeking to the
	// next batch advances us to the correct statement.
	if err := finishQuery(generateError, conn); err != nil {
		t.Fatal(err)
	}
	// We're about to seek to the next batch but, as per seek's contract, seeking
	// can only be called when there is something in the buffer. Since the buffer
	// is filled concurrently with this code, we call CurCmd to ensure that
	// there's something in there.
	if _, err := rd.CurCmd(ctx); err != nil {
		t.Fatal(err)
	}
	// Skip all the remaining messages in the batch.
	if err := rd.SeekToNextBatch(ctx); err != nil {
		t.Fatal(err)
	}
	// We got to the COMMIT that pgx pushed to match the BEGIN it generated for
	// the batch.
	expectExecStmt(ctx, t, "COMMIT TRANSACTION", &rd, conn, queryStringComplete)
	expectExecStmt(ctx, t, "SELECT 9", &rd, conn, queryStringComplete)

	// Test that parse error turns into SendError.
	expectSendError(ctx, t, pgerror.CodeSyntaxError, &rd, conn)

	clientWG.Done()

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// processPgxStartup processes the first few queries that the pgx driver
// automatically sends on a new connection that has been established.
func processPgxStartup(
	t *testing.T, ctx context.Context, s serverutils.TestServerInterface, c *conn,
) error {
	rd := sql.MakeStmtBufReader(c.stmtBuf)

	for {
		cmd, err := rd.CurCmd(ctx)
		if err != nil {
			return err
		}

		exec, ok := cmd.(sql.ExecStmt)
		if !ok {
			return nil
		}
		query := exec.Stmt.String()
		if !strings.HasPrefix(query, "SELECT t.oid") {
			return nil
		}
		if err := execQuery(ctx, query, s, c); err != nil {
			return err
		}
		rd.AdvanceOne(ctx)
	}
}

// execQuery executes a query on the passed-in server and send the results on c.
func execQuery(
	ctx context.Context, query string, s serverutils.TestServerInterface, c *conn,
) error {
	rows, cols, err := s.InternalExecutor().(sqlutil.InternalExecutor).QueryRows(
		ctx, "pgx init" /* opName */, query,
	)
	if err != nil {
		return err
	}
	return sendResult(ctx, c, cols, rows)
}

func client(ctx context.Context, serverAddr net.Addr, wg *sync.WaitGroup) error {
	host, ports, err := net.SplitHostPort(serverAddr.String())
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(ports)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(
		pgx.ConnConfig{
			Host: host,
			Port: uint16(port),
			User: "root",
			// Setting this so that the queries sent by pgx to initialize the
			// connection are not using prepared statements. That simplifies the
			// scaffolding of the test.
			PreferSimpleProtocol: true,
		})
	if err != nil {
		return err
	}
	conn.SetLogger(pgxTestLogger{})

	if _, err := conn.Exec("select 1"); err != nil {
		return err
	}
	if _, err := conn.Exec("select 2"); err != nil {
		return err
	}
	if _, err := conn.Prepare("p1", "select 'p1'"); err != nil {
		return err
	}
	if _, err := conn.ExecEx(
		ctx, "p1",
		// We set these options because apparently that's how I tell pgx that it
		// should check whether "p1" is a prepared statement.
		&pgx.QueryExOptions{SimpleProtocol: false}); err != nil {
		return err
	}

	// Send a group of statements as one query string using the simple protocol.
	// We'll check that we receive them one by one, but marked as a batch.
	if _, err := conn.Exec("select 4; select 5; select 6;"); err != nil {
		return err
	}

	batch := conn.BeginBatch()
	batch.Queue("select 7", nil, nil, nil)
	batch.Queue("select 8", nil, nil, nil)
	if err := batch.Send(context.TODO(), &pgx.TxOptions{}); err != nil {
		return err
	}
	if err := batch.Close(); err != nil {
		// Swallow the error that we injected.
		if !strings.Contains(err.Error(), "injected") {
			return err
		}
	}

	if _, err := conn.Exec("select 9"); err != nil {
		return err
	}
	if _, err := conn.Exec("bogus statement failing to parse"); err != nil {
		return err
	}

	wg.Wait()

	return conn.Close()
}

// waitForClientConn blocks until a client connects and performs the pgwire
// handshake. This emulates what pgwire.Server does.
func waitForClientConn(ln net.Listener) (*conn, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}

	var buf pgwirebase.ReadBuffer
	_, err = buf.ReadUntypedMsg(conn)
	if err != nil {
		return nil, err
	}
	version, err := buf.GetUint32()
	if err != nil {
		return nil, err
	}
	if version != version30 {
		return nil, errors.Errorf("unexpected protocol version: %d", version)
	}

	// Consumer the connection options.
	if _, err := parseOptions(context.TODO(), buf.Msg); err != nil {
		return nil, err
	}

	metrics := makeServerMetrics(nil /* internalMemMetrics */, metric.TestSampleInterval)
	pgwireConn := newConn(conn, &metrics)
	return pgwireConn, nil
}

// sendResult serializes a set of rows in pgwire format and sends them on a
// connection.
//
// TODO(andrei): Tests using this should probably switch to using the similar
// routines in the connection once conn learns how to write rows.
func sendResult(
	ctx context.Context, c *conn, cols sqlbase.ResultColumns, rows []tree.Datums,
) error {
	if err := c.sendRowDescription(ctx, cols, nil /* formatCodes */, &c.wr); err != nil {
		return err
	}

	for _, row := range rows {
		c.writeBuf.initMsg(pgwirebase.ServerMsgDataRow)
		c.writeBuf.putInt16(int16(len(row)))
		for _, col := range row {
			c.writeBuf.writeTextDatum(ctx, col, time.UTC /* sessionLoc */)
		}

		if err := c.writeBuf.finishMsg(&c.wr); err != nil {
			return err
		}
	}

	return finishQuery(execute, c)
}

type executeType int

const (
	queryStringComplete executeType = iota
	queryStringIncomplete
)

func expectExecStmt(
	ctx context.Context, t *testing.T, expSQL string, rd *sql.StmtBufReader, c *conn, typ executeType,
) {
	cmd, err := rd.CurCmd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne(ctx)

	es, ok := cmd.(sql.ExecStmt)
	if !ok {
		t.Fatalf("%s: expected command ExecStmt, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}

	if es.Stmt.String() != expSQL {
		t.Fatalf("%s: expected %s, got %s", testutils.Caller(1), expSQL, es.Stmt.String())
	}

	if es.ParseStart == (time.Time{}) {
		t.Fatalf("%s: ParseStart not filled in", testutils.Caller(1))
	}
	if es.ParseEnd == (time.Time{}) {
		t.Fatalf("%s: ParseEnd not filled in", testutils.Caller(1))
	}
	if typ == queryStringComplete {
		if err := finishQuery(execute, c); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := finishQuery(commandComplete, c); err != nil {
			t.Fatal(err)
		}
	}
}

func expectPrepareStmt(
	ctx context.Context, t *testing.T, expName string, expSQL string, rd *sql.StmtBufReader, c *conn,
) {
	cmd, err := rd.CurCmd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne(ctx)

	pr, ok := cmd.(sql.PrepareStmt)
	if !ok {
		t.Fatalf("%s: expected command PrepareStmt, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}

	if pr.Name != expName {
		t.Fatalf("%s: expected name %s, got %s", testutils.Caller(1), expName, pr.Name)
	}

	if pr.Stmt.String() != expSQL {
		t.Fatalf("%s: expected %s, got %s", testutils.Caller(1), expSQL, pr.Stmt.String())
	}

	if err := finishQuery(prepare, c); err != nil {
		t.Fatal(err)
	}
}

func expectDescribeStmt(
	ctx context.Context,
	t *testing.T,
	expName string,
	expType pgwirebase.PrepareType,
	rd *sql.StmtBufReader,
	c *conn,
) {
	cmd, err := rd.CurCmd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne(ctx)

	desc, ok := cmd.(sql.DescribeStmt)
	if !ok {
		t.Fatalf("%s: expected command DescribeStmt, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}

	if desc.Name != expName {
		t.Fatalf("%s: expected name %s, got %s", testutils.Caller(1), expName, desc.Name)
	}

	if desc.Type != expType {
		t.Fatalf("%s: expected type %s, got %s", testutils.Caller(1), expType, desc.Type)
	}

	if err := finishQuery(describe, c); err != nil {
		t.Fatal(err)
	}
}

func expectBindStmt(
	ctx context.Context, t *testing.T, expName string, rd *sql.StmtBufReader, c *conn,
) {
	cmd, err := rd.CurCmd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne(ctx)

	bd, ok := cmd.(sql.BindStmt)
	if !ok {
		t.Fatalf("%s: expected command BindStmt, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}

	if bd.PreparedStatementName != expName {
		t.Fatalf("%s: expected name %s, got %s", testutils.Caller(1), expName, bd.PreparedStatementName)
	}

	if err := finishQuery(bind, c); err != nil {
		t.Fatal(err)
	}
}

func expectExecPortal(
	ctx context.Context, t *testing.T, expName string, rd *sql.StmtBufReader, c *conn,
) {
	cmd, err := rd.CurCmd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne(ctx)

	ep, ok := cmd.(sql.ExecPortal)
	if !ok {
		t.Fatalf("%s: expected command ExecPortal, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}

	if ep.Name != expName {
		t.Fatalf("%s: expected name %s, got %s", testutils.Caller(1), expName, ep.Name)
	}

	if err := finishQuery(execPortal, c); err != nil {
		t.Fatal(err)
	}
}

func expectSendError(
	ctx context.Context, t *testing.T, pgErrCode string, rd *sql.StmtBufReader, c *conn,
) {
	cmd, err := rd.CurCmd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne(ctx)

	se, ok := cmd.(sql.SendError)
	if !ok {
		t.Fatalf("%s: expected command SendError, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}

	pg, ok := se.Err.(*pgerror.Error)
	if !ok {
		t.Fatalf("expected pgerror, got %T (%s)", se.Err, se.Err)
	}
	if pg.Code != pgErrCode {
		t.Fatalf("expected code %s, got: %s", pgErrCode, pg.Code)
	}

	if err := finishQuery(execPortal, c); err != nil {
		t.Fatal(err)
	}
}

type finishType int

const (
	execute finishType = iota
	// commandComplete is like execute, except that it marks the completion of a
	// query in a larger query string and so no ReadyForQuery message should be
	// sent.
	commandComplete
	prepare
	bind
	describe
	execPortal
	generateError
)

// Send a CommandComplete/ReadyForQuery to signal that the rows are done.
func finishQuery(t finishType, c *conn) error {
	switch t {
	case execPortal:
		fallthrough
	case commandComplete:
		fallthrough
	case execute:
		c.writeBuf.initMsg(pgwirebase.ServerMsgCommandComplete)
		// HACK: This message is supposed to contains a command tag but this test is
		// not sure about how to produce one and it works without it.
		c.writeBuf.nullTerminate()
	case prepare:
		// pgx doesn't send a Sync in between prepare (Parse protocol message) and
		// the subsequent Describe, so we're not going to send a ReadyForQuery
		// below.
		c.writeBuf.initMsg(pgwirebase.ServerMsgParseComplete)
	case describe:
		if err := c.sendRowDescription(
			context.TODO(), nil /* columns */, nil /* formatCodes */, &c.wr,
		); err != nil {
			return err
		}
	case bind:
		// pgx doesn't send a Sync mesage in between Bind and Execute, so we're not
		// going to send a ReadyForQuery below.
		c.writeBuf.initMsg(pgwirebase.ServerMsgBindComplete)
	case generateError:
		c.writeBuf.initMsg(pgwirebase.ServerMsgErrorResponse)
		c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
		c.writeBuf.writeTerminatedString("ERROR")
		c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldMsgPrimary)
		c.writeBuf.writeTerminatedString("injected")
		c.writeBuf.nullTerminate()
		if err := c.writeBuf.finishMsg(&c.wr); err != nil {
			return err
		}
	}

	if err := c.writeBuf.finishMsg(&c.wr); err != nil {
		return err
	}

	if t != commandComplete && t != bind && t != prepare {
		c.writeBuf.initMsg(pgwirebase.ServerMsgReady)
		c.writeBuf.writeByte('I') // transaction status: no txn
		if err := c.writeBuf.finishMsg(&c.wr); err != nil {
			return err
		}
	}
	return c.wr.Flush()
}

type pgxTestLogger struct{}

func (l pgxTestLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	log.Infof(context.TODO(), "pgx log [%s] %s - %s", level, msg, data)
}

// pgxTestLogger implements pgx.Logger.
var _ pgx.Logger = pgxTestLogger{}
