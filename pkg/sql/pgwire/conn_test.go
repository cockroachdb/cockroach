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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true, UseDatabase: "system"})
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
	serveCtx, stopServe := context.WithCancel(ctx)
	g.Go(func() error {
		return conn.serveImpl(
			serveCtx,
			func() bool { return false }, /* draining */
			// sqlServer - nil means don't create a command processor and a write side of the conn
			nil,
			mon.BoundAccount{}, /* reserved */
			s.Stopper())
	})
	defer stopServe()

	if err := processPgxStartup(ctx, s, conn); err != nil {
		t.Fatal(err)
	}

	// Now we'll expect to receive the commands corresponding to the operations in
	// client().
	rd := sql.MakeStmtBufReader(conn.stmtBuf)
	expectExecStmt(ctx, t, "SELECT 1", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)
	expectExecStmt(ctx, t, "SELECT 2", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)
	expectPrepareStmt(ctx, t, "p1", "SELECT 'p1'", &rd, conn)
	expectDescribeStmt(ctx, t, "p1", pgwirebase.PrepareStatement, &rd, conn)
	expectSync(ctx, t, &rd)
	expectBindStmt(ctx, t, "p1", &rd, conn)
	expectExecPortal(ctx, t, "", &rd, conn)
	// Check that a query string with multiple queries sent using the simple
	// protocol is broken up.
	expectSync(ctx, t, &rd)
	expectExecStmt(ctx, t, "SELECT 4", &rd, conn, queryStringIncomplete)
	expectExecStmt(ctx, t, "SELECT 5", &rd, conn, queryStringIncomplete)
	expectExecStmt(ctx, t, "SELECT 6", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)

	// Check that the batching works like the client intended.

	// pgx wraps batchs in transactions.
	expectExecStmt(ctx, t, "BEGIN TRANSACTION", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)
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
	if _, err := rd.CurCmd(); err != nil {
		t.Fatal(err)
	}
	// Skip all the remaining messages in the batch.
	if err := rd.SeekToNextBatch(); err != nil {
		t.Fatal(err)
	}
	// We got to the COMMIT that pgx pushed to match the BEGIN it generated for
	// the batch.
	expectSync(ctx, t, &rd)
	expectExecStmt(ctx, t, "COMMIT TRANSACTION", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)
	expectExecStmt(ctx, t, "SELECT 9", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)

	// Test that parse error turns into SendError.
	expectSendError(ctx, t, pgerror.CodeSyntaxError, &rd, conn)

	clientWG.Done()

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// processPgxStartup processes the first few queries that the pgx driver
// automatically sends on a new connection that has been established.
func processPgxStartup(ctx context.Context, s serverutils.TestServerInterface, c *conn) error {
	rd := sql.MakeStmtBufReader(c.stmtBuf)

	for {
		cmd, err := rd.CurCmd()
		if err != nil {
			return err
		}

		if _, ok := cmd.(sql.Sync); ok {
			rd.AdvanceOne()
			continue
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
		rd.AdvanceOne()
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
			Database:             "system",
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
	pgwireConn := newConn(conn, sql.SessionArgs{}, &metrics, &sql.ExecutorConfig{})
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
	if err := c.writeRowDescription(ctx, cols, nil /* formatCodes */, c.conn); err != nil {
		return err
	}

	for _, row := range rows {
		c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
		c.msgBuilder.putInt16(int16(len(row)))
		for _, col := range row {
			c.msgBuilder.writeTextDatum(ctx, col, time.UTC /* sessionLoc */)
		}

		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
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
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

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
		if err := finishQuery(cmdComplete, c); err != nil {
			t.Fatal(err)
		}
	}
}

func expectPrepareStmt(
	ctx context.Context, t *testing.T, expName string, expSQL string, rd *sql.StmtBufReader, c *conn,
) {
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

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
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

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
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

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

func expectSync(ctx context.Context, t *testing.T, rd *sql.StmtBufReader) {
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	_, ok := cmd.(sql.Sync)
	if !ok {
		t.Fatalf("%s: expected command Sync, got: %T (%+v)", testutils.Caller(1), cmd, cmd)
	}
}

func expectExecPortal(
	ctx context.Context, t *testing.T, expName string, rd *sql.StmtBufReader, c *conn,
) {
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

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
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

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
	// cmdComplete is like execute, except that it marks the completion of a query
	// in a larger query string and so no ReadyForQuery message should be sent.
	cmdComplete
	prepare
	bind
	describe
	execPortal
	generateError
)

// Send a CommandComplete/ReadyForQuery to signal that the rows are done.
func finishQuery(t finishType, c *conn) error {
	var skipFinish bool

	switch t {
	case execPortal:
		fallthrough
	case cmdComplete:
		fallthrough
	case execute:
		c.msgBuilder.initMsg(pgwirebase.ServerMsgCommandComplete)
		// HACK: This message is supposed to contains a command tag but this test is
		// not sure about how to produce one and it works without it.
		c.msgBuilder.nullTerminate()
	case prepare:
		// pgx doesn't send a Sync in between prepare (Parse protocol message) and
		// the subsequent Describe, so we're not going to send a ReadyForQuery
		// below.
		c.msgBuilder.initMsg(pgwirebase.ServerMsgParseComplete)
	case describe:
		skipFinish = true
		if err := c.writeRowDescription(
			context.TODO(), nil /* columns */, nil /* formatCodes */, c.conn,
		); err != nil {
			return err
		}
	case bind:
		// pgx doesn't send a Sync mesage in between Bind and Execute, so we're not
		// going to send a ReadyForQuery below.
		c.msgBuilder.initMsg(pgwirebase.ServerMsgBindComplete)
	case generateError:
		c.msgBuilder.initMsg(pgwirebase.ServerMsgErrorResponse)
		c.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
		c.msgBuilder.writeTerminatedString("ERROR")
		c.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldMsgPrimary)
		c.msgBuilder.writeTerminatedString("injected")
		c.msgBuilder.nullTerminate()
		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
			return err
		}
	}

	if !skipFinish {
		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
			return err
		}
	}

	if t != cmdComplete && t != bind && t != prepare {
		c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
		c.msgBuilder.writeByte('I') // transaction status: no txn
		if err := c.msgBuilder.finishMsg(c.conn); err != nil {
			return err
		}
	}
	return nil
}

type pgxTestLogger struct{}

func (l pgxTestLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	log.Infof(context.TODO(), "pgx log [%s] %s - %s", level, msg, data)
}

// pgxTestLogger implements pgx.Logger.
var _ pgx.Logger = pgxTestLogger{}

// Test that closing a client connection such that producing results rows
// encounters network errors doesn't crash the server (#23694).
//
// We'll run a query that produces a bunch of rows and close the connection as
// soon as the client received anything, this way ensuring that:
// a) the query started executing when the connection is closed, and so it's
// likely to observe a network error and not a context cancelation, and
// b) the connection's server-side results buffer has overflowed, and so
// attempting to produce results (through CommandResult.AddRow()) observes
// network errors.
func TestConnClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Make the connections' results buffers really small so that it overflows
		// when we produce a few results.
		ConnResultsBufferBytes: 10,
		// Andrei is too lazy to figure out the incantation for telling pgx about
		// our test certs.
		Insecure: true,
	})
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE DATABASE test")
	r.Exec(t, "CREATE TABLE test.test AS SELECT * FROM generate_series(1,100)")

	host, ports, err := net.SplitHostPort(s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(ports)
	if err != nil {
		t.Fatal(err)
	}
	// We test both with and without DistSQL, as the way that network errors are
	// observed depends on the engine.
	testutils.RunTrueAndFalse(t, "useDistSQL", func(t *testing.T, useDistSQL bool) {
		conn, err := pgx.Connect(pgx.ConnConfig{
			Host:     host,
			Port:     uint16(port),
			User:     "root",
			Database: "system",
		})
		if err != nil {
			t.Fatal(err)
		}
		var query string
		if useDistSQL {
			query = `SET DISTSQL = 'always'`
		} else {
			query = `SET DISTSQL = 'off'`
		}
		if _, err := conn.Exec(query); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query("SELECT * FROM test.test")
		if err != nil {
			t.Fatal(err)
		}
		if hasResults := rows.Next(); !hasResults {
			t.Fatal("expected results")
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
