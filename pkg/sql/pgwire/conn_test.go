// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgproto3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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
	defer s.Stopper().Stop(context.Background())

	// Start a pgwire "server".
	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverAddr := ln.Addr()
	log.Infof(context.Background(), "started listener on %s", serverAddr)

	var g errgroup.Group
	ctx := context.Background()

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
		conn.serveImpl(
			serveCtx,
			func() bool { return false }, /* draining */
			// sqlServer - nil means don't create a command processor and a write side of the conn
			nil,
			mon.BoundAccount{}, /* reserved */
			authOptions{testingSkipAuth: true},
			s.Stopper())
		return nil
	})
	defer stopServe()

	if err := processPgxStartup(ctx, s, conn); err != nil {
		t.Fatal(err)
	}

	// Now we'll expect to receive the commands corresponding to the operations in
	// client().
	rd := sql.MakeStmtBufReader(&conn.stmtBuf)
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
	expectSendError(ctx, t, pgcode.Syntax, &rd, conn)

	clientWG.Done()

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// processPgxStartup processes the first few queries that the pgx driver
// automatically sends on a new connection that has been established.
func processPgxStartup(ctx context.Context, s serverutils.TestServerInterface, c *conn) error {
	rd := sql.MakeStmtBufReader(&c.stmtBuf)

	for {
		cmd, err := rd.CurCmd()
		if err != nil {
			log.Errorf(ctx, "CurCmd error: %v", err)
			return err
		}

		if _, ok := cmd.(sql.Sync); ok {
			log.Infof(ctx, "advancing Sync")
			rd.AdvanceOne()
			continue
		}

		exec, ok := cmd.(sql.ExecStmt)
		if !ok {
			log.Infof(ctx, "stop wait at: %v", cmd)
			return nil
		}
		query := exec.AST.String()
		if !strings.HasPrefix(query, "SELECT t.oid") {
			log.Infof(ctx, "stop wait at query: %s", query)
			return nil
		}
		if err := execQuery(ctx, query, s, c); err != nil {
			log.Errorf(ctx, "execQuery %s error: %v", query, err)
			return err
		}
		log.Infof(ctx, "executed query: %s", query)
		rd.AdvanceOne()
	}
}

// execQuery executes a query on the passed-in server and send the results on c.
func execQuery(
	ctx context.Context, query string, s serverutils.TestServerInterface, c *conn,
) error {
	rows, cols, err := s.InternalExecutor().(sqlutil.InternalExecutor).QueryWithCols(
		ctx, "test", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser, Database: "system"},
		query,
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
			Logger: pgxTestLogger{},
			Host:   host,
			Port:   uint16(port),
			User:   "root",
			// Setting this so that the queries sent by pgx to initialize the
			// connection are not using prepared statements. That simplifies the
			// scaffolding of the test.
			PreferSimpleProtocol: true,
			Database:             "system",
		})
	if err != nil {
		return err
	}

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
	if err := batch.Send(context.Background(), &pgx.TxOptions{}); err != nil {
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

	// Consume the connection options.
	if _, err := parseClientProvidedSessionParameters(context.Background(), nil, &buf); err != nil {
		return nil, err
	}

	metrics := makeServerMetrics(sql.MemoryMetrics{} /* sqlMemMetrics */, metric.TestSampleInterval)
	pgwireConn := newConn(conn, sql.SessionArgs{ConnResultsBufferSize: 16 << 10}, &metrics, nil)
	return pgwireConn, nil
}

func makeTestingConvCfg() sessiondata.DataConversionConfig {
	return sessiondata.DataConversionConfig{
		Location:          time.UTC,
		BytesEncodeFormat: lex.BytesEncodeHex,
	}
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

	defaultConv := makeTestingConvCfg()
	for _, row := range rows {
		c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
		c.msgBuilder.putInt16(int16(len(row)))
		for i, col := range row {
			c.msgBuilder.writeTextDatum(ctx, col, defaultConv, cols[i].Typ)
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
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	es, ok := cmd.(sql.ExecStmt)
	if !ok {
		t.Fatalf("expected command ExecStmt, got: %T (%+v)", cmd, cmd)
	}

	if es.AST.String() != expSQL {
		t.Fatalf("expected %s, got %s", expSQL, es.AST.String())
	}

	if es.ParseStart == (time.Time{}) {
		t.Fatalf("ParseStart not filled in")
	}
	if es.ParseEnd == (time.Time{}) {
		t.Fatalf("ParseEnd not filled in")
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
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	pr, ok := cmd.(sql.PrepareStmt)
	if !ok {
		t.Fatalf("expected command PrepareStmt, got: %T (%+v)", cmd, cmd)
	}

	if pr.Name != expName {
		t.Fatalf("expected name %s, got %s", expName, pr.Name)
	}

	if pr.AST.String() != expSQL {
		t.Fatalf("expected %s, got %s", expSQL, pr.AST.String())
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
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	desc, ok := cmd.(sql.DescribeStmt)
	if !ok {
		t.Fatalf("expected command DescribeStmt, got: %T (%+v)", cmd, cmd)
	}

	if desc.Name != expName {
		t.Fatalf("expected name %s, got %s", expName, desc.Name)
	}

	if desc.Type != expType {
		t.Fatalf("expected type %s, got %s", expType, desc.Type)
	}

	if err := finishQuery(describe, c); err != nil {
		t.Fatal(err)
	}
}

func expectBindStmt(
	ctx context.Context, t *testing.T, expName string, rd *sql.StmtBufReader, c *conn,
) {
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	bd, ok := cmd.(sql.BindStmt)
	if !ok {
		t.Fatalf("expected command BindStmt, got: %T (%+v)", cmd, cmd)
	}

	if bd.PreparedStatementName != expName {
		t.Fatalf("expected name %s, got %s", expName, bd.PreparedStatementName)
	}

	if err := finishQuery(bind, c); err != nil {
		t.Fatal(err)
	}
}

func expectSync(ctx context.Context, t *testing.T, rd *sql.StmtBufReader) {
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	_, ok := cmd.(sql.Sync)
	if !ok {
		t.Fatalf("expected command Sync, got: %T (%+v)", cmd, cmd)
	}
}

func expectExecPortal(
	ctx context.Context, t *testing.T, expName string, rd *sql.StmtBufReader, c *conn,
) {
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	ep, ok := cmd.(sql.ExecPortal)
	if !ok {
		t.Fatalf("expected command ExecPortal, got: %T (%+v)", cmd, cmd)
	}

	if ep.Name != expName {
		t.Fatalf("expected name %s, got %s", expName, ep.Name)
	}

	if err := finishQuery(execPortal, c); err != nil {
		t.Fatal(err)
	}
}

func expectSendError(
	ctx context.Context, t *testing.T, pgErrCode pgcode.Code, rd *sql.StmtBufReader, c *conn,
) {
	t.Helper()
	cmd, err := rd.CurCmd()
	if err != nil {
		t.Fatal(err)
	}
	rd.AdvanceOne()

	se, ok := cmd.(sql.SendError)
	if !ok {
		t.Fatalf("expected command SendError, got: %T (%+v)", cmd, cmd)
	}

	if code := pgerror.GetPGCode(se.Err); code != pgErrCode {
		t.Fatalf("expected code %s, got: %s", pgErrCode, code)
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
			context.Background(), nil /* columns */, nil /* formatCodes */, c.conn,
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
	log.Infof(context.Background(), "pgx log [%s] %s - %s", level, msg, data)
}

// pgxTestLogger implements pgx.Logger.
var _ pgx.Logger = pgxTestLogger{}

// Test that closing a pgwire connection causes transactions to be rolled back
// and release their locks.
func TestConnCloseReleasesLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We're going to test closing the connection in both the Open and Aborted
	// state.
	testutils.RunTrueAndFalse(t, "open state", func(t *testing.T, open bool) {
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)

		pgURL, cleanupFunc := sqlutils.PGUrl(
			t, s.ServingSQLAddr(), "testConnClose" /* prefix */, url.User(security.RootUser),
		)
		defer cleanupFunc()
		db, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer db.Close()

		r := sqlutils.MakeSQLRunner(db)
		r.Exec(t, "CREATE DATABASE test")
		r.Exec(t, "CREATE TABLE test.t (x int primary key)")

		pgxConfig, err := pgx.ParseConnectionString(pgURL.String())
		if err != nil {
			t.Fatal(err)
		}

		conn, err := pgx.Connect(pgxConfig)
		require.NoError(t, err)
		tx, err := conn.Begin()
		require.NoError(t, err)
		_, err = tx.Exec("INSERT INTO test.t(x) values (1)")
		require.NoError(t, err)
		readCh := make(chan error)
		go func() {
			conn2, err := pgx.Connect(pgxConfig)
			require.NoError(t, err)
			_, err = conn2.Exec("SELECT * FROM test.t")
			readCh <- err
		}()

		select {
		case err := <-readCh:
			t.Fatalf("unexpected read unblocked: %v", err)
		case <-time.After(10 * time.Millisecond):
		}

		if !open {
			_, err = tx.Exec("bogus")
			require.NotNil(t, err)
		}
		err = conn.Close()
		require.NoError(t, err)
		select {
		case readErr := <-readCh:
			require.NoError(t, readErr)
		case <-time.After(10 * time.Second):
			t.Fatal("read not unblocked in a timely manner")
		}
	})
}

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
func TestConnCloseWhileProducingRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Disable results buffering.
	if _, err := db.Exec(
		`SET CLUSTER SETTING sql.defaults.results_buffer.size = '0'`,
	); err != nil {
		t.Fatal(err)
	}
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "testConnClose" /* prefix */, url.User(security.RootUser),
	)
	defer cleanupFunc()
	noBufferDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer noBufferDB.Close()

	r := sqlutils.MakeSQLRunner(noBufferDB)
	r.Exec(t, "CREATE DATABASE test")
	r.Exec(t, "CREATE TABLE test.test AS SELECT * FROM generate_series(1,100)")

	pgxConfig, err := pgx.ParseConnectionString(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	// We test both with and without DistSQL, as the way that network errors are
	// observed depends on the engine.
	testutils.RunTrueAndFalse(t, "useDistSQL", func(t *testing.T, useDistSQL bool) {
		conn, err := pgx.Connect(pgxConfig)
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

// TestMaliciousInputs verifies that known malicious inputs sent to
// a v3Conn don't crash the server.
func TestMaliciousInputs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	for _, tc := range [][]byte{
		// This byte string sends a pgwirebase.ClientMsgClose message type. When
		// ReadBuffer.readUntypedMsg is called, the 4 bytes is subtracted
		// from the size, leaving a 0-length ReadBuffer. Following this,
		// handleClose is called with the empty buffer, which calls
		// getPrepareType. Previously, getPrepareType would crash on an
		// empty buffer. This is now fixed.
		{byte(pgwirebase.ClientMsgClose), 0x00, 0x00, 0x00, 0x04},
		// This byte string exploited the same bug using a pgwirebase.ClientMsgDescribe
		// message type.
		{byte(pgwirebase.ClientMsgDescribe), 0x00, 0x00, 0x00, 0x04},
		// This would cause ReadBuffer.getInt16 to overflow, resulting in a
		// negative value being used for an allocation size.
		{byte(pgwirebase.ClientMsgParse), 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0xff, 0xff},
	} {
		t.Run("", func(t *testing.T) {
			w, r := net.Pipe()
			defer w.Close()
			defer r.Close()

			go func() {
				// This io.Copy will discard all bytes from w until w is closed.
				// This is needed because sends on the net.Pipe are synchronous, so
				// the conn will block if we don't read whatever it tries to send.
				// The reason this works is that ioutil.devNull implements ReadFrom
				// as an infinite loop, so it will Read continuously until it hits an
				// error (on w.Close()).
				_, _ = io.Copy(ioutil.Discard, w)
			}()

			errChan := make(chan error, 1)
			go func() {
				// Write the malicious data.
				if _, err := w.Write(tc); err != nil {
					errChan <- err
					return
				}

				// Sync and terminate if a panic did not occur to stop the server.
				// We append a 4-byte trailer to each to signify a zero length message. See
				// lib/pq.conn.sendSimpleMessage for a similar approach to simple messages.
				_, _ = w.Write([]byte{byte(pgwirebase.ClientMsgSync), 0x00, 0x00, 0x00, 0x04})
				_, _ = w.Write([]byte{byte(pgwirebase.ClientMsgTerminate), 0x00, 0x00, 0x00, 0x04})
				close(errChan)
			}()

			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			sqlMetrics := sql.MakeMemMetrics("test" /* endpoint */, time.Second /* histogramWindow */)
			metrics := makeServerMetrics(sqlMetrics, time.Second /* histogramWindow */)

			conn := newConn(
				// ConnResultsBufferBytes - really small so that it overflows
				// when we produce a few results.
				r, sql.SessionArgs{ConnResultsBufferSize: 10}, &metrics,
				nil,
			)
			// Ignore the error from serveImpl. There might be one when the client
			// sends malformed input.
			conn.serveImpl(
				ctx,
				func() bool { return false }, /* draining */
				nil,                          /* sqlServer */
				mon.BoundAccount{},           /* reserved */
				authOptions{testingSkipAuth: true},
				stopper,
			)
			if err := <-errChan; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestReadTimeoutConn asserts that a readTimeoutConn performs reads normally
// and exits with an appropriate error when exit conditions are satisfied.
func TestReadTimeoutConnExits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Cannot use net.Pipe because deadlines are not supported.
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	log.Infof(context.Background(), "started listener on %s", ln.Addr())
	defer func() {
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	expectedRead := []byte("expectedRead")

	// Start a goroutine that performs reads using a readTimeoutConn.
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- func() error {
			c, err := ln.Accept()
			if err != nil {
				return err
			}
			defer c.Close()

			readTimeoutConn := newReadTimeoutConn(c, func() error { return ctx.Err() })
			// Assert that reads are performed normally.
			readBytes := make([]byte, len(expectedRead))
			if _, err := readTimeoutConn.Read(readBytes); err != nil {
				return err
			}
			if !bytes.Equal(readBytes, expectedRead) {
				return errors.Errorf("expected %v got %v", expectedRead, readBytes)
			}

			// The main goroutine will cancel the context, which should abort
			// this read with an appropriate error.
			_, err = readTimeoutConn.Read(make([]byte, 1))
			return err
		}()
	}()

	c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Write(expectedRead); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errChan:
		t.Fatalf("goroutine unexpectedly returned: %v", err)
	default:
	}
	cancel()
	if err := <-errChan; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConnResultsBufferSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Check that SHOW results_buffer_size correctly exposes the value when it
	// inherits the default.
	{
		var size string
		require.NoError(t, db.QueryRow(`SHOW results_buffer_size`).Scan(&size))
		require.Equal(t, `16384`, size)
	}

	pgURL, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	q := pgURL.Query()

	q.Add(`results_buffer_size`, `foo`)
	pgURL.RawQuery = q.Encode()
	{
		errDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer errDB.Close()
		_, err = errDB.Exec(`SELECT 1`)
		require.EqualError(t, err,
			`pq: error parsing results_buffer_size option value 'foo' as bytes`)
	}

	q.Del(`results_buffer_size`)
	q.Add(`results_buffer_size`, `-1`)
	pgURL.RawQuery = q.Encode()
	{
		errDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer errDB.Close()
		_, err = errDB.Exec(`SELECT 1`)
		require.EqualError(t, err, `pq: results_buffer_size option value '-1' cannot be negative`)
	}

	// Set the results_buffer_size to a very small value, eliminating buffering.
	q.Del(`results_buffer_size`)
	q.Add(`results_buffer_size`, `2`)
	pgURL.RawQuery = q.Encode()

	noBufferDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer noBufferDB.Close()

	var size string
	require.NoError(t, noBufferDB.QueryRow(`SHOW results_buffer_size`).Scan(&size))
	require.Equal(t, `2`, size)

	// Run a query that immediately returns one result and then pauses for a
	// long time while computing the second.
	rows, err := noBufferDB.Query(
		`SELECT a, if(a = 1, pg_sleep(99999), false) from (VALUES (0), (1)) AS foo (a)`)
	require.NoError(t, err)

	// Verify that the first result has been flushed.
	require.True(t, rows.Next())
	var a int
	var b bool
	require.NoError(t, rows.Scan(&a, &b))
	require.Equal(t, 0, a)
	require.False(t, b)
}

// Test that closing a connection while authentication was ongoing cancels the
// auhentication process. In other words, this checks that the server is reading
// from the connection while authentication is ongoing and so it reacts to the
// connection closing.
func TestConnCloseCancelsAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	authBlocked := make(chan struct{})
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Insecure: true,
			Knobs: base.TestingKnobs{
				PGWireTestingKnobs: &sql.PGWireTestingKnobs{
					AuthHook: func(ctx context.Context) error {
						// Notify the test.
						authBlocked <- struct{}{}
						// Wait for context cancelation.
						<-ctx.Done()
						// Notify the test.
						close(authBlocked)
						return fmt.Errorf("test auth canceled")
					},
				},
			},
		})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// We're going to open a client connection and do the minimum so that the
	// server gets to the authentication phase, where it will block.
	conn, err := net.Dial("tcp", s.ServingSQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	fe, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := fe.Send(&pgproto3.StartupMessage{ProtocolVersion: version30}); err != nil {
		t.Fatal(err)
	}

	// Wait for server to block the auth.
	<-authBlocked
	// Close the connection. This is supposed to unblock the auth by canceling its
	// ctx.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	// Check that the auth process indeed noticed the cancelation.
	<-authBlocked
}
