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
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
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
	defer log.Scope(t).Close(t)

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
			authOptions{testingSkipAuth: true, connType: hba.ConnHostAny},
		)
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
	expectDescribeStmt(ctx, t, "", pgwirebase.PreparePortal, &rd, conn)
	expectExecPortal(ctx, t, "", &rd, conn)
	// Check that a query string with multiple queries sent using the simple
	// protocol is broken up.
	expectSync(ctx, t, &rd)
	expectExecStmt(ctx, t, "SELECT 4", &rd, conn, queryStringIncomplete)
	expectExecStmt(ctx, t, "SELECT 5", &rd, conn, queryStringIncomplete)
	expectExecStmt(ctx, t, "SELECT 6", &rd, conn, queryStringComplete)
	expectSync(ctx, t, &rd)

	// Check that the batching works like the client intended.

	// This batch was wrapped in a transaction.
	expectExecStmt(ctx, t, "BEGIN TRANSACTION", &rd, conn, queryStringComplete)
	expectExecStmt(ctx, t, "SELECT 7", &rd, conn, queryStringComplete)
	expectExecStmt(ctx, t, "SELECT 8", &rd, conn, queryStringComplete)
	// Now we'll send an error, in the middle of this batch. pgx will stop waiting
	// for results for commands in the batch. We'll then test that seeking to the
	// next batch advances us to the correct statement.
	if err := finishQuery(generateError, conn); err != nil {
		t.Fatal(err)
	}
	// We got to the COMMIT at the end of the batch.
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

func TestConnMessageTooBig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, mainDB, _ := serverutils.StartServer(t, params)
	defer mainDB.Close()
	defer s.Stopper().Stop(context.Background())

	// Form a 1MB string.
	longStr := "a"
	for len(longStr) < 1<<20 {
		longStr += longStr
	}
	shortStr := "b"

	_, err := mainDB.Exec("CREATE TABLE tbl(str TEXT)")
	require.NoError(t, err)

	testCases := []struct {
		desc              string
		shortStrAction    func(*pgx.Conn) error
		longStrAction     func(*pgx.Conn) error
		postLongStrAction func(*pgx.Conn) error
		expectedErrRegex  string
	}{
		{
			desc: "simple query",
			shortStrAction: func(c *pgx.Conn) error {
				_, err := c.Exec(ctx, fmt.Sprintf(`SELECT '%s'`, shortStr))
				return err
			},
			longStrAction: func(c *pgx.Conn) error {
				_, err := c.Exec(ctx, fmt.Sprintf(`SELECT '%s'`, longStr))
				return err
			},
			postLongStrAction: func(c *pgx.Conn) error {
				_, err := c.Exec(ctx, "SELECT 1")
				return err
			},
			expectedErrRegex: "message size 1.0 MiB bigger than maximum allowed message size 32 KiB",
		},
		{
			desc: "copy",
			shortStrAction: func(c *pgx.Conn) error {
				_, err := c.PgConn().CopyFrom(
					ctx,
					strings.NewReader(fmt.Sprintf("%s\n", shortStr)),
					"COPY tbl(str) FROM STDIN",
				)
				return err
			},
			longStrAction: func(c *pgx.Conn) error {
				_, err := c.PgConn().CopyFrom(
					ctx,
					strings.NewReader(fmt.Sprintf("%s\n", longStr)),
					"COPY tbl(str) FROM STDIN",
				)
				return err
			},
			postLongStrAction: func(c *pgx.Conn) error {
				_, err := c.PgConn().CopyFrom(
					ctx,
					strings.NewReader(fmt.Sprintf("%s\n", shortStr)),
					"COPY tbl(str) FROM STDIN",
				)
				return err
			},
			// pgx breaks up the data into 64 KiB chunks. See
			// https://github.com/jackc/pgconn/blob/53f5fed36c570f0b5c98d6ec2415658c7b9bd11c/pgconn.go#L1217
			expectedErrRegex: "message size 64 KiB bigger than maximum allowed message size 32 KiB",
		},
		{
			desc: "prepared statement has string",
			shortStrAction: func(c *pgx.Conn) error {
				_, err := c.Prepare(ctx, "short_statement", fmt.Sprintf("SELECT $1::string, '%s'", shortStr))
				if err != nil {
					return err
				}
				r := c.QueryRow(ctx, "short_statement", shortStr)
				var str string
				return r.Scan(&str, &str)
			},
			longStrAction: func(c *pgx.Conn) error {
				_, err := c.Prepare(ctx, "long_statement", fmt.Sprintf("SELECT $1::string, '%s'", longStr))
				if err != nil {
					return err
				}
				r := c.QueryRow(ctx, "long_statement", longStr)
				var str string
				return r.Scan(&str, &str)
			},
			postLongStrAction: func(c *pgx.Conn) error {
				_, err := c.Exec(ctx, "SELECT 1")
				return err
			},
			expectedErrRegex: "message size 1.0 MiB bigger than maximum allowed message size 32 KiB",
		},
		{
			desc: "prepared statement with argument",
			shortStrAction: func(c *pgx.Conn) error {
				_, err := c.Prepare(ctx, "short_arg", "SELECT $1::string")
				if err != nil {
					return err
				}
				r := c.QueryRow(ctx, "short_arg", shortStr)
				var str string
				return r.Scan(&str)
			},
			longStrAction: func(c *pgx.Conn) error {
				_, err := c.Prepare(ctx, "long_arg", "SELECT $1::string")
				if err != nil {
					return err
				}
				r := c.QueryRow(ctx, "long_arg", longStr)
				var str string
				return r.Scan(&str)
			},
			postLongStrAction: func(c *pgx.Conn) error {
				// The test reuses the same connection, so this makes sure that the
				// prepared statement is still usable even after we ended a query with a
				// message too large error.
				// The test sets the max message size to 32 KiB. Subtracting off 24
				// bytes from that represents the largest query that will still run
				// properly. (The request has 16 other bytes to send besides our
				// string and 8 more bytes for the name of the prepared statement.)
				borderlineStr := string(make([]byte, (32*1024)-24))
				r := c.QueryRow(ctx, "long_arg", borderlineStr)
				var str string
				return r.Scan(&str)
			},
			expectedErrRegex: "message size 1.0 MiB bigger than maximum allowed message size 32 KiB",
		},
	}

	pgURL, cleanup := sqlutils.PGUrl(
		t,
		s.ServingSQLAddr(),
		"TestBigClientMessage",
		url.User(security.RootUser),
	)
	defer cleanup()

	t.Run("allow big messages", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				conf, err := pgx.ParseConfig(pgURL.String())
				require.NoError(t, err)

				t.Run("short string", func(t *testing.T) {
					c, err := pgx.ConnectConfig(ctx, conf)
					require.NoError(t, err)
					defer func() { _ = c.Close(ctx) }()
					require.NoError(t, tc.shortStrAction(c))
				})

				t.Run("long string", func(t *testing.T) {
					c, err := pgx.ConnectConfig(ctx, conf)
					require.NoError(t, err)
					defer func() { _ = c.Close(ctx) }()
					require.NoError(t, tc.longStrAction(c))
				})
			})
		}
	})

	// Set the cluster setting to be less than 1MB.
	_, err = mainDB.Exec(`SET CLUSTER SETTING sql.conn.max_read_buffer_message_size = '32 KiB'`)
	require.NoError(t, err)

	t.Run("disallow big messages", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				conf, err := pgx.ParseConfig(pgURL.String())
				require.NoError(t, err)

				t.Run("short string", func(t *testing.T) {
					c, err := pgx.ConnectConfig(ctx, conf)
					require.NoError(t, err)
					defer func() { _ = c.Close(ctx) }()
					require.NoError(t, tc.shortStrAction(c))
				})

				t.Run("long string", func(t *testing.T) {
					var gotErr error
					var c *pgx.Conn
					defer func() {
						if c != nil {
							_ = c.Close(ctx)
						}
					}()
					// Allow the cluster setting to propagate.
					testutils.SucceedsSoon(t, func() error {
						var err error
						c, err = pgx.ConnectConfig(ctx, conf)
						require.NoError(t, err)

						err = tc.longStrAction(c)
						if err != nil {
							gotErr = err
							return nil
						}
						defer func() { _ = c.Close(ctx) }()
						return errors.Newf("expected error")
					})

					// We should still be able to use the connection afterwards.
					require.Error(t, gotErr)
					require.Regexp(t, tc.expectedErrRegex, gotErr.Error())
					require.NoError(t, tc.postLongStrAction(c))
				})
			})
		}
	})
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
	it, err := s.InternalExecutor().(sqlutil.InternalExecutor).QueryIteratorEx(
		ctx, "test", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName(), Database: "system"},
		query,
	)
	if err != nil {
		return err
	}
	var rows []tree.Datums
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		rows = append(rows, it.Cur())
	}
	if err != nil {
		return err
	}
	return sendResult(ctx, c, it.Types(), rows)
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
	cfg, err := pgx.ParseConfig(
		fmt.Sprintf("postgresql://%s@%s:%d/system?sslmode=disable", security.RootUser, host, port),
	)
	if err != nil {
		return err
	}
	cfg.Logger = pgxTestLogger{}
	// Setting this so that the queries sent by pgx to initialize the
	// connection are not using prepared statements. That simplifies the
	// scaffolding of the test.
	cfg.PreferSimpleProtocol = true
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, "select 1"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "select 2"); err != nil {
		return err
	}
	if _, err := conn.Prepare(ctx, "p1", "select 'p1'"); err != nil {
		return err
	}
	if _, err := conn.Exec(
		ctx, "p1",
		// We set these options because apparently that's how I tell pgx that it
		// should check whether "p1" is a prepared statement.
		pgx.QuerySimpleProtocol(false),
	); err != nil {
		return err
	}

	// Send a group of statements as one query string using the simple protocol.
	// We'll check that we receive them one by one, but marked as a batch.
	if _, err := conn.Exec(ctx, "select 4; select 5; select 6;"); err != nil {
		return err
	}

	batch := &pgx.Batch{}
	batch.Queue("BEGIN")
	batch.Queue("select 7")
	batch.Queue("select 8")
	batch.Queue("COMMIT")

	batchResults := conn.SendBatch(ctx, batch)
	if err := batchResults.Close(); err != nil {
		// Swallow the error that we injected.
		if !strings.Contains(err.Error(), "injected") {
			return err
		}
	}

	if _, err := conn.Exec(ctx, "select 9"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "bogus statement failing to parse"); err != nil {
		return err
	}

	wg.Wait()

	return conn.Close(ctx)
}

// waitForClientConn blocks until a client connects and performs the pgwire
// handshake. This emulates what pgwire.Server does.
func waitForClientConn(ln net.Listener) (*conn, error) {
	conn, _, err := getSessionArgs(ln, false /* trustRemoteAddr */)
	if err != nil {
		return nil, err
	}

	metrics := makeServerMetrics(sql.MemoryMetrics{} /* sqlMemMetrics */, metric.TestSampleInterval)
	pgwireConn := newConn(conn, sql.SessionArgs{ConnResultsBufferSize: 16 << 10}, &metrics, timeutil.Now(), nil)
	return pgwireConn, nil
}

// getSessionArgs blocks until a client connects and returns the connection
// together with session arguments or an error.
func getSessionArgs(ln net.Listener, trustRemoteAddr bool) (net.Conn, sql.SessionArgs, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, sql.SessionArgs{}, err
	}

	buf := pgwirebase.MakeReadBuffer()
	_, err = buf.ReadUntypedMsg(conn)
	if err != nil {
		return nil, sql.SessionArgs{}, err
	}
	version, err := buf.GetUint32()
	if err != nil {
		return nil, sql.SessionArgs{}, err
	}
	if version != version30 {
		return nil, sql.SessionArgs{}, errors.Errorf("unexpected protocol version: %d", version)
	}

	args, err := parseClientProvidedSessionParameters(
		context.Background(), nil, &buf, conn.RemoteAddr(), trustRemoteAddr,
	)
	return conn, args, err
}

func makeTestingConvCfg() (sessiondatapb.DataConversionConfig, *time.Location) {
	return sessiondatapb.DataConversionConfig{
		BytesEncodeFormat: lex.BytesEncodeHex,
	}, time.UTC
}

// sendResult serializes a set of rows in pgwire format and sends them on a
// connection.
//
// TODO(andrei): Tests using this should probably switch to using the similar
// routines in the connection once conn learns how to write rows.
func sendResult(
	ctx context.Context, c *conn, cols colinfo.ResultColumns, rows []tree.Datums,
) error {
	if err := c.writeRowDescription(ctx, cols, nil /* formatCodes */, c.conn); err != nil {
		return err
	}

	defaultConv, defaultLoc := makeTestingConvCfg()
	for _, row := range rows {
		c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
		c.msgBuilder.putInt16(int16(len(row)))
		for i, col := range row {
			c.msgBuilder.writeTextDatum(ctx, col, defaultConv, defaultLoc, cols[i].Typ)
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

func (l pgxTestLogger) Log(
	ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{},
) {
	log.Infof(ctx, "pgx log [%s] %s - %s", level, msg, data)
}

// pgxTestLogger implements pgx.Logger.
var _ pgx.Logger = pgxTestLogger{}

// Test that closing a pgwire connection causes transactions to be rolled back
// and release their locks.
func TestConnCloseReleasesLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

		pgxConfig, err := pgx.ParseConfig(pgURL.String())
		if err != nil {
			t.Fatal(err)
		}

		conn, err := pgx.ConnectConfig(ctx, pgxConfig)
		require.NoError(t, err)
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO test.t(x) values (1)")
		require.NoError(t, err)
		readCh := make(chan error)
		go func() {
			conn2, err := pgx.ConnectConfig(ctx, pgxConfig)
			require.NoError(t, err)
			_, err = conn2.Exec(ctx, "SELECT * FROM test.t")
			readCh <- err
		}()

		select {
		case err := <-readCh:
			t.Fatalf("unexpected read unblocked: %v", err)
		case <-time.After(10 * time.Millisecond):
		}

		if !open {
			_, err = tx.Exec(ctx, "bogus")
			require.NotNil(t, err)
		}
		err = conn.Close(ctx)
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
	defer log.Scope(t).Close(t)
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

	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	// We test both with and without DistSQL, as the way that network errors are
	// observed depends on the engine.
	testutils.RunTrueAndFalse(t, "useDistSQL", func(t *testing.T, useDistSQL bool) {
		conn, err := pgx.ConnectConfig(ctx, pgxConfig)
		if err != nil {
			t.Fatal(err)
		}
		var query string
		if useDistSQL {
			query = `SET DISTSQL = 'always'`
		} else {
			query = `SET DISTSQL = 'off'`
		}
		if _, err := conn.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, "SELECT * FROM test.test")
		if err != nil {
			t.Fatal(err)
		}
		if hasResults := rows.Next(); !hasResults {
			t.Fatal("expected results")
		}
		if err := conn.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// TestMaliciousInputs verifies that known malicious inputs sent to
// a v3Conn don't crash the server.
func TestMaliciousInputs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

			sqlMetrics := sql.MakeMemMetrics("test" /* endpoint */, time.Second /* histogramWindow */)
			metrics := makeServerMetrics(sqlMetrics, time.Second /* histogramWindow */)

			conn := newConn(
				r,
				// ConnResultsBufferSize - really small so that it overflows
				// when we produce a few results.
				sql.SessionArgs{ConnResultsBufferSize: 10},
				&metrics,
				timeutil.Now(),
				nil,
			)
			// Ignore the error from serveImpl. There might be one when the client
			// sends malformed input.
			conn.serveImpl(
				ctx,
				func() bool { return false }, /* draining */
				nil,                          /* sqlServer */
				mon.BoundAccount{},           /* reserved */
				authOptions{testingSkipAuth: true, connType: hba.ConnHostAny},
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
	defer log.Scope(t).Close(t)
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

			readTimeoutConn := NewReadTimeoutConn(c, func() error { return ctx.Err() })
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
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
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
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
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

// TestConnServerAbortsOnRepeatedErrors checks that if the server keeps seeing
// a non-connection-closed error repeatedly, then it eventually the server
// aborts the connection.
func TestConnServerAbortsOnRepeatedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var shouldError uint32 = 0
	testingKnobError := fmt.Errorf("a random error")
	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Insecure: true,
			Knobs: base.TestingKnobs{
				PGWireTestingKnobs: &sql.PGWireTestingKnobs{
					AfterReadMsgTestingKnob: func(ctx context.Context) error {
						if atomic.LoadUint32(&shouldError) == 0 {
							return nil
						}
						return testingKnobError
					},
				},
			},
		})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	defer db.Close()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	atomic.StoreUint32(&shouldError, 1)
	for i := 0; i < maxRepeatedErrorCount+100; i++ {
		var s int
		err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&s)
		if err != nil {
			if strings.Contains(err.Error(), testingKnobError.Error()) {
				continue
			}
			if errors.Is(err, driver.ErrBadConn) {
				// The server closed the connection, which is what we want!
				require.GreaterOrEqualf(t, i, maxRepeatedErrorCount,
					"the server should have aborted after seeing %d errors",
					maxRepeatedErrorCount,
				)
				return
			}
		}
	}
	require.FailNow(t, "should have seen ErrBadConn before getting here")
}

func TestParseClientProvidedSessionParameters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a pgwire "server".
	addr := util.TestAddr
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Fatal(err)
	}
	serverAddr := ln.Addr()
	log.Infof(context.Background(), "started listener on %s", serverAddr)
	testCases := []struct {
		desc   string
		query  string
		assert func(t *testing.T, args sql.SessionArgs, err error)
	}{
		{
			desc:  "user is set from query",
			query: "user=root",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "root", args.User.Normalized())
			},
		},
		{
			desc:  "user is ignored in options",
			query: "user=root&options=-c%20user=test_user_from_options",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "root", args.User.Normalized())
				_, ok := args.SessionDefaults["user"]
				require.False(t, ok)
			},
		},
		{
			desc:  "results_buffer_size is not configurable from options",
			query: "user=root&options=-c%20results_buffer_size=42",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "options: parameter \"results_buffer_size\" cannot be changed", err)
			},
		},
		{
			desc:  "crdb:remote_addr is ignored in options",
			query: "user=root&options=-c%20crdb%3Aremote_addr=2.3.4.5%3A5432",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.NotEqual(t, "2.3.4.5:5432", args.RemoteAddr.String())
			},
		},
		{
			desc:  "more keys than values in options error",
			query: "user=root&options=-c%20search_path==public,test,default",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "option \"search_path==public,test,default\" is invalid, check '='", err)
			},
		},
		{
			desc:  "more values than keys in options error",
			query: "user=root&options=-c%20search_path",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "option \"search_path\" is invalid, check '='", err)
			},
		},
		{
			desc:  "success parsing encoded options",
			query: "user=root&options=-c%20search_path%3ddefault%2Ctest",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "default,test", args.SessionDefaults["search_path"])
			},
		},
		{
			desc:  "success parsing options with no space after '-c'",
			query: "user=root&options=-csearch_path=default,test -coptimizer_use_multicol_stats=true",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "default,test", args.SessionDefaults["search_path"])
				require.Equal(t, "true", args.SessionDefaults["optimizer_use_multicol_stats"])
			},
		},
		{
			desc:  "success parsing options with a tab (%09) separating the options",
			query: "user=root&options=-csearch_path=default,test%09-coptimizer_use_multicol_stats=true",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "default,test", args.SessionDefaults["search_path"])
				require.Equal(t, "true", args.SessionDefaults["optimizer_use_multicol_stats"])
			},
		},
		{
			desc:  "error when no leading '-c'",
			query: "user=root&options=search_path=default",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "option \"search_path=default\" is invalid, must have prefix '-c' or '--'", err)
			},
		},
		{
			desc:  "'-c' with no leading space belongs to prev value",
			query: "user=root&options=-c search_path=default-c",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "default-c", args.SessionDefaults["search_path"])
			},
		},
		{
			desc:  "fail to parse '-c' with no leading space",
			query: "user=root&options=-c search_path=default-c optimizer_use_multicol_stats=true",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "option \"optimizer_use_multicol_stats=true\" is invalid, must have prefix '-c' or '--'", err)
			},
		},
		{
			desc:  "parse multiple options successfully",
			query: "user=root&options=-c%20search_path=default,test%20-c%20optimizer_use_multicol_stats=true",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "default,test", args.SessionDefaults["search_path"])
				require.Equal(t, "true", args.SessionDefaults["optimizer_use_multicol_stats"])
			},
		},
		{
			desc:  "success parsing option with space in value",
			query: "user=root&options=-c default_transaction_isolation=READ\\ UNCOMMITTED",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "READ UNCOMMITTED", args.SessionDefaults["default_transaction_isolation"])
			},
		},
		{
			desc:  "remote_addr missing port",
			query: "user=root&crdb:remote_addr=5.4.3.2",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "invalid address format: address 5.4.3.2: missing port in address", err)
			},
		},
		{
			desc:  "remote_addr port must be numeric",
			query: "user=root&crdb:remote_addr=5.4.3.2:port",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "remote port is not numeric", err)
			},
		},
		{
			desc:  "remote_addr host must be numeric",
			query: "user=root&crdb:remote_addr=ip:5432",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.Error(t, err)
				require.Regexp(t, "remote address is not numeric", err)
			},
		},
		{
			desc:  "success setting remote address from query",
			query: "user=root&crdb:remote_addr=2.3.4.5:5432",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "2.3.4.5:5432", args.RemoteAddr.String())
			},
		},
		{
			desc:  "normalize to lower case in options parameter",
			query: "user=root&options=-c DateStyle=YMD,ISO",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "YMD,ISO", args.SessionDefaults["datestyle"])
			},
		},
		{
			desc:  "normalize to lower case in query parameters",
			query: "user=root&DateStyle=ISO,YMD",
			assert: func(t *testing.T, args sql.SessionArgs, err error) {
				require.NoError(t, err)
				require.Equal(t, "ISO,YMD", args.SessionDefaults["datestyle"])
			},
		},
	}

	baseURL := fmt.Sprintf("postgres://%s/system?sslmode=disable", serverAddr)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			var connErr error
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				url := fmt.Sprintf("%s&%s", baseURL, tc.query)
				var c *pgx.Conn
				c, connErr = pgx.Connect(ctx, url)
				if connErr != nil {
					return
				}
				// ignore the error because there is no answer from the server, we are
				// interested in parsing session arguments only
				_ = c.Ping(ctx)
				// closing connection immediately, since getSessionArgs is blocking
				_ = c.Close(ctx)
			}()
			// Wait for the client to connect and perform the handshake.
			_, args, err := getSessionArgs(ln, true /* trustRemoteAddr */)
			tc.assert(t, args, err)
		})
	}
}

func TestSetSessionArguments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	defer db.Close()

	_, err := db.Exec("SET CLUSTER SETTING sql.defaults.datestyle.enabled = true")
	require.NoError(t, err)
	_, err = db.Exec("SET CLUSTER SETTING sql.defaults.intervalstyle.enabled = true")
	require.NoError(t, err)

	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "testConnClose" /* prefix */, url.User(security.RootUser),
	)
	defer cleanupFunc()

	q := pgURL.Query()
	q.Add("options", "  --user=test -c    search_path=public,testsp %20 "+
		"--default-transaction-isolation=read\\ uncommitted   "+
		"-capplication_name=test  "+
		"--DateStyle=ymd\\ ,\\ iso\\  "+
		"-c intervalstyle%3DISO_8601 "+
		"-ccustom_option.custom_option=test2")
	pgURL.RawQuery = q.Encode()
	noBufferDB, err := gosql.Open("postgres", pgURL.String())

	if err != nil {
		t.Fatal(err)
	}
	defer noBufferDB.Close()

	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.ConnectConfig(ctx, pgxConfig)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := conn.Query(ctx, "show all")
	if err != nil {
		t.Fatal(err)
	}

	expectedOptions := map[string]string{
		"search_path": "public, testsp",
		// setting an isolation level is a noop:
		// all transactions execute with serializable isolation.
		"default_transaction_isolation": "serializable",
		"application_name":              "test",
		"datestyle":                     "ISO, YMD",
		"intervalstyle":                 "iso_8601",
	}
	expectedFoundOptions := len(expectedOptions)

	var foundOptions int
	var variable, value string
	for rows.Next() {
		err = rows.Scan(&variable, &value)
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := expectedOptions[variable]; ok {
			foundOptions++
			if v != value {
				t.Fatalf("option %q expected value %q, actual %q", variable, v, value)
			}
		}
	}
	require.Equal(t, expectedFoundOptions, foundOptions)

	// Custom session options don't show up on SHOW ALL
	var customOption string
	require.NoError(t, conn.QueryRow(ctx, "SHOW custom_option.custom_option").Scan(&customOption))
	require.Equal(t, "test2", customOption)

	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRoleDefaultSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	defer db.Close()

	_, err := db.ExecContext(ctx, "CREATE ROLE testuser WITH LOGIN")
	require.NoError(t, err)

	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "TestRoleDefaultSettings" /* prefix */, url.User("testuser"),
	)
	defer cleanupFunc()

	for i, tc := range []struct {
		setupStmt             string
		postConnectStmt       string
		databaseOverride      string
		searchPathOptOverride string
		userOverride          string
		expectedSearchPath    string
	}{
		// The test cases need to be in order since the default settings have
		// an order of precedence that is being checked here.
		{
			setupStmt:          "ALTER ROLE ALL SET search_path = 'a'",
			expectedSearchPath: "a",
		},
		{
			setupStmt:          "ALTER ROLE ALL IN DATABASE defaultdb SET search_path = 'b'",
			expectedSearchPath: "b",
		},
		{
			setupStmt:          "ALTER ROLE testuser SET search_path = 'c'",
			expectedSearchPath: "c",
		},
		{
			setupStmt:          "ALTER ROLE testuser IN DATABASE defaultdb SET search_path = 'd'",
			expectedSearchPath: "d",
		},
		{
			// Connecting to a different database should use the role-wide default.
			databaseOverride:   "postgres",
			expectedSearchPath: "c",
		},
		{
			// Connecting to a non-existent database should use the role-wide default
			// (and should not error). After connecting, we need to switch to a
			// real database so that `SHOW var` works correctly.
			databaseOverride:   "this_is_not_a_database",
			postConnectStmt:    "SET DATABASE = defaultdb",
			expectedSearchPath: "c",
		},
		{
			// The setting in the connection URL should take precedence.
			searchPathOptOverride: "e",
			expectedSearchPath:    "e",
		},
		{
			// Connecting as a different user, should use the database-wide default.
			setupStmt:          "CREATE ROLE testuser2 WITH LOGIN",
			userOverride:       "testuser2",
			databaseOverride:   "defaultdb",
			expectedSearchPath: "b",
		},
		{
			// Connecting as a different user and to a different database should
			// use the global default.
			userOverride:       "testuser2",
			databaseOverride:   "postgres",
			expectedSearchPath: "a",
		},
		{
			// Test that RESETing the global default works.
			setupStmt:          "ALTER ROLE ALL RESET search_path",
			userOverride:       "testuser2",
			databaseOverride:   "postgres",
			expectedSearchPath: `"$user", public`,
		},
		{
			// Change an existing default setting.
			setupStmt:          "ALTER ROLE testuser IN DATABASE defaultdb SET search_path = 'f'",
			expectedSearchPath: "f",
		},
		{
			// RESET after connecting should go back to the per-role default setting.
			setupStmt:          "",
			postConnectStmt:    "SET search_path = 'new'; RESET search_path;",
			expectedSearchPath: "f",
		},
		{
			// RESET should use the query param as the default if it was provided.
			setupStmt:             "",
			searchPathOptOverride: "g",
			postConnectStmt:       "SET search_path = 'new'; RESET ALL;",
			expectedSearchPath:    "g",
		},
		{
			setupStmt:          "ALTER ROLE testuser IN DATABASE defaultdb SET search_path = DEFAULT",
			expectedSearchPath: "c",
		},
		{
			setupStmt:          "ALTER ROLE testuser SET search_path TO DEFAULT",
			expectedSearchPath: "b",
		},
		{
			// Add a default setting for a different variable.
			setupStmt:          "ALTER ROLE ALL IN DATABASE defaultdb SET serial_normalization = sql_sequence",
			expectedSearchPath: "b",
		},
		{
			// RESETing the other variable should not affect search_path.
			setupStmt:          "ALTER ROLE ALL IN DATABASE defaultdb RESET serial_normalization",
			expectedSearchPath: "b",
		},
		{
			// The global default was already reset earlier, so there should be
			// no default setting after this.
			setupStmt:          "ALTER ROLE ALL IN DATABASE defaultdb RESET ALL",
			expectedSearchPath: `"$user", public`,
		},
	} {
		t.Run(fmt.Sprintf("TestRoleDefaultSettings-%d", i), func(t *testing.T) {
			_, err := db.ExecContext(ctx, tc.setupStmt)
			require.NoError(t, err)

			pgURLCopy := pgURL
			if tc.userOverride != "" {
				newPGURL, cleanupFunc := sqlutils.PGUrl(
					t, s.ServingSQLAddr(), "TestRoleDefaultSettings" /* prefix */, url.User(tc.userOverride),
				)
				defer cleanupFunc()
				pgURLCopy = newPGURL
			}
			pgURLCopy.Path = tc.databaseOverride
			if tc.searchPathOptOverride != "" {
				q := pgURLCopy.Query()
				q.Add("search_path", tc.searchPathOptOverride)
				pgURLCopy.RawQuery = q.Encode()
			}

			thisDB, err := gosql.Open("postgres", pgURLCopy.String())
			require.NoError(t, err)
			defer thisDB.Close()

			if tc.postConnectStmt != "" {
				_, err = thisDB.ExecContext(ctx, tc.postConnectStmt)
				require.NoError(t, err)
			}

			var actual string
			err = thisDB.QueryRow("SHOW search_path").Scan(&actual)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSearchPath, actual)
		})
	}
}

func TestPGWireRejectsNewConnIfTooManyConns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testServer, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer testServer.Stopper().Stop(ctx)

	// Users.
	admin := security.RootUser
	nonAdmin := security.TestUser

	// openConnWithUser opens a connection to the testServer for the given user
	// and always returns an associated cleanup function, even in case of error,
	// which should be called. The returned cleanup function is idempotent.
	openConnWithUser := func(user string) (*pgx.Conn, func(), error) {
		pgURL, cleanup := sqlutils.PGUrlWithOptionalClientCerts(
			t,
			testServer.ServingSQLAddr(),
			t.Name(),
			url.UserPassword(user, user),
			user == admin,
		)
		defer cleanup()
		conn, err := pgx.Connect(ctx, pgURL.String())
		if err != nil {
			return nil, func() {}, err
		}
		return conn, func() {
			require.NoError(t, conn.Close(ctx))
		}, nil
	}

	openConnWithUserSuccess := func(user string) (*pgx.Conn, func()) {
		conn, cleanup, err := openConnWithUser(user)
		if err != nil {
			defer cleanup()
			t.FailNow()
		}
		return conn, cleanup
	}

	openConnWithUserError := func(user string) {
		_, cleanup, err := openConnWithUser(user)
		defer cleanup()
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgcode.TooManyConnections.String(), pgErr.Code)
	}

	getConnectionCount := func() int {
		return int(testServer.SQLServer().(*sql.Server).GetConnectionCount())
	}

	getMaxConnections := func() int {
		conn, cleanup := openConnWithUserSuccess(admin)
		defer cleanup()
		var maxConnections int
		err := conn.QueryRow(ctx, "SHOW CLUSTER SETTING server.max_connections_per_gateway").Scan(&maxConnections)
		require.NoError(t, err)
		return maxConnections
	}

	setMaxConnections := func(maxConnections int) {
		conn, cleanup := openConnWithUserSuccess(admin)
		defer cleanup()
		_, err := conn.Exec(ctx, "SET CLUSTER SETTING server.max_connections_per_gateway = $1", maxConnections)
		require.NoError(t, err)
	}

	createUser := func(user string) {
		conn, cleanup := openConnWithUserSuccess(admin)
		defer cleanup()
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %[1]s WITH PASSWORD '%[1]s'", user))
		require.NoError(t, err)
	}

	// create nonAdmin
	createUser(nonAdmin)
	require.Equal(t, 0, getConnectionCount())

	// assert default value
	require.Equal(t, -1, getMaxConnections())
	require.Equal(t, 0, getConnectionCount())

	t.Run("0 max_connections", func(t *testing.T) {
		setMaxConnections(0)
		require.Equal(t, 0, getConnectionCount())
		// can't connect with nonAdmin
		openConnWithUserError(nonAdmin)
		require.Equal(t, 0, getConnectionCount())
		// can connect with admin
		_, adminCleanup := openConnWithUserSuccess(admin)
		require.Equal(t, 1, getConnectionCount())
		adminCleanup()
		require.Equal(t, 0, getConnectionCount())
	})

	t.Run("1 max_connections nonAdmin -> admin", func(t *testing.T) {
		setMaxConnections(1)
		require.Equal(t, 0, getConnectionCount())
		// can connect with nonAdmin
		_, nonAdminCleanup := openConnWithUserSuccess(nonAdmin)
		require.Equal(t, 1, getConnectionCount())
		// can connect with admin
		_, adminCleanup := openConnWithUserSuccess(admin)
		require.Equal(t, 2, getConnectionCount())
		adminCleanup()
		require.Equal(t, 1, getConnectionCount())
		nonAdminCleanup()
		require.Equal(t, 0, getConnectionCount())
	})

	t.Run("1 max_connections admin -> nonAdmin", func(t *testing.T) {
		setMaxConnections(1)
		require.Equal(t, 0, getConnectionCount())
		// can connect with admin
		_, adminCleanup := openConnWithUserSuccess(admin)
		require.Equal(t, 1, getConnectionCount())
		// can't connect with nonAdmin
		openConnWithUserError(nonAdmin)
		require.Equal(t, 1, getConnectionCount())
		adminCleanup()
		require.Equal(t, 0, getConnectionCount())
	})

	t.Run("-1 max_connections", func(t *testing.T) {
		setMaxConnections(-1)
		require.Equal(t, 0, getConnectionCount())
		// can connect with multiple nonAdmin
		_, nonAdminCleanup1 := openConnWithUserSuccess(nonAdmin)
		require.Equal(t, 1, getConnectionCount())
		_, nonAdminCleanup2 := openConnWithUserSuccess(nonAdmin)
		require.Equal(t, 2, getConnectionCount())
		// can connect with admin
		_, adminCleanup := openConnWithUserSuccess(admin)
		require.Equal(t, 3, getConnectionCount())
		adminCleanup()
		require.Equal(t, 2, getConnectionCount())
		nonAdminCleanup1()
		require.Equal(t, 1, getConnectionCount())
		nonAdminCleanup2()
		require.Equal(t, 0, getConnectionCount())
	})
}
