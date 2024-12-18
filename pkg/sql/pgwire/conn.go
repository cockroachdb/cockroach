// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/pgreplparser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/pgrepltree"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// conn implements a pgwire network connection (version 3 of the protocol,
// implemented by Postgres v7.4 and later). conn.serve() reads protocol
// messages, transforms them into commands that it pushes onto a StmtBuf (where
// they'll be picked up and executed by the connExecutor).
// The connExecutor produces results for the commands, which are delivered to
// the client through the sql.ClientComm interface, implemented by this conn
// (code is in command_result.go).
type conn struct {
	// errWriter is used to send back error payloads to the client.
	errWriter

	conn net.Conn

	cancelConn context.CancelFunc

	sessionArgs sql.SessionArgs
	metrics     *tenantSpecificMetrics

	curDestMetrics atomic.Pointer[destinationMetrics]
	destMetrics    func() *destinationMetrics

	// startTime is the time when the connection attempt was first received
	// by the server.
	startTime time.Time

	// rd is a buffered reader consuming conn. All reads from conn go through
	// this.
	rd bufio.Reader

	// parser is used to avoid allocating a parser each time.
	parser parser.Parser

	// stmtBuf is populated with commands queued for execution by this conn.
	stmtBuf sql.StmtBuf

	// res is used to avoid allocations in the conn's ClientComm implementation.
	res commandResult

	// err is an error, accessed atomically. It represents any error encountered
	// while accessing the underlying network connection. This can read via
	// GetErr() by anybody. If it is found to be != nil, the conn is no longer to
	// be used.
	err atomic.Value

	// writerState groups together all aspects of the write-side state of the
	// connection.
	writerState struct {
		fi flushInfo
		// buf contains command results (rows, etc.) until they're flushed to the
		// network connection.
		buf    bytes.Buffer
		tagBuf [64]byte
	}

	readBuf    pgwirebase.ReadBuffer
	msgBuilder writeBuffer

	// vecsScratch is a scratch space used by bufferBatch.
	vecsScratch coldata.TypedVecs

	sv *settings.Values

	// alwaysLogAuthActivity is used force-enables logging of authn events.
	alwaysLogAuthActivity bool
}

func (c *conn) setErr(err error) {
	c.err.Store(err)
}

func (c *conn) GetErr() error {
	err := c.err.Load()
	if err != nil {
		return err.(error)
	}
	return nil
}

func (c *conn) sendError(ctx context.Context, err error) error {
	// We could, but do not, report server-side network errors while
	// trying to send the client error. This is because clients that
	// receive error payload are highly correlated with clients
	// disconnecting abruptly.
	_ /* err */ = c.writeErr(ctx, err, c.conn)
	return err
}

func (c *conn) verboseAuthLogEnabled() bool {
	return c.alwaysLogAuthActivity || logVerboseSessionAuth.Get(c.sv)
}

// processCommands authenticates the connection and then processes commands
// from c.stmtBuf.
//
// Args:
// ac: An interface used by the authentication process to receive password data
// and to ultimately declare the authentication successful.
// reserved: Reserved memory. This method takes ownership and guarantees that it
// will be closed when this function returns.
func (c *conn) processCommands(
	ctx context.Context,
	authOpt authOptions,
	ac AuthConn,
	sqlServer *sql.Server,
	reserved *mon.BoundAccount,
	onDefaultIntSizeChange func(newSize int32),
	sessionID clusterunique.ID,
) {
	// reservedOwned is true while we own reserved, false when we pass ownership
	// away.
	reservedOwned := true
	var retErr error
	var connHandler sql.ConnectionHandler
	var authOK bool
	var connCloseAuthHandler func()
	defer func() {
		// Release resources, if we still own them.
		if reservedOwned {
			reserved.Clear(ctx)
		}
		// Notify the connection's goroutine that we're terminating. The
		// connection might know already, as it might have triggered this
		// goroutine's finish, but it also might be us that we're triggering the
		// connection's death. This context cancelation serves to interrupt a
		// network read on the connection's goroutine.
		c.cancelConn()

		pgwireKnobs := sqlServer.GetExecutorConfig().PGWireTestingKnobs
		if pgwireKnobs != nil && pgwireKnobs.CatchPanics {
			if r := recover(); r != nil {
				// Catch the panic and return it to the client as an error.
				if err, ok := r.(error); ok {
					// Mask the cause but keep the details.
					retErr = errors.Handled(err)
				} else {
					retErr = errors.Newf("%+v", r)
				}
				retErr = pgerror.WithCandidateCode(retErr, pgcode.CrashShutdown)
				// Add a prefix. This also adds a stack trace.
				retErr = errors.Wrap(retErr, "caught fatal error")
				_ = c.writeErr(ctx, retErr, &c.writerState.buf)
				_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)
				c.stmtBuf.Close()
			}
		}
		if !authOK {
			ac.AuthFail(retErr)
		}
		if connCloseAuthHandler != nil {
			connCloseAuthHandler()
		}
	}()

	// Authenticate the connection.
	if connCloseAuthHandler, retErr = c.handleAuthentication(
		ctx, ac, authOpt, sqlServer.GetExecutorConfig(),
	); retErr != nil {
		// Auth failed or some other error.
		return
	}

	var decrementConnectionCount func()
	if decrementConnectionCount, retErr = sqlServer.IncrementConnectionCount(c.sessionArgs); retErr != nil {
		// This will return pgcode.TooManyConnections which is used by the sql proxy
		// to skip failed auth throttle (as in this case the auth was fine but the
		// error occurred before sending back auth ok msg)
		_ = c.sendError(ctx, retErr)
		return
	}
	defer decrementConnectionCount()

	if retErr = c.authOKMessage(); retErr != nil {
		return
	}

	// Inform the client of the default session settings.
	connHandler, retErr = c.sendInitialConnData(ctx, sqlServer, onDefaultIntSizeChange, sessionID)
	if retErr != nil {
		return
	}
	// Signal the connection was established to the authenticator.
	ac.AuthOK(ctx)
	ac.LogAuthOK(ctx)
	ac.LogAuthInfof(ctx, redact.Sprintf(
		"session created with SessionDefaults=%s and CustomOptions=%s",
		c.sessionArgs.SessionDefaults,
		c.sessionArgs.CustomOptionSessionDefaults,
	))

	// We count the connection establish latency until we are ready to
	// serve a SQL query. It includes the time it takes to authenticate and
	// send the initial ReadyForQuery message.
	duration := timeutil.Since(c.startTime).Nanoseconds()
	c.metrics.ConnLatency.RecordValue(duration)

	// Mark the authentication as succeeded in case a panic
	// is thrown below and we need to report to the client
	// using the defer above.
	authOK = true

	// Now actually process commands.
	reservedOwned = false // We're about to pass ownership away.
	retErr = sqlServer.ServeConn(
		ctx,
		connHandler,
		reserved,
		c.cancelConn,
	)
}

func (c *conn) bufferParamStatus(param, value string) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterStatus)
	c.msgBuilder.writeTerminatedString(param)
	c.msgBuilder.writeTerminatedString(value)
	return c.msgBuilder.finishMsg(&c.writerState.buf)
}

func (c *conn) bufferNotice(ctx context.Context, noticeErr pgnotice.Notice) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgNoticeResponse)
	return c.writeErrFields(ctx, noticeErr, &c.writerState.buf)
}

func (c *conn) sendInitialConnData(
	ctx context.Context,
	sqlServer *sql.Server,
	onDefaultIntSizeChange func(newSize int32),
	sessionID clusterunique.ID,
) (sql.ConnectionHandler, error) {
	connHandler, err := sqlServer.SetupConn(
		ctx,
		c.sessionArgs,
		&c.stmtBuf,
		c,
		c.metrics.SQLMemMetrics,
		onDefaultIntSizeChange,
		sessionID,
	)
	if err != nil {
		_ /* err */ = c.writeErr(ctx, err, c.conn)
		return sql.ConnectionHandler{}, err
	}

	// Send the initial "status parameters" to the client.  This
	// overlaps partially with session variables. The client wants to
	// see the values that result from the combination of server-side
	// defaults with client-provided values.
	// For details see: https://www.postgresql.org/docs/10/static/libpq-status.html
	for _, param := range statusReportParams {
		param := param
		value := connHandler.GetParamStatus(ctx, param)
		if err := c.bufferParamStatus(param, value); err != nil {
			return sql.ConnectionHandler{}, err
		}
	}
	// The two following status parameters have no equivalent session
	// variable.
	if err := c.bufferParamStatus("session_authorization", c.sessionArgs.User.Normalized()); err != nil {
		return sql.ConnectionHandler{}, err
	}

	if err := c.bufferInitialReadyForQuery(connHandler.GetQueryCancelKey()); err != nil {
		return sql.ConnectionHandler{}, err
	}
	// We don't have a CmdPos to pass in, since we haven't received any commands
	// yet, so we just use the initial lastFlushed value.
	if err := c.Flush(c.writerState.fi.lastFlushed); err != nil {
		return sql.ConnectionHandler{}, err
	}
	return connHandler, nil
}

// bufferInitialReadyForQuery sends the final messages of the connection
// handshake. This includes a BackendKeyData message and a ServerMsgReady
// message indicating that there is no active transaction.
func (c *conn) bufferInitialReadyForQuery(queryCancelKey pgwirecancel.BackendKeyData) error {
	// Send our BackendKeyData to the client, so they can cancel the connection.
	c.msgBuilder.initMsg(pgwirebase.ServerMsgBackendKeyData)
	c.msgBuilder.putInt64(int64(queryCancelKey))
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		return err
	}

	// An initial ServerMsgReady message is part of the handshake.
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(byte(sql.IdleTxnBlock))
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		return err
	}
	return nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleSimpleQuery(
	ctx context.Context,
	buf *pgwirebase.ReadBuffer,
	timeReceived crtime.Mono,
	unqualifiedIntSize *types.T,
) error {
	query, err := buf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	startParse := crtime.NowMono()
	if c.sessionArgs.ReplicationMode != sessiondatapb.ReplicationMode_REPLICATION_MODE_DISABLED &&
		pgreplparser.IsReplicationProtocolCommand(query) {
		stmt, err := pgreplparser.Parse(query)
		if err != nil {
			log.SqlExec.Infof(ctx, "could not parse simple query in replication protocol: %s", query)
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		switch stmt.AST.(type) {
		case *pgrepltree.IdentifySystem:
		default:
			log.SqlExec.Infof(ctx, "unhandled replication protocol query: %s", query)
			return c.stmtBuf.Push(ctx, sql.SendError{
				Err: unimplemented.NewWithIssueDetail(0, fmt.Sprintf("%T", stmt.AST), "replication protocol command not implemented"),
			})
		}
		endParse := crtime.NowMono()
		return c.stmtBuf.Push(
			ctx,
			sql.ExecStmt{
				Statement:                            stmt,
				TimeReceived:                         timeReceived,
				ParseStart:                           startParse,
				ParseEnd:                             endParse,
				LastInBatch:                          true,
				LastInBatchBeforeShowCommitTimestamp: false,
			},
		)
	}
	stmts, err := c.parser.ParseWithInt(query, unqualifiedIntSize)
	if err != nil {
		log.SqlExec.Infof(ctx, "could not parse simple query: %s", query)
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	endParse := crtime.NowMono()

	if len(stmts) == 0 {
		return c.stmtBuf.Push(
			ctx, sql.ExecStmt{
				Statement:    statements.Statement[tree.Statement]{},
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			})
	}

	for i := range stmts {
		// The CopyFrom statement is special. We need to detect it so we can hand
		// control of the connection, through the stmtBuf, to a copyMachine, and
		// block this network routine until control is passed back.
		if cp, ok := stmts[i].AST.(*tree.CopyFrom); ok {
			if len(stmts) != 1 {
				// NOTE(andrei): I don't know if Postgres supports receiving a COPY
				// together with other statements in the "simple" protocol, but I'd
				// rather not worry about it since execution of COPY is special - it
				// takes control over the connection.
				return c.stmtBuf.Push(
					ctx,
					sql.SendError{
						Err: pgwirebase.NewProtocolViolationErrorf(
							"COPY together with other statements in a query string is not supported"),
					})
			}
			var wg sync.WaitGroup
			var once sync.Once
			wg.Add(1)
			cmd := sql.CopyIn{
				Conn:         c,
				ParsedStmt:   stmts[i],
				Stmt:         cp,
				TimeReceived: timeReceived,
				ParseStart:   startParse,
				ParseEnd:     endParse,
			}
			cmd.CopyDone.WaitGroup = &wg
			cmd.CopyDone.Once = &once
			if err := c.stmtBuf.Push(ctx, cmd); err != nil {
				return err
			}
			wg.Wait()
			return nil
		}
		if cp, ok := stmts[i].AST.(*tree.CopyTo); ok {
			if len(stmts) != 1 {
				// NOTE(andrei): I don't know if Postgres supports receiving a COPY
				// together with other statements in the "simple" protocol, but I'd
				// rather not worry about it since execution of COPY is special - it
				// takes control over the connection.
				return c.stmtBuf.Push(
					ctx,
					sql.SendError{
						Err: pgwirebase.NewProtocolViolationErrorf(
							"COPY together with other statements in a query string is not supported"),
					})
			}
			if err := c.stmtBuf.Push(
				ctx,
				sql.CopyOut{
					ParsedStmt:   stmts[i],
					Stmt:         cp,
					TimeReceived: timeReceived,
					ParseStart:   startParse,
					ParseEnd:     endParse,
				},
			); err != nil {
				return err
			}
			return nil
		}

		// Determine whether there is only SHOW COMMIT TIMESTAMP after this
		// statement in the batch. That case should be treated as though it
		// were the last statement in the batch.
		lastBeforeShowCommitTimestamp := func() bool {
			n := len(stmts)
			isShowCommitTimestamp := func(s statements.Statement[tree.Statement]) bool {
				_, ok := s.AST.(*tree.ShowCommitTimestamp)
				return ok
			}
			return n > 1 && i == n-2 && isShowCommitTimestamp(stmts[n-1])
		}

		if err := c.stmtBuf.Push(
			ctx,
			sql.ExecStmt{
				Statement:                            stmts[i],
				TimeReceived:                         timeReceived,
				ParseStart:                           startParse,
				ParseEnd:                             endParse,
				LastInBatch:                          i == len(stmts)-1,
				LastInBatchBeforeShowCommitTimestamp: lastBeforeShowCommitTimestamp(),
			}); err != nil {
			return err
		}
	}
	return nil
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleParse(ctx context.Context, nakedIntSize *types.T) error {
	telemetry.Inc(sqltelemetry.ParseRequestCounter)
	name, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	query, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	// The client may provide type information for (some of) the placeholders.
	numQArgTypes, err := c.readBuf.GetUint16()
	if err != nil {
		return err
	}
	inTypeHints := make([]oid.Oid, numQArgTypes)
	for i := range inTypeHints {
		typ, err := c.readBuf.GetUint32()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		inTypeHints[i] = oid.Oid(typ)
	}

	startParse := crtime.NowMono()
	stmts, err := c.parser.ParseWithInt(query, nakedIntSize)
	if err != nil {
		log.SqlExec.Infof(ctx, "could not parse: %s", query)
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	if len(stmts) > 1 {
		err := pgerror.WrongNumberOfPreparedStatements(len(stmts))
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var stmt statements.Statement[tree.Statement]
	if len(stmts) == 1 {
		stmt = stmts[0]
	}
	// len(stmts) == 0 results in a nil (empty) statement.

	// We take max(len(s.Types), stmt.NumPlaceHolders) as the length of types.
	numParams := len(inTypeHints)
	if stmt.NumPlaceholders > numParams {
		numParams = stmt.NumPlaceholders
	}

	var sqlTypeHints tree.PlaceholderTypes
	if len(inTypeHints) > 0 {
		// Prepare the mapping of SQL placeholder names to types. Pre-populate it with
		// the type hints received from the client, if any.
		sqlTypeHints = make(tree.PlaceholderTypes, numParams)
		for i, t := range inTypeHints {
			if t == 0 {
				continue
			}
			// If the OID is user defined or unknown, then write nil into the type
			// hints and let the consumer of the PrepareStmt resolve the types.
			if t == oid.T_unknown || types.IsOIDUserDefinedType(t) {
				sqlTypeHints[i] = nil
				continue
			}
			// This special case for json, json[] is here so we can support decoding
			// parameters with oid=json/json[] without adding full support for these
			// type.
			// TODO(sql-exp): Remove this if we support JSON.
			if t == oid.T_json {
				sqlTypeHints[i] = types.Json
				continue
			}
			if t == oid.T__json {
				sqlTypeHints[i] = types.JSONArrayForDecodingOnly
				continue
			}
			v, ok := types.OidToType[t]
			if !ok {
				err := pgwirebase.NewProtocolViolationErrorf("unknown oid type: %v", t)
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			sqlTypeHints[i] = v
		}
	}

	endParse := crtime.NowMono()

	if _, ok := stmt.AST.(*tree.CopyFrom); ok {
		// We don't support COPY in extended protocol because it'd be complicated:
		// it wouldn't be the preparing, but the execution that would need to
		// execute the copyMachine.
		// Be aware that the copyMachine assumes it always runs in the simple
		// protocol, so if we ever support this, many parts of the copyMachine
		// would need to be changed.
		// Also note that COPY FROM in extended mode seems to be quite broken in
		// Postgres too:
		// https://www.postgresql.org/message-id/flat/CAMsr%2BYGvp2wRx9pPSxaKFdaObxX8DzWse%2BOkWk2xpXSvT0rq-g%40mail.gmail.com#CAMsr+YGvp2wRx9pPSxaKFdaObxX8DzWse+OkWk2xpXSvT0rq-g@mail.gmail.com
		return c.stmtBuf.Push(ctx, sql.SendError{Err: fmt.Errorf("CopyFrom not supported in extended protocol mode")})
	}

	return c.stmtBuf.Push(
		ctx,
		sql.PrepareStmt{
			Name:         name,
			Statement:    stmt,
			TypeHints:    sqlTypeHints,
			RawTypeHints: inTypeHints,
			ParseStart:   startParse,
			ParseEnd:     endParse,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleDescribe(ctx context.Context) error {
	telemetry.Inc(sqltelemetry.DescribeRequestCounter)
	typ, err := c.readBuf.GetPrepareType()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	name, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(
		ctx,
		sql.DescribeStmt{
			Name: tree.Name(name),
			Type: typ,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleClose(ctx context.Context) error {
	telemetry.Inc(sqltelemetry.CloseRequestCounter)
	typ, err := c.readBuf.GetPrepareType()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	name, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(
		ctx,
		sql.DeletePreparedStmt{
			Name: name,
			Type: typ,
		})
}

// If no format codes are provided then all arguments/result-columns use
// the default format, text.
var formatCodesAllText = []pgwirebase.FormatCode{pgwirebase.FormatText}

// handleBind queues instructions for creating a portal from a prepared
// statement.
// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleBind(ctx context.Context) error {
	telemetry.Inc(sqltelemetry.BindRequestCounter)
	portalName, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	statementName, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}

	// From the docs on number of argument format codes to bind:
	// This can be zero to indicate that there are no arguments or that the
	// arguments all use the default format (text); or one, in which case the
	// specified format code is applied to all arguments; or it can equal the
	// actual number of arguments.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numQArgFormatCodes, err := c.readBuf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var qArgFormatCodes []pgwirebase.FormatCode
	switch numQArgFormatCodes {
	case 0:
		// No format codes means all arguments are passed as text.
		qArgFormatCodes = formatCodesAllText
	case 1:
		// `1` means read one code and apply it to every argument.
		ch, err := c.readBuf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		code := pgwirebase.FormatCode(ch)
		if code == pgwirebase.FormatText {
			qArgFormatCodes = formatCodesAllText
		} else {
			qArgFormatCodes = []pgwirebase.FormatCode{code}
		}
	default:
		qArgFormatCodes = make([]pgwirebase.FormatCode, numQArgFormatCodes)
		// Read one format code for each argument and apply it to that argument.
		for i := range qArgFormatCodes {
			ch, err := c.readBuf.GetUint16()
			if err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			qArgFormatCodes[i] = pgwirebase.FormatCode(ch)
		}
	}

	numValues, err := c.readBuf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	qargs := make([][]byte, numValues)
	for i := 0; i < int(numValues); i++ {
		plen, err := c.readBuf.GetUint32()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		if int32(plen) == -1 {
			// The argument is a NULL value.
			qargs[i] = nil
			continue
		}
		b, err := c.readBuf.GetBytes(int(plen))
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		qargs[i] = b
	}

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := c.readBuf.GetUint16()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	var columnFormatCodes []pgwirebase.FormatCode
	switch numColumnFormatCodes {
	case 0:
		// All columns will use the text format.
		columnFormatCodes = formatCodesAllText
	case 1:
		// All columns will use the one specified format.
		ch, err := c.readBuf.GetUint16()
		if err != nil {
			return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
		}
		code := pgwirebase.FormatCode(ch)
		if code == pgwirebase.FormatText {
			columnFormatCodes = formatCodesAllText
		} else {
			columnFormatCodes = []pgwirebase.FormatCode{code}
		}
	default:
		columnFormatCodes = make([]pgwirebase.FormatCode, numColumnFormatCodes)
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			ch, err := c.readBuf.GetUint16()
			if err != nil {
				return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
			}
			columnFormatCodes[i] = pgwirebase.FormatCode(ch)
		}
	}
	return c.stmtBuf.Push(
		ctx,
		sql.BindStmt{
			PreparedStatementName: statementName,
			PortalName:            portalName,
			Args:                  qargs,
			ArgFormatCodes:        qArgFormatCodes,
			OutFormats:            columnFormatCodes,
		})
}

// An error is returned iff the statement buffer has been closed. In that case,
// the connection should be considered toast.
func (c *conn) handleExecute(
	ctx context.Context, timeReceived crtime.Mono, followedBySync bool,
) error {
	telemetry.Inc(sqltelemetry.ExecuteRequestCounter)
	portalName, err := c.readBuf.GetUnsafeString()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	limit, err := c.readBuf.GetUint32()
	if err != nil {
		return c.stmtBuf.Push(ctx, sql.SendError{Err: err})
	}
	return c.stmtBuf.Push(ctx, sql.ExecPortal{
		Name:           portalName,
		TimeReceived:   timeReceived,
		Limit:          int(limit),
		FollowedBySync: followedBySync,
	})
}

func (c *conn) handleFlush(ctx context.Context) error {
	telemetry.Inc(sqltelemetry.FlushRequestCounter)
	return c.stmtBuf.Push(ctx, sql.Flush{})
}

// BeginCopyIn is part of the pgwirebase.Conn interface.
func (c *conn) BeginCopyIn(
	ctx context.Context, columns []colinfo.ResultColumn, format pgwirebase.FormatCode,
) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCopyInResponse)
	c.msgBuilder.writeByte(byte(format))
	c.msgBuilder.putInt16(int16(len(columns)))
	for range columns {
		c.msgBuilder.putInt16(int16(format))
	}
	return c.msgBuilder.finishMsg(c.conn)
}

// Rd is part of the pgwirebase.Conn interface.
func (c *conn) Rd() pgwirebase.BufferedReader {
	return &pgwireReader{conn: c}
}

// flushInfo encapsulates information about what results have been flushed to
// the network.
type flushInfo struct {
	// buf is a reference to writerState.buf.
	buf *bytes.Buffer
	// lastFlushed indicates the highest command for which results have been
	// flushed. The command may have further results in the buffer that haven't
	// been flushed.
	lastFlushed sql.CmdPos
	// cmdStarts maintains the state about where the results for the respective
	// positions begin. We utilize the invariant that positions are
	// monotonically increasing sequences.
	cmdStarts ring.Buffer[cmdIdx]
}

type cmdIdx struct {
	pos sql.CmdPos
	idx int
}

// registerCmd updates cmdStarts buffer when the first result for a new command
// is received.
func (fi *flushInfo) registerCmd(pos sql.CmdPos) {
	if fi.cmdStarts.Len() > 0 && fi.cmdStarts.GetLast().pos >= pos {
		// Not a new command, nothing to do.
		return
	}
	fi.cmdStarts.AddLast(cmdIdx{pos: pos, idx: fi.buf.Len()})
}

func cookTag(
	tagStr string, buf []byte, stmtType tree.StatementReturnType, rowsAffected int,
) []byte {
	if tagStr == "INSERT" {
		// From the postgres docs (49.5. Message Formats):
		// `INSERT oid rows`... oid is the object ID of the inserted row if
		//	rows is 1 and the target table has OIDs; otherwise oid is 0.
		tagStr = "INSERT 0"
	}
	tag := append(buf, tagStr...)

	switch stmtType {
	case tree.RowsAffected, tree.CopyIn, tree.CopyOut:
		tag = append(tag, ' ')
		tag = strconv.AppendInt(tag, int64(rowsAffected), 10)

	case tree.Rows:
		if tagStr != "SHOW" && tagStr != "EXPLAIN" && tagStr != "CALL" {
			tag = append(tag, ' ')
			tag = strconv.AppendUint(tag, uint64(rowsAffected), 10)
		}

	case tree.Ack, tree.DDL:
		if tagStr == "SELECT" {
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(rowsAffected), 10)
		}

	default:
		panic(errors.AssertionFailedf("unexpected result type %v", stmtType))
	}

	return tag
}

// bufferRow serializes a row and adds it to the buffer. Depending on the buffer
// size limit, bufferRow may flush the buffered data to the connection.
func (c *conn) bufferRow(ctx context.Context, row tree.Datums, r *commandResult) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
	c.msgBuilder.putInt16(int16(len(row)))
	for i, col := range row {
		fmtCode, err := r.GetFormatCode(i)
		if err != nil {
			return err
		}
		switch fmtCode {
		case pgwirebase.FormatText:
			c.msgBuilder.writeTextDatum(ctx, col, r.conv, r.location, r.types[i])
		case pgwirebase.FormatBinary:
			c.msgBuilder.writeBinaryDatum(ctx, col, r.location, r.types[i])
		default:
			c.msgBuilder.setError(errors.Errorf("unsupported format code %s", fmtCode))
		}
	}
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		return err
	}
	if err := c.maybeFlush(r.pos, r.bufferingDisabled); err != nil {
		return err
	}
	c.maybeReallocate()
	return nil
}

// bufferBatch serializes a batch and adds all the rows from it to the buffer.
// It is a noop for zero-length batch. Depending on the buffer size limit,
// bufferBatch may flush the buffered data to the connection.
func (c *conn) bufferBatch(ctx context.Context, batch coldata.Batch, r *commandResult) error {
	sel := batch.Selection()
	n := batch.Length()
	if n > 0 {
		c.vecsScratch.SetBatch(batch)
		// Make sure that c doesn't hold on to the memory of the batch.
		defer c.vecsScratch.Reset()
		width := int16(len(c.vecsScratch.Vecs))
		for i := 0; i < n; i++ {
			rowIdx := i
			if sel != nil {
				rowIdx = sel[rowIdx]
			}
			c.msgBuilder.initMsg(pgwirebase.ServerMsgDataRow)
			c.msgBuilder.putInt16(width)
			for vecIdx := 0; vecIdx < len(c.vecsScratch.Vecs); vecIdx++ {
				fmtCode, err := r.GetFormatCode(vecIdx)
				if err != nil {
					return err
				}
				switch fmtCode {
				case pgwirebase.FormatText:
					c.msgBuilder.writeTextColumnarElement(ctx, &c.vecsScratch, vecIdx, rowIdx, r.conv, r.location)
				case pgwirebase.FormatBinary:
					c.msgBuilder.writeBinaryColumnarElement(ctx, &c.vecsScratch, vecIdx, rowIdx, r.location)
				default:
					c.msgBuilder.setError(errors.Errorf("unsupported format code %s", fmtCode))
				}
			}
			if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
				return err
			}
			if err := c.maybeFlush(r.pos, r.bufferingDisabled); err != nil {
				return err
			}
			c.maybeReallocate()
		}
	}
	return nil
}

// tenantEgressCounter implements the sql.TenantNetworkEgressCounter interface.
type tenantEgressCounter struct {
	buf  writeBuffer
	vecs coldata.TypedVecs
}

var _ sql.TenantNetworkEgressCounter = &tenantEgressCounter{}

func newTenantEgressCounter() sql.TenantNetworkEgressCounter {
	counter := &tenantEgressCounter{}
	counter.buf.init(nil /* byteCount */)
	return counter
}

func init() {
	sql.NewTenantNetworkEgressCounter = newTenantEgressCounter
}

// GetRowNetworkEgress returns an estimate of the number of bytes that would be
// sent over the network if the given row was written to the client. It does this
// by encoding and buffering the row in text format, then measuring the buffer
// size before clearing it.
func (c *tenantEgressCounter) GetRowNetworkEgress(
	ctx context.Context, row tree.Datums, typs []*types.T,
) (egress int64) {
	// Each row uses 5 bytes for the message type and length.
	egress = 5

	var conv sessiondatapb.DataConversionConfig
	for i := range row {
		// Use the default values for the DataConversionConfig and location.
		// We use the writeText variant here because this function will only ever
		// be called in the context of EXPLAIN ANALYZE, which obfuscates the format
		// the client will use when actually executing the query. This should still
		// provide an accurate estimate, since most or all of the common data types
		// take up the same amount of space between the text and binary formats.
		c.buf.writeTextDatum(ctx, row[i], conv, nil /* sessionLoc */, typs[i])
		egress += int64(c.buf.Len())
		c.buf.reset()
	}
	return egress
}

// GetBatchNetworkEgress returns an estimate of the number of bytes that would
// be sent over the network if the given batch was written to the client.
func (c *tenantEgressCounter) GetBatchNetworkEgress(
	ctx context.Context, batch coldata.Batch,
) (egress int64) {
	// Each row uses 5 bytes for the message type and length.
	egress = 5 * int64(batch.Length())

	var conv sessiondatapb.DataConversionConfig
	c.vecs.SetBatch(batch)
	sel := batch.Selection()
	for vecIdx := range c.vecs.Vecs {
		for i := 0; i < batch.Length(); i++ {
			rowIdx := i
			if sel != nil {
				rowIdx = sel[i]
			}
			// Use the default values for the DataConversionConfig and location.
			// See the comment in getRowNetworkEgress for why the writeText variant
			// is used here instead of writeBinary.
			c.buf.writeTextColumnarElement(ctx, &c.vecs, vecIdx, rowIdx, conv, time.UTC)
			egress += int64(c.buf.Len())
			c.buf.reset()
		}
	}
	return egress
}

func (c *conn) bufferReadyForQuery(txnStatus byte) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgReady)
	c.msgBuilder.writeByte(txnStatus)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferParseComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParseComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferBindComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgBindComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferCloseComplete() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCloseComplete)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferCommandComplete(tag []byte) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCommandComplete)
	c.msgBuilder.write(tag)
	c.msgBuilder.nullTerminate()
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferPortalSuspended() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgPortalSuspended)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferErr(ctx context.Context, err error) {
	if err := c.writeErr(ctx, err, &c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferEmptyQueryResponse() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgEmptyQuery)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

type errWriter struct {
	sv         *settings.Values
	msgBuilder *writeBuffer
}

func (w *errWriter) writeErr(ctx context.Context, err error, out io.Writer) error {
	// Record telemetry for the error.
	sqltelemetry.RecordError(ctx, err, w.sv)
	w.msgBuilder.initMsg(pgwirebase.ServerMsgErrorResponse)
	return w.writeErrFields(ctx, err, out)
}

func (w *errWriter) writeErrFields(ctx context.Context, err error, out io.Writer) error {
	pgErr := pgerror.Flatten(err)

	w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	w.msgBuilder.writeTerminatedString(pgErr.Severity)

	w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSeverityNonLocalized)
	w.msgBuilder.writeTerminatedString(pgErr.Severity)

	w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSQLState)
	w.msgBuilder.writeTerminatedString(pgErr.Code)

	if pgErr.Detail != "" {
		w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldDetail)
		w.msgBuilder.writeTerminatedString(pgErr.Detail)
	}

	if pgErr.Hint != "" {
		w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldHint)
		w.msgBuilder.writeTerminatedString(pgErr.Hint)
	}

	if pgErr.ConstraintName != "" {
		w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldConstraintName)
		w.msgBuilder.writeTerminatedString(pgErr.ConstraintName)
	}

	if pgErr.Source != nil {
		errCtx := pgErr.Source
		if errCtx.File != "" {
			w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFile)
			w.msgBuilder.writeTerminatedString(errCtx.File)
		}

		if errCtx.Line > 0 {
			w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcLine)
			w.msgBuilder.writeTerminatedString(strconv.Itoa(int(errCtx.Line)))
		}

		if errCtx.Function != "" {
			w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFunction)
			w.msgBuilder.writeTerminatedString(errCtx.Function)
		}
	}

	w.msgBuilder.putErrFieldMsg(pgwirebase.ServerErrFieldMsgPrimary)
	w.msgBuilder.writeTerminatedString(pgErr.Message)

	w.msgBuilder.nullTerminate()
	return w.msgBuilder.finishMsg(out)
}

func (c *conn) bufferParamDesc(types []oid.Oid) {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgParameterDescription)
	c.msgBuilder.putInt16(int16(len(types)))
	for _, t := range types {
		c.msgBuilder.putInt32(int32(t))
	}
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferNoDataMsg() {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgNoData)
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err from buffer"))
	}
}

func (c *conn) bufferCopyOut(columns []colinfo.ResultColumn, format pgwirebase.FormatCode) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCopyOutResponse)
	c.msgBuilder.writeByte(byte(format))
	c.msgBuilder.putInt16(int16(len(columns)))
	for range columns {
		c.msgBuilder.putInt16(int16(format))
	}
	return c.msgBuilder.finishMsg(&c.writerState.buf)
}

func (c *conn) bufferCopyData(copyData []byte, res *commandResult) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCopyDataCommand)
	if _, err := c.msgBuilder.Write(copyData); err != nil {
		return err
	}
	if err := c.msgBuilder.finishMsg(&c.writerState.buf); err != nil {
		return err
	}
	if err := c.maybeFlush(res.pos, res.bufferingDisabled); err != nil {
		return err
	}
	c.maybeReallocate()
	return nil
}

func (c *conn) bufferCopyDone() error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgCopyDoneCommand)
	return c.msgBuilder.finishMsg(&c.writerState.buf)
}

// writeRowDescription writes a row description to the given writer.
//
// formatCodes specifies the format for each column. It can be nil, in which
// case all columns will use FormatText.
//
// If an error is returned, it has also been saved on c.err.
func (c *conn) writeRowDescription(
	ctx context.Context,
	columns []colinfo.ResultColumn,
	formatCodes []pgwirebase.FormatCode,
	w io.Writer,
) error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgRowDescription)
	c.msgBuilder.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(ctx, "pgwire: writing column %s of type: %s", column.Name, column.Typ)
		}
		c.msgBuilder.writeTerminatedString(column.Name)
		typ := pgTypeForParserType(column.Typ)
		c.msgBuilder.putInt32(int32(column.TableID))        // Table OID (optional).
		c.msgBuilder.putInt16(int16(column.PGAttributeNum)) // Column attribute ID (optional).
		c.msgBuilder.putInt32(int32(typ.oid))
		c.msgBuilder.putInt16(int16(typ.size))
		c.msgBuilder.putInt32(column.GetTypeModifier()) // Type modifier
		if formatCodes == nil {
			c.msgBuilder.putInt16(int16(pgwirebase.FormatText))
		} else {
			c.msgBuilder.putInt16(int16(formatCodes[i]))
		}
	}
	if err := c.msgBuilder.finishMsg(w); err != nil {
		c.setErr(err)
		return err
	}
	return nil
}

// Flush is part of the ClientComm interface.
//
// In case conn.err is set, this is a no-op - the previous err is returned.
func (c *conn) Flush(pos sql.CmdPos) error {
	// Check that there were no previous network errors. If there were, we'd
	// probably also fail the write below, but this check is here to make
	// absolutely sure that we don't send some results after we previously had
	// failed to send others.
	if err := c.GetErr(); err != nil {
		return err
	}

	c.writerState.fi.lastFlushed = pos
	// Make sure that the entire cmdStarts buffer is drained.
	// Use (*ring.Buffer).Reset instead of (*ring.Buffer).Discard to preserve
	// the underlying ring buffer memory for reuse.
	c.writerState.fi.cmdStarts.Reset()

	_ /* n */, err := c.writerState.buf.WriteTo(c.conn)
	if err != nil {
		c.setErr(err)
		return err
	}
	return nil
}

// maybeFlush flushes the buffer to the network connection if it exceeded
// sessionArgs.ConnResultsBufferSize or if buffering is disabled.
func (c *conn) maybeFlush(pos sql.CmdPos, bufferingDisabled bool) error {
	// Note that ConnResultsBufferSize cannot be changed during a session, so it
	// is safe to use the value stored on sessionArgs.
	if !bufferingDisabled && int64(c.writerState.buf.Len()) <= c.sessionArgs.ConnResultsBufferSize {
		return nil
	}
	return c.Flush(pos)
}

// maybeReallocate checks whether the internal slices used to buffer data have
// overflowed their limits. If so, they will be reallocated to a smaller size.
// maybeReallocate should only be called after the connection has been flushed
// and a query has just been processed.
func (c *conn) maybeReallocate() {
	// Note that ConnResultsBufferSize cannot be changed during a session, so it
	// is safe to use the value stored on sessionArgs.
	limit := int(c.sessionArgs.ConnResultsBufferSize)
	if c.msgBuilder.wrapped.Len() == 0 && c.msgBuilder.wrapped.Cap() > limit {
		c.msgBuilder.wrapped = bytes.Buffer{}
	}
	if c.writerState.buf.Len() == 0 && c.writerState.buf.Cap() > limit {
		c.writerState.buf = bytes.Buffer{}
	}
}

// LockCommunication is part of the ClientComm interface.
//
// The current implementation of conn writes results to the network
// synchronously, as they are produced (modulo buffering). Therefore, there's
// nothing to "lock" - communication is naturally blocked as the command
// processor won't write any more results.
func (c *conn) LockCommunication() sql.ClientLock {
	return (*clientConnLock)(&c.writerState.fi)
}

// clientConnLock is the connection's implementation of sql.ClientLock. It lets
// the sql module lock the flushing of results and find out what has already
// been flushed.
type clientConnLock flushInfo

var _ sql.ClientLock = &clientConnLock{}

// Close is part of the sql.ClientLock interface.
func (cl *clientConnLock) Close() {
	// Nothing to do. See LockCommunication note.
}

// ClientPos is part of the sql.ClientLock interface.
func (cl *clientConnLock) ClientPos() sql.CmdPos {
	return cl.lastFlushed
}

// RTrim is part of the sql.ClientLock interface.
func (cl *clientConnLock) RTrim(ctx context.Context, pos sql.CmdPos) {
	if pos <= cl.lastFlushed {
		panic(errors.AssertionFailedf("asked to trim to pos: %d, below the last flush: %d", pos, cl.lastFlushed))
	}
	// If we don't have a start index for pos yet, it must be that no results
	// for it yet have been produced yet.
	truncateIdx := cl.buf.Len()
	// Update cmdStarts buffer: delete commands that were trimmed from the back
	// of the cmdStarts buffer.
	for cl.cmdStarts.Len() > 0 {
		cmdStart := cl.cmdStarts.GetLast()
		if cmdStart.pos < pos {
			break
		}
		truncateIdx = cmdStart.idx
		cl.cmdStarts.RemoveLast()
	}
	cl.buf.Truncate(truncateIdx)
}

// CreateStatementResult is part of the sql.ClientComm interface.
func (c *conn) CreateStatementResult(
	stmt tree.Statement,
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondatapb.DataConversionConfig,
	location *time.Location,
	limit int,
	portalName string,
	implicitTxn bool,
	portalPausability sql.PortalPausablity,
) sql.CommandResult {
	return c.newCommandResult(descOpt, pos, stmt, formatCodes, conv, location, limit, portalName, implicitTxn, portalPausability)
}

// CreateSyncResult is part of the sql.ClientComm interface.
func (c *conn) CreateSyncResult(pos sql.CmdPos) sql.SyncResult {
	return c.newMiscResult(pos, readyForQuery)
}

// CreateFlushResult is part of the sql.ClientComm interface.
func (c *conn) CreateFlushResult(pos sql.CmdPos) sql.FlushResult {
	return c.newMiscResult(pos, flush)
}

// CreateDrainResult is part of the sql.ClientComm interface.
func (c *conn) CreateDrainResult(pos sql.CmdPos) sql.DrainResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreateBindResult is part of the sql.ClientComm interface.
func (c *conn) CreateBindResult(pos sql.CmdPos) sql.BindResult {
	return c.newMiscResult(pos, bindComplete)
}

// CreatePrepareResult is part of the sql.ClientComm interface.
func (c *conn) CreatePrepareResult(pos sql.CmdPos) sql.ParseResult {
	return c.newMiscResult(pos, parseComplete)
}

// CreateDescribeResult is part of the sql.ClientComm interface.
func (c *conn) CreateDescribeResult(pos sql.CmdPos) sql.DescribeResult {
	return c.newMiscResult(pos, noCompletionMsg)
}

// CreateEmptyQueryResult is part of the sql.ClientComm interface.
func (c *conn) CreateEmptyQueryResult(pos sql.CmdPos) sql.EmptyQueryResult {
	return c.newMiscResult(pos, emptyQueryResponse)
}

// CreateDeleteResult is part of the sql.ClientComm interface.
func (c *conn) CreateDeleteResult(pos sql.CmdPos) sql.DeleteResult {
	return c.newMiscResult(pos, closeComplete)
}

// CreateErrorResult is part of the sql.ClientComm interface.
func (c *conn) CreateErrorResult(pos sql.CmdPos) sql.ErrorResult {
	res := c.newMiscResult(pos, noCompletionMsg)
	res.errExpected = true
	return res
}

// CreateCopyInResult is part of the sql.ClientComm interface.
func (c *conn) CreateCopyInResult(cmd sql.CopyIn, pos sql.CmdPos) sql.CopyInResult {
	res := c.newMiscResult(pos, commandComplete)
	res.stmtType = cmd.Stmt.StatementReturnType()
	res.cmdCompleteTag = cmd.Stmt.StatementTag()
	return res
}

// CreateCopyOutResult is part of the sql.ClientComm interface.
func (c *conn) CreateCopyOutResult(cmd sql.CopyOut, pos sql.CmdPos) sql.CopyOutResult {
	res := c.newMiscResult(pos, commandComplete)
	res.stmtType = cmd.Stmt.StatementReturnType()
	res.cmdCompleteTag = cmd.Stmt.StatementTag()
	return res
}

// pgwireReader is an io.Reader that wraps a conn, maintaining its metrics as
// it is consumed.
type pgwireReader struct {
	conn *conn
}

// pgwireReader implements the pgwirebase.BufferedReader interface.
var _ pgwirebase.BufferedReader = &pgwireReader{}

// Read is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) Read(p []byte) (int, error) {
	n, err := r.conn.rd.Read(p)
	r.conn.destMetrics().BytesInCount.Inc(int64(n))
	return n, err
}

// ReadString is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadString(delim byte) (string, error) {
	s, err := r.conn.rd.ReadString(delim)
	r.conn.destMetrics().BytesInCount.Inc(int64(len(s)))
	return s, err
}

// ReadByte is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadByte() (byte, error) {
	b, err := r.conn.rd.ReadByte()
	if err == nil {
		r.conn.destMetrics().BytesInCount.Inc(1)
	}
	return b, err
}

// statusReportParams is a list of session variables that are also
// reported as server run-time parameters in the pgwire connection
// initialization.
//
// The standard PostgreSQL status vars are listed here:
// https://www.postgresql.org/docs/10/static/libpq-status.html
var statusReportParams = []string{
	"server_version",
	"server_encoding",
	"client_encoding",
	"application_name",
	// Note: session_authorization is handled specially in serveImpl().
	"DateStyle",
	"IntervalStyle",
	"is_superuser",
	"TimeZone",
	"integer_datetimes",
	"standard_conforming_strings",
	"crdb_version", // CockroachDB extension.
}

// testingStatusReportParams is the minimum set of status parameters
// needed to make pgx tests in the local package happy.
var testingStatusReportParams = map[string]string{
	"client_encoding":             "UTF8",
	"standard_conforming_strings": "on",
}
