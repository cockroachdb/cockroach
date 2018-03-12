// Copyright 2015 The Cockroach Authors.
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
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"

	"bytes"
	"io"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	authOK                int32 = 0
	authCleartextPassword int32 = 3
)

// readTimeoutConn overloads net.Conn.Read by periodically calling
// checkExitConds() and aborting the read if an error is returned.
type readTimeoutConn struct {
	net.Conn
	checkExitConds func() error
}

func newReadTimeoutConn(c net.Conn, checkExitConds func() error) net.Conn {
	// net.Pipe does not support setting deadlines. See
	// https://github.com/golang/go/blob/go1.7.4/src/net/pipe.go#L57-L67
	//
	// TODO(andrei): starting with Go 1.10, pipes are supposed to support
	// timeouts, so this should go away when we upgrade the compiler.
	if c.LocalAddr().Network() == "pipe" {
		return c
	}
	return &readTimeoutConn{
		Conn:           c,
		checkExitConds: checkExitConds,
	}
}

func (c *readTimeoutConn) Read(b []byte) (int, error) {
	// readTimeout is the amount of time ReadTimeoutConn should wait on a
	// read before checking for exit conditions. The tradeoff is between the
	// time it takes to react to session context cancellation and the overhead
	// of waking up and checking for exit conditions.
	const readTimeout = 150 * time.Millisecond

	// Remove the read deadline when returning from this function to avoid
	// unexpected behavior.
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	for {
		if err := c.checkExitConds(); err != nil {
			return 0, err
		}
		if err := c.SetReadDeadline(timeutil.Now().Add(readTimeout)); err != nil {
			return 0, err
		}
		n, err := c.Conn.Read(b)
		// Continue if the error is due to timing out.
		if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		}
		return n, err
	}
}

type v3Conn struct {
	conn        net.Conn
	rd          *bufio.Reader
	wr          *bufio.Writer
	executor    *sql.Executor
	readBuf     pgwirebase.ReadBuffer
	writeBuf    *writeBuffer
	tagBuf      [64]byte
	sessionArgs sql.SessionArgs
	session     *sql.Session

	// The logic governing these guys is hairy, and is not sufficiently
	// specified in documentation. Consult the sources before you modify:
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	doingExtendedQueryMessage, ignoreTillSync bool

	// The above comment also holds for this boolean, which can be set to cause
	// the backend to *not* send another ready for query message. This behavior
	// is required when the backend is supposed to drop messages, such as when
	// it gets extra data after an error happened during a COPY operation.
	doNotSendReadyForQuery bool

	metrics *ServerMetrics

	sqlMemoryPool *mon.BytesMonitor

	streamingState streamingState

	// curStmtErr is the error encountered during the execution of the current SQL
	// statement.
	curStmtErr error
}

type streamingState struct {

	/* Current batch state */

	// formatCodes is an array of which indicates whether each column of a row
	// should be sent as binary or text format. If it is nil then we send as text.
	formatCodes     []pgwirebase.FormatCode
	sendDescription bool
	// limit is a feature of pgwire that we don't really support. We accept it and
	// don't complain as long as the statement produces fewer results than this.
	limit      int
	emptyQuery bool
	err        error

	// hasSentResults is set if any results have been sent on the client
	// connection since the last time Close() or Flush() were called. This is used
	// to back the ResultGroup.ResultsSentToClient() interface.
	hasSentResults bool

	// TODO(tso): this can theoretically be combined with v3conn.writeBuf.
	// Currently we write to write to the v3conn.writeBuf, then we take those
	// bytes and write them to buf. We do this since we need to length prefix
	// each message and this is icky to figure out ahead of time.
	buf bytes.Buffer

	// txnStartIdx is the start of the current transaction in the buf. We keep
	// track of this so that we can reset the current transaction if we retry.
	txnStartIdx int

	/* Current statement state */

	pgTag         string
	columns       sqlbase.ResultColumns
	statementType tree.StatementType
	rowsAffected  int
	// firstRow is true when we haven't sent a row back in a result of type
	// tree.Rows. We only want to send the description once per result.
	firstRow bool
}

func (s *streamingState) reset(
	formatCodes []pgwirebase.FormatCode, sendDescription bool, limit int,
) {
	s.formatCodes = formatCodes
	s.sendDescription = sendDescription
	s.limit = limit
	s.emptyQuery = false
	s.hasSentResults = false
	s.txnStartIdx = 0
	s.err = nil
	s.buf.Reset()
}

func makeV3Conn(
	conn net.Conn, metrics *ServerMetrics, sqlMemoryPool *mon.BytesMonitor, executor *sql.Executor,
) v3Conn {
	return v3Conn{
		conn:          conn,
		rd:            bufio.NewReader(conn),
		wr:            bufio.NewWriter(conn),
		writeBuf:      newWriteBuffer(metrics.BytesOutCount),
		metrics:       metrics,
		executor:      executor,
		sqlMemoryPool: sqlMemoryPool,
	}
}

func (c *v3Conn) finish(ctx context.Context) {
	// This is better than always flushing on error.
	if err := c.wr.Flush(); err != nil {
		log.Error(ctx, err)
	}
	_ = c.conn.Close()
}

func parseOptions(ctx context.Context, data []byte) (sql.SessionArgs, error) {
	args := sql.SessionArgs{}
	buf := pgwirebase.ReadBuffer{Msg: data}
	for {
		key, err := buf.GetString()
		if err != nil {
			return sql.SessionArgs{}, errors.Errorf("error reading option key: %s", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := buf.GetString()
		if err != nil {
			return sql.SessionArgs{}, errors.Errorf("error reading option value: %s", err)
		}
		switch key {
		case "database":
			args.Database = value
		case "user":
			args.User = value
		case "application_name":
			args.ApplicationName = value
		default:
			if log.V(1) {
				log.Warningf(ctx, "unrecognized configuration parameter %q", key)
			}
		}
	}
	return args, nil
}

// statusReportParams is a static mapping from run-time parameters to their respective
// hard-coded values, each of which is to be returned as part of the status report
// during connection initialization.
var statusReportParams = map[string]string{
	"client_encoding": "UTF8",
	"DateStyle":       "ISO",
	// All datetime binary formats expect 64-bit integer microsecond values.
	// This param needs to be provided to clients or some may provide 64-bit
	// floating-point microsecond values instead, which was a legacy datetime
	// binary format.
	"integer_datetimes": "on",
	// The latest version of the docs that was consulted during the development
	// of this package. We specify this version to avoid having to support old
	// code paths which various client tools fall back to if they can't
	// determine that the server is new enough.
	"server_version": sql.PgServerVersion,
	// The current CockroachDB version string.
	"crdb_version": build.GetInfo().Short(),
	// If this parameter is not present, some drivers (including Python's psycopg2)
	// will add redundant backslash escapes for compatibility with non-standard
	// backslash handling in older versions of postgres.
	"standard_conforming_strings": "on",
}

// handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's
// name, if different from the one given initially. Note: at this
// point the sql.Session does not exist yet! If need exists to access the
// database to look up authentication data, use the internal executor.
func (c *v3Conn) handleAuthentication(ctx context.Context, insecure bool) error {
	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, hashedPassword, err := sql.GetUserHashedPassword(
		ctx, c.executor.Cfg(), c.metrics.internalMemMetrics, c.sessionArgs.User,
	)
	if err != nil {
		return c.sendError(err)
	}
	if !exists {
		return c.sendError(errors.Errorf("user %s does not exist", c.sessionArgs.User))
	}

	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		var authenticationHook security.UserAuthHook

		tlsState := tlsConn.ConnectionState()
		// If no certificates are provided, default to password
		// authentication.
		if len(tlsState.PeerCertificates) == 0 {
			password, err := c.sendAuthPasswordRequest()
			if err != nil {
				return c.sendError(err)
			}
			authenticationHook = security.UserAuthPasswordHook(
				insecure, password, hashedPassword,
			)
		} else {
			// Normalize the username contained in the certificate.
			tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
				tlsState.PeerCertificates[0].Subject.CommonName,
			).Normalize()
			var err error
			authenticationHook, err = security.UserAuthCertHook(insecure, &tlsState)
			if err != nil {
				return c.sendError(err)
			}
		}

		if err := authenticationHook(c.sessionArgs.User, true /* public */); err != nil {
			return c.sendError(err)
		}
	}

	c.writeBuf.initMsg(pgwirebase.ServerMsgAuth)
	c.writeBuf.putInt32(authOK)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) setupSession(ctx context.Context, reserved mon.BoundAccount) error {
	c.sessionArgs.RemoteAddr = c.conn.RemoteAddr()
	c.session = sql.NewSession(
		ctx, c.sessionArgs, c.executor, &c.metrics.SQLMemMetrics, c,
	)
	c.session.StartMonitor(c.sqlMemoryPool, reserved)
	return nil
}

func (c *v3Conn) closeSession(ctx context.Context) {
	c.session.Finish(c.executor)
	c.session = nil
}

func (c *v3Conn) serve(ctx context.Context, draining func() bool, reserved mon.BoundAccount) error {
	for key, value := range statusReportParams {
		c.writeBuf.initMsg(pgwirebase.ServerMsgParameterStatus)
		c.writeBuf.writeTerminatedString(key)
		c.writeBuf.writeTerminatedString(value)
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			reserved.Close(ctx)
			return err
		}
	}
	if err := c.wr.Flush(); err != nil {
		reserved.Close(ctx)
		return err
	}

	ctx = log.WithLogTagStr(ctx, "user", c.sessionArgs.User)
	if err := c.setupSession(ctx, reserved); err != nil {
		return err
	}
	// Now that a Session has been set up, further operations done on behalf of
	// this session use Session.Ctx() (which may diverge from this method's ctx).

	defer func() {
		if r := recover(); r != nil {
			// If we're panicking, use an emergency session shutdown so that
			// the monitors don't shout that they are unhappy.
			c.session.EmergencyClose()
			panic(r)
		}
		c.closeSession(ctx)
	}()

	// Once a session has been set up, the underlying net.Conn is switched to
	// a conn that exits if the session's context is canceled or if the server
	// is draining and the session does not have an ongoing transaction.
	c.conn = newReadTimeoutConn(c.conn, func() error {
		if err := func() error {
			if draining() && c.session.TxnState.State() == sql.NoTxn {
				return errors.New(ErrDraining)
			}
			return c.session.Ctx().Err()
		}(); err != nil {
			return newAdminShutdownErr(err)
		}
		return nil
	})
	c.rd = bufio.NewReader(c.conn)

	for {
		if !c.doingExtendedQueryMessage && !c.doNotSendReadyForQuery {
			c.writeBuf.initMsg(pgwirebase.ServerMsgReady)
			var txnStatus byte
			switch c.session.TxnState.State() {
			case sql.Aborted, sql.RestartWait:
				// We send status "InFailedTransaction" also for state RestartWait
				// because GO's lib/pq freaks out if we invent a new status.
				txnStatus = 'E'
			case sql.Open, sql.AutoRetry:
				txnStatus = 'T'
			case sql.NoTxn:
				// We're not in a txn (i.e. the last txn was committed).
				txnStatus = 'I'
			case sql.CommitWait:
				// We need to lie to pgwire and claim that we're still
				// in a txn. Otherwise drivers freak out.
				// This state is not part of the Postgres protocol.
				txnStatus = 'T'
			}

			if log.V(2) {
				log.Infof(c.session.Ctx(), "pgwire: %s: %q", pgwirebase.ServerMsgReady, txnStatus)
			}
			c.writeBuf.writeByte(txnStatus)
			if err := c.writeBuf.finishMsg(c.wr); err != nil {
				return err
			}
			// We only flush on every message if not doing an extended query.
			// If we are, wait for an explicit Flush message. See:
			// http://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY.
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
		c.doNotSendReadyForQuery = false
		typ, n, err := c.readBuf.ReadTypedMsg(c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			return err
		}
		// When an error occurs handling an extended query message, we have to ignore
		// any messages until we get a sync.
		if c.ignoreTillSync && typ != pgwirebase.ClientMsgSync {
			if log.V(2) {
				log.Infof(c.session.Ctx(), "pgwire: ignoring %s till sync", typ)
			}
			continue
		}
		if log.V(2) {
			log.Infof(c.session.Ctx(), "pgwire: processing %s", typ)
		}
		switch typ {
		case pgwirebase.ClientMsgSync:
			c.doingExtendedQueryMessage = false
			c.ignoreTillSync = false

		case pgwirebase.ClientMsgSimpleQuery:
			c.doingExtendedQueryMessage = false
			err = c.handleSimpleQuery(&c.readBuf)

		case pgwirebase.ClientMsgTerminate:
			return nil

		case pgwirebase.ClientMsgParse:
			c.doingExtendedQueryMessage = true
			err = c.handleParse(&c.readBuf)

		case pgwirebase.ClientMsgDescribe:
			c.doingExtendedQueryMessage = true
			err = c.handleDescribe(c.session.Ctx(), &c.readBuf)

		case pgwirebase.ClientMsgClose:
			c.doingExtendedQueryMessage = true
			err = c.handleClose(c.session.Ctx(), &c.readBuf)

		case pgwirebase.ClientMsgBind:
			c.doingExtendedQueryMessage = true
			err = c.handleBind(c.session.Ctx(), &c.readBuf)

		case pgwirebase.ClientMsgExecute:
			c.doingExtendedQueryMessage = true
			err = c.handleExecute(&c.readBuf)

		case pgwirebase.ClientMsgFlush:
			c.doingExtendedQueryMessage = true
			err = c.wr.Flush()

		case pgwirebase.ClientMsgCopyData, pgwirebase.ClientMsgCopyDone, pgwirebase.ClientMsgCopyFail:
			// We don't want to send a ready for query message here - we're supposed
			// to ignore these messages, per the protocol spec. This state will
			// happen when an error occurs on the server-side during a copy
			// operation: the server will send an error and a ready message back to
			// the client, and must then ignore further copy messages. See
			// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
			c.doNotSendReadyForQuery = true

		default:
			return c.sendError(pgwirebase.NewUnrecognizedMsgTypeErr(typ))
		}
		if err != nil {
			return err
		}
	}
}

// sendAuthPasswordRequest requests a cleartext password from the client and
// returns it.
func (c *v3Conn) sendAuthPasswordRequest() (string, error) {
	c.writeBuf.initMsg(pgwirebase.ServerMsgAuth)
	c.writeBuf.putInt32(authCleartextPassword)
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return "", err
	}
	if err := c.wr.Flush(); err != nil {
		return "", err
	}

	typ, n, err := c.readBuf.ReadTypedMsg(c.rd)
	c.metrics.BytesInCount.Inc(int64(n))
	if err != nil {
		return "", err
	}

	if typ != pgwirebase.ClientMsgPassword {
		return "", errors.Errorf("invalid response to authentication request: %s", typ)
	}

	return c.readBuf.GetString()
}

func (c *v3Conn) handleSimpleQuery(buf *pgwirebase.ReadBuffer) error {
	defer c.session.FinishPlan()
	query, err := buf.GetString()
	if err != nil {
		return err
	}

	tracing.AnnotateTrace()
	c.streamingState.reset(
		nil /* formatCodes */, true /* sendDescription */, 0, /* limit */
	)
	c.session.ResultsWriter = c
	if err := c.executor.ExecuteStatements(c.session, query, nil); err != nil {
		if err := c.setError(err); err != nil {
			return err
		}
	}
	return c.done()
}

func (c *v3Conn) handleParse(buf *pgwirebase.ReadBuffer) error {
	name, err := buf.GetString()
	if err != nil {
		return err
	}
	// The unnamed prepared statement can be freely overwritten.
	if name != "" {
		if c.session.PreparedStatements.Exists(name) {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeDuplicatePreparedStatementError, "prepared statement %q already exists", name))
		}
	}
	query, err := buf.GetString()
	if err != nil {
		return err
	}
	// The client may provide type information for (some of) the
	// placeholders. Read this first.
	numQArgTypes, err := buf.GetUint16()
	if err != nil {
		return err
	}
	inTypeHints := make([]oid.Oid, numQArgTypes)
	for i := range inTypeHints {
		typ, err := buf.GetUint32()
		if err != nil {
			return c.sendError(pgerror.NewError(pgerror.CodeProtocolViolationError, err.Error()))
		}
		inTypeHints[i] = oid.Oid(typ)
	}
	// Prepare the mapping of SQL placeholder names to
	// types. Pre-populate it with the type hints received from the
	// client, if any.
	sqlTypeHints := make(tree.PlaceholderTypes)
	for i, t := range inTypeHints {
		if t == 0 {
			continue
		}
		v, ok := types.OidToType[t]
		if !ok {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError, "unknown oid type: %v", t))
		}
		sqlTypeHints[strconv.Itoa(i+1)] = v
	}
	// Create the new PreparedStatement in the connection's Session.
	stmt, err := c.session.PreparedStatements.NewFromString(c.executor, name, query, sqlTypeHints)
	if err != nil {
		return c.sendError(err)
	}
	// Convert the inferred SQL types back to an array of pgwire Oids.
	inTypes := make([]oid.Oid, 0, len(stmt.TypeHints))
	if len(stmt.TypeHints) > pgwirebase.MaxPreparedStatementArgs {
		return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError,
			"more than %d arguments to prepared statement: %d",
			pgwirebase.MaxPreparedStatementArgs, len(stmt.TypeHints)))
	}
	for k, t := range stmt.TypeHints {
		i, err := strconv.Atoi(k)
		if err != nil || i < 1 {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeUndefinedParameterError, "invalid placeholder name: $%s", k))
		}
		// Placeholder names are 1-indexed; the arrays in the protocol are
		// 0-indexed.
		i--
		// Grow inTypes to be at least as large as i. Prepopulate all
		// slots with the hints provided, if any.
		for j := len(inTypes); j <= i; j++ {
			inTypes = append(inTypes, 0)
			if j < len(inTypeHints) {
				inTypes[j] = inTypeHints[j]
			}
		}
		// OID to Datum is not a 1-1 mapping (for example, int4 and int8
		// both map to TypeInt), so we need to maintain the types sent by
		// the client.
		if inTypes[i] != 0 {
			continue
		}
		inTypes[i] = t.Oid()
	}
	for i, t := range inTypes {
		if t == 0 {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeIndeterminateDatatypeError, "could not determine data type of placeholder $%d", i+1))
		}
	}
	// Attach pgwire-specific metadata to the PreparedStatement.
	stmt.InTypes = inTypes
	c.writeBuf.initMsg(pgwirebase.ServerMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

// stmtHasNoData returns true if describing a result of the input statement
// type should return NoData.
func stmtHasNoData(stmt tree.Statement) bool {
	return stmt == nil || stmt.StatementType() != tree.Rows
}

func (c *v3Conn) handleDescribe(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	typ, err := buf.GetPrepareType()
	if err != nil {
		return c.sendError(err)
	}
	name, err := buf.GetString()
	if err != nil {
		return err
	}
	switch typ {
	case pgwirebase.PrepareStatement:
		stmt, ok := c.session.PreparedStatements.Get(name)
		if !ok {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError, "unknown prepared statement %q", name))
		}

		c.writeBuf.initMsg(pgwirebase.ServerMsgParameterDescription)
		c.writeBuf.putInt16(int16(len(stmt.InTypes)))
		for _, t := range stmt.InTypes {
			c.writeBuf.putInt32(int32(t))
		}
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}

		if stmtHasNoData(stmt.Statement) {
			return c.sendNoData(c.wr)
		}
		return c.sendRowDescription(ctx, stmt.Columns, nil, c.wr)
	case pgwirebase.PreparePortal:
		portal, ok := c.session.PreparedPortals.Get(name)
		if !ok {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidCursorNameError, "unknown portal %q", name))
		}

		if stmtHasNoData(portal.Stmt.Statement) {
			return c.sendNoData(c.wr)
		}
		return c.sendRowDescription(ctx, portal.Stmt.Columns, portal.OutFormats, c.wr)
	default:
		return errors.Errorf("unknown describe type: %s", typ)
	}
}

func (c *v3Conn) handleClose(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	typ, err := buf.GetPrepareType()
	if err != nil {
		return c.sendError(err)
	}
	name, err := buf.GetString()
	if err != nil {
		return err
	}
	switch typ {
	case pgwirebase.PrepareStatement:
		c.session.PreparedStatements.Delete(ctx, name)
	case pgwirebase.PreparePortal:
		c.session.PreparedPortals.Delete(ctx, name)
	default:
		return errors.Errorf("unknown close type: %s", typ)
	}
	c.writeBuf.initMsg(pgwirebase.ServerMsgCloseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleBind(ctx context.Context, buf *pgwirebase.ReadBuffer) error {
	portalName, err := buf.GetString()
	if err != nil {
		return err
	}
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if c.session.PreparedPortals.Exists(portalName) {
			return c.sendError(pgerror.NewErrorf(pgerror.CodeDuplicateCursorError, "portal %q already exists", portalName))
		}
	}
	statementName, err := buf.GetString()
	if err != nil {
		return err
	}
	stmt, ok := c.session.PreparedStatements.Get(statementName)
	if !ok {
		return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError, "unknown prepared statement %q", statementName))
	}

	numQArgs := uint16(len(stmt.InTypes))
	qArgFormatCodes := make([]pgwirebase.FormatCode, numQArgs)
	// From the docs on number of argument format codes to bind:
	// This can be zero to indicate that there are no arguments or that the
	// arguments all use the default format (text); or one, in which case the
	// specified format code is applied to all arguments; or it can equal the
	// actual number of arguments.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numQArgFormatCodes, err := buf.GetUint16()
	if err != nil {
		return err
	}
	switch numQArgFormatCodes {
	case 0:
	case 1:
		// `1` means read one code and apply it to every argument.
		c, err := buf.GetUint16()
		if err != nil {
			return err
		}
		fmtCode := pgwirebase.FormatCode(c)
		for i := range qArgFormatCodes {
			qArgFormatCodes[i] = fmtCode
		}
	case numQArgs:
		// Read one format code for each argument and apply it to that argument.
		for i := range qArgFormatCodes {
			c, err := buf.GetUint16()
			if err != nil {
				return err
			}
			qArgFormatCodes[i] = pgwirebase.FormatCode(c)
		}
	default:
		return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError, "wrong number of format codes specified: %d for %d arguments", numQArgFormatCodes, numQArgs))
	}

	numValues, err := buf.GetUint16()
	if err != nil {
		return err
	}
	if numValues != numQArgs {
		return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError, "expected %d arguments, got %d", numQArgs, numValues))
	}
	qargs := tree.QueryArguments{}
	for i, t := range stmt.InTypes {
		plen, err := buf.GetUint32()
		if err != nil {
			return err
		}
		k := strconv.Itoa(i + 1)
		if int32(plen) == -1 {
			// The argument is a NULL value.
			qargs[k] = tree.DNull
			continue
		}
		b, err := buf.GetBytes(int(plen))
		if err != nil {
			return err
		}
		d, err := pgwirebase.DecodeOidDatum(t, qArgFormatCodes[i], b)
		if err != nil {
			return c.sendError(errors.Wrapf(err, "error in argument for $%d", i+1))
		}
		qargs[k] = d
	}

	numColumns := uint16(len(stmt.Columns))
	columnFormatCodes := make([]pgwirebase.FormatCode, numColumns)

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.GetUint16()
	if err != nil {
		return err
	}
	switch numColumnFormatCodes {
	case 0:
	case 1:
		// Read one code and apply it to every column.
		c, err := buf.GetUint16()
		if err != nil {
			return err
		}
		fmtCode := pgwirebase.FormatCode(c)
		for i := range columnFormatCodes {
			columnFormatCodes[i] = fmtCode
		}
	case numColumns:
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			c, err := buf.GetUint16()
			if err != nil {
				return err
			}
			columnFormatCodes[i] = pgwirebase.FormatCode(c)
		}
	default:
		return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError, "expected 0, 1, or %d for number of format codes, got %d", numColumns, numColumnFormatCodes))
	}
	// Create the new PreparedPortal in the connection's Session.
	portal, err := c.session.PreparedPortals.New(ctx, portalName, stmt, qargs)
	if err != nil {
		return err
	}

	if log.V(2) {
		log.Infof(ctx, "portal: %q for %q, args %q, formats %q", portalName, stmt.Statement, qargs, columnFormatCodes)
	}

	portal.OutFormats = columnFormatCodes
	c.writeBuf.initMsg(pgwirebase.ServerMsgBindComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleExecute(buf *pgwirebase.ReadBuffer) error {
	defer c.session.FinishPlan()
	portalName, err := buf.GetString()
	if err != nil {
		return err
	}
	portal, ok := c.session.PreparedPortals.Get(portalName)
	if !ok {
		return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidCursorNameError, "unknown portal %q", portalName))
	}
	limit, err := buf.GetUint32()
	if err != nil {
		return err
	}

	stmt := portal.Stmt
	pinfo := &tree.PlaceholderInfo{
		TypeHints: stmt.TypeHints,
		Types:     stmt.Types,
		Values:    portal.Qargs,
	}

	tracing.AnnotateTrace()
	c.streamingState.reset(portal.OutFormats, false /* sendDescription */, int(limit))
	c.session.ResultsWriter = c
	err = c.executor.ExecutePreparedStatement(c.session, stmt, pinfo)
	if err != nil {
		if err := c.setError(err); err != nil {
			return err
		}
	}
	return c.done()
}

func (c *v3Conn) sendCommandComplete(tag []byte, w io.Writer) error {
	c.writeBuf.initMsg(pgwirebase.ServerMsgCommandComplete)
	c.writeBuf.write(tag)
	c.writeBuf.nullTerminate()
	return c.writeBuf.finishMsg(w)
}

func (c *v3Conn) sendError(err error) error {
	c.executor.RecordError(err)
	if c.doingExtendedQueryMessage {
		c.ignoreTillSync = true
	}

	c.writeBuf.initMsg(pgwirebase.ServerMsgErrorResponse)

	c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldSeverity)
	c.writeBuf.writeTerminatedString("ERROR")

	pgErr, ok := pgerror.GetPGCause(err)
	var code string
	if ok {
		code = pgErr.Code
	} else {
		code = pgerror.CodeInternalError
	}

	c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldSQLState)
	c.writeBuf.writeTerminatedString(code)

	if ok && pgErr.Detail != "" {
		c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFileldDetail)
		c.writeBuf.writeTerminatedString(pgErr.Detail)
	}

	if ok && pgErr.Hint != "" {
		c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFileldHint)
		c.writeBuf.writeTerminatedString(pgErr.Hint)
	}

	if ok && pgErr.Source != nil {
		errCtx := pgErr.Source
		if errCtx.File != "" {
			c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFile)
			c.writeBuf.writeTerminatedString(errCtx.File)
		}

		if errCtx.Line > 0 {
			c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldSrcLine)
			c.writeBuf.writeTerminatedString(strconv.Itoa(int(errCtx.Line)))
		}

		if errCtx.Function != "" {
			c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldSrcFunction)
			c.writeBuf.writeTerminatedString(errCtx.Function)
		}
	}

	c.writeBuf.putErrFieldMsg(pgwirebase.ServerErrFieldMsgPrimary)
	c.writeBuf.writeTerminatedString(err.Error())

	c.writeBuf.nullTerminate()
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	return c.wr.Flush()
}

// sendNoData sends NoData message when there aren't any rows to
// send. This must be set to true iff we are responding in the
// Extended Query protocol and the portal or statement will not return
// rows. See the notes about the NoData message in the Extended Query
// section of the docs here:
// https://www.postgresql.org/docs/9.6/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
func (c *v3Conn) sendNoData(w io.Writer) error {
	c.writeBuf.initMsg(pgwirebase.ServerMsgNoData)
	return c.writeBuf.finishMsg(w)
}

// sendRowDescription sends a row description over the wire for the given
// slice of columns.
func (c *v3Conn) sendRowDescription(
	ctx context.Context,
	columns []sqlbase.ResultColumn,
	formatCodes []pgwirebase.FormatCode,
	w io.Writer,
) error {
	c.writeBuf.initMsg(pgwirebase.ServerMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(c.session.Ctx(), "pgwire: writing column %s of type: %T", column.Name, column.Typ)
		}
		c.writeBuf.writeTerminatedString(column.Name)

		typ := pgTypeForParserType(column.Typ)
		c.writeBuf.putInt32(0) // Table OID (optional).
		c.writeBuf.putInt16(0) // Column attribute ID (optional).
		c.writeBuf.putInt32(int32(typ.oid))
		c.writeBuf.putInt16(int16(typ.size))
		// The type modifier (atttypmod) is used to include various extra information
		// about the type being sent. -1 is used for values which don't make use of
		// atttypmod and is generally an acceptable catch-all for those that do.
		// See https://www.postgresql.org/docs/9.6/static/catalog-pg-attribute.html
		// for information on atttypmod. In theory we differ from Postgres by never
		// giving the scale/precision, and by not including the length of a VARCHAR,
		// but it's not clear if any drivers/ORMs depend on this.
		//
		// TODO(justin): It would be good to include this information when possible.
		c.writeBuf.putInt32(-1)
		if formatCodes == nil {
			c.writeBuf.putInt16(int16(pgwirebase.FormatText))
		} else {
			c.writeBuf.putInt16(int16(formatCodes[i]))
		}
	}
	return c.writeBuf.finishMsg(w)
}

// BeginCopyIn is part of the pgwirebase.Conn interface.
func (c *v3Conn) BeginCopyIn(ctx context.Context, columns []sqlbase.ResultColumn) error {
	c.writeBuf.initMsg(pgwirebase.ServerMsgCopyInResponse)
	c.writeBuf.writeByte(byte(pgwirebase.FormatText))
	c.writeBuf.putInt16(int16(len(columns)))
	for range columns {
		c.writeBuf.putInt16(int16(pgwirebase.FormatText))
	}
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return sql.NewWireFailureError(err)
	}
	if err := c.wr.Flush(); err != nil {
		return sql.NewWireFailureError(err)
	}
	return nil
}

// NewResultsGroup is part of the ResultsWriter interface.
func (c *v3Conn) NewResultsGroup() sql.ResultsGroup {
	return c
}

// SetEmptyQuery is part of the ResultsWriter interface.
func (c *v3Conn) SetEmptyQuery() {
	c.streamingState.emptyQuery = true
}

// SetEmptyQuery implements the ResultsGroup interface.
func (c *v3Conn) NewStatementResult() sql.StatementResult {
	c.curStmtErr = nil
	return c
}

// SetError is part of the sql.StatementResult interface.
func (c *v3Conn) SetError(err error) {
	c.curStmtErr = err
}

// Err is part of the sql.StatementResult interface.
func (c *v3Conn) Err() error {
	return c.curStmtErr
}

// ResultsSentToClient implements the ResultsGroup interface.
func (c *v3Conn) ResultsSentToClient() bool {
	return c.streamingState.hasSentResults
}

// Close implements the ResultsGroup interface.
func (c *v3Conn) Close() {
	// TODO(andrei): should we flush here?

	s := &c.streamingState
	s.txnStartIdx = s.buf.Len()
	// TODO(andrei): emptyQuery is a per-statement field. It shouldn't be set in a
	// per-group method.
	s.emptyQuery = false
	s.hasSentResults = false
}

// Reset implements the ResultsGroup interface.
func (c *v3Conn) Reset(ctx context.Context) {
	s := &c.streamingState
	if s.hasSentResults {
		panic("cannot reset if we've already sent results for group")
	}
	s.emptyQuery = false
	s.buf.Truncate(s.txnStartIdx)
}

// Flush implements the ResultsGroup interface.
func (c *v3Conn) Flush(ctx context.Context) error {
	if err := c.flush(true /* forceSend */); err != nil {
		return err
	}
	// hasSentResults is relative to the Flush() point, so we reset it here.
	c.streamingState.hasSentResults = false
	return nil
}

// BeginResult implements the StatementResult interface.
func (c *v3Conn) BeginResult(stmt tree.Statement) {
	state := &c.streamingState
	state.pgTag = stmt.StatementTag()
	state.statementType = stmt.StatementType()
	state.rowsAffected = 0
	state.firstRow = true
}

// GetPGTag implements the StatementResult interface.
func (c *v3Conn) PGTag() string {
	return c.streamingState.pgTag
}

// SetColumns implements the StatementResult interface.
func (c *v3Conn) SetColumns(columns sqlbase.ResultColumns) {
	c.streamingState.columns = columns
}

// RowsAffected implements the StatementResult interface.
func (c *v3Conn) RowsAffected() int {
	return c.streamingState.rowsAffected
}

// CloseResult implements the StatementResult interface.
// It sends a "command complete" server message.
func (c *v3Conn) CloseResult() error {
	state := &c.streamingState
	if state.err != nil {
		return state.err
	}

	ctx := c.session.Ctx()
	formatCodes := state.formatCodes
	limit := state.limit

	if err := c.flush(false /* forceSend */); err != nil {
		return err
	}

	if limit != 0 && state.statementType == tree.Rows && state.rowsAffected > state.limit {
		return c.setError(pgerror.NewErrorf(
			pgerror.CodeInternalError,
			"execute row count limits not supported: %d of %d", limit, state.rowsAffected,
		))
	}

	if state.pgTag == "INSERT" {
		// From the postgres docs (49.5. Message Formats):
		// `INSERT oid rows`... oid is the object ID of the inserted row if
		//	rows is 1 and the target table has OIDs; otherwise oid is 0.
		state.pgTag = "INSERT 0"
	}
	tag := append(c.tagBuf[:0], state.pgTag...)

	switch state.statementType {
	case tree.RowsAffected:
		tag = append(tag, ' ')
		tag = strconv.AppendInt(tag, int64(state.rowsAffected), 10)
		return c.sendCommandComplete(tag, &state.buf)

	case tree.Rows:
		if state.firstRow && state.sendDescription {
			if err := c.sendRowDescription(ctx, state.columns, formatCodes, &state.buf); err != nil {
				return err
			}
		}

		tag = append(tag, ' ')
		tag = strconv.AppendUint(tag, uint64(state.rowsAffected), 10)
		return c.sendCommandComplete(tag, &state.buf)

	case tree.Ack, tree.DDL:
		if state.pgTag == "SELECT" {
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(state.rowsAffected), 10)
		}
		return c.sendCommandComplete(tag, &state.buf)

	case tree.CopyIn:
		// Nothing to do. The CommandComplete message has been sent elsewhere.
		panic(fmt.Sprintf("CopyIn statements should have been handled elsewhere " +
			"and not produce results"))
	default:
		panic(fmt.Sprintf("unexpected result type %v", state.statementType))
	}
}

func (c *v3Conn) setError(err error) error {
	if _, isWireFailure := err.(sql.WireFailureError); isWireFailure {
		return err
	}

	state := &c.streamingState
	if state.err != nil {
		return state.err
	}

	state.hasSentResults = true
	state.err = err
	state.buf.Truncate(state.txnStartIdx)
	if err := c.flush(true /* forceSend */); err != nil {
		return sql.NewWireFailureError(err)
	}
	if err := c.sendError(err); err != nil {
		return sql.NewWireFailureError(err)
	}
	return nil
}

// StatementType implements the StatementResult interface.
func (c *v3Conn) StatementType() tree.StatementType {
	return c.streamingState.statementType
}

// IncrementRowsAffected implements the StatementResult interface.
func (c *v3Conn) IncrementRowsAffected(n int) {
	c.streamingState.rowsAffected += n
}

// AddRow implements the StatementResult interface.
func (c *v3Conn) AddRow(ctx context.Context, row tree.Datums) error {
	state := &c.streamingState
	if state.err != nil {
		return state.err
	}

	if state.statementType != tree.Rows {
		return c.setError(pgerror.NewError(
			pgerror.CodeInternalError, "cannot use AddRow() with statements that don't return rows"))
	}

	// The final tag will need to know the total row count.
	state.rowsAffected++

	formatCodes := state.formatCodes

	// First row and description needed: do it.
	if state.firstRow && state.sendDescription {
		if err := c.sendRowDescription(ctx, state.columns, formatCodes, &state.buf); err != nil {
			return err
		}
	}
	state.firstRow = false

	c.writeBuf.initMsg(pgwirebase.ServerMsgDataRow)
	c.writeBuf.putInt16(int16(len(row)))
	for i, col := range row {
		fmtCode := pgwirebase.FormatText
		if formatCodes != nil {
			fmtCode = formatCodes[i]
		}
		switch fmtCode {
		case pgwirebase.FormatText:
			c.writeBuf.writeTextDatum(ctx, col, c.session.Location())
		case pgwirebase.FormatBinary:
			c.writeBuf.writeBinaryDatum(ctx, col, c.session.Location())
		default:
			c.writeBuf.setError(errors.Errorf("unsupported format code %s", fmtCode))
		}
	}

	if err := c.writeBuf.finishMsg(&state.buf); err != nil {
		return err
	}

	return c.flush(false /* forceSend */)
}

func (c *v3Conn) done() error {
	if err := c.flush(true /* forceSend */); err != nil {
		return err
	}

	state := &c.streamingState
	if state.err != nil {
		return nil
	}

	var err error
	if state.emptyQuery {
		// Generally a commandComplete message is written by each statement as it
		// finishes writing its results. Except in this emptyQuery case, where the
		// protocol mandates a particular response.
		c.writeBuf.initMsg(pgwirebase.ServerMsgEmptyQuery)
		err = c.writeBuf.finishMsg(c.wr)
	}

	if err != nil {
		return sql.NewWireFailureError(err)
	}
	return nil
}

// flush writes the streaming buffer to the underlying connection. If force
// is true then we will write any data we have buffered, otherwise we will
// only write when we exceed our buffer size.
func (c *v3Conn) flush(forceSend bool) error {
	state := &c.streamingState
	if state.buf.Len() == 0 {
		return nil
	}

	if forceSend || state.buf.Len() > c.executor.Cfg().ConnResultsBufferBytes {
		state.hasSentResults = true
		state.txnStartIdx = 0
		if _, err := state.buf.WriteTo(c.wr); err != nil {
			return sql.NewWireFailureError(err)
		}
		if err := c.wr.Flush(); err != nil {
			return sql.NewWireFailureError(err)
		}
	}

	return nil
}

// Rd is part of the pgwirebase.Conn interface.
func (c *v3Conn) Rd() pgwirebase.BufferedReader {
	return &pgwireReader{conn: c}
}

// SendCommandComplete is part of the pgwirebase.Conn interface.
func (c *v3Conn) SendCommandComplete(tag []byte) error {
	return c.sendCommandComplete(tag, &c.streamingState.buf)
}

// v3Conn implements pgwirebase.Conn.
var _ pgwirebase.Conn = &v3Conn{}

// pgwireReader is an io.Reader that wrapps a v3Conn, maintaining its metrics as
// it is consumed.
type pgwireReader struct {
	conn *v3Conn
}

// pgwireReader implements the pgwirebase.BufferedReader interface.
var _ pgwirebase.BufferedReader = &pgwireReader{}

// Read is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) Read(p []byte) (int, error) {
	n, err := r.conn.rd.Read(p)
	r.conn.metrics.BytesInCount.Inc(int64(n))
	return n, err
}

// ReadString is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadString(delim byte) (string, error) {
	s, err := r.conn.rd.ReadString(delim)
	r.conn.metrics.BytesInCount.Inc(int64(len(s)))
	return s, err
}

// ReadByte is part of the pgwirebase.BufferedReader interface.
func (r *pgwireReader) ReadByte() (byte, error) {
	b, err := r.conn.rd.ReadByte()
	if err == nil {
		r.conn.metrics.BytesInCount.Inc(1)
	}
	return b, err
}

func newAdminShutdownErr(err error) error {
	return pgerror.NewErrorf(pgerror.CodeAdminShutdownError, err.Error())
}
