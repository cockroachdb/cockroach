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
//
// Author: Ben Darnell
// Author: Tamir Duberstein (tamird@gmail.com)

package pgwire

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

//go:generate stringer -type=clientMessageType
type clientMessageType byte

//go:generate stringer -type=serverMessageType
type serverMessageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	clientMsgBind        clientMessageType = 'B'
	clientMsgClose       clientMessageType = 'C'
	clientMsgCopyData    clientMessageType = 'd'
	clientMsgCopyDone    clientMessageType = 'c'
	clientMsgCopyFail    clientMessageType = 'f'
	clientMsgDescribe    clientMessageType = 'D'
	clientMsgExecute     clientMessageType = 'E'
	clientMsgFlush       clientMessageType = 'H'
	clientMsgParse       clientMessageType = 'P'
	clientMsgPassword    clientMessageType = 'p'
	clientMsgSimpleQuery clientMessageType = 'Q'
	clientMsgSync        clientMessageType = 'S'
	clientMsgTerminate   clientMessageType = 'X'

	serverMsgAuth                 serverMessageType = 'R'
	serverMsgBindComplete         serverMessageType = '2'
	serverMsgCommandComplete      serverMessageType = 'C'
	serverMsgCloseComplete        serverMessageType = '3'
	serverMsgCopyInResponse       serverMessageType = 'G'
	serverMsgDataRow              serverMessageType = 'D'
	serverMsgEmptyQuery           serverMessageType = 'I'
	serverMsgErrorResponse        serverMessageType = 'E'
	serverMsgNoData               serverMessageType = 'n'
	serverMsgParameterDescription serverMessageType = 't'
	serverMsgParameterStatus      serverMessageType = 'S'
	serverMsgParseComplete        serverMessageType = '1'
	serverMsgReady                serverMessageType = 'Z'
	serverMsgRowDescription       serverMessageType = 'T'
)

//go:generate stringer -type=serverErrFieldType
type serverErrFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
const (
	serverErrFieldSeverity    serverErrFieldType = 'S'
	serverErrFieldSQLState    serverErrFieldType = 'C'
	serverErrFieldMsgPrimary  serverErrFieldType = 'M'
	serverErrFileldDetail     serverErrFieldType = 'D'
	serverErrFileldHint       serverErrFieldType = 'H'
	serverErrFieldSrcFile     serverErrFieldType = 'F'
	serverErrFieldSrcLine     serverErrFieldType = 'L'
	serverErrFieldSrcFunction serverErrFieldType = 'R'
)

//go:generate stringer -type=prepareType
type prepareType byte

const (
	prepareStatement prepareType = 'S'
	preparePortal    prepareType = 'P'
)

const (
	authOK                int32 = 0
	authCleartextPassword int32 = 3
)

// preparedStatementMeta is pgwire-specific metadata which is attached to each
// sql.PreparedStatement on a v3Conn's sql.Session.
type preparedStatementMeta struct {
	inTypes []oid.Oid
}

// preparedPortalMeta is pgwire-specific metadata which is attached to each
// sql.PreparedPortal on a v3Conn's sql.Session.
type preparedPortalMeta struct {
	outFormats []formatCode
}

// readTimeoutConn overloads net.Conn.Read by periodically calling
// checkExitConds() and aborting the read if an error is returned.
type readTimeoutConn struct {
	net.Conn
	checkExitConds func() error
}

func newReadTimeoutConn(c net.Conn, checkExitConds func() error) net.Conn {
	// net.Pipe does not support setting deadlines. See
	// https://github.com/golang/go/blob/go1.7.4/src/net/pipe.go#L57-L67
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
	readBuf     readBuffer
	writeBuf    writeBuffer
	tagBuf      [64]byte
	sessionArgs sql.SessionArgs
	session     *sql.Session

	// The logic governing these guys is hairy, and is not sufficiently
	// specified in documentation. Consult the sources before you modify:
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	doingExtendedQueryMessage, ignoreTillSync bool

	metrics *ServerMetrics

	sqlMemoryPool *mon.MemoryMonitor
}

func makeV3Conn(
	conn net.Conn, metrics *ServerMetrics, sqlMemoryPool *mon.MemoryMonitor, executor *sql.Executor,
) v3Conn {
	return v3Conn{
		conn:          conn,
		rd:            bufio.NewReader(conn),
		wr:            bufio.NewWriter(conn),
		writeBuf:      writeBuffer{bytecount: metrics.BytesOutCount},
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

func parseOptions(data []byte) (sql.SessionArgs, error) {
	args := sql.SessionArgs{}
	buf := readBuffer{msg: data}
	for {
		key, err := buf.getString()
		if err != nil {
			return sql.SessionArgs{}, errors.Errorf("error reading option key: %s", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := buf.getString()
		if err != nil {
			return sql.SessionArgs{}, errors.Errorf("error reading option value: %s", err)
		}
		switch key {
		case "database":
			args.Database = value
		case "user":
			args.User = value
		default:
			if log.V(1) {
				log.Warningf(context.TODO(), "unrecognized configuration parameter %q", key)
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
}

// handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's
// name, if different from the one given initially. Note: at this
// point the sql.Session does not exist yet! If need exists to access the
// database to look up authentication data, use the internal executor.
func (c *v3Conn) handleAuthentication(ctx context.Context, insecure bool) error {
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		var authenticationHook security.UserAuthHook

		// Check that the requested user exists and retrieve the hashed
		// password in case password authentication is needed.
		hashedPassword, err := sql.GetUserHashedPassword(
			ctx, c.executor, c.metrics.internalMemMetrics, c.sessionArgs.User,
		)
		if err != nil {
			return c.sendError(err)
		}

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
			tlsState.PeerCertificates[0].Subject.CommonName = parser.Name(
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

	c.writeBuf.initMsg(serverMsgAuth)
	c.writeBuf.putInt32(authOK)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) setupSession(ctx context.Context, reserved mon.BoundAccount) error {
	c.session = sql.NewSession(
		ctx, c.sessionArgs, c.executor, c.conn.RemoteAddr(), &c.metrics.SQLMemMetrics,
	)
	c.session.StartMonitor(c.sqlMemoryPool, reserved)
	return nil
}

func (c *v3Conn) closeSession(ctx context.Context) {
	c.session.Finish(c.executor)
	c.session = nil
}

func (c *v3Conn) serve(ctx context.Context, reserved mon.BoundAccount) error {
	for key, value := range statusReportParams {
		c.writeBuf.initMsg(serverMsgParameterStatus)
		c.writeBuf.writeTerminatedString(key)
		c.writeBuf.writeTerminatedString(value)
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}

	ctx = log.WithLogTagStr(ctx, "user", c.sessionArgs.User)
	if err := c.setupSession(ctx, reserved); err != nil {
		return err
	}
	defer c.closeSession(ctx)

	// Once a session has been set up, the underlying net.Conn is switched to
	// a conn that exits if the session's context is cancelled.
	c.conn = newReadTimeoutConn(c.conn, c.session.Ctx().Err)
	c.rd = bufio.NewReader(c.conn)

	for {
		if !c.doingExtendedQueryMessage {
			c.writeBuf.initMsg(serverMsgReady)
			var txnStatus byte
			switch c.session.TxnState.State {
			case sql.Aborted, sql.RestartWait:
				// We send status "InFailedTransaction" also for state RestartWait
				// because GO's lib/pq freaks out if we invent a new status.
				txnStatus = 'E'
			case sql.Open:
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
				log.Infof(ctx, "pgwire: %s: %q", serverMsgReady, txnStatus)
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
		typ, n, err := c.readBuf.readTypedMsg(c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			return err
		}
		// When an error occurs handling an extended query message, we have to ignore
		// any messages until we get a sync.
		if c.ignoreTillSync && typ != clientMsgSync {
			if log.V(2) {
				log.Infof(ctx, "pgwire: ignoring %s till sync", typ)
			}
			continue
		}
		if log.V(2) {
			log.Infof(ctx, "pgwire: processing %s", typ)
		}
		switch typ {
		case clientMsgSync:
			c.doingExtendedQueryMessage = false
			c.ignoreTillSync = false

		case clientMsgSimpleQuery:
			c.doingExtendedQueryMessage = false
			err = c.handleSimpleQuery(ctx, &c.readBuf)

		case clientMsgTerminate:
			return nil

		case clientMsgParse:
			c.doingExtendedQueryMessage = true
			err = c.handleParse(ctx, &c.readBuf)

		case clientMsgDescribe:
			c.doingExtendedQueryMessage = true
			err = c.handleDescribe(ctx, &c.readBuf)

		case clientMsgClose:
			c.doingExtendedQueryMessage = true
			err = c.handleClose(&c.readBuf)

		case clientMsgBind:
			c.doingExtendedQueryMessage = true
			err = c.handleBind(&c.readBuf)

		case clientMsgExecute:
			c.doingExtendedQueryMessage = true
			err = c.handleExecute(ctx, &c.readBuf)

		case clientMsgFlush:
			c.doingExtendedQueryMessage = true
			err = c.wr.Flush()

		case clientMsgCopyData, clientMsgCopyDone, clientMsgCopyFail:
			// The spec says to drop any extra of these messages.

		default:
			return c.sendError(newUnrecognizedMsgTypeErr(typ))
		}
		if err != nil {
			return err
		}
	}
}

// sendAuthPasswordRequest requests a cleartext password from the client and
// returns it.
func (c *v3Conn) sendAuthPasswordRequest() (string, error) {
	c.writeBuf.initMsg(serverMsgAuth)
	c.writeBuf.putInt32(authCleartextPassword)
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return "", err
	}
	if err := c.wr.Flush(); err != nil {
		return "", err
	}

	typ, n, err := c.readBuf.readTypedMsg(c.rd)
	c.metrics.BytesInCount.Inc(int64(n))
	if err != nil {
		return "", err
	}

	if typ != clientMsgPassword {
		return "", errors.Errorf("invalid response to authentication request: %s", typ)
	}

	return c.readBuf.getString()
}

func (c *v3Conn) handleSimpleQuery(ctx context.Context, buf *readBuffer) error {
	query, err := buf.getString()
	if err != nil {
		return err
	}

	return c.executeStatements(ctx, query, nil, nil, true, 0)
}

func (c *v3Conn) handleParse(ctx context.Context, buf *readBuffer) error {
	name, err := buf.getString()
	if err != nil {
		return err
	}
	// The unnamed prepared statement can be freely overwritten.
	if name != "" {
		if c.session.PreparedStatements.Exists(name) {
			return c.sendInternalError(fmt.Sprintf("prepared statement %q already exists", name))
		}
	}
	query, err := buf.getString()
	if err != nil {
		return err
	}
	// The client may provide type information for (some of) the
	// placeholders. Read this first.
	numQArgTypes, err := buf.getUint16()
	if err != nil {
		return err
	}
	inTypeHints := make([]oid.Oid, numQArgTypes)
	for i := range inTypeHints {
		typ, err := buf.getUint32()
		if err != nil {
			return err
		}
		inTypeHints[i] = oid.Oid(typ)
	}
	// Prepare the mapping of SQL placeholder names to
	// types. Pre-populate it with the type hints received from the
	// client, if any.
	sqlTypeHints := make(parser.PlaceholderTypes)
	for i, t := range inTypeHints {
		if t == 0 {
			continue
		}
		v, ok := parser.OidToType[t]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown oid type: %v", t))
		}
		sqlTypeHints[fmt.Sprint(i+1)] = v
	}
	// Create the new PreparedStatement in the connection's Session.
	stmt, err := c.session.PreparedStatements.New(ctx, c.executor, name, query, sqlTypeHints)
	if err != nil {
		return c.sendError(err)
	}
	// Convert the inferred SQL types back to an array of pgwire Oids.
	inTypes := make([]oid.Oid, 0, len(stmt.SQLTypes))
	for k, t := range stmt.SQLTypes {
		i, err := strconv.Atoi(k)
		if err != nil || i < 1 {
			return c.sendInternalError(fmt.Sprintf("invalid placeholder name: $%s", k))
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
			return c.sendInternalError(
				fmt.Sprintf("could not determine data type of placeholder $%d", i+1))
		}
	}
	// Attach pgwire-specific metadata to the PreparedStatement.
	stmt.ProtocolMeta = preparedStatementMeta{inTypes: inTypes}
	c.writeBuf.initMsg(serverMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleDescribe(ctx context.Context, buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendError(err)
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		stmt, ok := c.session.PreparedStatements.Get(name)
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", name))
		}

		stmtMeta := stmt.ProtocolMeta.(preparedStatementMeta)
		c.writeBuf.initMsg(serverMsgParameterDescription)
		c.writeBuf.putInt16(int16(len(stmtMeta.inTypes)))
		for _, t := range stmtMeta.inTypes {
			c.writeBuf.putInt32(int32(t))
		}
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}

		return c.sendRowDescription(ctx, stmt.Columns, nil)
	case preparePortal:
		portal, ok := c.session.PreparedPortals.Get(name)
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown portal %q", name))
		}

		portalMeta := portal.ProtocolMeta.(preparedPortalMeta)
		return c.sendRowDescription(ctx, portal.Stmt.Columns, portalMeta.outFormats)
	default:
		return errors.Errorf("unknown describe type: %s", typ)
	}
}

func (c *v3Conn) handleClose(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendError(err)
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		c.session.PreparedStatements.Delete(name)
	case preparePortal:
		c.session.PreparedPortals.Delete(name)
	default:
		return errors.Errorf("unknown close type: %s", typ)
	}
	c.writeBuf.initMsg(serverMsgCloseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleBind(buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if c.session.PreparedPortals.Exists(portalName) {
			return c.sendInternalError(fmt.Sprintf("portal %q already exists", portalName))
		}
	}
	statementName, err := buf.getString()
	if err != nil {
		return err
	}
	stmt, ok := c.session.PreparedStatements.Get(statementName)
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", statementName))
	}

	stmtMeta := stmt.ProtocolMeta.(preparedStatementMeta)
	numQArgs := uint16(len(stmtMeta.inTypes))
	qArgFormatCodes := make([]formatCode, numQArgs)
	// From the docs on number of argument format codes to bind:
	// This can be zero to indicate that there are no arguments or that the
	// arguments all use the default format (text); or one, in which case the
	// specified format code is applied to all arguments; or it can equal the
	// actual number of arguments.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numQArgFormatCodes, err := buf.getUint16()
	if err != nil {
		return err
	}
	switch numQArgFormatCodes {
	case 0:
	case 1:
		// `1` means read one code and apply it to every argument.
		c, err := buf.getUint16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range qArgFormatCodes {
			qArgFormatCodes[i] = fmtCode
		}
	case numQArgs:
		// Read one format code for each argument and apply it to that argument.
		for i := range qArgFormatCodes {
			c, err := buf.getUint16()
			if err != nil {
				return err
			}
			qArgFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("wrong number of format codes specified: %d for %d arguments", numQArgFormatCodes, numQArgs))
	}

	numValues, err := buf.getUint16()
	if err != nil {
		return err
	}
	if numValues != numQArgs {
		return c.sendInternalError(fmt.Sprintf("expected %d arguments, got %d", numQArgs, numValues))
	}
	qargs := parser.QueryArguments{}
	for i, t := range stmtMeta.inTypes {
		plen, err := buf.getUint32()
		if err != nil {
			return err
		}
		k := fmt.Sprint(i + 1)
		if int32(plen) == -1 {
			// The argument is a NULL value.
			qargs[k] = parser.DNull
			continue
		}
		b, err := buf.getBytes(int(plen))
		if err != nil {
			return err
		}
		d, err := decodeOidDatum(t, qArgFormatCodes[i], b)
		if err != nil {
			return c.sendInternalError(fmt.Sprintf("error in argument for $%d: %s", i+1, err))
		}
		qargs[k] = d
	}

	numColumns := uint16(len(stmt.Columns))
	columnFormatCodes := make([]formatCode, numColumns)

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.getUint16()
	if err != nil {
		return err
	}
	switch numColumnFormatCodes {
	case 0:
	case 1:
		// Read one code and apply it to every column.
		c, err := buf.getUint16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range columnFormatCodes {
			columnFormatCodes[i] = fmtCode
		}
	case numColumns:
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			c, err := buf.getUint16()
			if err != nil {
				return err
			}
			columnFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("expected 0, 1, or %d for number of format codes, got %d", numColumns, numColumnFormatCodes))
	}
	// Create the new PreparedPortal in the connection's Session.
	portal, err := c.session.PreparedPortals.New(portalName, stmt, qargs)
	if err != nil {
		return err
	}
	// Attach pgwire-specific metadata to the PreparedPortal.
	portal.ProtocolMeta = preparedPortalMeta{outFormats: columnFormatCodes}
	c.writeBuf.initMsg(serverMsgBindComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleExecute(ctx context.Context, buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}
	portal, ok := c.session.PreparedPortals.Get(portalName)
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown portal %q", portalName))
	}
	limit, err := buf.getUint32()
	if err != nil {
		return err
	}

	stmt := portal.Stmt
	portalMeta := portal.ProtocolMeta.(preparedPortalMeta)
	pinfo := parser.PlaceholderInfo{
		Types:  stmt.SQLTypes,
		Values: portal.Qargs,
	}

	return c.executeStatements(ctx, stmt.Query, &pinfo, portalMeta.outFormats, false, int(limit))
}

func (c *v3Conn) executeStatements(
	ctx context.Context,
	stmts string,
	pinfo *parser.PlaceholderInfo,
	formatCodes []formatCode,
	sendDescription bool,
	limit int,
) error {
	tracing.AnnotateTrace()
	// Note: sql.Executor gets its Context from c.session.context, which
	// has been bound by v3Conn.setupSession().
	results := c.executor.ExecuteStatements(c.session, stmts, pinfo)
	defer results.Close()

	tracing.AnnotateTrace()
	if results.Empty {
		// Skip executor and just send EmptyQueryResponse.
		c.writeBuf.initMsg(serverMsgEmptyQuery)
		return c.writeBuf.finishMsg(c.wr)
	}
	return c.sendResponse(ctx, results.ResultList, formatCodes, sendDescription, limit)
}

func (c *v3Conn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(serverMsgCommandComplete)
	c.writeBuf.write(tag)
	c.writeBuf.nullTerminate()
	return c.writeBuf.finishMsg(c.wr)
}

// TODO(andrei): Figure out the correct codes to send for all the errors
// in this file and remove this function.
func (c *v3Conn) sendInternalError(errToSend string) error {
	err := errors.Errorf(errToSend)
	err = pgerror.WithPGCode(err, pgerror.CodeInternalError)
	err = pgerror.WithSourceContext(err, 1)
	return c.sendError(err)
}

func (c *v3Conn) sendError(err error) error {
	if c.doingExtendedQueryMessage {
		c.ignoreTillSync = true
	}

	c.writeBuf.initMsg(serverMsgErrorResponse)

	c.writeBuf.putErrFieldMsg(serverErrFieldSeverity)
	c.writeBuf.writeTerminatedString("ERROR")

	code, ok := pgerror.PGCode(err)
	if !ok {
		code = pgerror.CodeInternalError
	}
	c.writeBuf.putErrFieldMsg(serverErrFieldSQLState)
	c.writeBuf.writeTerminatedString(code)

	if detail, ok := pgerror.Detail(err); ok {
		c.writeBuf.putErrFieldMsg(serverErrFileldDetail)
		c.writeBuf.writeTerminatedString(detail)
	}

	if hint, ok := pgerror.Hint(err); ok {
		c.writeBuf.putErrFieldMsg(serverErrFileldHint)
		c.writeBuf.writeTerminatedString(hint)
	}

	if errCtx, ok := pgerror.SourceContext(err); ok {
		if errCtx.File != "" {
			c.writeBuf.putErrFieldMsg(serverErrFieldSrcFile)
			c.writeBuf.writeTerminatedString(errCtx.File)
		}

		if errCtx.Line > 0 {
			c.writeBuf.putErrFieldMsg(serverErrFieldSrcLine)
			c.writeBuf.writeTerminatedString(strconv.Itoa(errCtx.Line))
		}

		if errCtx.Function != "" {
			c.writeBuf.putErrFieldMsg(serverErrFieldSrcFunction)
			c.writeBuf.writeTerminatedString(errCtx.Function)
		}
	}

	c.writeBuf.putErrFieldMsg(serverErrFieldMsgPrimary)
	c.writeBuf.writeTerminatedString(err.Error())

	c.writeBuf.nullTerminate()
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	return c.wr.Flush()
}

// sendResponse sends the results as a query response.
func (c *v3Conn) sendResponse(
	ctx context.Context,
	results sql.ResultList,
	formatCodes []formatCode,
	sendDescription bool,
	limit int,
) error {
	if len(results) == 0 {
		return c.sendCommandComplete(nil)
	}
	for _, result := range results {
		if result.Err != nil {
			if err := c.sendError(result.Err); err != nil {
				return err
			}
			break
		}
		if limit != 0 && result.Rows != nil && result.Rows.Len() > limit {
			if err := c.sendInternalError(fmt.Sprintf("execute row count limits not supported: %d of %d", limit, result.Rows.Len())); err != nil {
				return err
			}
			break
		}

		if result.PGTag == "INSERT" {
			// From the postgres docs (49.5. Message Formats):
			// `INSERT oid rows`... oid is the object ID of the inserted row if
			//	rows is 1 and the target table has OIDs; otherwise oid is 0.
			result.PGTag = "INSERT 0"
		}
		tag := append(c.tagBuf[:0], result.PGTag...)

		switch result.Type {
		case parser.RowsAffected:
			// Send CommandComplete.
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(result.RowsAffected), 10)
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		case parser.Rows:
			if sendDescription {
				if err := c.sendRowDescription(ctx, result.Columns, formatCodes); err != nil {
					return err
				}
			}

			// Send DataRows.
			nRows := result.Rows.Len()
			for rowIdx := 0; rowIdx < nRows; rowIdx++ {
				row := result.Rows.At(rowIdx)
				c.writeBuf.initMsg(serverMsgDataRow)
				c.writeBuf.putInt16(int16(len(row)))
				for i, col := range row {
					fmtCode := formatText
					if formatCodes != nil {
						fmtCode = formatCodes[i]
					}
					switch fmtCode {
					case formatText:
						c.writeBuf.writeTextDatum(col, c.session.Location)
					case formatBinary:
						c.writeBuf.writeBinaryDatum(col, c.session.Location)
					default:
						c.writeBuf.setError(errors.Errorf("unsupported format code %s", fmtCode))
					}
				}
				if err := c.writeBuf.finishMsg(c.wr); err != nil {
					return err
				}
			}

			// Send CommandComplete.
			tag = append(tag, ' ')
			tag = strconv.AppendUint(tag, uint64(result.Rows.Len()), 10)
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		case parser.Ack, parser.DDL:
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		case parser.CopyIn:
			if err := c.copyIn(result.Columns); err != nil {
				return err
			}

		default:
			panic(fmt.Sprintf("unexpected result type %v", result.Type))
		}
	}

	return nil
}

func (c *v3Conn) sendRowDescription(
	ctx context.Context, columns []sql.ResultColumn, formatCodes []formatCode,
) error {
	if len(columns) == 0 {
		c.writeBuf.initMsg(serverMsgNoData)
		return c.writeBuf.finishMsg(c.wr)
	}

	c.writeBuf.initMsg(serverMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof(ctx, "pgwire: writing column %s of type: %T", column.Name, column.Typ)
		}
		c.writeBuf.writeTerminatedString(column.Name)

		typ := pgTypeForParserType(column.Typ)
		c.writeBuf.putInt32(0) // Table OID (optional).
		c.writeBuf.putInt16(0) // Column attribute ID (optional).
		c.writeBuf.putInt32(int32(typ.oid))
		c.writeBuf.putInt16(int16(typ.size))
		c.writeBuf.putInt32(0) // Type modifier (none of our supported types have modifiers).
		if formatCodes == nil {
			c.writeBuf.putInt16(int16(formatText))
		} else {
			c.writeBuf.putInt16(int16(formatCodes[i]))
		}
	}
	return c.writeBuf.finishMsg(c.wr)
}

// See: https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-COPY
func (c *v3Conn) copyIn(columns []sql.ResultColumn) error {
	defer c.session.CopyEnd()

	c.writeBuf.initMsg(serverMsgCopyInResponse)
	c.writeBuf.writeByte(byte(formatText))
	c.writeBuf.putInt16(int16(len(columns)))
	for range columns {
		c.writeBuf.putInt16(int16(formatText))
	}
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}

	for {
		typ, n, err := c.readBuf.readTypedMsg(c.rd)
		c.metrics.BytesInCount.Inc(int64(n))
		if err != nil {
			return err
		}
		var sr sql.StatementResults
		var done bool
		switch typ {
		case clientMsgCopyData:
			// Note: sql.Executor gets its Context from c.session.context, which
			// has been bound by v3Conn.setupSession().
			sr = c.executor.CopyData(c.session, string(c.readBuf.msg))

		case clientMsgCopyDone:
			// Note: sql.Executor gets its Context from c.session.context, which
			// has been bound by v3Conn.setupSession().
			sr = c.executor.CopyDone(c.session)
			done = true

		case clientMsgCopyFail:
			done = true

		case clientMsgFlush, clientMsgSync:
			// Spec says to "ignore Flush and Sync messages received during copy-in mode".

		default:
			return c.sendError(newUnrecognizedMsgTypeErr(typ))
		}
		for _, res := range sr.ResultList {
			if res.Err != nil {
				return c.sendError(res.Err)
			}
		}
		if done {
			return nil
		}
	}
}

func newUnrecognizedMsgTypeErr(typ clientMessageType) error {
	err := errors.Errorf("unrecognized client message type %v", typ)
	err = pgerror.WithPGCode(err, pgerror.CodeProtocolViolationError)
	return pgerror.WithSourceContext(err, 1)
}
