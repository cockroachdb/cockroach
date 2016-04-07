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
	"fmt"
	"net"
	"reflect"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/pq/oid"
)

//go:generate stringer -type=clientMessageType
type clientMessageType byte

//go:generate stringer -type=serverMessageType
type serverMessageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	clientMsgSimpleQuery clientMessageType = 'Q'
	clientMsgParse       clientMessageType = 'P'
	clientMsgTerminate   clientMessageType = 'X'
	clientMsgDescribe    clientMessageType = 'D'
	clientMsgSync        clientMessageType = 'S'
	clientMsgClose       clientMessageType = 'C'
	clientMsgBind        clientMessageType = 'B'
	clientMsgExecute     clientMessageType = 'E'
	clientMsgFlush       clientMessageType = 'H'

	serverMsgAuth                 serverMessageType = 'R'
	serverMsgCommandComplete      serverMessageType = 'C'
	serverMsgDataRow              serverMessageType = 'D'
	serverMsgErrorResponse        serverMessageType = 'E'
	serverMsgParseComplete        serverMessageType = '1'
	serverMsgReady                serverMessageType = 'Z'
	serverMsgRowDescription       serverMessageType = 'T'
	serverMsgEmptyQuery           serverMessageType = 'I'
	serverMsgParameterDescription serverMessageType = 't'
	serverMsgBindComplete         serverMessageType = '2'
	serverMsgParameterStatus      serverMessageType = 'S'
	serverMsgNoData               serverMessageType = 'n'
)

//go:generate stringer -type=prepareType
type prepareType byte

const (
	prepareStatement prepareType = 'S'
	preparePortal    prepareType = 'P'
)

const (
	authOK int32 = 0
)

// preparedStatement is a SQL statement that has been parsed and the types
// of arguments and results have been determined.
type preparedStatement struct {
	query       string
	inTypes     []oid.Oid
	columns     []sql.ResultColumn
	portalNames map[string]struct{}
}

// preparedPortal is a preparedStatement that has been bound with parameters.
type preparedPortal struct {
	stmt       preparedStatement
	stmtName   string
	params     []parser.Datum
	outFormats []formatCode
}

type v3Conn struct {
	conn     net.Conn
	rd       *bufio.Reader
	wr       *bufio.Writer
	executor *sql.Executor
	readBuf  readBuffer
	writeBuf writeBuffer
	tagBuf   [64]byte
	session  *sql.Session

	preparedStatements map[string]preparedStatement
	preparedPortals    map[string]preparedPortal

	// The logic governing these guys is hairy, and is not sufficiently
	// specified in documentation. Consult the sources before you modify:
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	doingExtendedQueryMessage, ignoreTillSync bool

	metrics *serverMetrics
}

func makeV3Conn(
	conn net.Conn, executor *sql.Executor,
	metrics *serverMetrics, sessionArgs sql.SessionArgs) v3Conn {
	return v3Conn{
		conn:               conn,
		rd:                 bufio.NewReader(conn),
		wr:                 bufio.NewWriter(conn),
		executor:           executor,
		writeBuf:           writeBuffer{bytecount: metrics.bytesOutCount},
		preparedStatements: make(map[string]preparedStatement),
		preparedPortals:    make(map[string]preparedPortal),
		metrics:            metrics,
		session:            sql.NewSession(sessionArgs, executor, conn.RemoteAddr()),
	}
}

func (c *v3Conn) finish() {
	// This is better than always flushing on error.
	if err := c.wr.Flush(); err != nil {
		log.Error(err)
	}
	_ = c.conn.Close()
	c.session.Finish()
}

func parseOptions(data []byte) (sql.SessionArgs, error) {
	args := sql.SessionArgs{}
	buf := readBuffer{msg: data}
	for {
		key, err := buf.getString()
		if err != nil {
			return sql.SessionArgs{}, util.Errorf("error reading option key: %s", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := buf.getString()
		if err != nil {
			return sql.SessionArgs{}, util.Errorf("error reading option value: %s", err)
		}
		switch key {
		case "database":
			args.Database = value
		case "user":
			args.User = value
		default:
			if log.V(1) {
				log.Warningf("unrecognized configuration parameter %q", key)
			}
		}
	}
	return args, nil
}

func (c *v3Conn) serve(authenticationHook func(string, bool) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if authenticationHook != nil {
		if err := authenticationHook(c.session.User, true /* public */); err != nil {
			return c.sendInternalError(err.Error())
		}
	}
	c.writeBuf.initMsg(serverMsgAuth)
	c.writeBuf.putInt32(authOK)
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	for key, value := range map[string]string{
		"client_encoding": "UTF8",
		"DateStyle":       "ISO",
		// The latest version of the docs that was consulted during the development
		// of this package. We specify this version to avoid having to support old
		// code paths which various client tools fall back to if they can't
		// determine that the server is new enough.
		"server_version": "9.5.0",
	} {
		c.writeBuf.initMsg(serverMsgParameterStatus)
		for _, str := range [...]string{key, value} {
			if err := c.writeBuf.writeString(str); err != nil {
				return err
			}
		}
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}

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
				log.Infof("pgwire: %s: %q", serverMsgReady, txnStatus)
			}
			c.writeBuf.WriteByte(txnStatus)
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
		c.metrics.bytesInCount.Inc(int64(n))
		if err != nil {
			return err
		}
		// When an error occurs handling an extended query message, we have to ignore
		// any messages until we get a sync.
		if c.ignoreTillSync && typ != clientMsgSync {
			if log.V(2) {
				log.Infof("pgwire: ignoring %s till sync", typ)
			}
			continue
		}
		if log.V(2) {
			log.Infof("pgwire: processing %s", typ)
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
			err = c.handleDescribe(&c.readBuf)

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

		default:
			err = c.sendInternalError(fmt.Sprintf("unrecognized client message type %s", typ))
		}
		if err != nil {
			return err
		}
	}
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
		if _, ok := c.preparedStatements[name]; ok {
			return c.sendInternalError(fmt.Sprintf("prepared statement %q already exists", name))
		}
	}
	query, err := buf.getString()
	if err != nil {
		return err
	}
	numParamTypes, err := buf.getInt16()
	if err != nil {
		return err
	}

	inTypeHints := make([]oid.Oid, numParamTypes)
	for i := range inTypeHints {
		typ, err := buf.getInt32()
		if err != nil {
			return err
		}
		inTypeHints[i] = oid.Oid(typ)
	}
	args := make(parser.MapArgs)
	for i, t := range inTypeHints {
		if t == 0 {
			continue
		}
		v, ok := oidToDatum[t]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown oid type: %v", t))
		}
		args[fmt.Sprint(i+1)] = v
	}
	cols, pErr := c.executor.Prepare(ctx, query, c.session, args)
	if pErr != nil {
		return c.sendPError(pErr)
	}
	pq := preparedStatement{
		query:       query,
		inTypes:     make([]oid.Oid, 0, len(args)),
		portalNames: make(map[string]struct{}),
		columns:     cols,
	}
	for k, v := range args {
		i, err := strconv.Atoi(k)
		if err != nil {
			return c.sendInternalError(fmt.Sprintf("non-integer parameter: %s", k))
		}
		// ValArgs are 1-indexed, pq.inTypes are 0-indexed.
		i--
		if i < 0 {
			return c.sendInternalError(fmt.Sprintf("there is no parameter $%s", k))
		}
		// Grow pq.inTypes to be at least as large as i.
		for j := len(pq.inTypes); j <= i; j++ {
			pq.inTypes = append(pq.inTypes, 0)
			if j < len(inTypeHints) {
				pq.inTypes[j] = inTypeHints[j]
			}
		}
		// OID to Datum is not a 1-1 mapping (for example, int4 and int8 both map
		// to DummyInt), so we need to maintain the types sent by the client.
		if pq.inTypes[i] != 0 {
			continue
		}
		id, ok := datumToOid[reflect.TypeOf(v)]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown datum type: %s", v.Type()))
		}
		pq.inTypes[i] = id
	}
	for i := range pq.inTypes {
		if pq.inTypes[i] == 0 {
			return c.sendInternalError(
				fmt.Sprintf("could not determine data type of parameter $%d", i+1))
		}
	}
	c.preparedStatements[name] = pq
	c.writeBuf.initMsg(serverMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleDescribe(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendInternalError(err.Error())
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		stmt, ok := c.preparedStatements[name]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", name))
		}
		c.writeBuf.initMsg(serverMsgParameterDescription)
		c.writeBuf.putInt16(int16(len(stmt.inTypes)))
		for _, t := range stmt.inTypes {
			c.writeBuf.putInt32(int32(t))
		}
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}

		return c.sendRowDescription(stmt.columns, nil)
	case preparePortal:
		prtl, ok := c.preparedPortals[name]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown portal %q", name))
		}
		stmt, ok := c.preparedStatements[prtl.stmtName]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", name))
		}

		return c.sendRowDescription(stmt.columns, prtl.outFormats)
	default:
		return util.Errorf("unknown describe type: %s", typ)
	}
}

func (c *v3Conn) handleClose(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendInternalError(err.Error())
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		if stmt, ok := c.preparedStatements[name]; ok {
			for portalName := range stmt.portalNames {
				delete(c.preparedPortals, portalName)
			}
		}
		delete(c.preparedStatements, name)
	case preparePortal:
		if prtl, ok := c.preparedPortals[name]; ok {
			if stmt, ok := c.preparedStatements[prtl.stmtName]; ok {
				delete(stmt.portalNames, name)
			}
		}
		delete(c.preparedPortals, name)
	default:
		return util.Errorf("unknown close type: %s", typ)
	}
	return nil
}

func (c *v3Conn) handleBind(buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := c.preparedPortals[portalName]; ok {
			return c.sendInternalError(fmt.Sprintf("portal %q already exists", portalName))
		}
	}
	statementName, err := buf.getString()
	if err != nil {
		return err
	}
	stmt, ok := c.preparedStatements[statementName]
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", statementName))
	}

	numParams := int16(len(stmt.inTypes))
	paramFormatCodes := make([]formatCode, numParams)

	// From the docs on number of parameter format codes to bind:
	// This can be zero to indicate that there are no parameters or that the
	// parameters all use the default format (text); or one, in which case the
	// specified format code is applied to all parameters; or it can equal the
	// actual number of parameters.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numParamFormatCodes, err := buf.getInt16()
	if err != nil {
		return err
	}
	switch numParamFormatCodes {
	case 0:
	case 1:
		// `1` means read one code and apply it to every param.
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range paramFormatCodes {
			paramFormatCodes[i] = fmtCode
		}
	case numParams:
		// Read one format code for each param and apply it to that param.
		for i := range paramFormatCodes {
			c, err := buf.getInt16()
			if err != nil {
				return err
			}
			paramFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("wrong number of format codes specified: %d for %d paramaters", numParamFormatCodes, numParams))
	}

	numValues, err := buf.getInt16()
	if err != nil {
		return err
	}
	if numValues != numParams {
		return c.sendInternalError(fmt.Sprintf("expected %d parameters, got %d", numParams, numValues))
	}
	params := make([]parser.Datum, numParams)
	for i, t := range stmt.inTypes {
		plen, err := buf.getInt32()
		if err != nil {
			return err
		}
		if plen == -1 {
			// TODO(mjibson): a NULL parameter, figure out what this should do
			continue
		}
		b, err := buf.getBytes(int(plen))
		if err != nil {
			return err
		}
		d, err := decodeOidDatum(t, paramFormatCodes[i], b)
		if err != nil {
			return c.sendInternalError(fmt.Sprintf("param $%d: %s", i+1, err))
		}
		params[i] = d
	}

	numColumns := int16(len(stmt.columns))
	columnFormatCodes := make([]formatCode, numColumns)

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.getInt16()
	if err != nil {
		return err
	}
	switch numColumnFormatCodes {
	case 0:
	case 1:
		// Read one code and apply it to every column.
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range columnFormatCodes {
			columnFormatCodes[i] = formatCode(fmtCode)
		}
	case numColumns:
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			c, err := buf.getInt16()
			if err != nil {
				return err
			}
			columnFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("expected 0, 1, or %d for number of format codes, got %d", numColumns, numColumnFormatCodes))
	}
	stmt.portalNames[portalName] = struct{}{}
	c.preparedPortals[portalName] = preparedPortal{
		stmt:       stmt,
		stmtName:   statementName,
		params:     params,
		outFormats: columnFormatCodes,
	}
	c.writeBuf.initMsg(serverMsgBindComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleExecute(ctx context.Context, buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}
	portal, ok := c.preparedPortals[portalName]
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown portal %q", portalName))
	}
	limit, err := buf.getInt32()
	if err != nil {
		return err
	}

	return c.executeStatements(ctx, portal.stmt.query, portal.params, portal.outFormats, false, limit)
}

func (c *v3Conn) executeStatements(
	ctx context.Context,
	stmts string,
	params []parser.Datum,
	formatCodes []formatCode,
	sendDescription bool,
	limit int32,
) error {
	tracing.AnnotateTrace()
	results := c.executor.ExecuteStatements(ctx, c.session, stmts, params)

	tracing.AnnotateTrace()
	if results.Empty {
		// Skip executor and just send EmptyQueryResponse.
		c.writeBuf.initMsg(serverMsgEmptyQuery)
		return c.writeBuf.finishMsg(c.wr)
	}
	return c.sendResponse(results.ResultList, formatCodes, sendDescription, limit)
}

func (c *v3Conn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(serverMsgCommandComplete)
	c.writeBuf.Write(tag)
	c.writeBuf.WriteByte(0)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) sendPError(pErr *roachpb.Error) error {
	var errCode string
	if sqlErr, ok := pErr.GetDetail().(*roachpb.ErrorWithPGCode); ok {
		errCode = sqlErr.ErrorCode
	} else {
		errCode = sql.CodeInternalError
	}
	return c.sendError(errCode, pErr.String())
}

// TODO(andrei): Figure out the correct codes to send for all the errors
// in this file and remove this function.
func (c *v3Conn) sendInternalError(errToSend string) error {
	return c.sendError(sql.CodeInternalError, errToSend)
}

// errCode is a postgres error code, plus our extensions.
// See http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html
func (c *v3Conn) sendError(errCode string, errToSend string) error {
	if c.doingExtendedQueryMessage {
		c.ignoreTillSync = true
	}

	c.writeBuf.initMsg(serverMsgErrorResponse)
	if err := c.writeBuf.WriteByte('S'); err != nil {
		return err
	}
	if err := c.writeBuf.writeString("ERROR"); err != nil {
		return err
	}
	if err := c.writeBuf.WriteByte('C'); err != nil {
		return err
	}
	if err := c.writeBuf.writeString(errCode); err != nil {
		return err
	}
	if err := c.writeBuf.WriteByte('M'); err != nil {
		return err
	}
	if err := c.writeBuf.writeString(errToSend); err != nil {
		return err
	}
	if err := c.writeBuf.WriteByte(0); err != nil {
		return err
	}
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	return c.wr.Flush()
}

func (c *v3Conn) sendResponse(results sql.ResultList, formatCodes []formatCode, sendDescription bool, limit int32) error {
	if len(results) == 0 {
		return c.sendCommandComplete(nil)
	}
	for _, result := range results {
		if result.PErr != nil {
			if err := c.sendPError(result.PErr); err != nil {
				return err
			}
			break
		}
		if limit != 0 && len(result.Rows) > int(limit) {
			if err := c.sendInternalError(fmt.Sprintf("execute row count limits not supported: %d of %d", limit, len(result.Rows))); err != nil {
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
				if err := c.sendRowDescription(result.Columns, formatCodes); err != nil {
					return err
				}
			}

			// Send DataRows.
			for _, row := range result.Rows {
				c.writeBuf.initMsg(serverMsgDataRow)
				c.writeBuf.putInt16(int16(len(row.Values)))
				for i, col := range row.Values {
					fmtCode := formatText
					if formatCodes != nil {
						fmtCode = formatCodes[i]
					}
					switch fmtCode {
					case formatText:
						if err := c.writeBuf.writeTextDatum(col); err != nil {
							return err
						}
					case formatBinary:
						if err := c.writeBuf.writeBinaryDatum(col); err != nil {
							return err
						}
					default:
						return util.Errorf("unsupported format code %s", fmtCode)
					}
				}
				if err := c.writeBuf.finishMsg(c.wr); err != nil {
					return err
				}
			}

			// Send CommandComplete.
			tag = append(tag, ' ')
			tag = appendUint(tag, uint(len(result.Rows)))
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		// Ack messages do not have a corresponding protobuf field, so handle those
		// with a default.
		// This also includes DDLs which want CommandComplete as well.
		default:
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *v3Conn) sendRowDescription(columns []sql.ResultColumn, formatCodes []formatCode) error {
	if len(columns) == 0 {
		c.writeBuf.initMsg(serverMsgNoData)
		return c.writeBuf.finishMsg(c.wr)
	}

	c.writeBuf.initMsg(serverMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))
	for i, column := range columns {
		if log.V(2) {
			log.Infof("pgwire writing column %s of type: %T", column.Name, column.Typ)
		}
		if err := c.writeBuf.writeString(column.Name); err != nil {
			return err
		}

		typ := typeForDatum(column.Typ)
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

func appendUint(in []byte, u uint) []byte {
	var buf []byte
	if cap(in)-len(in) >= 10 {
		buf = in[len(in) : len(in)+10]
	} else {
		buf = make([]byte, 10)
	}
	i := len(buf)

	for u >= 10 {
		i--
		q := u / 10
		buf[i] = byte(u - q*10 + '0')
		u = q
	}
	// u < 10
	i--
	buf[i] = byte(u + '0')

	return append(in, buf[i:]...)
}
