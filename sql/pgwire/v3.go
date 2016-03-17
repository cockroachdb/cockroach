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
	"golang.org/x/net/trace"

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
	opts     opts
	executor *sql.Executor
	readBuf  readBuffer
	writeBuf writeBuffer
	tagBuf   [64]byte
	session  sql.Session

	preparedStatements map[string]preparedStatement
	preparedPortals    map[string]preparedPortal

	// The logic governing these guys is hairy, and is not sufficiently
	// specified in documentation. Consult the sources before you modify:
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	doingExtendedQueryMessage, ignoreTillSync bool

	metrics *serverMetrics
}

type opts struct {
	user string
}

func makeV3Conn(conn net.Conn, executor *sql.Executor, metrics *serverMetrics) v3Conn {
	return v3Conn{
		conn:               conn,
		rd:                 bufio.NewReader(conn),
		wr:                 bufio.NewWriter(conn),
		executor:           executor,
		writeBuf:           writeBuffer{bytecount: metrics.bytesOutCount},
		preparedStatements: make(map[string]preparedStatement),
		preparedPortals:    make(map[string]preparedPortal),
		metrics:            metrics,
	}
}

func (c *v3Conn) finish() {
	// This is better than always flushing on error.
	if err := c.wr.Flush(); err != nil {
		log.Error(err)
	}
	_ = c.conn.Close()
	if c.session.Trace != nil {
		c.session.Trace.Finish()
	}
}

func (c *v3Conn) parseOptions(data []byte) error {
	defer func() {
		c.session.Trace = trace.New("sql."+c.opts.user, c.conn.RemoteAddr().String())
		c.session.Trace.SetMaxEvents(100)
	}()

	buf := readBuffer{msg: data}
	for {
		key, err := buf.getString()
		if err != nil {
			return util.Errorf("error reading option key: %s", err)
		}
		if len(key) == 0 {
			return nil
		}
		value, err := buf.getString()
		if err != nil {
			return util.Errorf("error reading option value: %s", err)
		}
		switch key {
		case "database":
			c.session.Database = value
		case "user":
			c.opts.user = value
		default:
			if log.V(1) {
				log.Warningf("unrecognized configuration parameter %q", key)
			}
		}
	}
}

func (c *v3Conn) serve(authenticationHook func(string, bool) error) error {
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	if authenticationHook != nil {
		if err := authenticationHook(c.opts.user, true /* public */); err != nil {
			return c.sendError(err.Error())
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
			case sql.Aborted:
				txnStatus = 'E'
			case sql.Open:
				txnStatus = 'T'
			case sql.RestartWait:
				// This state is not part of the Postgres protocol.
				txnStatus = 'R'
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
		}
		// If the buffer is empty (which is the case if ignoring messages),
		// this does nothing.
		if err := c.wr.Flush(); err != nil {
			return err
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

		default:
			err = c.sendError(fmt.Sprintf("unrecognized client message type %s", typ))
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
			return c.sendError(fmt.Sprintf("prepared statement %q already exists", name))
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
			return c.sendError(fmt.Sprintf("unknown oid type: %v", t))
		}
		args[fmt.Sprint(i+1)] = v
	}
	cols, pErr := c.executor.Prepare(ctx, c.opts.user, query, &c.session, args)
	if pErr != nil {
		return c.sendError(pErr.String())
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
			return c.sendError(fmt.Sprintf("non-integer parameter: %s", k))
		}
		// ValArgs are 1-indexed, pq.inTypes are 0-indexed.
		i--
		if i < 0 {
			return c.sendError(fmt.Sprintf("there is no parameter $%s", k))
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
			return c.sendError(fmt.Sprintf("unknown datum type: %s", v.Type()))
		}
		pq.inTypes[i] = id
	}
	for i := range pq.inTypes {
		if pq.inTypes[i] == 0 {
			return c.sendError(fmt.Sprintf("could not determine data type of parameter $%d", i+1))
		}
	}
	c.preparedStatements[name] = pq
	c.writeBuf.initMsg(serverMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleDescribe(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendError(err.Error())
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	switch typ {
	case prepareStatement:
		stmt, ok := c.preparedStatements[name]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown prepared statement %q", name))
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
			return c.sendError(fmt.Sprintf("unknown portal %q", name))
		}
		stmt, ok := c.preparedStatements[prtl.stmtName]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown prepared statement %q", name))
		}

		return c.sendRowDescription(stmt.columns, prtl.outFormats)
	default:
		return util.Errorf("unknown describe type: %s", typ)
	}
}

func (c *v3Conn) handleClose(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendError(err.Error())
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
			return c.sendError(fmt.Sprintf("portal %q already exists", portalName))
		}
	}
	statementName, err := buf.getString()
	if err != nil {
		return err
	}
	stmt, ok := c.preparedStatements[statementName]
	if !ok {
		return c.sendError(fmt.Sprintf("unknown prepared statement %q", statementName))
	}
	numParamFormatCodes, err := buf.getInt16()
	if err != nil {
		return err
	}
	numParams := len(stmt.inTypes)
	if int(numParamFormatCodes) > numParams {
		return c.sendError(fmt.Sprintf("too many format codes specified: %d for %d paramaters", numParamFormatCodes, numParams))
	}
	paramFormatCodes := make([]formatCode, numParams)
	for i := range paramFormatCodes[:numParamFormatCodes] {
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		paramFormatCodes[i] = formatCode(c)
	}
	if numParamFormatCodes == 1 {
		fmtCode := paramFormatCodes[0]

		for i := range paramFormatCodes {
			paramFormatCodes[i] = fmtCode
		}
	}
	numValues, err := buf.getInt16()
	if err != nil {
		return err
	}
	if int(numValues) != numParams {
		return c.sendError(fmt.Sprintf("expected %d parameters, got %d", numParams, numValues))
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
			return c.sendError(fmt.Sprintf("param $%d: %s", i+1, err))
		}
		params[i] = d
	}

	numColumnFormatCodes, err := buf.getInt16()
	if err != nil {
		return err
	}
	numColumns := len(stmt.columns)
	columnFormatCodes := make([]formatCode, numColumns)
	for i := range columnFormatCodes[:numColumnFormatCodes] {
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		columnFormatCodes[i] = formatCode(c)
	}
	if numColumnFormatCodes == 1 {
		fmtCode := columnFormatCodes[0]

		for i := range columnFormatCodes {
			columnFormatCodes[i] = formatCode(fmtCode)
		}
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
		return c.sendError(fmt.Sprintf("unknown portal %q", portalName))
	}
	limit, err := buf.getInt32()
	if err != nil {
		return err
	}

	return c.executeStatements(ctx, portal.stmt.query, portal.params, portal.outFormats, false, limit)
}

func (c *v3Conn) executeStatements(ctx context.Context, stmts string, params []parser.Datum, formatCodes []formatCode, sendDescription bool, limit int32) error {
	tracing.AnnotateTrace()
	results := c.executor.ExecuteStatements(ctx, c.opts.user, &c.session, stmts, params)

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

func (c *v3Conn) sendError(errToSend string) error {
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
	// "XX000" is "internal error".
	// TODO(bdarnell): map our errors to appropriate postgres error
	// codes as defined in
	// http://www.postgresql.org/docs/9.4/static/errcodes-appendix.html
	if err := c.writeBuf.writeString("XX000"); err != nil {
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
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) sendResponse(results sql.ResultList, formatCodes []formatCode, sendDescription bool, limit int32) error {
	if len(results) == 0 {
		return c.sendCommandComplete(nil)
	}
	for _, result := range results {
		if result.PErr != nil {
			if err := c.sendError(result.PErr.String()); err != nil {
				return err
			}
			break
		}
		if limit != 0 && len(result.Rows) > int(limit) {
			if err := c.sendError(fmt.Sprintf("execute row count limits not supported: %d of %d", limit, len(result.Rows))); err != nil {
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
