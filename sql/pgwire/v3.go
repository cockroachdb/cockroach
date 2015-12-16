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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package pgwire

import (
	"bufio"
	"fmt"
	"net"
	"strconv"

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
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
	clientBind           clientMessageType = 'B'
	clientExecute        clientMessageType = 'E'

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
)

type prepareType byte

const (
	statement prepareType = 'S'
	portal    prepareType = 'P'
)

const (
	authOK int32 = 0
)

type preparedStatement struct {
	query       string
	inTypes     []oid.Oid
	columns     []*driver.Response_Result_Rows_Column
	portalNames map[string]struct{}
}

type preparedPortal struct {
	statement     preparedStatement
	statementName string
	params        []driver.Datum
	outFormats    []formatCode
}

type v3Conn struct {
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
}

type opts struct {
	user, database string
}

func makeV3Conn(conn net.Conn, executor *sql.Executor) v3Conn {
	return v3Conn{
		rd:                 bufio.NewReader(conn),
		wr:                 bufio.NewWriter(conn),
		executor:           executor,
		preparedStatements: make(map[string]preparedStatement),
		preparedPortals:    make(map[string]preparedPortal),
	}
}

func (c *v3Conn) parseOptions(data []byte) error {
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
			c.opts.database = value
		case "user":
			c.opts.user = value
		default:
			log.Warningf("unrecognized configuration parameter %q", key)
		}
	}
}

func (c *v3Conn) serve(authenticationHook func(string, bool) error) error {
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
	if err := c.wr.Flush(); err != nil {
		return err
	}

	for {
		if !c.doingExtendedQueryMessage {
			c.writeBuf.initMsg(serverMsgReady)
			var txnStatus byte = 'I'
			if sessionTxn := c.session.Txn; sessionTxn != nil {
				switch sessionTxn.Txn.Status {
				case roachpb.PENDING:
					txnStatus = 'T'
				case roachpb.COMMITTED:
					txnStatus = 'I'
				case roachpb.ABORTED:
					txnStatus = 'E'
				}
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
		typ, err := c.readBuf.readTypedMsg(c.rd)
		if err != nil {
			return err
		}
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
		case clientMsgSimpleQuery:
			c.doingExtendedQueryMessage = false
			err = c.handleSimpleQuery(&c.readBuf)

		case clientMsgParse:
			c.doingExtendedQueryMessage = true
			err = c.handleParse(&c.readBuf)

		case clientMsgTerminate:
			return nil

		case clientMsgDescribe:
			c.doingExtendedQueryMessage = true
			err = c.handleDescribe(&c.readBuf)

		case clientMsgSync:
			c.doingExtendedQueryMessage = false
			c.ignoreTillSync = false

		case clientMsgClose:
			c.doingExtendedQueryMessage = true
			err = c.handleClose(&c.readBuf)

		case clientBind:
			c.doingExtendedQueryMessage = true
			err = c.handleBind(&c.readBuf)

		case clientExecute:
			c.doingExtendedQueryMessage = true
			err = c.handleExecute(&c.readBuf)

		default:
			err = c.sendError(fmt.Sprintf("unrecognized client message type %s", typ))
		}
		if err != nil {
			return err
		}
	}
}

func (c *v3Conn) handleSimpleQuery(buf *readBuffer) error {
	query, err := buf.getString()
	if err != nil {
		return err
	}

	c.session.Database = c.opts.database

	req := driver.Request{
		User: c.opts.user,
		Sql:  query,
	}
	if req.Session, err = c.session.Marshal(); err != nil {
		return err
	}

	resp, _, err := c.executor.Execute(req)
	if err != nil {
		return c.sendError(err.Error())
	}

	c.session.Reset()
	if err := c.session.Unmarshal(resp.Session); err != nil {
		return err
	}

	c.opts.database = c.session.Database

	return c.sendResponse(resp, nil, true)
}

func (c *v3Conn) handleParse(buf *readBuffer) error {
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
	pq := preparedStatement{
		query:       query,
		inTypes:     make([]oid.Oid, numParamTypes),
		portalNames: make(map[string]struct{}),
	}
	for i := range pq.inTypes {
		typ, err := buf.getInt32()
		if err != nil {
			return err
		}
		pq.inTypes[i] = oid.Oid(typ)
	}
	stmt, err := parser.ParseOne(query, parser.Traditional)
	if err != nil {
		return c.sendError(err.Error())
	}
	args := make(parser.MapArgs)
	for i, t := range pq.inTypes {
		v, ok := oidToDatum[t]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown oid type: %v", t))
		}
		args[fmt.Sprint(i+1)] = v
	}
	if err := parser.FillArgsOptional(stmt, args); err != nil {
		return c.sendError(err.Error())
	}
	if err := parser.CheckArgs(stmt, args); err != nil {
		return c.sendError(err.Error())
	}
	if err := parser.FillArgs(stmt, args); err != nil {
		return c.sendError(err.Error())
	}
	// Check a second time with no args to make sure there are no ValArgs left.
	if err := parser.CheckArgs(stmt, nil); err != nil {
		return c.sendError(err.Error())
	}
	pq.inTypes = make([]oid.Oid, len(args))
	for k, v := range args {
		i, err := strconv.Atoi(k)
		if err != nil {
			return c.sendError(fmt.Sprintf("non-integer parameter name: %s", k))
		}
		id, ok := datumToOid[v]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown datum type: %s", v))
		}
		pq.inTypes[i-1] = id
	}
	cols, err := c.executor.StatementResult(c.opts.user, stmt)
	if err != nil {
		return c.sendError(err.Error())
	}
	pq.columns = cols
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
	var stmt preparedStatement
	switch typ {
	case statement:
		var ok bool
		if stmt, ok = c.preparedStatements[name]; !ok {
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
	case portal:
		prtl, ok := c.preparedPortals[name]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown portal %q", name))
		}
		if stmt, ok = c.preparedStatements[prtl.statementName]; !ok {
			return c.sendError(fmt.Sprintf("unknown prepared statement %q", name))
		}
	}

	return c.sendRowDescription(stmt.columns)
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
	case statement:
		if stmt, ok := c.preparedStatements[name]; ok {
			for portalName := range stmt.portalNames {
				delete(c.preparedPortals, portalName)
			}
		}
		delete(c.preparedStatements, name)
	case portal:
		if prtl, ok := c.preparedPortals[name]; ok {
			if stmt, ok := c.preparedStatements[prtl.statementName]; ok {
				delete(stmt.portalNames, name)
			}
		}
		delete(c.preparedPortals, name)
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
	params := make([]driver.Datum, numParams)
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
		var d driver.Datum
		switch t {
		case oid.T_bool:
			switch paramFormatCodes[i] {
			case formatText:
				var v bool
				switch string(b) {
				case "true":
					v = true
				case "false":
					v = false
				default:
					return c.sendError(fmt.Sprintf("unknown bool value: %s", b))
				}
				d.Payload = &driver.Datum_BoolVal{BoolVal: v}
			default:
				return c.sendError("unsupported: binary bool parameter")
			}
		case oid.T_int8:
			switch paramFormatCodes[i] {
			case formatText:
				i, err := strconv.ParseInt(string(b), 10, 64)
				if err != nil {
					return c.sendError(fmt.Sprintf("unknown int value: %s", b))
				}
				d.Payload = &driver.Datum_IntVal{IntVal: i}
			default:
				return c.sendError("unsupported: binary int parameter")
			}
		case oid.T_float8:
			switch paramFormatCodes[i] {
			case formatText:
				f, err := strconv.ParseFloat(string(b), 64)
				if err != nil {
					return c.sendError(fmt.Sprintf("unknown float value: %s", b))
				}
				d.Payload = &driver.Datum_FloatVal{FloatVal: f}
			default:
				return c.sendError("unsupported: binary float parameter")
			}
		default:
			return c.sendError(fmt.Sprintf("unsupported: %v", t))
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
		statement:     stmt,
		statementName: statementName,
		params:        params,
		outFormats:    columnFormatCodes,
	}
	c.writeBuf.initMsg(serverMsgBindComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleExecute(buf *readBuffer) error {
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
	if limit != 0 {
		return c.sendError("execute row count limits not supported")
	}

	c.session.Database = c.opts.database

	resp, _, err := c.executor.ExecuteStatement(c.opts.user, c.session, portal.statement.query, portal.params)
	if err != nil {
		return c.sendError(err.Error())
	}

	// TODO(mjibson): is this session stuff correct?
	c.session.Reset()
	if err := c.session.Unmarshal(resp.Session); err != nil {
		return err
	}

	c.opts.database = c.session.Database
	return c.sendResponse(resp, portal.outFormats, false)
}

func (c *v3Conn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(serverMsgCommandComplete)
	c.writeBuf.Write(tag)
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

func (c *v3Conn) sendResponse(resp driver.Response, formatCodes []formatCode, sendDescription bool) error {
	if len(resp.Results) == 0 {
		return c.sendCommandComplete(nil)
	}
	for _, result := range resp.Results {
		if result.Error != nil {
			if err := c.sendError(*result.Error); err != nil {
				return err
			}
			continue
		}

		switch result := result.GetUnion().(type) {
		case *driver.Response_Result_DDL_:
			// Send EmptyQueryResponse.
			c.writeBuf.initMsg(serverMsgEmptyQuery)
			return c.writeBuf.finishMsg(c.wr)

		case *driver.Response_Result_RowsAffected:
			// Send CommandComplete.
			// TODO(bdarnell): tags for other types of commands.
			tag := append(c.tagBuf[:0], "SELECT "...)
			tag = strconv.AppendInt(tag, int64(result.RowsAffected), 10)
			tag = append(tag, byte(0))
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		case *driver.Response_Result_Rows_:
			resultRows := result.Rows

			if sendDescription {
				if err := c.sendRowDescription(resultRows.Columns); err != nil {
					return err
				}
			}

			if formatCodes == nil {
				formatCodes = make([]formatCode, len(resultRows.Columns))
				for i, column := range resultRows.Columns {
					formatCodes[i] = typeForDatum(column.Typ).preferredFormat
				}
			}

			// Send DataRows.
			for _, row := range resultRows.Rows {
				c.writeBuf.initMsg(serverMsgDataRow)
				c.writeBuf.putInt16(int16(len(row.Values)))
				for i, col := range row.Values {
					switch formatCode := formatCodes[i]; formatCode {
					case formatText:
						if err := c.writeBuf.writeTextDatum(col); err != nil {
							return err
						}
					case formatBinary:
						if err := c.writeBuf.writeBinaryDatum(col); err != nil {
							return err
						}
					default:
						return util.Errorf("unsupported format code %s", formatCode)
					}
				}
				if err := c.writeBuf.finishMsg(c.wr); err != nil {
					return err
				}
			}

			// Send CommandComplete.
			// TODO(bdarnell): tags for other types of commands.
			tag := append(c.tagBuf[:0], "SELECT "...)
			tag = appendUint(tag, uint(len(resultRows.Rows)))
			tag = append(tag, byte(0))
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *v3Conn) sendRowDescription(columns []*driver.Response_Result_Rows_Column) error {
	c.writeBuf.initMsg(serverMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))
	for _, column := range columns {
		if log.V(2) {
			log.Infof("pgwire writing column %s of type: %T", column.Name, column.Typ.Payload)
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
		c.writeBuf.putInt16(int16(typ.preferredFormat))
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
