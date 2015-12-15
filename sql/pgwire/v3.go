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

type messageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	serverMsgAuth                 messageType = 'R'
	serverMsgCommandComplete                  = 'C'
	serverMsgDataRow                          = 'D'
	serverMsgErrorResponse                    = 'E'
	serverMsgParseComplete                    = '1'
	serverMsgReady                            = 'Z'
	serverMsgRowDescription                   = 'T'
	serverMsgEmptyQuery                       = 'I'
	serverMsgParameterDescription             = 't'
	serverMsgBindComplete                     = '2'

	clientMsgSimpleQuery = 'Q'
	clientMsgParse       = 'P'
	clientMsgTerminate   = 'X'
	clientMsgDescribe    = 'D'
	clientMsgSync        = 'S'
	clientMsgClose       = 'C'
	clientBind           = 'B'
	clientExecute        = 'E'
)

const (
	authOK int32 = 0
)

type parsedQuery struct {
	query   string
	types   []oid.Oid
	columns []*driver.Response_Result_Rows_Column
	params  []driver.Datum
}

type v3Conn struct {
	rd              *bufio.Reader
	wr              *bufio.Writer
	opts            opts
	executor        *sql.Executor
	parsed          map[string]parsedQuery
	readBuf         readBuffer
	writeBuf        writeBuffer
	tagBuf          [64]byte
	session         sql.Session
	pq              *parsedQuery
	inExtendedQuery bool
	waitForSync bool
}

type opts struct {
	user, database string
}

func makeV3Conn(conn net.Conn, executor *sql.Executor) v3Conn {
	return v3Conn{
		rd:       bufio.NewReader(conn),
		wr:       bufio.NewWriter(conn),
		executor: executor,
		parsed:   make(map[string]parsedQuery),
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
	// Check Postgres sources for when this should be set.
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c
	c.inExtendedQuery = false
	for {
		if !c.inExtendedQuery {
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
				log.Infof("pgwire writing transaction status: %q", txnStatus)
			}
			c.writeBuf.WriteByte(txnStatus)
			if err := c.writeBuf.finishMsg(c.wr); err != nil {
				return err
			}
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
		typ, err := c.readBuf.readTypedMsg(c.rd)
		if err != nil {
			return err
		}
		fmt.Println("SRV GOT", string(typ))
		if c.waitForSync && typ != clientMsgSync {
			fmt.Println("SRV wait for sync, ignore", string(typ))
			continue
		}
		switch typ {
		case clientMsgSimpleQuery:
			err = c.handleSimpleQuery(&c.readBuf)

		case clientMsgParse:
			c.inExtendedQuery = true
			err = c.handleParse(&c.readBuf)

		case clientMsgTerminate:
			return nil

		case clientMsgDescribe:
			c.inExtendedQuery = true
			err = c.handleDescribe(&c.readBuf)

		case clientMsgSync:
			c.inExtendedQuery = false
			c.waitForSync = false

		case clientMsgClose:
			c.inExtendedQuery = true
			err = c.handleClose(&c.readBuf)

		case clientBind:
			c.inExtendedQuery = true
			err = c.handleBind(&c.readBuf)

		case clientExecute:
			c.inExtendedQuery = true
			err = c.handleExecute(&c.readBuf)

		default:
			log.Fatalf("unrecognized client message type %c", typ)
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

	return c.sendResponse(resp, true)
}

func (c *v3Conn) handleParse(buf *readBuffer) error {
	name, err := buf.getString()
	if err != nil {
		return err
	}
	if _, ok := c.parsed[name]; ok && name != "" {
		return c.sendError(fmt.Sprintf("prepared statement %q already exists", name))
	}
	query, err := buf.getString()
	if err != nil {
		return err
	}
	stmts, err := parser.ParseTraditional(query)
	if err != nil {
		return c.sendError(err.Error())
	}
	if len(stmts) != 1 {
		return c.sendError("expected exactly one statement")
	}
	stmt := stmts[0]
	numTypes, err := buf.getInt16()
	if err != nil {
		return err
	}
	pq := parsedQuery{
		query: query,
		types: make([]oid.Oid, numTypes),
	}
	for i := int16(0); i < numTypes; i++ {
		typ, err := buf.getInt32()
		if err != nil {
			return err
		}
		pq.types[i] = oid.Oid(typ)
	}
	args := make(parser.MapArgs)
	for i, t := range pq.types {
		v, ok := oidToDatum[t]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown oid type: %v", t))
		}
		args[fmt.Sprint(i+1)] = v
	}
	if err := parser.FillArgsOptional(stmt, args); err != nil {
		return c.sendError(err.Error())
	}
	if err := CheckArgs(stmt, args); err != nil {
		return c.sendError(err.Error())
	}
	if err := parser.FillArgs(stmt, args); err != nil {
		return c.sendError(err.Error())
	}
	// Check a second time with no args to make sure there are no ValArgs left.
	if err := CheckArgs(stmt, nil); err != nil {
		return c.sendError(err.Error())
	}
	pq.types = make([]oid.Oid, len(args))
	for k, v := range args {
		i, err := strconv.Atoi(k)
		if err != nil {
			return c.sendError(fmt.Sprintf("non-integer parameter name: %s", k))
		}
		id, ok := datumToOid[v]
		if !ok {
			return c.sendError(fmt.Sprintf("unknown datum type: %s", v))
		}
		pq.types[i-1] = id
	}
	cols, err := c.executor.StatementResult(c.opts.user, stmt)
	if err != nil {
		return c.sendError(err.Error())
	}
	pq.columns = cols
	c.parsed[name] = pq
	c.writeBuf.initMsg(serverMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

type checkVisitor struct {
	args parser.MapArgs
	err  error
}

func (v *checkVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.err != nil {
		return nil, expr
	}
	_, err := expr.TypeCheck(v.args)
	if err != nil {
		v.err = err
	}
	return v, expr
}

func CheckArgs(stmt parser.Statement, args parser.MapArgs) error {
	v := checkVisitor{args: args}
	parser.WalkStmt(&v, stmt)
	return v.err
}

func (c *v3Conn) handleDescribe(buf *readBuffer) error {
	kind, err := buf.getByte()
	if err != nil {
		return err
	} else if kind != 'S' {
		return c.sendError("portals are unsupported")
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	pq, ok := c.parsed[name]
	if !ok {
		return c.sendError(fmt.Sprintf("unknown prepared statement %q", name))
	}
	c.writeBuf.initMsg(serverMsgParameterDescription)
	c.writeBuf.putInt16(int16(len(pq.types)))
	for _, t := range pq.types {
		c.writeBuf.putInt32(int32(t))
	}
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	return c.sendRowDescription(pq.columns)
}

func (c *v3Conn) handleClose(buf *readBuffer) error {
	kind, err := buf.getByte()
	if err != nil {
		return err
	} else if kind != 'S' {
		return c.sendError("portals are unsupported")
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	delete(c.parsed, name)
	c.pq = nil
	return nil
}

func (c *v3Conn) handleBind(buf *readBuffer) error {
	c.pq = nil
	if portal, err := buf.getString(); err != nil {
		return err
	} else if portal != "" {
		return c.sendError("portals are unsupported")
	}
	name, err := buf.getString()
	if err != nil {
		return err
	}
	pq, ok := c.parsed[name]
	if !ok {
		return c.sendError(fmt.Sprintf("unknown prepared statement %q", name))
	}
	numCodes, err := buf.getInt16()
	if err != nil {
		return err
	}
	formatCodes := make(map[int]int16)
	for i := 0; i < int(numCodes); i++ {
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		formatCodes[i] = c
		fmt.Println("SRV FMT CODE", i, formatCodes[i])
	}
	np, err := buf.getInt16()
	if err != nil {
		return err
	}
	numParams := int(np)
	if numParams != len(pq.types) {
		return c.sendError(fmt.Sprintf("expected %d parameters, got %d", len(pq.types), numParams))
	}
	if len(formatCodes) == 1 {
		for i := 1; i < numParams; i++ {
			formatCodes[i] = formatCodes[0]
		}
	}
	params := make([]driver.Datum, numParams)
	for i, t := range pq.types {
		_, _ = i, t
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
			switch formatCodes[i] {
			case 0:
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
			switch formatCodes[i] {
			case 0:
				i, err := strconv.ParseInt(string(b), 10, 64)
				if err != nil {
					return c.sendError(fmt.Sprintf("unknown int value: %s", b))
				}
				d.Payload = &driver.Datum_IntVal{IntVal: i}
			default:
				return c.sendError("unsupported: binary int parameter")
			}
		case oid.T_float8:
			switch formatCodes[i] {
			case 0:
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
	pq.params = params
	c.pq = &pq
	c.writeBuf.initMsg(serverMsgBindComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) handleExecute(buf *readBuffer) error {
	if portal, err := buf.getString(); err != nil {
		return err
	} else if portal != "" {
		return c.sendError("portals are unsupported")
	}
	limit, err := buf.getInt32()
	if err != nil {
		return err
	}
	if limit != 0 {
		return c.sendError("execute row count limits not supported")
	}
	if c.pq == nil {
		return c.sendError("no existing prepared statement")
	}

	c.session.Database = c.opts.database

	resp, _, err := c.executor.ExecuteStatement(c.opts.user, c.session, c.pq.query, c.pq.params)
	if err != nil {
		return c.sendError(err.Error())
	}

	// TODO(mjibson): is this session stuff correct?
	c.session.Reset()
	if err := c.session.Unmarshal(resp.Session); err != nil {
		return err
	}

	c.opts.database = c.session.Database
	return c.sendResponse(resp, false)
}

func (c *v3Conn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(serverMsgCommandComplete)
	c.writeBuf.Write(tag)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) sendError(errToSend string) error {
	if c.inExtendedQuery {
		c.waitForSync = true
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
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	return c.wr.Flush()
}

func (c *v3Conn) sendResponse(resp driver.Response, sendDescription bool) error {
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

			// Send DataRows.
			for _, row := range resultRows.Rows {
				c.writeBuf.initMsg(serverMsgDataRow)
				c.writeBuf.putInt16(int16(len(row.Values)))
				for _, col := range row.Values {
					if err := c.writeBuf.writeDatum(col); err != nil {
						return err
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
