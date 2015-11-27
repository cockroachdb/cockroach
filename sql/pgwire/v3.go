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
	"net"
	"strconv"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/lib/pq/oid"
)

type messageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	serverMsgAuth            messageType = 'R'
	serverMsgCommandComplete             = 'C'
	serverMsgDataRow                     = 'D'
	serverMsgErrorResponse               = 'E'
	serverMsgParseComplete               = '1'
	serverMsgReady                       = 'Z'
	serverMsgRowDescription              = 'T'
	serverMsgEmptyQuery                  = 'I'

	clientMsgSimpleQuery = 'Q'
	clientMsgParse       = 'P'
	clientMsgTerminate   = 'X'
)

const (
	authOK int32 = 0
)

type parsedQuery struct {
	query string
	types []oid.Oid
}

type v3Conn struct {
	rd       *bufio.Reader
	wr       *bufio.Writer
	opts     map[string]string
	executor *sql.Executor
	parsed   map[string]parsedQuery
	readBuf  readBuffer
	writeBuf writeBuffer
	tagBuf   [64]byte
	session  sql.Session
}

func newV3Conn(conn net.Conn, data []byte, executor *sql.Executor) (*v3Conn, error) {
	v3conn := &v3Conn{
		rd:       bufio.NewReader(conn),
		wr:       bufio.NewWriter(conn),
		opts:     map[string]string{},
		executor: executor,
		parsed:   map[string]parsedQuery{},
	}
	if err := v3conn.parseOptions(data); err != nil {
		return nil, err
	}
	return v3conn, nil
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
		c.opts[key] = value
	}
}

func (c *v3Conn) serve() error {
	// TODO(bdarnell): real auth flow. For now just accept anything.
	c.writeBuf.initMsg(serverMsgAuth)
	c.writeBuf.putInt32(authOK)
	if err := c.writeBuf.finishMsg(c.wr); err != nil {
		return err
	}
	if err := c.wr.Flush(); err != nil {
		return err
	}
	for {
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
		typ, err := c.readBuf.readTypedMsg(c.rd)
		if err != nil {
			return err
		}
		switch typ {
		case clientMsgSimpleQuery:
			err = c.handleSimpleQuery(&c.readBuf)

		case clientMsgParse:
			err = c.handleParse(&c.readBuf)

		case clientMsgTerminate:
			return nil

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

	c.session.Database = c.opts["DATABASE"]

	req := driver.Request{
		User:    c.opts["user"],
		Sql:     query,
		Session: make([]byte, c.session.Size()),
	}
	if _, err := c.session.MarshalTo(req.Session); err != nil {
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

	c.opts["DATABASE"] = c.session.Database

	return c.sendResponse(resp)
}

func (c *v3Conn) handleParse(buf *readBuffer) error {
	name, err := buf.getString()
	if err != nil {
		return err
	}
	if _, ok := c.parsed[name]; ok && name != "" {
		return util.Errorf("prepared statement %q already exists", name)
	}
	query, err := buf.getString()
	if err != nil {
		return err
	}
	pq := parsedQuery{query: query}
	numTypes, err := buf.getInt16()
	if err != nil {
		return err
	}
	pq.types = make([]oid.Oid, numTypes)
	for i := int16(0); i < numTypes; i++ {
		typ, err := buf.getInt32()
		if err != nil {
			return err
		}
		pq.types[i] = oid.Oid(typ)
	}
	c.parsed[name] = pq
	c.writeBuf.initMsg(serverMsgParseComplete)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(serverMsgCommandComplete)
	c.writeBuf.Write(tag)
	return c.writeBuf.finishMsg(c.wr)
}

func (c *v3Conn) sendError(errToSend string) error {
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

func (c *v3Conn) sendResponse(resp driver.Response) error {
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

			// Send RowDescription.
			c.writeBuf.initMsg(serverMsgRowDescription)
			c.writeBuf.putInt16(int16(len(resultRows.Columns)))
			for _, column := range resultRows.Columns {
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
			if err := c.writeBuf.finishMsg(c.wr); err != nil {
				return err
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
