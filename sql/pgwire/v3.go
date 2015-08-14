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
	"bytes"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/lib/pq/oid"
)

type messageType byte

const (
	serverMsgAuth            messageType = 'R'
	serverMsgCommandComplete             = 'C'
	serverMsgDataRow                     = 'D'
	serverMsgErrorResponse               = 'E'
	serverMsgParseComplete               = '1'
	serverMsgReady                       = 'Z'
	serverMsgRowDescription              = 'T'

	clientMsgSimpleQuery = 'Q'
	clientMsgParse       = 'P'
)

const (
	authOK int32 = 0
)

type parsedQuery struct {
	query string
	types []oid.Oid
}

type v3Conn struct {
	conn     net.Conn
	opts     map[string]string
	executor sql.Executor
	parsed   map[string]parsedQuery
}

func newV3Conn(conn net.Conn, data []byte, executor sql.Executor) (*v3Conn, error) {
	v3conn := &v3Conn{
		conn:     conn,
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
	buf := bytes.NewBuffer(data)
	for {
		key, err := readString(buf)
		if err != nil {
			return util.Errorf("error reading option key: %s", err)
		}
		if len(key) == 0 {
			return nil
		}
		value, err := readString(buf)
		if err != nil {
			return util.Errorf("error reading option value: %s", err)
		}
		c.opts[key] = value
	}
}

func (c *v3Conn) serve() error {
	rd := bufio.NewReader(c.conn)
	// TODO(bdarnell): real auth flow. For now just accept anything.
	var authOKMsg bytes.Buffer
	if err := binary.Write(&authOKMsg, binary.BigEndian, authOK); err != nil {
		return err
	}
	if err := writeTypedMsg(c.conn, serverMsgAuth, authOKMsg.Bytes()); err != nil {
		return err
	}
	for {
		// TODO(bdarnell): change the 'I' below based on transaction status.
		if err := writeTypedMsg(c.conn, serverMsgReady, []byte{'I'}); err != nil {
			return err
		}
		typ, msg, err := readTypedMsg(rd)
		if err != nil {
			return err
		}
		buf := bytes.NewBuffer(msg)
		switch typ {
		case clientMsgSimpleQuery:
			err = c.handleSimpleQuery(buf)

		case clientMsgParse:
			err = c.handleParse(buf)

		default:
			log.Fatalf("unrecognized client message type %c", typ)
		}
		if err != nil {
			return err
		}
	}
}

func (c *v3Conn) handleSimpleQuery(buf *bytes.Buffer) error {
	query, err := readString(buf)
	if err != nil {
		return err
	}

	var req driver.Request
	// TODO(bdarnell): authentication
	req.User = "root"
	req.Sql = query

	resp, _, err := c.executor.Execute(req)
	if err != nil {
		// TODO(bdarnell): send error instead of killing connection
		return err
	}

	return c.sendResponse(resp)
}

func (c *v3Conn) handleParse(buf *bytes.Buffer) error {
	name, err := readString(buf)
	if err != nil {
		return err
	}
	if _, ok := c.parsed[name]; ok && name != "" {
		return util.Errorf("prepared statement %q already exists", name)
	}
	query, err := readString(buf)
	if err != nil {
		return err
	}
	pq := parsedQuery{query: query}
	var numTypes int16
	if err := binary.Read(buf, binary.BigEndian, &numTypes); err != nil {
		return err
	}
	pq.types = make([]oid.Oid, numTypes)
	for i := int16(0); i < numTypes; i++ {
		var typ int32
		if err := binary.Read(buf, binary.BigEndian, &typ); err != nil {
			return err
		}
		pq.types[i] = oid.Oid(typ)
	}
	c.parsed[name] = pq
	return writeTypedMsg(c.conn, serverMsgParseComplete, nil)
}

func (c *v3Conn) sendCommandComplete(tag string) error {
	var b bytes.Buffer
	if err := writeString(&b, tag); err != nil {
		return err
	}
	return writeTypedMsg(c.conn, serverMsgCommandComplete, b.Bytes())
}

func (c *v3Conn) sendError(errToSend error) error {
	var buf bytes.Buffer
	if err := buf.WriteByte('S'); err != nil {
		return err
	}
	if err := writeString(&buf, "ERROR"); err != nil {
		return err
	}
	if err := buf.WriteByte('C'); err != nil {
		return err
	}
	// "XX000" is "internal error".
	// TODO(bdarnell): map our errors to appropriate postgres error
	// codes as defined in
	// http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
	if err := writeString(&buf, "XX000"); err != nil {
		return err
	}
	if err := buf.WriteByte('M'); err != nil {
		return err
	}
	if err := writeString(&buf, errToSend.Error()); err != nil {
		return err
	}
	if err := buf.WriteByte(0); err != nil {
		return err
	}
	return writeTypedMsg(c.conn, serverMsgErrorResponse, buf.Bytes())
}

func (c *v3Conn) sendResponse(resp driver.Response) error {
	if len(resp.Results) == 0 {
		return c.sendCommandComplete("")
	}
	for _, result := range resp.Results {
		if result.Error != nil {
			if err := c.sendError(result.Error); err != nil {
				return err
			}
			continue
		}
		// Send RowDescription.
		var rowDesc bytes.Buffer
		if err := binary.Write(&rowDesc, binary.BigEndian, int16(len(result.Columns))); err != nil {
			return err
		}
		for i, column := range result.Columns {
			if err := writeString(&rowDesc, column); err != nil {
				return err
			}
			// TODO(bdarnell): Use column metadata instead of first row.
			typ := typeForDatum(result.Rows[0].Values[i])
			data := []interface{}{
				int32(0), // Table OID (optional).
				int16(0), // Column attribute ID (optional).
				int32(typ.oid),
				int16(typ.size),
				int32(0), // Type modifier (none of our supported types have modifiers).
				int16(typ.preferredFormat),
			}
			for _, x := range data {
				if err := binary.Write(&rowDesc, binary.BigEndian, x); err != nil {
					return err
				}
			}
		}
		if err := writeTypedMsg(c.conn, serverMsgRowDescription, rowDesc.Bytes()); err != nil {
			return err
		}

		// Send DataRows.
		for _, row := range result.Rows {
			var dataRow bytes.Buffer
			if err := binary.Write(&dataRow, binary.BigEndian, int16(len(row.Values))); err != nil {
				return err
			}
			for _, col := range row.Values {
				if err := writeDatum(&dataRow, col); err != nil {
					return err
				}
			}
			if err := writeTypedMsg(c.conn, serverMsgDataRow, dataRow.Bytes()); err != nil {
				return err
			}
		}

		// Send CommandComplete.
		// TODO(bdarnell): tags for other types of commands.
		tag := fmt.Sprintf("SELECT %d", len(result.Rows))
		if err := c.sendCommandComplete(tag); err != nil {
			return err
		}
	}

	return nil
}
