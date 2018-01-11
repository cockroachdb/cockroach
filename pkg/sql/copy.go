// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// copyMachine supports the Copy-in pgwire subprotocol (COPY...FROM STDIN). The
// machine is created by the Executor when that statement is executed; from that
// moment on, the machine takes control of the pgwire connection until
// copyMachine.run() returns. During this time, the machine is responsible for
// sending all the protocol messages (including the messages that are usually
// associated with statement results). Errors however are not sent on the
// connection by the machine, if any, the caller is responsible for sending
// them.
//
// Incoming data is buffered and batched; batches are turned into insertNodes
// that are executed. INSERT priviledges are required on the destination table.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html
// and: https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-COPY
type copyMachine struct {
	table         tree.TableExpr
	columns       tree.UnresolvedNames
	resultColumns sqlbase.ResultColumns
	buf           bytes.Buffer
	rows          []*tree.Tuple
	// insertedRows keeps track of the total number of rows inserted by the
	// machine.
	insertedRows int
	rowsMemAcc   mon.BoundAccount

	// conn is the pgwire connection from which data is to be read.
	conn pgwirebase.Conn
}

// newCopyMachine creates a new copyMachine.
func newCopyMachine(
	ctx context.Context, conn pgwirebase.Conn, params runParams, n *tree.CopyFrom,
) (*copyMachine, error) {
	cn := &copyMachine{
		conn:    conn,
		table:   &n.Table,
		columns: n.Columns,
	}

	tn, err := n.Table.NormalizeWithDatabaseName(params.p.SessionData().Database)
	if err != nil {
		return nil, err
	}
	en, err := params.p.makeEditNode(ctx, tn, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	cols, err := params.p.processColumns(en.tableDesc, n.Columns)
	if err != nil {
		return nil, err
	}
	cn.resultColumns = make(sqlbase.ResultColumns, len(cols))
	for i, c := range cols {
		cn.resultColumns[i] = sqlbase.ResultColumn{Typ: c.Type.ToDatumType()}
	}
	cn.rowsMemAcc = params.extendedEvalCtx.Mon.MakeBoundAccount()
	return cn, nil
}

// run consumes all the copy-in data from the network connection and inserts it
// in the database.
func (c *copyMachine) run(params runParams) error {
	err := c.runInternal(params)
	defer c.rowsMemAcc.Close(params.ctx)
	return err
}

func (c *copyMachine) runInternal(params runParams) error {
	// Send the message describing the columns to the client.
	if err := c.conn.BeginCopyIn(params.ctx, c.resultColumns); err != nil {
		return err
	}

	// Read from the connection until we see an ClientMsgCopyDone.
	readBuf := pgwirebase.ReadBuffer{}
	for {
		typ, _, err := readBuf.ReadTypedMsg(c.conn.Rd())
		if err != nil {
			return err
		}
		var done bool
		switch typ {
		case pgwirebase.ClientMsgCopyData:
			if err := c.processCopyData(
				string(readBuf.Msg), params, false, /* final */
			); err != nil {
				return err
			}
		case pgwirebase.ClientMsgCopyDone:
			// If there's a line in the buffer without \n at EOL, add it here.
			if c.buf.Len() > 0 {
				if err := c.addRow(params.ctx, c.buf.Bytes(), params.EvalContext()); err != nil {
					return err
				}
			}
			if err := c.processCopyData(
				"" /* data */, params, true, /* final */
			); err != nil {
				return err
			}
			done = true
		case pgwirebase.ClientMsgCopyFail:
			return fmt.Errorf("client canceled COPY")
		case pgwirebase.ClientMsgFlush, pgwirebase.ClientMsgSync:
			// Spec says to "ignore Flush and Sync messages received during copy-in mode".
		default:
			return pgwirebase.NewUnrecognizedMsgTypeErr(typ)
		}
		if done {
			break
		}
	}

	// Finalize execution by sending the statement tag and number of rows
	// inserted.
	dummy := tree.CopyFrom{}
	tag := []byte(dummy.StatementTag())
	tag = append(tag, ' ')
	tag = strconv.AppendInt(tag, int64(c.insertedRows), 10 /* base */)
	return c.conn.SendCommandComplete(tag)
}

const (
	nullString = `\N`
	lineDelim  = '\n'
)

var (
	fieldDelim = []byte{'\t'}
)

// processCopyData buffers incoming data and, once the buffer fills up, inserts
// the accumulated rows.
//
// Args:
// final: If set, buffered data is written even if the buffer is not full.
func (c *copyMachine) processCopyData(data string, params runParams, final bool) error {
	// When this many rows are in the copy buffer, they are inserted.
	const copyBatchRowSize = 100

	c.buf.WriteString(data)
	for c.buf.Len() > 0 {
		line, err := c.buf.ReadBytes(lineDelim)
		if err != nil {
			if err != io.EOF {
				return err
			}
		} else {
			// Remove lineDelim from end.
			line = line[:len(line)-1]
			// Remove a single '\r' at EOL, if present.
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
		}
		if c.buf.Len() == 0 && bytes.Equal(line, []byte(`\.`)) {
			break
		}
		if err := c.addRow(params.ctx, line, params.EvalContext()); err != nil {
			return err
		}
	}
	// Only do work if we have a full batch of rows or this is the end.
	if ln := len(c.rows); ln == 0 || (ln < copyBatchRowSize && !final) {
		return nil
	}
	return c.insertRows(params)
}

// insertRows transforms the buffered rows into an insertNode and executes it.
func (c *copyMachine) insertRows(params runParams) error {
	vc := &tree.ValuesClause{Tuples: c.rows}
	numRows := len(c.rows)
	// Reuse the same backing array once the Insert is complete.
	c.rows = c.rows[:0]
	c.rowsMemAcc.Clear(params.ctx)

	in := tree.Insert{
		Table:   c.table,
		Columns: c.columns,
		Rows: &tree.Select{
			Select: vc,
		},
		Returning: tree.AbsentReturningClause,
	}
	insertNode, err := params.p.Insert(params.ctx, &in, nil /* desiredTypes */)
	if err != nil {
		return err
	}
	defer insertNode.Close(params.ctx)

	if err := startPlan(params, insertNode); err != nil {
		return err
	}
	rows, err := countRowsAffected(params, insertNode)
	if err != nil {
		return err
	}
	if rows != numRows {
		log.Fatalf(params.ctx, "didn't insert all buffered rows and yet no error was reported. "+
			"Inserted %d out of %d rows.", rows, numRows)
	}
	c.insertedRows += rows
	return nil
}

func (c *copyMachine) addRow(ctx context.Context, line []byte, evalCtx *tree.EvalContext) error {
	var err error
	parts := bytes.Split(line, fieldDelim)
	if len(parts) != len(c.resultColumns) {
		return fmt.Errorf("expected %d values, got %d", len(c.resultColumns), len(parts))
	}
	exprs := make(tree.Exprs, len(parts))
	for i, part := range parts {
		s := string(part)
		if s == nullString {
			exprs[i] = tree.DNull
			continue
		}
		switch t := c.resultColumns[i].Typ; t {
		case types.Bytes,
			types.Date,
			types.Interval,
			types.INet,
			types.String,
			types.Timestamp,
			types.TimestampTZ,
			types.UUID:
			s, err = decodeCopy(s)
			if err != nil {
				return err
			}
		}
		d, err := parser.ParseStringAs(c.resultColumns[i].Typ, s, evalCtx)
		if err != nil {
			return err
		}

		sz := d.Size()
		if err := c.rowsMemAcc.Grow(ctx, int64(sz)); err != nil {
			return err
		}

		exprs[i] = d
	}
	tuple := &tree.Tuple{Exprs: exprs}
	if err := c.rowsMemAcc.Grow(ctx, int64(unsafe.Sizeof(*tuple))); err != nil {
		return err
	}

	c.rows = append(c.rows, tuple)
	return nil
}

// decodeCopy unescapes a single COPY field.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html#AEN74432
func decodeCopy(in string) (string, error) {
	var buf bytes.Buffer
	start := 0
	for i, n := 0, len(in); i < n; i++ {
		if in[i] != '\\' {
			continue
		}
		buf.WriteString(in[start:i])
		i++
		if i >= n {
			return "", fmt.Errorf("unknown escape sequence: %q", in[i-1:])
		}

		ch := in[i]
		if decodedChar := decodeMap[ch]; decodedChar != 0 {
			buf.WriteByte(decodedChar)
		} else if ch == 'x' {
			// \x can be followed by 1 or 2 hex digits.
			i++
			if i >= n {
				return "", fmt.Errorf("unknown escape sequence: %q", in[i-2:])
			}
			ch = in[i]
			digit, ok := decodeHexDigit(ch)
			if !ok {
				return "", fmt.Errorf("unknown escape sequence: %q", in[i-2:i])
			}
			if i+1 < n {
				if v, ok := decodeHexDigit(in[i+1]); ok {
					i++
					digit <<= 4
					digit += v
				}
			}
			buf.WriteByte(digit)
		} else if ch >= '0' && ch <= '7' {
			digit, _ := decodeOctDigit(ch)
			// 1 to 2 more octal digits follow.
			if i+1 < n {
				if v, ok := decodeOctDigit(in[i+1]); ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			if i+1 < n {
				if v, ok := decodeOctDigit(in[i+1]); ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			buf.WriteByte(digit)
		} else {
			return "", fmt.Errorf("unknown escape sequence: %q", in[i-1:i+1])
		}
		start = i + 1
	}
	buf.WriteString(in[start:])
	return buf.String(), nil
}

func decodeDigit(c byte, onlyOctal bool) (byte, bool) {
	switch {
	case c >= '0' && c <= '7':
		return c - '0', true
	case !onlyOctal && c >= '8' && c <= '9':
		return c - '0', true
	case !onlyOctal && c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case !onlyOctal && c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func decodeOctDigit(c byte) (byte, bool) { return decodeDigit(c, true) }
func decodeHexDigit(c byte) (byte, bool) { return decodeDigit(c, false) }

var decodeMap = map[byte]byte{
	'b':  '\b',
	'f':  '\f',
	'n':  '\n',
	'r':  '\r',
	't':  '\t',
	'v':  '\v',
	'\\': '\\',
}
