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
//
// Author: Matt Jibson

package sql

import (
	"bytes"
	"fmt"
	"io"
	"time"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// COPY FROM is not a usual planNode. After a COPY FROM is executed as a
// planNode and until an error or it is explicitly completed, the planner
// carries a reference to a copyNode to send the contents of copy data
// payloads using the executor's Copy methods. Attempting to execute non-COPY
// statements before the COPY has finished will result in an error.
//
// The copyNode hold two buffers: raw bytes and rows. All copy data is
// appended to the byte buffer. Afterward, rows are extracted from the
// bytes. When the number of rows has reached some limit (whose purpose is
// to increase performance by batching inserts), they are inserted with an
// insertNode. A CopyDone message will flush and insert all remaining data.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html
type copyNode struct {
	p             *planner
	table         parser.TableExpr
	columns       parser.UnresolvedNames
	resultColumns ResultColumns
	buf           bytes.Buffer
	rows          []*parser.Tuple
	rowsMemAcc    WrappableMemoryAccount
}

func (n *copyNode) Columns() ResultColumns                            { return n.resultColumns }
func (*copyNode) Ordering() orderingInfo                              { return orderingInfo{} }
func (*copyNode) Values() parser.Datums                               { return nil }
func (*copyNode) MarkDebug(_ explainMode)                             {}
func (*copyNode) Next(context.Context) (bool, error)                  { return false, nil }
func (*copyNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) { panic("unimplemented") }

func (n *copyNode) Close(ctx context.Context) {
	n.rowsMemAcc.Wsession(n.p.session).Close(ctx)
}

func (*copyNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}

// CopyFrom begins a COPY.
// Privileges: INSERT on table.
func (p *planner) CopyFrom(
	ctx context.Context, n *parser.CopyFrom, autoCommit bool,
) (planNode, error) {
	cn := &copyNode{
		table:   &n.Table,
		columns: n.Columns,
	}

	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}
	en, err := p.makeEditNode(ctx, tn, autoCommit, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	cols, err := p.processColumns(en.tableDesc, n.Columns)
	if err != nil {
		return nil, err
	}
	cn.resultColumns = make(ResultColumns, len(cols))
	for i, c := range cols {
		cn.resultColumns[i] = ResultColumn{Typ: c.Type.ToDatumType()}
	}
	cn.p = p
	cn.rowsMemAcc = p.session.OpenAccount()
	return cn, nil
}

// Start implements the planNode interface.
func (n *copyNode) Start(context.Context) error {
	// Should never happen because the executor prevents non-COPY messages during
	// a COPY.
	if n.p.session.copyFrom != nil {
		return fmt.Errorf("COPY already in progress")
	}
	n.p.session.copyFrom = n
	return nil
}

// CopyDataBlock represents a data block of a COPY FROM statement.
type CopyDataBlock struct {
	Done bool
}

type copyMsg int

const (
	copyMsgNone copyMsg = iota
	copyMsgData
	copyMsgDone

	nullString = `\N`
	lineDelim  = '\n'
)

var (
	fieldDelim = []byte{'\t'}
)

// ProcessCopyData appends data to the planner's internal COPY state as
// parsed datums. Since the COPY protocol allows any length of data to be
// sent in a message, there's no guarantee that data contains a complete row
// (or even a complete datum). It is thus valid to have no new rows added
// to the internal state after this call.
func (s *Session) ProcessCopyData(
	ctx context.Context, data string, msg copyMsg,
) (parser.StatementList, error) {
	cf := s.copyFrom
	buf := cf.buf

	switch msg {
	case copyMsgData:
		// ignore
	case copyMsgDone:
		var err error
		// If there's a line in the buffer without \n at EOL, add it here.
		if buf.Len() > 0 {
			err = cf.addRow(ctx, buf.Bytes())
		}
		return parser.StatementList{CopyDataBlock{Done: true}}, err
	default:
		return nil, fmt.Errorf("expected copy command")
	}

	buf.WriteString(data)
	for buf.Len() > 0 {
		line, err := buf.ReadBytes(lineDelim)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
		} else {
			// Remove lineDelim from end.
			line = line[:len(line)-1]
			// Remove a single '\r' at EOL, if present.
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
		}
		if buf.Len() == 0 && bytes.Equal(line, []byte(`\.`)) {
			break
		}
		if err := cf.addRow(ctx, line); err != nil {
			return nil, err
		}
	}
	return parser.StatementList{CopyDataBlock{}}, nil
}

func (n *copyNode) addRow(ctx context.Context, line []byte) error {
	var err error
	parts := bytes.Split(line, fieldDelim)
	if len(parts) != len(n.resultColumns) {
		return fmt.Errorf("expected %d values, got %d", len(n.resultColumns), len(parts))
	}
	exprs := make(parser.Exprs, len(parts))
	acc := n.rowsMemAcc.Wsession(n.p.session)
	for i, part := range parts {
		s := string(part)
		if s == nullString {
			exprs[i] = parser.DNull
			continue
		}
		var d parser.Datum
		switch t := n.resultColumns[i].Typ; t {
		case parser.TypeBool:
			d, err = parser.ParseDBool(s)
		case parser.TypeBytes:
			s, err = decodeCopy(s)
			d = parser.NewDBytes(parser.DBytes(s))
		case parser.TypeDate:
			s, err = decodeCopy(s)
			if err != nil {
				break
			}
			d, err = parser.ParseDDate(s, n.p.session.Location)
		case parser.TypeDecimal:
			d, err = parser.ParseDDecimal(s)
		case parser.TypeFloat:
			d, err = parser.ParseDFloat(s)
		case parser.TypeInt:
			d, err = parser.ParseDInt(s)
		case parser.TypeInterval:
			s, err = decodeCopy(s)
			if err != nil {
				break
			}
			d, err = parser.ParseDInterval(s)
		case parser.TypeString:
			s, err = decodeCopy(s)
			d = parser.NewDString(s)
		case parser.TypeTimestamp:
			s, err = decodeCopy(s)
			if err != nil {
				break
			}
			d, err = parser.ParseDTimestamp(s, time.Microsecond)
		case parser.TypeTimestampTZ:
			s, err = decodeCopy(s)
			if err != nil {
				break
			}
			d, err = parser.ParseDTimestampTZ(s, n.p.session.Location, time.Microsecond)
		default:
			return fmt.Errorf("unknown type %s", t)
		}
		if err != nil {
			return err
		}

		sz := d.Size()
		if err := acc.Grow(ctx, int64(sz)); err != nil {
			return err
		}

		exprs[i] = d
	}
	tuple := &parser.Tuple{Exprs: exprs}
	if err := acc.Grow(ctx, int64(unsafe.Sizeof(*tuple))); err != nil {
		return err
	}

	n.rows = append(n.rows, tuple)
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

// CopyData is the statement type after a block of COPY data has been
// received. There may be additional rows ready to insert. If so, return an
// insertNode, otherwise emptyNode.
func (p *planner) CopyData(
	ctx context.Context, n CopyDataBlock, autoCommit bool,
) (planNode, error) {
	// When this many rows are in the copy buffer, they are inserted.
	const copyRowSize = 100

	cf := p.session.copyFrom

	// Only do work if we have lots of rows or this is the end.
	if ln := len(cf.rows); ln == 0 || (ln < copyRowSize && !n.Done) {
		return &emptyNode{}, nil
	}

	vc := &parser.ValuesClause{Tuples: cf.rows}
	// Reuse the same backing array once the Insert is complete.
	cf.rows = cf.rows[:0]
	cf.rowsMemAcc.Wsession(p.session).Clear(ctx)

	in := parser.Insert{
		Table:   cf.table,
		Columns: cf.columns,
		Rows: &parser.Select{
			Select: vc,
		},
		Returning: parser.AbsentReturningClause,
	}
	return p.Insert(ctx, &in, nil, autoCommit)
}

// Format implements the NodeFormatter interface.
func (CopyDataBlock) Format(buf *bytes.Buffer, f parser.FmtFlags) {}

// StatementType implements the Statement interface.
func (CopyDataBlock) StatementType() parser.StatementType { return parser.RowsAffected }

// StatementTag returns a short string identifying the type of statement.
func (CopyDataBlock) StatementTag() string { return "" }
func (CopyDataBlock) String() string       { return "" }
