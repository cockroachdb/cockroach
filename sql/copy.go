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
	"time"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

type copyNode struct {
	table         parser.TableExpr
	columns       parser.UnresolvedNames
	resultColumns []ResultColumn
	buf           bytes.Buffer
	rows          []*parser.Tuple
}

func (n *copyNode) Columns() []ResultColumn           { return n.resultColumns }
func (*copyNode) Ordering() orderingInfo              { return orderingInfo{} }
func (*copyNode) Values() parser.DTuple               { return nil }
func (*copyNode) ExplainTypes(_ func(string, string)) {}
func (*copyNode) Start() error                        { return nil }
func (*copyNode) SetLimitHint(_ int64, _ bool)        {}
func (*copyNode) MarkDebug(_ explainMode)             {}
func (*copyNode) expandPlan() error                   { return nil }
func (*copyNode) Next() (bool, error)                 { return false, nil }

func (*copyNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	return "copy", "-", nil
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
func (p *planner) CopyFrom(n *parser.CopyFrom, autoCommit bool) (planNode, error) {
	cn := &copyNode{
		table:   &n.Table,
		columns: n.Columns,
	}

	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}
	en, err := p.makeEditNode(tn, autoCommit, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	cols, err := p.processColumns(en.tableDesc, n.Columns)
	if err != nil {
		return nil, err
	}
	cn.resultColumns = make([]ResultColumn, len(cols))
	for i, c := range cols {
		cn.resultColumns[i] = ResultColumn{Typ: c.Type.ToDatumType()}
	}

	p.copyFrom = cn
	return cn, nil
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
)

const (
	lineDelim  = '\n'
	fieldDelim = '\t'
)

var nullString = `\N`

// ProcessCopyData appends data to the planner's internal COPY state as
// parsed datums. Since the COPY protocol allows any length of data to be
// sent in a message, there's no guarantee that data contains a complete row
// (or even a complete datum). It is thus valid to have no new rows added
// to the internal state after this call.
func (p *planner) ProcessCopyData(data string, msg copyMsg) (parser.StatementList, error) {
	switch msg {
	case copyMsgData:
		// ignore
	case copyMsgDone:
		return parser.StatementList{CopyDataBlock{Done: true}}, nil
	default:
		return nil, fmt.Errorf("expected copy command")
	}

	cf := p.copyFrom
	buf := cf.buf
	buf.WriteString(data)
	for buf.Len() > 0 {
		b := buf.Bytes()
		// Don't blindly use buf.ReadBytes because if lineDelim is not present we
		// must rewrite the bytes to buf. Instead check if lineDelim is present.
		if bytes.IndexByte(b, lineDelim) == -1 {
			continue
		}
		line, err := buf.ReadBytes(lineDelim)
		if err != nil {
			return nil, err
		}
		// Remove lineDelim from end.
		line = line[:len(line)-1]
		// Remove a single '\r' at EOL, if present.
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		parts := bytes.Split(line, []byte{fieldDelim})
		if len(parts) != len(cf.resultColumns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(cf.resultColumns), len(parts))
		}
		exprs := make(parser.Exprs, len(parts))
		for i, part := range parts {
			s := string(part)
			if s == nullString {
				exprs[i] = parser.DNull
				continue
			}
			var d parser.Datum
			switch t := cf.resultColumns[i].Typ.(type) {
			case *parser.DBool:
				d, err = parser.ParseDBool(s)
			case *parser.DBytes:
				s, err = decodeCopy(s)
				d = parser.NewDBytes(parser.DBytes(s))
			case *parser.DDate:
				s, err = decodeCopy(s)
				if err != nil {
					break
				}
				d, err = parser.ParseDDate(s, p.session.Location)
			case *parser.DDecimal:
				d, err = parser.ParseDDecimal(s)
			case *parser.DFloat:
				d, err = parser.ParseDFloat(s)
			case *parser.DInt:
				d, err = parser.ParseDInt(s)
			case *parser.DInterval:
				s, err = decodeCopy(s)
				if err != nil {
					break
				}
				d, err = parser.ParseDInterval(s)
			case *parser.DString:
				s, err = decodeCopy(s)
				d = parser.NewDString(s)
			case *parser.DTimestamp:
				s, err = decodeCopy(s)
				if err != nil {
					break
				}
				d, err = parser.ParseDTimestamp(s, p.session.Location, time.Nanosecond)
			case *parser.DTimestampTZ:
				s, err = decodeCopy(s)
				if err != nil {
					break
				}
				d, err = parser.ParseDTimestampTZ(s, p.session.Location, time.Nanosecond)
			default:
				return nil, fmt.Errorf("unknown type %s (%T)", t.Type(), t)
			}
			if err != nil {
				return nil, err
			}
			exprs[i] = d
		}
		cf.rows = append(cf.rows, &parser.Tuple{Exprs: exprs})
	}
	return parser.StatementList{CopyDataBlock{}}, nil
}

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
			digit, ok := hexDigit[ch]
			if !ok {
				return "", fmt.Errorf("unknown escape sequence: %q", in[i-2:i])
			}
			if i+1 < n {
				if v, ok := hexDigit[in[i+1]]; ok {
					i++
					digit <<= 4
					digit += v
				}
			}
			buf.WriteByte(digit)
		} else if ch >= '0' && ch <= '7' {
			digit := octalDigit[ch]
			// 1 to 2 more octal digits follow.
			if i+1 < n {
				if v, ok := octalDigit[in[i+1]]; ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			if i+1 < n {
				if v, ok := octalDigit[in[i+1]]; ok {
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

var decodeMap = map[byte]byte{
	'b':  '\b',
	'f':  '\f',
	'n':  '\n',
	'r':  '\r',
	't':  '\t',
	'v':  '\v',
	'\\': '\\',
}

var octalDigit = map[byte]byte{
	'0': 0,
	'1': 1,
	'2': 2,
	'3': 3,
	'4': 4,
	'5': 5,
	'6': 6,
	'7': 7,
}

var hexDigit = map[byte]byte{
	'0': 0,
	'1': 1,
	'2': 2,
	'3': 3,
	'4': 4,
	'5': 5,
	'6': 6,
	'7': 7,
	'8': 8,
	'9': 9,
	'a': 10,
	'b': 11,
	'c': 12,
	'd': 13,
	'e': 14,
	'f': 15,
	'A': 10,
	'B': 11,
	'C': 12,
	'D': 13,
	'E': 14,
	'F': 15,
}

func (p *planner) CopyData(n CopyDataBlock, autoCommit bool) (planNode, error) {
	const copyRowSize = 100

	cf := p.copyFrom

	// Only do work if we have lots of rows or this is the end.
	if ln := len(cf.rows); ln == 0 || (ln < copyRowSize && !n.Done) {
		return &emptyNode{}, nil
	}

	vc := &parser.ValuesClause{Tuples: cf.rows}
	// Reuse the same backing array once the Insert is complete.
	cf.rows = cf.rows[:0]

	in := parser.Insert{
		Table:   cf.table,
		Columns: cf.columns,
		Rows: &parser.Select{
			Select: vc,
		},
	}
	return p.Insert(&in, nil, autoCommit)
}

// Format implements the NodeFormatter interface.
func (CopyDataBlock) Format(buf *bytes.Buffer, f parser.FmtFlags) {}

// StatementType implements the Statement interface.
func (CopyDataBlock) StatementType() parser.StatementType { return parser.RowsAffected }

// StatementTag returns a short string identifying the type of statement.
func (CopyDataBlock) StatementTag() string { return "" }
func (CopyDataBlock) String() string       { return "" }
