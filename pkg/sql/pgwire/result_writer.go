// Copyright 2017 The Cockroach Authors.
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
// Author: Tristan Ohlson (tsohlson@gmail.com)

package pgwire

import (
	"fmt"
	"strconv"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
)

type StreamingWriter struct {
	conn            *v3Conn
	hasSentResults  bool
	formatCodes     []formatCode
	sendDescription bool
	limit           int
	pgtag           string
	columns         sqlbase.ResultColumns
	statementType   parser.StatementType
	rowsAffected    int
	firstRow        bool
	tag             []byte
}

func (s *StreamingWriter) BeginResult(pgtag string, statementType parser.StatementType) {
	fmt.Println("begin result")
	s.pgtag = pgtag
	s.statementType = statementType
	s.rowsAffected = 0
	s.firstRow = true
}

func (s *StreamingWriter) Type() parser.StatementType {
	fmt.Println("type")
	return s.statementType
}

func (s *StreamingWriter) SetRowsAffected(rowsAffected int) {
	fmt.Println("set rows affected")
	s.rowsAffected += rowsAffected
}

func (s *StreamingWriter) SetColumns(columns sqlbase.ResultColumns) {
	fmt.Println("set columns")
	s.columns = columns
}

func (s *StreamingWriter) AddRow(ctx context.Context, row parser.Datums) error {
	fmt.Println("add row")
	s.hasSentResults = true
	s.rowsAffected++
	if len(s.columns) == 0 {
		return nil
	}

	if s.statementType == parser.Rows {
		c := s.conn
		ctx := c.session.Ctx()
		formatCodes := s.formatCodes

		if s.firstRow {
			s.tag = append(c.tagBuf[:0], s.pgtag...)

			if s.sendDescription {
				// We're not allowed to send a NoData message here, even if there are
				// 0 result columns, since we're responding to a "Simple Query".
				if err := c.sendRowDescription(ctx, s.columns, formatCodes, false); err != nil {
					return err
				}
			}

			s.firstRow = false
		}

		c.writeBuf.initMsg(serverMsgDataRow)
		c.writeBuf.putInt16(int16(len(row)))
		for i, col := range row {
			fmtCode := formatText
			if formatCodes != nil {
				fmtCode = formatCodes[i]
			}
			switch fmtCode {
			case formatText:
				c.writeBuf.writeTextDatum(ctx, col, c.session.Location)
			case formatBinary:
				c.writeBuf.writeBinaryDatum(ctx, col, c.session.Location)
			default:
				c.writeBuf.setError(errors.Errorf("unsupported format code %s", fmtCode))
			}
		}
		if err := c.writeBuf.finishMsg(c.wr); err != nil {
			return err
		}
	}

	return nil
}

func (s *StreamingWriter) SendAck(pgTag string) {
	fmt.Println("send ack")
	s.hasSentResults = true
	if pgTag == "INSERT" {
		// From the postgres docs (49.5. Message Formats):
		// `INSERT oid rows`... oid is the object ID of the inserted row if
		//	rows is 1 and the target table has OIDs; otherwise oid is 0.
		pgTag = "INSERT 0"
	}
	tag := append(s.conn.tagBuf[:0], pgTag...)

	if pgTag == "SELECT" {
		tag = append(tag, ' ')
		tag = strconv.AppendInt(tag, 0, 10)
	}
	if err := s.conn.sendCommandComplete(tag); err != nil {
		//return err
	}
}

func (s *StreamingWriter) EndResult() {
	fmt.Println("end result")
	s.tag = append(s.conn.tagBuf[:0], ' ')
	s.tag = strconv.AppendUint(s.tag, uint64(s.rowsAffected), 10)
	s.conn.sendCommandComplete(s.tag)
}

func (s *StreamingWriter) Error(err error) {
	s.hasSentResults = true
	fmt.Printf("error: %v\n", err)
	if err := s.conn.sendError(err); err != nil {
		fmt.Printf("err streaming error: %v\n", err)
		//return err
	}
	//return nil
}

func (s *StreamingWriter) CanRetry() bool {
	fmt.Println("can retry")
	return true
}

//func (s *StreamingWriter) SendResult(result sql.Result) error {
//	s.hasSentResults = true
//	c := s.conn
//	ctx := c.session.Ctx()
//	limit := s.limit
//	formatCodes := s.formatCodes
//
//	defer func() {
//		result.Close(ctx)
//	}()
//
//	if result.Err != nil {
//		if err := c.sendError(result.Err); err != nil {
//			return err
//		}
//		return nil
//	}
//
//	if limit != 0 && result.Rows != nil && result.Rows.Len() > limit {
//		if err := c.sendInternalError(fmt.Sprintf("execute row count limits not supported: %d of %d", limit, result.Rows.Len())); err != nil {
//			return err
//		}
//		return nil
//	}
//
//	if result.PGTag == "INSERT" {
//		// From the postgres docs (49.5. Message Formats):
//		// `INSERT oid rows`... oid is the object ID of the inserted row if
//		//	rows is 1 and the target table has OIDs; otherwise oid is 0.
//		result.PGTag = "INSERT 0"
//	}
//	tag := append(c.tagBuf[:0], result.PGTag...)
//
//	switch result.Type {
//	case parser.RowsAffected:
//		// Send CommandComplete.
//		tag = append(tag, ' ')
//		tag = strconv.AppendInt(tag, int64(result.RowsAffected), 10)
//		if err := c.sendCommandComplete(tag); err != nil {
//			return err
//		}
//
//	case parser.Rows:
//		if s.sendDescription {
//			// We're not allowed to send a NoData message here, even if there are
//			// 0 result columns, since we're responding to a "Simple Query".
//			if err := c.sendRowDescription(ctx, result.Columns, formatCodes, false); err != nil {
//				return err
//			}
//		}
//
//		// Send DataRows.
//		nRows := result.Rows.Len()
//		for rowIdx := 0; rowIdx < nRows; rowIdx++ {
//			row := result.Rows.At(rowIdx)
//			c.writeBuf.initMsg(serverMsgDataRow)
//			c.writeBuf.putInt16(int16(len(row)))
//			for i, col := range row {
//				fmtCode := formatText
//				if formatCodes != nil {
//					fmtCode = formatCodes[i]
//				}
//				switch fmtCode {
//				case formatText:
//					c.writeBuf.writeTextDatum(ctx, col, c.session.Location)
//				case formatBinary:
//					c.writeBuf.writeBinaryDatum(ctx, col, c.session.Location)
//				default:
//					c.writeBuf.setError(errors.Errorf("unsupported format code %s", fmtCode))
//				}
//			}
//			if err := c.writeBuf.finishMsg(c.wr); err != nil {
//				return err
//			}
//		}
//
//		// Send CommandComplete.
//		tag = append(tag, ' ')
//		tag = strconv.AppendUint(tag, uint64(result.Rows.Len()), 10)
//		if err := c.sendCommandComplete(tag); err != nil {
//			return err
//		}
//
//	case parser.Ack, parser.DDL:
//		if result.PGTag == "SELECT" {
//			tag = append(tag, ' ')
//			tag = strconv.AppendInt(tag, int64(result.RowsAffected), 10)
//		}
//		if err := c.sendCommandComplete(tag); err != nil {
//			return err
//		}
//
//	case parser.CopyIn:
//		rows, err := c.copyIn(ctx, result.Columns, s)
//		if err != nil {
//			return err
//		}
//
//		// Send CommandComplete.
//		tag = append(tag, ' ')
//		tag = strconv.AppendInt(tag, rows, 10)
//		if err := c.sendCommandComplete(tag); err != nil {
//			return err
//		}
//
//	default:
//		panic(fmt.Sprintf("unexpected result type %v", result.Type))
//	}
//
//	return nil
//}

func (s *StreamingWriter) Done() {
	fmt.Println("done")
	if !s.hasSentResults {
		fmt.Println("no results sent")
		s.conn.writeBuf.initMsg(serverMsgEmptyQuery)
		s.conn.writeBuf.finishMsg(s.conn.wr)
	}

	fmt.Println("num rows: ", s.rowsAffected)

	// TODO(tohlson): check if the last type sent was rows
	s.EndResult()

	s.conn.session.FinishPlan()
}
