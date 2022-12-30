// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// copyToTranslater translates datums into the appropriate format for CopyTo.
type copyToTranslater interface {
	translateRow(datums tree.Datums, rcs colinfo.ResultColumns) ([]byte, error)
}

// textCopyToTranslater is the default text representation of COPY TO from postgres.
type textCopyToTranslater struct {
	copyOptions
	fieldBuffer bytes.Buffer
	rowBuffer   bytes.Buffer
	wireCtx     *tree.WireCtx
}

var _ copyToTranslater = (*textCopyToTranslater)(nil)

func (t *textCopyToTranslater) translateRow(
	datums tree.Datums, rcs colinfo.ResultColumns,
) ([]byte, error) {
	t.rowBuffer.Reset()
	for i, d := range datums {
		t.fieldBuffer.Reset()
		if i > 0 {
			t.rowBuffer.WriteByte(t.delimiter)
		}
		if d == tree.DNull {
			t.rowBuffer.WriteString(t.null)
			continue
		}
		if err := d.FormatWireText(t.wireCtx, rcs[i].Typ); err != nil {
			return nil, err
		}
		if err := encodeCopy(&t.rowBuffer, t.fieldBuffer.Bytes(), t.delimiter); err != nil {
			return nil, err
		}
	}
	t.rowBuffer.WriteByte('\n')
	return t.rowBuffer.Bytes(), nil
}

func runCopyTo(
	ctx context.Context, p *planner, txn *kv.Txn, cmd CopyOut,
) (numOutputRows int, retErr error) {
	copyOptions, err := processCopyOptions(ctx, p, cmd.Stmt.Options)
	if err != nil {
		return 0, err
	}

	wireFormat := pgwirebase.FormatText
	var t copyToTranslater
	switch cmd.Stmt.Options.CopyFormat {
	case tree.CopyFormatBinary:
		//wireFormat = pgwirebase.FormatBinary
		return 0, unimplemented.NewWithIssue(
			85571,
			"binary format for COPY TO not implemented",
		)
	case tree.CopyFormatCSV:
		return 0, unimplemented.NewWithIssue(85571, "CSV format for COPY TO not implemented")
	default:
		textTranslater := &textCopyToTranslater{
			copyOptions: copyOptions,
		}
		textTranslater.wireCtx = tree.NewWireCtx(
			&textTranslater.fieldBuffer,
			p.SessionData().DataConversionConfig,
			p.SessionData().Location,
		)
		t = textTranslater
	}

	var q string
	if cmd.Stmt.Statement != nil {
		q = cmd.Stmt.Statement.String()
	} else {
		// Construct a SELECT expression using the table name.
		// The columns are * if not specified, or otherwise specific each column
		// item in a SelectExpr.
		selectExprs := tree.SelectExprs{tree.StarSelectExpr()}
		if len(cmd.Stmt.Columns) > 0 {
			selectExprs = make(tree.SelectExprs, len(cmd.Stmt.Columns))
			for i, c := range cmd.Stmt.Columns {
				selectExprs[i] = tree.SelectExpr{Expr: &tree.ColumnItem{ColumnName: c}}
			}
		}
		q = (&tree.SelectClause{
			Exprs: selectExprs,
			From: tree.From{
				Tables: tree.TableExprs{&cmd.Stmt.Table},
			},
		}).String()
	}

	it, err := p.execCfg.InternalExecutorFactory.NewInternalExecutor(p.SessionData()).QueryIteratorEx(
		ctx,
		cmd.Stmt.String(),
		txn,
		sessiondata.NoSessionDataOverride,
		q,
	)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := it.Close(); err != nil {
			log.SqlExec.Errorf(ctx, "error closing iterator for %s: %+v", cmd, retErr)
		}
	}()

	// Send the message describing the columns to the client.
	if err := cmd.Conn.BeginCopyOut(ctx, it.Types(), wireFormat); err != nil {
		return 0, err
	}

	// Send all the rows out to the client.
	if err := func() error {
		for {
			next, err := it.Next(ctx)
			if err != nil {
				return err
			}
			if !next {
				break
			}
			numOutputRows++
			row, err := t.translateRow(it.Cur(), it.Types())
			if err != nil {
				return err
			}
			if err := cmd.Conn.SendCopyData(ctx, row); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return 0, err
	}
	return numOutputRows, cmd.Conn.SendCopyDone(ctx)
}

var encodeMap = func() map[byte]byte {
	ret := make(map[byte]byte, len(decodeMap))
	for k, v := range decodeMap {
		ret[v] = k
	}
	return ret
}()

// encodeCopy escapes a single COPY field.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html#AEN74432
// NOTE: we don't have to worry about hex in COPY TO.
func encodeCopy(w io.Writer, in []byte, delimiter byte) error {
	lastIndex := 0
	for i, r := range in {
		if escapeChar, ok := encodeMap[r]; ok || r == delimiter {
			// Write everything up to this point.
			if _, err := w.Write(in[lastIndex:i]); err != nil {
				return err
			}
			lastIndex = i + 1

			// Escape the current character, using the delimiter if needbe.
			if escapeChar == 0 {
				escapeChar = delimiter
			}
			if _, err := w.Write([]byte{'\\', escapeChar}); err != nil {
				return err
			}
		}
	}
	// Flush the remaining bytes.
	_, err := w.Write(in[lastIndex:])
	return err
}
