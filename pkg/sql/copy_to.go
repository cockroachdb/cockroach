// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// copyToTranslater translates datums into the appropriate format for CopyTo.
type copyToTranslater interface {
	translateRow(datums tree.Datums, rcs colinfo.ResultColumns) ([]byte, error)
	// headerRow returns the header row bytes, if applicable, and a bool to
	// determine whether one needs to be written.
	headerRow(rcs colinfo.ResultColumns) ([]byte, bool, error)
}

var _ copyToTranslater = (*textCopyToTranslater)(nil)
var _ copyToTranslater = (*csvCopyToTranslater)(nil)

// textCopyToTranslater is the default text representation of COPY TO from postgres.
type textCopyToTranslater struct {
	copyOptions
	rowBuffer bytes.Buffer
	fmtCtx    *tree.FmtCtx
}

func (t *textCopyToTranslater) translateRow(
	datums tree.Datums, rcs colinfo.ResultColumns,
) ([]byte, error) {
	t.rowBuffer.Reset()
	t.fmtCtx.Buffer.Reset()
	for i, d := range datums {
		if i > 0 {
			t.rowBuffer.WriteByte(t.delimiter)
		}
		if d == tree.DNull {
			t.rowBuffer.WriteString(t.null)
			continue
		}
		t.fmtCtx.FormatNode(d)
		if err := EncodeCopy(&t.rowBuffer, t.fmtCtx.Buffer.Bytes(), t.delimiter); err != nil {
			return nil, err
		}
		t.fmtCtx.Buffer.Reset()
	}
	t.rowBuffer.WriteByte('\n')
	return t.rowBuffer.Bytes(), nil
}

func (t *textCopyToTranslater) headerRow(rcs colinfo.ResultColumns) ([]byte, bool, error) {
	return nil, false, nil
}

// csvCopyToTranslater is the CSV representation of COPY TO from postgres.
type csvCopyToTranslater struct {
	copyOptions
	b      bytes.Buffer
	fmtCtx *tree.FmtCtx
	w      *csv.Writer
}

func (c *csvCopyToTranslater) translateRow(
	datums tree.Datums, rcs colinfo.ResultColumns,
) ([]byte, error) {
	c.b.Reset()
	c.fmtCtx.Buffer.Reset()
	for _, d := range datums {
		if d == tree.DNull {
			if err := c.w.WriteField(bytes.NewBufferString(c.null)); err != nil {
				return nil, err
			}
			continue
		}

		c.fmtCtx.FormatNode(d)
		if c.fmtCtx.Buffer.Len() == 0 {
			// Empty fields must force an empty quote to differentiate from NULL.
			if err := c.w.ForceEmptyField(); err != nil {
				return nil, err
			}
		} else {
			if err := c.w.WriteField(bytes.NewBuffer(c.fmtCtx.Buffer.Bytes())); err != nil {
				return nil, err
			}
		}
		c.fmtCtx.Buffer.Reset()
	}
	if err := c.w.FinishRecord(); err != nil {
		return nil, err
	}
	c.w.Flush()
	return c.b.Bytes(), nil
}

func (c *csvCopyToTranslater) headerRow(rcs colinfo.ResultColumns) ([]byte, bool, error) {
	if !c.csvExpectHeader {
		return nil, false, nil
	}
	c.b.Reset()
	for _, rc := range rcs {
		if err := c.w.WriteField(bytes.NewBufferString(rc.Name)); err != nil {
			return nil, false, err
		}
	}
	if err := c.w.FinishRecord(); err != nil {
		return nil, false, err
	}
	c.w.Flush()
	return c.b.Bytes(), true, nil
}

func runCopyTo(
	ctx context.Context, p *planner, txn *kv.Txn, cmd CopyOut, res CopyOutResult,
) (numOutputRows int, retErr error) {
	copyOptions, err := processCopyOptions(ctx, p, cmd.Stmt.Options)
	if err != nil {
		return 0, err
	}

	wireFormat := pgwirebase.FormatText
	var t copyToTranslater
	switch cmd.Stmt.Options.CopyFormat {
	case tree.CopyFormatBinary:
		// wireFormat = pgwirebase.FormatBinary
		return 0, unimplemented.NewWithIssue(
			97180,
			"binary format for COPY TO not implemented",
		)
	case tree.CopyFormatCSV:
		csvTranslater := &csvCopyToTranslater{
			copyOptions: copyOptions,
			fmtCtx:      p.EvalContext().FmtCtx(tree.FmtPgwireText),
		}
		csvTranslater.w = csv.NewWriter(&csvTranslater.b)
		csvTranslater.w.Comma = rune(copyOptions.delimiter)
		if copyOptions.csvEscape != 0 {
			csvTranslater.w.Escape = copyOptions.csvEscape
		}
		t = csvTranslater
	default:
		textTranslater := &textCopyToTranslater{
			copyOptions: copyOptions,
			fmtCtx:      p.EvalContext().FmtCtx(tree.FmtPgwireText),
		}
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

	it, err := p.InternalSQLTxn().QueryIteratorEx(
		ctx,
		redact.RedactableString(tree.AsStringWithFlags(cmd.Stmt, tree.FmtMarkRedactionNode)),
		p.Txn(),
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
	if err := res.SendCopyOut(ctx, it.Types(), wireFormat); err != nil {
		return 0, err
	}

	if err := func() error {
		// Send header row if requested.
		// Send all the rows out to the client.
		if row, ok, err := t.headerRow(it.Types()); err != nil {
			return err
		} else if ok {
			if err := res.SendCopyData(ctx, row, true /* isHeader */); err != nil {
				return err
			}
		}

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
			if err := res.SendCopyData(ctx, row, false /* isHeader */); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return 0, err
	}
	return numOutputRows, res.SendCopyDone(ctx)
}

var encodeMap = func() map[byte]byte {
	ret := make(map[byte]byte, len(decodeMap))
	for k, v := range decodeMap {
		ret[v] = k
	}
	return ret
}()

// EncodeCopy escapes a single COPY field.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html#AEN74432
// NOTE: we don't have to worry about hex in COPY TO.
func EncodeCopy(w io.Writer, in []byte, delimiter byte) error {
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
