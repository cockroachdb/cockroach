// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	mysql "github.com/xwb1989/sqlparser"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type mysqldumpReader struct {
	conv     rowConverter
	debugRow func(tree.Datums)
}

var _ inputConverter = &mysqldumpReader{}

func newMysqldumpReader(
	kvCh chan kvBatch, table *sqlbase.TableDescriptor, evalCtx *tree.EvalContext,
) (*mysqldumpReader, error) {
	conv, err := newRowConverter(table, evalCtx, kvCh)
	if err != nil {
		return nil, err
	}
	return &mysqldumpReader{conv: *conv}, err
}

func (m *mysqldumpReader) start(ctx ctxgroup.Group) {
}

func (m *mysqldumpReader) inputFinished(ctx context.Context) {
	close(m.conv.kvCh)
}

func (m *mysqldumpReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	var count int64
	r := bufio.NewReaderSize(input, 1024*64)
	tokens := mysql.NewTokenizer(r)

	for {
		stmt, err := mysql.ParseNext(tokens)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "mysql parse error")
		}
		switch i := stmt.(type) {
		case *mysql.Insert:
			rows, ok := i.Rows.(mysql.Values)
			if !ok {
				return errors.Errorf("unexpected insert row type %T: %v", rows, i.Rows)
			}
			for _, inputRow := range rows {
				count++
				if expected, got := len(m.conv.visibleCols), len(inputRow); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, inputRow)
				}
				for i, raw := range inputRow {
					converted, err := mysqlToCockroach(raw, m.conv.visibleColTypes[i], m.conv.evalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d", count)
					}
					m.conv.datums[i] = converted
				}
				if err := m.conv.row(ctx, inputIdx, count); err != nil {
					return err
				}
				if m.debugRow != nil {
					m.debugRow(m.conv.datums)
				}
			}
		default:
			if log.V(3) {
				log.Infof(ctx, "ignoring %T stmt: %v", i, i)
			}
			continue
		}
	}
	return m.conv.sendBatch(ctx)
}

func mysqlToCockroach(
	raw mysql.Expr, want types.T, evalContext *tree.EvalContext,
) (tree.Datum, error) {
	/* Possible mysql value types:
	StrVal
	IntVal
	FloatVal
	HexNum
	HexVal
	ValArg
	BitVal
	*/

	switch v := raw.(type) {
	case mysql.BoolVal:
		if v {
			return tree.DBoolTrue, nil
		}
		return tree.DBoolFalse, nil
	case *mysql.SQLVal:
		switch v.Type {
		case mysql.StrVal:
			return tree.ParseStringAs(want, string(v.Val), evalContext)
		case mysql.IntVal:
			return tree.ParseStringAs(want, string(v.Val), evalContext)
		case mysql.FloatVal:
			return tree.ParseStringAs(want, string(v.Val), evalContext)
		case mysql.HexVal:
			v, err := v.HexDecode()
			return tree.NewDBytes(tree.DBytes(v)), err
		// Don't expect ValArg since dump files do not use args.
		// TODO(dt): Do we need to handle HexNum or BitVal?
		default:
			return nil, fmt.Errorf("unsupported value type %c: %v", v.Type, v)
		}
	case *mysql.NullVal:
		return tree.DNull, nil

	default:
		return nil, errors.Errorf("unexpected value type %T: %v", v, v)
	}

}
