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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	mysql "github.com/xwb1989/sqlparser"
	mysqltypes "github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	var inserts, count int64
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

			if expected, found := m.conv.tableDesc.Name, i.Table.Name.String(); expected != found {
				// This row does not belong to the table being loaded. If we've already
				// seen rows, we can assume we're done, otherwise keep reading.
				if inserts > 0 {
					break
				}
				continue
			}
			inserts++
			rows, ok := i.Rows.(mysql.Values)
			if !ok {
				return errors.Errorf(
					"insert statement %d: unexpected insert row type %T: %v", inserts, rows, i.Rows,
				)
			}
			startingCount := count
			for _, inputRow := range rows {
				count++
				if expected, got := len(m.conv.visibleCols), len(inputRow); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, inputRow)
				}
				for i, raw := range inputRow {
					converted, err := mysqlToCockroach(raw, m.conv.visibleColTypes[i], m.conv.evalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
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

func readMysqlCreateTable(
	input io.Reader, evalCtx *tree.EvalContext, match string,
) (*sqlbase.TableDescriptor, error) {
	r := bufio.NewReaderSize(input, 1024*64)
	tokens := mysql.NewTokenizer(r)

	var found []string
	for {
		stmt, err := mysql.ParseNext(tokens)
		if err == io.EOF {
			if match != "" {
				return nil, errors.Errorf("table %q not found in file (found tables: %s)", match, strings.Join(found, ", "))
			}
			return nil, errors.Errorf("no table definition found")
		}
		if err != nil {
			return nil, errors.Wrap(err, "mysql parse error")
		}
		if i, ok := stmt.(*mysql.DDL); ok && i.Action == mysql.CreateStr {
			name := i.NewName.Name.String()
			if match != "" && match != name {
				found = append(found, name)
				continue
			}
			id := sqlbase.ID(int(defaultCSVTableID))
			return mysqlTableToCockroach(evalCtx, id, name, i.TableSpec)
		}
	}
}

func mysqlTableToCockroach(
	evalCtx *tree.EvalContext, id sqlbase.ID, name string, in *mysql.TableSpec,
) (*sqlbase.TableDescriptor, error) {
	if in == nil {
		return nil, errors.Errorf("could not read definition for table %q (possible unsupoprted type?)", name)
	}

	time := hlc.Timestamp{WallTime: evalCtx.GetStmtTimestamp().UnixNano()}
	priv := sqlbase.NewDefaultPrivilegeDescriptor()
	desc := sql.InitTableDescriptor(id, defaultCSVParentID, name, time, priv)

	colNames := make(map[string]string)
	for _, raw := range in.Columns {
		def, err := mysqlColToCockroach(raw.Name.String(), raw.Type)
		if err != nil {
			return nil, err
		}
		col, _, _, err := sqlbase.MakeColumnDefDescs(def, &tree.SemaContext{}, evalCtx)
		if err != nil {
			return nil, err
		}
		desc.AddColumn(*col)
		colNames[raw.Name.String()] = col.Name
	}

	for _, raw := range in.Indexes {
		var elems tree.IndexElemList
		for _, col := range raw.Columns {
			elems = append(elems, tree.IndexElem{Column: tree.Name(colNames[col.Column.String()])})
		}
		idx := sqlbase.IndexDescriptor{Name: raw.Info.Name.String(), Unique: raw.Info.Unique}
		if err := idx.FillColumns(elems); err != nil {
			return nil, err
		}
		if err := desc.AddIndex(idx, raw.Info.Primary); err != nil {
			return nil, err
		}
	}

	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := desc.AllocateIDs()
	return &desc, err
}

func mysqlColToCockroach(name string, col mysql.ColumnType) (*tree.ColumnTableDef, error) {
	def := &tree.ColumnTableDef{Name: tree.Name(name)}

	var length, scale int

	if col.Length != nil {
		num, err := strconv.Atoi(string(col.Length.Val))
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse length for column %q", name)
		}
		length = num
	}

	if col.Scale != nil {
		num, err := strconv.Atoi(string(col.Scale.Val))
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse scale for column %q", name)
		}
		scale = num
	}

	switch col.SQLType() {
	case mysqltypes.Char:
		def.Type = strOpt(coltypes.Char, length)
	case mysqltypes.VarChar:
		def.Type = strOpt(coltypes.VarChar, length)
	case mysqltypes.Text:
		def.Type = strOpt(coltypes.Text, length)
	case mysqltypes.Blob:
		def.Type = coltypes.Bytes
	case mysqltypes.VarBinary:
		def.Type = coltypes.Bytes
	case mysqltypes.Binary:
		def.Type = coltypes.Bytes

	case mysqltypes.Int8:
		def.Type = coltypes.SmallInt
	case mysqltypes.Uint8:
		def.Type = coltypes.SmallInt
	case mysqltypes.Int16:
		def.Type = coltypes.SmallInt
	case mysqltypes.Uint16:
		def.Type = coltypes.SmallInt
	case mysqltypes.Int24:
		def.Type = coltypes.Int
	case mysqltypes.Uint24:
		def.Type = coltypes.Int
	case mysqltypes.Int32:
		def.Type = coltypes.Int
	case mysqltypes.Uint32:
		def.Type = coltypes.Int
	case mysqltypes.Int64:
		def.Type = coltypes.BigInt
	case mysqltypes.Uint64:
		def.Type = coltypes.BigInt

	case mysqltypes.Float32:
		def.Type = coltypes.Float4
	case mysqltypes.Float64:
		def.Type = coltypes.Double

	case mysqltypes.Decimal:
		def.Type = decOpt(coltypes.Decimal, length, scale)

	case mysqltypes.Date:
		def.Type = coltypes.Date
	case mysqltypes.Time:
		def.Type = coltypes.Time
	case mysqltypes.Timestamp:
		def.Type = coltypes.TimestampWithTZ
	case mysqltypes.Datetime:
		def.Type = coltypes.TimestampWithTZ
	case mysqltypes.Year:
		def.Type = coltypes.SmallInt

	case mysqltypes.Enum:
		fallthrough
	case mysqltypes.Set:
		fallthrough
	case mysqltypes.Geometry:
		fallthrough
	case mysqltypes.TypeJSON:
		// TODO(dt): is our type close enough to use here?
		fallthrough
	case mysqltypes.Bit:
		// TODO(dt): is our type close enough to use here?
		fallthrough
	default:
		return nil, errors.Errorf("unsupported mysql type %q", col.Type)
	}
	if col.NotNull {
		def.Nullable.Nullability = tree.NotNull
	} else {
		def.Nullable.Nullability = tree.Null
	}
	if col.Default != nil && !bytes.EqualFold(col.Default.Val, []byte("null")) {
		expr, err := parser.ParseExpr(string(col.Default.Val))
		if err != nil {
			return nil, errors.Wrapf(err, "unsupported default expression for column %q", name)
		}
		def.DefaultExpr.Expr = expr
	}
	return def, nil
}

func strOpt(in *coltypes.TString, i int) coltypes.T {
	if i == 0 {
		return in
	}
	res := *in
	res.N = i
	return &res
}

func decOpt(in *coltypes.TDecimal, prec, scale int) coltypes.T {
	if prec == 0 && scale == 0 {
		return in
	}
	res := *in
	res.Prec = prec
	res.Scale = scale
	return &res
}
