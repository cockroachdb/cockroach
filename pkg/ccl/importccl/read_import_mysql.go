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
	mysqltypes "vitess.io/vitess/go/sqltypes"
	mysql "vitess.io/vitess/go/vt/sqlparser"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// mysqldumpReader reads the default output of `mysqldump`, which consists of
// SQL statements, in MySQL-dialect, namely CREATE TABLE and INSERT statements,
// with some additional statements that contorl the loading process like LOCK or
// UNLOCK (which are simply ignored for the purposed of this reader). Data for
// tables with names that appear in the `tables` map is converted to Cockroach
// KVs using the mapped converter and sent to kvCh.
type mysqldumpReader struct {
	evalCtx  *tree.EvalContext
	tables   map[string]*rowConverter
	kvCh     chan kvBatch
	debugRow func(tree.Datums)
}

var _ inputConverter = &mysqldumpReader{}

func newMysqldumpReader(
	kvCh chan kvBatch, tables map[string]*sqlbase.TableDescriptor, evalCtx *tree.EvalContext,
) (*mysqldumpReader, error) {
	res := &mysqldumpReader{evalCtx: evalCtx, kvCh: kvCh}

	converters := make(map[string]*rowConverter, len(tables))
	for name, table := range tables {
		if table == nil {
			converters[name] = nil
			continue
		}
		conv, err := newRowConverter(table, evalCtx, kvCh)
		if err != nil {
			return nil, err
		}
		converters[name] = conv
	}
	res.tables = converters
	return res, nil
}

func (m *mysqldumpReader) start(ctx ctxgroup.Group) {
}

func (m *mysqldumpReader) inputFinished(ctx context.Context) {
	close(m.kvCh)
}

func (m *mysqldumpReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	var inserts, count int64
	r := bufio.NewReaderSize(input, 1024*64)
	tokens := mysql.NewTokenizer(r)
	tokens.SkipSpecialComments = true

	for {
		stmt, err := mysql.ParseNextStrictDDL(tokens)
		if err == io.EOF {
			break
		}
		if err == mysql.ErrEmpty {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "mysql parse error")
		}
		switch i := stmt.(type) {
		case *mysql.Insert:
			name := i.Table.Name.String()
			conv, ok := m.tables[lex.NormalizeName(name)]
			if !ok {
				// not importing this table.
				continue
			}
			if conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name)
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
				if expected, got := len(conv.visibleCols), len(inputRow); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, inputRow)
				}
				for i, raw := range inputRow {
					converted, err := mysqlValueToDatum(raw, conv.visibleColTypes[i], conv.evalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					conv.datums[i] = converted
				}
				if err := conv.row(ctx, inputIdx, count); err != nil {
					return err
				}
				if m.debugRow != nil {
					m.debugRow(conv.datums)
				}
			}
		default:
			if log.V(3) {
				log.Infof(ctx, "ignoring %T stmt: %v", i, i)
			}
			continue
		}
	}
	for _, conv := range m.tables {
		if err := conv.sendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

// mysqlValueToDatum attempts to convert a value, as parsed from a mysqldump
// INSERT statement, in to a Cockroach Datum of type `desired`. The MySQL parser
// does not parse the values themselves to Go primitivies, rather leaving the
// original bytes uninterpreted in a value wrapper. The possible mysql value
// wrapper types are: StrVal, IntVal, FloatVal, HexNum, HexVal, ValArg, BitVal
// as well as NullVal.
func mysqlValueToDatum(
	raw mysql.Expr, desired types.T, evalContext *tree.EvalContext,
) (tree.Datum, error) {
	switch v := raw.(type) {
	case mysql.BoolVal:
		if v {
			return tree.DBoolTrue, nil
		}
		return tree.DBoolFalse, nil
	case *mysql.SQLVal:
		switch v.Type {
		case mysql.StrVal:
			return tree.ParseStringAs(desired, string(v.Val), evalContext)
		case mysql.IntVal:
			return tree.ParseStringAs(desired, string(v.Val), evalContext)
		case mysql.FloatVal:
			return tree.ParseStringAs(desired, string(v.Val), evalContext)
		case mysql.HexVal:
			v, err := v.HexDecode()
			return tree.NewDBytes(tree.DBytes(v)), err
		// ValArg appears to be for placeholders, which should not appear in dumps.
		// TODO(dt): Do we need to handle HexNum or BitVal?
		default:
			return nil, fmt.Errorf("unsupported value type %c: %v", v.Type, v)
		}

	case *mysql.UnaryExpr:
		if v.Operator != "-" {
			return nil, errors.Errorf("unexpected operator: %q", v.Operator)
		}
		parsed, err := mysqlValueToDatum(v.Expr, desired, evalContext)
		if err != nil {
			return nil, err
		}
		switch i := parsed.(type) {
		case *tree.DInt:
			return tree.NewDInt(-*i), nil
		case *tree.DFloat:
			return tree.NewDFloat(-*i), nil
		case *tree.DDecimal:
			dec := &i.Decimal
			dd := &tree.DDecimal{}
			dd.Decimal.Neg(dec)
			return dd, nil
		default:
			return nil, errors.Errorf("unsupported negation of %T", i)
		}

	case *mysql.NullVal:
		return tree.DNull, nil

	default:
		return nil, errors.Errorf("unexpected value type %T: %v", v, v)
	}
}

// readMysqlCreateTable parses mysql-dialect SQL from input to extract table
// definitions and return them as Cockroach's TableDescriptors. If `match` is
// non-empty, only the table with that name is returned and an error is returned
// if a matching table is not found in the input. Otherwise, if match is empty,
// all tables encountered are returned (or an error is returned if no tables are
// found). Returned tables are given dummy, placeholder IDs -- it is up to the
// caller to allocate and assign real IDs.
func readMysqlCreateTable(
	ctx context.Context,
	input io.Reader,
	evalCtx *tree.EvalContext,
	startingID, parentID sqlbase.ID,
	match string,
	fks fkHandler,
) ([]*sqlbase.TableDescriptor, error) {
	match = lex.NormalizeName(match)
	r := bufio.NewReaderSize(input, 1024*64)
	tokens := mysql.NewTokenizer(r)
	tokens.SkipSpecialComments = true

	var ret []*sqlbase.TableDescriptor
	var fkDefs []delayedFK
	var found []string
	for {
		stmt, err := mysql.ParseNextStrictDDL(tokens)
		if err == io.EOF {
			if match != "" {
				return nil, errors.Errorf("table %q not found in file (found tables: %s)", match, strings.Join(found, ", "))
			}
			if ret == nil {
				return nil, errors.Errorf("no table definition found")
			}
			if err := addDelayedFKs(ctx, fkDefs, fks.resolver); err != nil {
				return nil, err
			}
			return ret, nil
		}
		if err == mysql.ErrEmpty {
			continue
		}
		if err != nil {
			return nil, errors.Wrap(err, "mysql parse error")
		}
		if i, ok := stmt.(*mysql.DDL); ok && i.Action == mysql.CreateStr {
			name := lex.NormalizeName(i.NewName.Name.String())
			if match != "" && match != name {
				found = append(found, name)
				continue
			}
			id := sqlbase.ID(int(startingID) + len(ret))
			tbl, moreFKs, err := mysqlTableToCockroach(ctx, evalCtx, parentID, id, name, i.TableSpec, fks)
			if err != nil {
				return nil, err
			}
			fkDefs = append(fkDefs, moreFKs...)
			if match == name {
				return []*sqlbase.TableDescriptor{tbl}, nil
			}
			if fks.allowed {
				fks.resolver[tbl.Name] = tbl
			}
			ret = append(ret, tbl)
		}
	}
}

// mysqlTableToCockroach creates a Cockroach TableDescriptor from a parsed mysql
// CREATE TABLE statement, converting columns and indexes to their closest
// Cockroach counterparts.
func mysqlTableToCockroach(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	parentID, id sqlbase.ID,
	name string,
	in *mysql.TableSpec,
	fks fkHandler,
) (*sqlbase.TableDescriptor, []delayedFK, error) {
	if in == nil {
		return nil, nil, errors.Errorf("could not read definition for table %q (possible unsupoprted type?)", name)
	}

	time := hlc.Timestamp{WallTime: evalCtx.GetStmtTimestamp().UnixNano()}
	priv := sqlbase.NewDefaultPrivilegeDescriptor()
	desc := sql.InitTableDescriptor(id, parentID, name, time, priv)

	colNames := make(map[string]string)
	checks := make(map[string]*tree.CheckConstraintTableDef)

	for _, raw := range in.Columns {
		def, err := mysqlColToCockroach(raw.Name.String(), raw.Type, checks)
		if err != nil {
			return nil, nil, err
		}
		// The new types in the table imported from MySQL do not (yet?)
		// use SERIAL so we need not process SERIAL types here.
		//
		// If/when we extend this functionality to support MySQL'sAUTO
		// INCREMENT, this will need to be extended -- see the comments on
		// MakeColumnDefDescs().
		col, _, _, err := sqlbase.MakeColumnDefDescs(def, &tree.SemaContext{}, evalCtx)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, err
		}
		if err := desc.AddIndex(idx, raw.Info.Primary); err != nil {
			return nil, nil, err
		}
	}

	if err := desc.AllocateIDs(); err != nil {
		return nil, nil, err
	}

	for _, check := range checks {
		ck, err := sql.MakeCheckConstraint(ctx, desc, check, nil, &tree.SemaContext{}, evalCtx, *tree.NewTableName("", tree.Name(name)))
		if err != nil {
			return nil, nil, err
		}
		desc.Checks = append(desc.Checks, ck)
	}

	var fkDefs []delayedFK
	for _, raw := range in.Constraints {
		switch i := raw.Details.(type) {
		case *mysql.ForeignKeyDefinition:
			if !fks.allowed {
				return nil, nil, errors.Errorf("foreign keys not supported: %s", mysql.String(raw))
			}
			if fks.skip {
				continue
			}
			fromCols := i.Source
			toTable := tree.MakeTableName(
				tree.Name(i.ReferencedTable.Qualifier.String()), tree.Name(i.ReferencedTable.Name.String()),
			)
			toCols := i.ReferencedColumns
			d := &tree.ForeignKeyConstraintTableDef{
				Name:     tree.Name(raw.Name),
				FromCols: toNameList(fromCols),
				ToCols:   toNameList(toCols),
			}

			if i.OnDelete != mysql.NoAction {
				d.Actions.Delete = mysqlActionToCockroach(i.OnDelete)
			}
			if i.OnUpdate != mysql.NoAction {
				d.Actions.Update = mysqlActionToCockroach(i.OnUpdate)
			}

			d.Table.TableNameReference = &toTable
			fkDefs = append(fkDefs, delayedFK{&desc, d})
		}
	}
	return &desc, fkDefs, nil
}

func mysqlActionToCockroach(action mysql.ReferenceAction) tree.ReferenceAction {
	switch action {
	case mysql.Restrict:
		return tree.Restrict
	case mysql.Cascade:
		return tree.Cascade
	case mysql.SetNull:
		return tree.SetNull
	case mysql.SetDefault:
		return tree.SetDefault
	}
	return tree.NoAction
}

type delayedFK struct {
	tbl *sqlbase.TableDescriptor
	def *tree.ForeignKeyConstraintTableDef
}

func addDelayedFKs(ctx context.Context, defs []delayedFK, resolver fkResolver) error {
	for _, def := range defs {
		if err := sql.ResolveFK(
			ctx, nil, resolver, def.tbl, def.def, map[sqlbase.ID]*sqlbase.TableDescriptor{}, sqlbase.ConstraintValidity_Validated,
		); err != nil {
			return err
		}
		if err := fixDescriptorFKState(def.tbl); err != nil {
			return err
		}
		if err := def.tbl.AllocateIDs(); err != nil {
			return err
		}
	}
	return nil
}

func toNameList(cols mysql.Columns) tree.NameList {
	res := make([]tree.Name, len(cols))
	for i := range cols {
		res[i] = tree.Name(cols[i].String())
	}
	return res
}

// mysqlColToCockroach attempts to convert a parsed MySQL column definition to a
// Cockroach column definition, mapping the mysql type to its closest Cockroach
// counterpart (or returning an error if it is unable to do so).
// To the extent possible, parameters such as length or precision are preseved
// even if they have only cosmetic (i.e. when viewing schemas) effects compared
// to their behavior in MySQL.
func mysqlColToCockroach(
	name string, col mysql.ColumnType, checks map[string]*tree.CheckConstraintTableDef,
) (*tree.ColumnTableDef, error) {
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

	switch typ := col.SQLType(); typ {

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
		def.Type = coltypes.String

		expr, err := parser.ParseExpr(fmt.Sprintf("%s in (%s)", name, strings.Join(col.EnumValues, ",")))
		if err != nil {
			return nil, err
		}
		checks[name] = &tree.CheckConstraintTableDef{
			Name: tree.Name(fmt.Sprintf("imported_from_enum_%s", name)),
			Expr: expr,
		}

	case mysqltypes.TypeJSON:
		def.Type = coltypes.JSON

	case mysqltypes.Set:
		fallthrough
	case mysqltypes.Geometry:
		fallthrough
	case mysqltypes.Bit:
		// TODO(dt): is our type close enough to use here?
		fallthrough
	default:
		return nil, pgerror.Unimplemented(fmt.Sprintf("import.mysqlcoltype.%s", typ), "unsupported mysql type %q", col.Type)
	}

	if col.NotNull {
		def.Nullable.Nullability = tree.NotNull
	} else {
		def.Nullable.Nullability = tree.Null
	}

	if col.Default != nil && !bytes.EqualFold(col.Default.Val, []byte("null")) {
		expr, err := parser.ParseExpr(string(col.Default.Val))
		if err != nil {
			return nil, pgerror.Unimplemented("import.mysql.default", "unsupported default expression for column %q: %v", name, err)
		}
		def.DefaultExpr.Expr = expr
	}

	return def, nil
}

// strOpt configures a string column type with the passed length if non-zero.
func strOpt(in *coltypes.TString, i int) coltypes.T {
	if i == 0 {
		return in
	}
	res := *in
	res.N = i
	return &res
}

// strOpt configures a decimal column type with passed options if non-zero.
func decOpt(in *coltypes.TDecimal, prec, scale int) coltypes.T {
	if prec == 0 && scale == 0 {
		return in
	}
	res := *in
	res.Prec = prec
	res.Scale = scale
	return &res
}
