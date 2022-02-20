// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	mysqltypes "vitess.io/vitess/go/sqltypes"
	mysql "vitess.io/vitess/go/vt/sqlparser"
)

// mysqldumpReader reads the default output of `mysqldump`, which consists of
// SQL statements, in MySQL-dialect, namely CREATE TABLE and INSERT statements,
// with some additional statements that control the loading process like LOCK or
// UNLOCK (which are simply ignored for the purposed of this reader). Data for
// tables with names that appear in the `tables` map is converted to Cockroach
// KVs using the mapped converter and sent to kvCh.
type mysqldumpReader struct {
	evalCtx  *tree.EvalContext
	tables   map[string]*row.DatumRowConverter
	kvCh     chan row.KVBatch
	debugRow func(tree.Datums)
	walltime int64
	opts     roachpb.MysqldumpOptions
}

var _ inputConverter = &mysqldumpReader{}

func newMysqldumpReader(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	kvCh chan row.KVBatch,
	walltime int64,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	evalCtx *tree.EvalContext,
	opts roachpb.MysqldumpOptions,
) (*mysqldumpReader, error) {
	res := &mysqldumpReader{evalCtx: evalCtx, kvCh: kvCh, walltime: walltime, opts: opts}

	converters := make(map[string]*row.DatumRowConverter, len(tables))
	for name, table := range tables {
		if table.Desc == nil {
			converters[name] = nil
			continue
		}
		conv, err := row.NewDatumRowConverter(ctx, semaCtx, tabledesc.NewBuilder(table.Desc).
			BuildImmutableTable(), nil /* targetColNames */, evalCtx, kvCh,
			nil /* seqChunkProvider */, nil /* metrics */)
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

func (m *mysqldumpReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
	user security.SQLUsername,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, m.readFile, makeExternalStorage, user)
}

func (m *mysqldumpReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	var inserts, count int64
	r := bufio.NewReaderSize(input, 1024*64)
	tableNameToRowsProcessed := make(map[string]int64)
	rowLimit := m.opts.RowLimit
	tokens := mysql.NewTokenizer(r)
	tokens.SkipSpecialComments = true

	for _, conv := range m.tables {
		conv.KvBatch.Source = inputIdx
		conv.FractionFn = input.ReadFraction
		conv.CompletedRowFn = func() int64 {
			return count
		}
	}

	for {
		stmt, err := mysql.ParseNextStrictDDL(tokens)
		if err == io.EOF {
			break
		}
		if errors.Is(err, mysql.ErrEmpty) {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "mysql parse error")
		}
		switch i := stmt.(type) {
		case *mysql.Insert:
			name := safeString(i.Table.Name)
			conv, ok := m.tables[lexbase.NormalizeName(name)]
			if !ok {
				// not importing this table.
				continue
			}
			if conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name)
			}
			inserts++
			timestamp := timestampAfterEpoch(m.walltime)
			rows, ok := i.Rows.(mysql.Values)
			if !ok {
				return errors.Errorf(
					"insert statement %d: unexpected insert row type %T: %v", inserts, rows, i.Rows,
				)
			}
			startingCount := count
			for _, inputRow := range rows {
				count++
				tableNameToRowsProcessed[name]++

				if count <= resumePos {
					continue
				}
				if rowLimit != 0 && tableNameToRowsProcessed[name] > rowLimit {
					break
				}
				if expected, got := len(conv.VisibleCols), len(inputRow); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, inputRow)
				}
				for i, raw := range inputRow {
					converted, err := mysqlValueToDatum(raw, conv.VisibleColTypes[i], conv.EvalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					conv.Datums[i] = converted
				}
				if err := conv.Row(ctx, inputIdx, count+int64(timestamp)); err != nil {
					return err
				}
				if m.debugRow != nil {
					m.debugRow(conv.Datums)
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
		if err := conv.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

const (
	zeroDate = "0000-00-00"
	zeroYear = "0000"
	zeroTime = "0000-00-00 00:00:00"
)

// mysqlValueToDatum attempts to convert a value, as parsed from a mysqldump
// INSERT statement, in to a Cockroach Datum of type `desired`. The MySQL parser
// does not parse the values themselves to Go primitivies, rather leaving the
// original bytes uninterpreted in a value wrapper. The possible mysql value
// wrapper types are: StrVal, IntVal, FloatVal, HexNum, HexVal, ValArg, BitVal
// as well as NullVal.
func mysqlValueToDatum(
	raw mysql.Expr, desired *types.T, evalContext *tree.EvalContext,
) (tree.Datum, error) {
	switch v := raw.(type) {
	case mysql.BoolVal:
		if v {
			return tree.DBoolTrue, nil
		}
		return tree.DBoolFalse, nil
	case *mysql.Literal:
		switch v.Type {
		case mysql.StrVal:
			s := string(v.Val)
			// https://github.com/cockroachdb/cockroach/issues/29298

			if strings.HasPrefix(s, zeroYear) {
				switch desired.Family() {
				case types.TimestampTZFamily, types.TimestampFamily:
					if s == zeroTime {
						return tree.DNull, nil
					}
				case types.DateFamily:
					if s == zeroDate {
						return tree.DNull, nil
					}
				}
			}
			// This uses ParseDatumStringAsWithRawBytes instead of ParseDatumStringAs since mysql emits
			// raw byte strings that do not use the same escaping as our ParseBytes
			// function expects, and the difference between ParseStringAs and
			// ParseDatumStringAs is whether or not it attempts to parse bytes.
			return rowenc.ParseDatumStringAsWithRawBytes(desired, s, evalContext)
		case mysql.IntVal:
			return rowenc.ParseDatumStringAs(desired, string(v.Val), evalContext)
		case mysql.FloatVal:
			return rowenc.ParseDatumStringAs(desired, string(v.Val), evalContext)
		case mysql.HexVal:
			v, err := v.HexDecode()
			return tree.NewDBytes(tree.DBytes(v)), err
		// ValArg appears to be for placeholders, which should not appear in dumps.
		// TODO(dt): Do we need to handle HexNum or BitVal?
		default:
			return nil, fmt.Errorf("unsupported value type %c: %v", v.Type, v)
		}

	case *mysql.UnaryExpr:
		switch v.Operator {
		case mysql.UMinusOp:
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
		case mysql.UBinaryOp:
			// TODO(dt): do we want to use this hint to change our decoding logic?
			return mysqlValueToDatum(v.Expr, desired, evalContext)
		default:
			return nil, errors.Errorf("unexpected operator: %q", v.Operator)
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
	p sql.JobExecContext,
	startingID descpb.ID,
	parentDB catalog.DatabaseDescriptor,
	match string,
	fks fkHandler,
	seqVals map[descpb.ID]int64,
	owner security.SQLUsername,
	walltime int64,
) ([]*tabledesc.Mutable, error) {
	match = lexbase.NormalizeName(match)
	r := bufio.NewReaderSize(input, 1024*64)
	tokens := mysql.NewTokenizer(r)
	tokens.SkipSpecialComments = true

	var ret []*tabledesc.Mutable
	var fkDefs []delayedFK
	var found bool
	var names []string
	for {
		stmt, err := mysql.ParseNextStrictDDL(tokens)
		if err == nil {
			err = tokens.LastError
		}
		if err == io.EOF {
			break
		}
		if errors.Is(err, mysql.ErrEmpty) {
			continue
		}
		if err != nil {
			return nil, errors.Wrap(err, "mysql parse error")
		}
		if i, ok := stmt.(*mysql.DDL); ok && i.Action == mysql.CreateDDLAction {
			name := safeString(i.Table.Name)
			if match != "" && match != name {
				names = append(names, name)
				continue
			}
			id := descpb.ID(int(startingID) + len(ret))
			tbl, moreFKs, err := mysqlTableToCockroach(ctx, evalCtx, p, parentDB, id, name, i.TableSpec, fks, seqVals, owner, walltime)
			if err != nil {
				return nil, err
			}
			fkDefs = append(fkDefs, moreFKs...)
			ret = append(ret, tbl...)
			if match == name {
				found = true
				break
			}
		}
	}
	if ret == nil {
		return nil, errors.Errorf("no table definitions found")
	}
	if match != "" && !found {
		return nil, errors.Errorf("table %q not found in file (found tables: %s)", match, strings.Join(names, ", "))
	}
	if err := addDelayedFKs(ctx, fkDefs, fks.resolver, evalCtx); err != nil {
		return nil, err
	}
	return ret, nil
}

type mysqlIdent interface{ CompliantName() string }

func safeString(in mysqlIdent) string {
	return lexbase.NormalizeName(in.CompliantName())
}

func safeName(in mysqlIdent) tree.Name {
	return tree.Name(safeString(in))
}

// mysqlTableToCockroach creates a Cockroach TableDescriptor from a parsed mysql
// CREATE TABLE statement, converting columns and indexes to their closest
// Cockroach counterparts.
func mysqlTableToCockroach(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	p sql.JobExecContext,
	parentDB catalog.DatabaseDescriptor,
	id descpb.ID,
	name string,
	in *mysql.TableSpec,
	fks fkHandler,
	seqVals map[descpb.ID]int64,
	owner security.SQLUsername,
	walltime int64,
) ([]*tabledesc.Mutable, []delayedFK, error) {
	if in == nil {
		return nil, nil, errors.Errorf("could not read definition for table %q (possible unsupported type?)", name)
	}

	time := hlc.Timestamp{WallTime: walltime}

	const seqOpt = "auto_increment="
	var seqName string
	var startingValue int64
	for _, opt := range strings.Fields(strings.ToLower(in.Options)) {
		if strings.HasPrefix(opt, seqOpt) {
			seqName = name + "_auto_inc"
			i, err := strconv.Atoi(strings.TrimPrefix(opt, seqOpt))
			if err != nil {
				return nil, nil, errors.Wrapf(err, "parsing AUTO_INCREMENT value")
			}
			startingValue = int64(i)
			break
		}
	}

	if seqName == "" {
		for _, raw := range in.Columns {
			if raw.Type.Autoincrement {
				seqName = name + "_auto_inc"
				break
			}
		}
	}
	publicSchema, err := getPublicSchemaDescForDatabase(ctx, p.ExecCfg(), parentDB)
	if err != nil {
		return nil, nil, err
	}
	var seqDesc *tabledesc.Mutable
	// If we have an auto-increment seq, create it and increment the id.
	if seqName != "" {
		var opts tree.SequenceOptions
		if startingValue != 0 {
			opts = tree.SequenceOptions{{Name: tree.SeqOptStart, IntVal: &startingValue}}
			seqVals[id] = startingValue
		}
		var err error
		privilegeDesc := catpb.NewBasePrivilegeDescriptor(owner)
		seqDesc, err = sql.NewSequenceTableDesc(
			ctx,
			nil, /* planner */
			evalCtx.Settings,
			seqName,
			opts,
			parentDB.GetID(),
			publicSchema.GetID(),
			id,
			time,
			privilegeDesc,
			tree.PersistencePermanent,
			// If this is multi-region, this will get added by WriteDescriptors.
			false, /* isMultiRegion */
		)
		if err != nil {
			return nil, nil, err
		}
		fks.resolver.tableNameToDesc[seqName] = seqDesc
		id++
	}

	stmt := &tree.CreateTable{Table: tree.MakeUnqualifiedTableName(tree.Name(name))}

	checks := make(map[string]*tree.CheckConstraintTableDef)

	for _, raw := range in.Columns {
		def, err := mysqlColToCockroach(safeString(raw.Name), raw.Type, checks)
		if err != nil {
			return nil, nil, err
		}
		if raw.Type.Autoincrement {

			expr, err := parser.ParseExpr(fmt.Sprintf("nextval('%s':::STRING)", seqName))
			if err != nil {
				return nil, nil, err
			}
			def.DefaultExpr.Expr = expr
		}
		// If the target table columns have data type INT or INTEGER, they need to
		// be updated to conform to the session variable `default_int_size`.
		if dType, ok := def.Type.(*types.T); ok {
			if dType.Equivalent(types.Int) && p != nil && p.SessionData() != nil {
				def.Type = parser.NakedIntTypeFromDefaultIntSize(p.SessionData().DefaultIntSize)
			}
		}
		stmt.Defs = append(stmt.Defs, def)
	}

	for _, raw := range in.Indexes {
		var elems tree.IndexElemList
		for _, col := range raw.Columns {
			elems = append(elems, tree.IndexElem{Column: safeName(col.Column)})
		}

		idxName := safeName(raw.Info.Name)
		// In MySQL, all PRIMARY KEY have the constraint name PRIMARY.
		// To match PostgreSQL, we want to rename this to the appropriate form.
		if raw.Info.Primary {
			idxName = tree.Name(tabledesc.PrimaryKeyIndexName(name))
		}
		idx := tree.IndexTableDef{Name: idxName, Columns: elems}
		if raw.Info.Primary || raw.Info.Unique {
			stmt.Defs = append(stmt.Defs, &tree.UniqueConstraintTableDef{IndexTableDef: idx, PrimaryKey: raw.Info.Primary})
		} else {
			stmt.Defs = append(stmt.Defs, &idx)
		}
	}

	for _, c := range checks {
		stmt.Defs = append(stmt.Defs, c)
	}

	semaCtx := tree.MakeSemaContext()
	semaCtxPtr := &semaCtx
	// p is nil in some tests.
	if p != nil && p.SemaCtx() != nil {
		semaCtxPtr = p.SemaCtx()
	}

	// Bundle imports do not support user defined types, and so we nil out the
	// type resolver to protect against unexpected behavior on UDT resolution.
	semaCtxPtr = makeSemaCtxWithoutTypeResolver(semaCtxPtr)
	desc, err := MakeSimpleTableDescriptor(
		ctx, semaCtxPtr, evalCtx.Settings, stmt, parentDB,
		publicSchema, id, fks, time.WallTime,
	)
	if err != nil {
		return nil, nil, err
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
			toTable := tree.MakeTableNameWithSchema(
				safeName(i.ReferencedTable.Qualifier),
				tree.PublicSchemaName,
				safeName(i.ReferencedTable.Name),
			)
			toCols := i.ReferencedColumns
			d := &tree.ForeignKeyConstraintTableDef{
				Name:     tree.Name(lexbase.NormalizeName(raw.Name)),
				FromCols: toNameList(fromCols),
				ToCols:   toNameList(toCols),
			}

			if i.OnDelete != mysql.NoAction {
				d.Actions.Delete = mysqlActionToCockroach(i.OnDelete)
			}
			if i.OnUpdate != mysql.NoAction {
				d.Actions.Update = mysqlActionToCockroach(i.OnUpdate)
			}

			d.Table = toTable
			fkDefs = append(fkDefs, delayedFK{
				db:  parentDB,
				sc:  schemadesc.GetPublicSchema(),
				tbl: desc,
				def: d,
			})
		}
	}
	fks.resolver.tableNameToDesc[desc.Name] = desc
	if seqDesc != nil {
		return []*tabledesc.Mutable{seqDesc, desc}, fkDefs, nil
	}
	return []*tabledesc.Mutable{desc}, fkDefs, nil
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
	db  catalog.DatabaseDescriptor
	sc  catalog.SchemaDescriptor
	tbl *tabledesc.Mutable
	def *tree.ForeignKeyConstraintTableDef
}

func addDelayedFKs(
	ctx context.Context, defs []delayedFK, resolver fkResolver, evalCtx *tree.EvalContext,
) error {
	for _, def := range defs {
		backrefs := map[descpb.ID]*tabledesc.Mutable{}
		if err := sql.ResolveFK(
			ctx, nil, &resolver, def.db, def.sc, def.tbl, def.def,
			backrefs, sql.NewTable,
			tree.ValidationDefault, evalCtx,
		); err != nil {
			return err
		}
		if err := fixDescriptorFKState(def.tbl); err != nil {
			return err
		}
		version := evalCtx.Settings.Version.ActiveVersion(ctx)
		if err := def.tbl.AllocateIDs(ctx, version); err != nil {
			return err
		}
	}
	return nil
}

func toNameList(cols mysql.Columns) tree.NameList {
	res := make([]tree.Name, len(cols))
	for i := range cols {
		res[i] = safeName(cols[i])
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
		def.Type = types.MakeChar(int32(length))
	case mysqltypes.VarChar:
		def.Type = types.MakeVarChar(int32(length))
	case mysqltypes.Text:
		def.Type = types.MakeString(int32(length))

	case mysqltypes.Blob:
		def.Type = types.Bytes
	case mysqltypes.VarBinary:
		def.Type = types.Bytes
	case mysqltypes.Binary:
		def.Type = types.Bytes

	case mysqltypes.Int8:
		def.Type = types.Int2
	case mysqltypes.Uint8:
		def.Type = types.Int2
	case mysqltypes.Int16:
		def.Type = types.Int2
	case mysqltypes.Uint16:
		def.Type = types.Int4
	case mysqltypes.Int24:
		def.Type = types.Int4
	case mysqltypes.Uint24:
		def.Type = types.Int4
	case mysqltypes.Int32:
		def.Type = types.Int4
	case mysqltypes.Uint32:
		def.Type = types.Int
	case mysqltypes.Int64:
		def.Type = types.Int
	case mysqltypes.Uint64:
		def.Type = types.Int

	case mysqltypes.Float32:
		def.Type = types.Float4
	case mysqltypes.Float64:
		def.Type = types.Float

	case mysqltypes.Decimal:
		def.Type = types.MakeDecimal(int32(length), int32(scale))

	case mysqltypes.Date:
		def.Type = types.Date
		if col.Default != nil {
			if lit, ok := col.Default.(*mysql.Literal); ok && bytes.Equal(lit.Val, []byte(zeroDate)) {
				col.Default = nil
			}
		}
	case mysqltypes.Time:
		def.Type = types.Time
	case mysqltypes.Timestamp:
		def.Type = types.TimestampTZ
		if col.Default != nil {
			if lit, ok := col.Default.(*mysql.Literal); ok && bytes.Equal(lit.Val, []byte(zeroTime)) {
				col.Default = nil
			}
		}
	case mysqltypes.Datetime:
		def.Type = types.TimestampTZ
		if col.Default != nil {
			if lit, ok := col.Default.(*mysql.Literal); ok && bytes.Equal(lit.Val, []byte(zeroTime)) {
				col.Default = nil
			}
		}
	case mysqltypes.Year:
		def.Type = types.Int2

	case mysqltypes.Enum:
		def.Type = types.String

		expr, err := parser.ParseExpr(fmt.Sprintf("%s in (%s)", name, strings.Join(col.EnumValues, ",")))
		if err != nil {
			return nil, err
		}
		checks[name] = &tree.CheckConstraintTableDef{
			Name: tree.Name(fmt.Sprintf("imported_from_enum_%s", name)),
			Expr: expr,
		}

	case mysqltypes.TypeJSON:
		def.Type = types.Jsonb

	case mysqltypes.Set:
		return nil, errors.WithHint(
			unimplemented.NewWithIssue(32560, "cannot import SET columns at this time"),
			"try converting the column to a 64-bit integer before import")
	case mysqltypes.Geometry:
		return nil, unimplemented.NewWithIssue(32559, "cannot import GEOMETRY columns at this time")
	case mysqltypes.Bit:
		return nil, errors.WithHint(
			unimplemented.NewWithIssue(32561, "cannot import BIT columns at this time"),
			"try converting the column to a 64-bit integer before import")
	default:
		return nil, unimplemented.Newf(fmt.Sprintf("import.mysqlcoltype.%s", typ),
			"unsupported mysql type %q", col.Type)
	}

	if col.NotNull {
		def.Nullable.Nullability = tree.NotNull
	} else {
		def.Nullable.Nullability = tree.Null
	}

	if col.Default != nil {
		if _, ok := col.Default.(*mysql.NullVal); !ok {
			if literal, ok := col.Default.(*mysql.Literal); ok && literal.Type == mysql.StrVal {
				// mysql.String(col.Default) returns a quoted string for string
				// literals. We should use the literal's Val instead.
				def.DefaultExpr.Expr = tree.NewStrVal(string(literal.Val))
			} else {
				exprString := mysql.String(col.Default)
				expr, err := parser.ParseExpr(exprString)
				if err != nil {
					// There is currently no way to wrap an error with an unimplemented
					// error.
					// nolint:errwrap
					return nil, errors.Wrapf(
						unimplemented.Newf("import.mysql.default", "unsupported default expression"),
						"error parsing %q for column %q: %v",
						exprString,
						name,
						err,
					)
				}
				def.DefaultExpr.Expr = expr
			}
		}
	}
	return def, nil
}
