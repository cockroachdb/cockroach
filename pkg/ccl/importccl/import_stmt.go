// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/gossipccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	csvDelimiter = "delimiter"
	csvComment   = "comment"
	csvNullIf    = "nullif"
	csvSkip      = "skip"

	mysqlOutfileRowSep   = "rows_terminated_by"
	mysqlOutfileFieldSep = "fields_terminated_by"
	mysqlOutfileEnclose  = "fields_enclosed_by"
	mysqlOutfileEscape   = "fields_escaped_by"

	importOptionTransform  = "transform"
	importOptionSSTSize    = "sstsize"
	importOptionDecompress = "decompress"
	importOptionOversample = "oversample"
	importOptionSkipFKs    = "skip_foreign_keys"

	pgCopyDelimiter = "delimiter"
	pgCopyNull      = "nullif"

	pgMaxRowSize = "max_row_size"
)

var importOptionExpectValues = map[string]sql.KVStringOptValidate{
	csvDelimiter: sql.KVStringOptRequireValue,
	csvComment:   sql.KVStringOptRequireValue,
	csvNullIf:    sql.KVStringOptRequireValue,
	csvSkip:      sql.KVStringOptRequireValue,

	mysqlOutfileRowSep:   sql.KVStringOptRequireValue,
	mysqlOutfileFieldSep: sql.KVStringOptRequireValue,
	mysqlOutfileEnclose:  sql.KVStringOptRequireValue,
	mysqlOutfileEscape:   sql.KVStringOptRequireValue,

	importOptionTransform:  sql.KVStringOptRequireValue,
	importOptionSSTSize:    sql.KVStringOptRequireValue,
	importOptionDecompress: sql.KVStringOptRequireValue,
	importOptionOversample: sql.KVStringOptRequireValue,

	importOptionSkipFKs: sql.KVStringOptRequireNoValue,

	pgMaxRowSize: sql.KVStringOptRequireValue,
}

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MinNonPredefinedUserDescID
	defaultCSVTableID  sqlbase.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(
	ctx context.Context, filename string, settings *cluster.Settings,
) (*tree.CreateTable, error) {
	store, err := storageccl.ExportStorageFromURI(ctx, filename, settings)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	create, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

type fkHandler struct {
	allowed  bool
	skip     bool
	resolver fkResolver
}

// NoFKs is used by formats that do not support FKs.
var NoFKs = fkHandler{resolver: make(fkResolver)}

// MakeSimpleTableDescriptor creates a MutableTableDescriptor from a CreateTable parse
// node without the full machinery. Many parts of the syntax are unsupported
// (see the implementation and TestMakeSimpleTableDescriptorErrors for details),
// but this is enough for our csv IMPORT and for some unit tests.
//
// Any occurrence of SERIAL in the column definitions is handled using
// the CockroachDB legacy behavior, i.e. INT NOT NULL DEFAULT
// unique_rowid().
func MakeSimpleTableDescriptor(
	ctx context.Context,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID,
	tableID sqlbase.ID,
	fks fkHandler,
	walltime int64,
) (*sqlbase.MutableTableDescriptor, error) {
	create.HoistConstraints()
	if create.IfNotExists {
		return nil, pgerror.Unimplemented("import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, pgerror.Unimplemented("import.interleave", "interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, pgerror.Unimplemented("import.create-as", "CREATE AS not supported")
	}

	filteredDefs := create.Defs[:0]
	for i := range create.Defs {
		switch def := create.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return nil, pgerror.Unimplemented("import.computed", "computed columns not supported: %s", tree.AsString(def))
			}

			if err := sql.SimplifySerialInColumnDefWithRowID(ctx, def, &create.Table); err != nil {
				return nil, err
			}

		case *tree.ForeignKeyConstraintTableDef:
			if !fks.allowed {
				return nil, pgerror.Unimplemented("import.fk", "this IMPORT format does not support foreign keys")
			}
			if fks.skip {
				continue
			}
			// Strip the schema/db prefix.
			def.Table = tree.MakeUnqualifiedTableName(def.Table.TableName)

		default:
			return nil, pgerror.Unimplemented(fmt.Sprintf("import.%T", def), "unsupported table definition: %s", tree.AsString(def))
		}
		// only append this def after we make it past the error checks and continues
		filteredDefs = append(filteredDefs, create.Defs[i])
	}
	create.Defs = filteredDefs

	semaCtx := tree.SemaContext{}
	evalCtx := tree.EvalContext{
		Context:  ctx,
		Sequence: &importSequenceOperators{},
	}
	affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		fks.resolver,
		st,
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		affected,
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		return nil, err
	}
	if err := fixDescriptorFKState(tableDesc.TableDesc()); err != nil {
		return nil, err
	}

	return &tableDesc, nil
}

// fixDescriptorFKState repairs validity and table states set during descriptor
// creation. sql.MakeTableDesc and ResolveFK set the table to the ADD state
// and mark references an validated. This function sets the table to PUBLIC
// and the FKs to unvalidated.
func fixDescriptorFKState(tableDesc *sqlbase.TableDescriptor) error {
	tableDesc.State = sqlbase.TableDescriptor_PUBLIC
	return tableDesc.ForeachNonDropIndex(func(idx *sqlbase.IndexDescriptor) error {
		if idx.ForeignKey.IsSet() {
			idx.ForeignKey.Validity = sqlbase.ConstraintValidity_Unvalidated
		}
		return nil
	})
}

var (
	errSequenceOperators = errors.New("sequence operations unsupported")
	errSchemaResolver    = errors.New("schema resolver unsupported")
)

// Implements the tree.SequenceOperators interface.
type importSequenceOperators struct {
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return parser.ParseTableName(sql)
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	return errSequenceOperators
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

type fkResolver map[string]*sqlbase.MutableTableDescriptor

var _ sql.SchemaResolver = fkResolver{}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) Txn() *client.Txn {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LogicalSchemaAccessor() sql.SchemaAccessor {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CurrentDatabase() string {
	return ""
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sessiondata.SearchPath{}
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CommonLookupFlags(required bool) sql.CommonLookupFlags {
	return sql.CommonLookupFlags{}
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) ObjectLookupFlags(required bool, requireMutable bool) sql.ObjectLookupFlags {
	return sql.ObjectLookupFlags{}
}

// Implements the tree.TableNameExistingResolver interface.
func (r fkResolver) LookupObject(
	ctx context.Context, requireMutable bool, dbName, scName, obName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if scName != "" {
		obName = strings.TrimPrefix(obName, scName+".")
	}
	tbl, ok := r[obName]
	if ok {
		return true, tbl, nil
	}
	names := make([]string, 0, len(r))
	for k := range r {
		names = append(names, k)
	}
	suggestions := strings.Join(names, ",")
	return false, nil, errors.Errorf("referenced table %q not found in tables being imported (%s)", obName, suggestions)
}

// Implements the tree.TableNameTargetResolver interface.
func (r fkResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	return false, nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableLookup, error) {
	return row.TableLookup{}, errSchemaResolver
}

const csvDatabaseName = "csv"

func finalizeCSVBackup(
	ctx context.Context,
	backupDesc *backupccl.BackupDescriptor,
	parentID sqlbase.ID,
	tables map[string]*sqlbase.TableDescriptor,
	es storageccl.ExportStorage,
	execCfg *sql.ExecutorConfig,
) error {
	sort.Sort(backupccl.BackupFileDescriptors(backupDesc.Files))

	backupDesc.Spans = make([]roachpb.Span, 0, len(tables))
	backupDesc.Descriptors = make([]sqlbase.Descriptor, 1, len(tables)+1)
	backupDesc.Descriptors[0] = *sqlbase.WrapDescriptor(
		&sqlbase.DatabaseDescriptor{Name: csvDatabaseName, ID: parentID},
	)

	for _, table := range tables {
		backupDesc.Spans = append(backupDesc.Spans, table.TableSpan())
		backupDesc.Descriptors = append(backupDesc.Descriptors, *sqlbase.WrapDescriptor(table))
	}

	backupDesc.FormatVersion = backupccl.BackupFormatInitialVersion
	backupDesc.BuildInfo = build.GetInfo()
	if execCfg != nil {
		backupDesc.NodeID = execCfg.NodeID.Get()
		backupDesc.ClusterID = execCfg.ClusterID()
	}
	descBuf, err := protoutil.Marshal(backupDesc)
	if err != nil {
		return err
	}
	return es.WriteFile(ctx, backupccl.BackupDescriptorName, bytes.NewReader(descBuf))
}

func importJobDescription(
	orig *tree.Import, defs tree.TableDefs, files []string, opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.CreateFile = nil
	stmt.CreateDefs = defs
	stmt.Files = nil
	for _, file := range files {
		clean, err := storageccl.SanitizeExportStorageURI(file)
		if err != nil {
			return "", err
		}
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		switch k {
		case importOptionTransform:
			clean, err := storageccl.SanitizeExportStorageURI(v)
			if err != nil {
				return "", err
			}
			v = clean
		}
		opt := tree.KVOption{Key: tree.Name(k)}
		val := importOptionExpectValues[k] == sql.KVStringOptRequireValue
		val = val || (importOptionExpectValues[k] == sql.KVStringOptAny && len(v) > 0)
		if val {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	return tree.AsStringWithFlags(&stmt, tree.FmtAlwaysQualifyTableNames), nil
}

// importPlanHook implements sql.PlanHookFn.
func importPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, nil, nil
	}

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, nil, err
	}

	var createFileFn func() (string, error)
	if !importStmt.Bundle && importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, nil, err
		}
	}

	optsFn, err := p.TypeAsStringOpts(importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, nil, nil, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer tracing.FinishSpan(span)

		walltime := p.ExecCfg().Clock.Now().WallTime

		if err := p.RequireSuperUser(ctx, "IMPORT"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("IMPORT cannot be used inside a transaction")
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		files, err := filesFn()
		if err != nil {
			return err
		}

		table := importStmt.Table
		transform := opts[importOptionTransform]

		var parentID sqlbase.ID
		if transform != "" {
			// If we're not ingesting the data, we don't care what DB we pick.
			parentID = defaultCSVParentID
		} else if table != nil {
			// We have a target table, so it might specify a DB in its name.
			found, descI, err := table.ResolveTarget(ctx,
				p, p.SessionData().Database, p.SessionData().SearchPath)

			if err != nil {
				return errors.Wrap(err, "resolving target import name")
			}
			if !found {
				// Check if database exists right now. It might not after the import is done,
				// but it's better to fail fast than wait until restore.
				return errors.Errorf("database does not exist: %q", table)
			}
			parentID = descI.(*sqlbase.DatabaseDescriptor).ID
		} else {
			// No target table means we're importing whatever we find into the session
			// database, so it must exist.
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, p.SessionData().Database, true /*required*/)
			if err != nil {
				return errors.Wrap(err, "could not resolve current database")
			}
			parentID = dbDesc.ID
		}

		format := roachpb.IOFileFormat{}
		switch importStmt.FileFormat {
		case "CSV":
			telemetry.Count("import.format.csv")
			format.Format = roachpb.IOFileFormat_CSV
			if override, ok := opts[csvDelimiter]; ok {
				comma, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrap(err, "invalid comma value")
				}
				format.Csv.Comma = comma
			}

			if override, ok := opts[csvComment]; ok {
				comment, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrap(err, "invalid comment value")
				}
				format.Csv.Comment = comment
			}

			if override, ok := opts[csvNullIf]; ok {
				format.Csv.NullEncoding = &override
			}

			if override, ok := opts[csvSkip]; ok {
				skip, err := strconv.Atoi(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %s value", csvSkip)
				}
				if skip < 0 {
					return errors.Errorf("%s must be >= 0", csvSkip)
				}
				// We need to handle the case where the user wants to skip records and the node
				// interpreting the statement might be newer than other nodes in the cluster.
				if !p.ExecCfg().Settings.Version.IsMinSupported(cluster.VersionImportSkipRecords) {
					return errors.Errorf("Using non-CSV import format requires all nodes to be upgraded to %s",
						cluster.VersionByKey(cluster.VersionImportSkipRecords))
				}
				format.Csv.Skip = uint32(skip)
			}
		case "MYSQLOUTFILE":
			telemetry.Count("import.format.mysqlout")
			format.Format = roachpb.IOFileFormat_MysqlOutfile
			format.MysqlOut = roachpb.MySQLOutfileOptions{
				RowSeparator:   '\n',
				FieldSeparator: '\t',
			}
			if override, ok := opts[mysqlOutfileRowSep]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.RowSeparator = c
			}

			if override, ok := opts[mysqlOutfileFieldSep]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %q value", mysqlOutfileFieldSep)
				}
				format.MysqlOut.FieldSeparator = c
			}

			if override, ok := opts[mysqlOutfileEnclose]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.Enclose = roachpb.MySQLOutfileOptions_Always
				format.MysqlOut.Encloser = c
			}

			if override, ok := opts[mysqlOutfileEscape]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.HasEscape = true
				format.MysqlOut.Escape = c
			}
		case "MYSQLDUMP":
			telemetry.Count("import.format.mysqldump")
			format.Format = roachpb.IOFileFormat_Mysqldump
		case "PGCOPY":
			telemetry.Count("import.format.pgcopy")
			format.Format = roachpb.IOFileFormat_PgCopy
			format.PgCopy = roachpb.PgCopyOptions{
				Delimiter: '\t',
				Null:      `\N`,
			}
			if override, ok := opts[pgCopyDelimiter]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %q value", pgCopyDelimiter)
				}
				format.PgCopy.Delimiter = c
			}
			if override, ok := opts[pgCopyNull]; ok {
				format.PgCopy.Null = override
			}
			maxRowSize := int32(defaultScanBuffer)
			if override, ok := opts[pgMaxRowSize]; ok {
				sz, err := humanizeutil.ParseBytes(override)
				if err != nil {
					return err
				}
				if sz < 1 || sz > math.MaxInt32 {
					return errors.Errorf("%s out of range: %d", pgMaxRowSize, sz)
				}
				maxRowSize = int32(sz)
			}
			format.PgCopy.MaxRowSize = maxRowSize
		case "PGDUMP":
			telemetry.Count("import.format.pgdump")
			format.Format = roachpb.IOFileFormat_PgDump
			maxRowSize := int32(defaultScanBuffer)
			if override, ok := opts[pgMaxRowSize]; ok {
				sz, err := humanizeutil.ParseBytes(override)
				if err != nil {
					return err
				}
				if sz < 1 || sz > math.MaxInt32 {
					return errors.Errorf("%s out of range: %d", pgMaxRowSize, sz)
				}
				maxRowSize = int32(sz)
			}
			format.PgDump.MaxRowSize = maxRowSize
		default:
			return pgerror.Unimplemented("import.format", "unsupported import format: %q", importStmt.FileFormat)
		}

		if format.Format != roachpb.IOFileFormat_CSV {
			if !p.ExecCfg().Settings.Version.IsMinSupported(cluster.VersionImportFormats) {
				return errors.Errorf("Using %s requires all nodes to be upgraded to %s",
					csvSkip, cluster.VersionByKey(cluster.VersionImportFormats))
			}
		}

		// sstSize, if 0, will be set to an appropriate default by the specific
		// implementation (local or distributed) since each has different optimal
		// settings.
		var sstSize int64
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}
		var oversample int64
		if override, ok := opts[importOptionOversample]; ok {
			os, err := strconv.ParseInt(override, 10, 64)
			if err != nil {
				return err
			}
			oversample = os
		}

		var skipFKs bool
		if _, ok := opts[importOptionSkipFKs]; ok {
			skipFKs = true
		}

		if override, ok := opts[importOptionDecompress]; ok {
			found := false
			for name, value := range roachpb.IOFileFormat_Compression_value {
				if strings.EqualFold(name, override) {
					format.Compression = roachpb.IOFileFormat_Compression(value)
					found = true
					break
				}
			}
			if !found {
				return pgerror.Unimplemented("import.compression", "unsupported compression value: %q", override)
			}
		}

		var tableDescs []*sqlbase.TableDescriptor
		var jobDesc string
		var names []string
		seqVals := make(map[sqlbase.ID]int64)
		if importStmt.Bundle {
			store, err := storageccl.ExportStorageFromURI(ctx, files[0], p.ExecCfg().Settings)
			if err != nil {
				return err
			}
			defer store.Close()
			raw, err := store.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer raw.Close()
			reader, err := decompressingReader(raw, files[0], format.Compression)
			if err != nil {
				return err
			}
			defer reader.Close()

			var match string
			if table != nil {
				match = table.TableName.String()
			}
			fks := fkHandler{skip: skipFKs, allowed: true, resolver: make(fkResolver)}
			switch format.Format {
			case roachpb.IOFileFormat_Mysqldump:
				evalCtx := &p.ExtendedEvalContext().EvalContext
				tableDescs, err = readMysqlCreateTable(ctx, reader, evalCtx, defaultCSVTableID, parentID, match, fks, seqVals)
			case roachpb.IOFileFormat_PgDump:
				evalCtx := &p.ExtendedEvalContext().EvalContext
				tableDescs, err = readPostgresCreateTable(reader, evalCtx, p.ExecCfg().Settings, match, parentID, walltime, fks, int(format.PgDump.MaxRowSize))
			default:
				return errors.Errorf("non-bundle format %q does not support reading schemas", format.Format.String())
			}
			if err != nil {
				return err
			}
			if tableDescs == nil && table != nil {
				names = []string{table.TableName.String()}
			}

			descStr, err := importJobDescription(importStmt, nil, files, opts)
			if err != nil {
				return err
			}
			jobDesc = descStr
		} else {
			if table == nil {
				return errors.Errorf("non-bundle format %q should always have a table name", importStmt.FileFormat)
			}
			var create *tree.CreateTable
			if importStmt.CreateDefs != nil {
				create = &tree.CreateTable{
					Table: *importStmt.Table,
					Defs:  importStmt.CreateDefs,
				}
			} else {
				filename, err := createFileFn()
				if err != nil {
					return err
				}
				create, err = readCreateTableFromStore(ctx, filename, p.ExecCfg().Settings)
				if err != nil {
					return err
				}

				if table.TableName != create.Table.TableName {
					return errors.Errorf(
						"importing table %s, but file specifies a schema for table %s",
						table.TableName, create.Table.TableName,
					)
				}
			}

			tbl, err := MakeSimpleTableDescriptor(
				ctx, p.ExecCfg().Settings, create, parentID, defaultCSVTableID, NoFKs, walltime)
			if err != nil {
				return err
			}
			tableDescs = []*sqlbase.TableDescriptor{tbl.TableDesc()}
			descStr, err := importJobDescription(importStmt, create.Defs, files, opts)
			if err != nil {
				return err
			}
			jobDesc = descStr
		}

		if transform != "" {
			transformStorage, err := storageccl.ExportStorageFromURI(ctx, transform, p.ExecCfg().Settings)
			if err != nil {
				return err
			}
			// Delay writing the BACKUP-CHECKPOINT file until as late as possible.
			err = backupccl.VerifyUsableExportTarget(ctx, transformStorage, transform)
			transformStorage.Close()
			if err != nil {
				return err
			}
			telemetry.Count("import.transform")
		} else {
			for _, tableDesc := range tableDescs {
				if err := backupccl.CheckTableExists(ctx, p.Txn(), parentID, tableDesc.Name); err != nil {
					return err
				}
			}
			// Verification steps have passed, generate a new table ID if we're
			// restoring. We do this last because we want to avoid calling
			// GenerateUniqueDescID if there's any kind of error above.
			// Reserving a table ID now means we can avoid the rekey work during restore.
			tableRewrites := make(backupccl.TableRewriteMap)
			newSeqVals := make(map[sqlbase.ID]int64, len(seqVals))
			for _, tableDesc := range tableDescs {
				id, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
				if err != nil {
					return err
				}
				tableRewrites[tableDesc.ID] = &jobspb.RestoreDetails_TableRewrite{
					TableID:  id,
					ParentID: parentID,
				}
				newSeqVals[id] = seqVals[tableDesc.ID]
			}
			seqVals = newSeqVals
			if err := backupccl.RewriteTableDescs(tableDescs, tableRewrites, ""); err != nil {
				return err
			}
		}

		tableDetails := make([]jobspb.ImportDetails_Table, 0, len(tableDescs))
		for _, tbl := range tableDescs {
			tableDetails = append(tableDetails, jobspb.ImportDetails_Table{Desc: tbl, SeqVal: seqVals[tbl.ID]})
		}
		for _, name := range names {
			tableDetails = append(tableDetails, jobspb.ImportDetails_Table{Name: name})
		}

		telemetry.CountBucketed("import.files", int64(len(files)))

		_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details: jobspb.ImportDetails{
				URIs:       files,
				Format:     format,
				ParentID:   parentID,
				Tables:     tableDetails,
				BackupPath: transform,
				SSTSize:    sstSize,
				Oversample: oversample,
				Walltime:   walltime,
				SkipFKs:    skipFKs,
			},
			Progress: jobspb.ImportProgress{},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, backupccl.RestoreHeader, nil, nil
}

func doDistributedCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	files []string,
	p sql.PlanHookState,
	parentID sqlbase.ID,
	tables map[string]*sqlbase.TableDescriptor,
	transformOnly string,
	format roachpb.IOFileFormat,
	walltime int64,
	sstSize int64,
	oversample int64,
) (roachpb.BulkOpSummary, error) {
	evalCtx := p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
	})
	rows := sqlbase.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	if err := sql.LoadCSV(
		ctx,
		p,
		job,
		sql.NewRowResultWriter(rows),
		tables,
		files,
		transformOnly,
		format,
		walltime,
		sstSize,
		oversample,
		func(descs map[sqlbase.ID]*sqlbase.TableDescriptor) (sql.KeyRewriter, error) {
			return storageccl.MakeKeyRewriter(descs)
		},
	); err != nil {

		// Check if this was a context canceled error and restart if it was.
		if s, ok := status.FromError(errors.Cause(err)); ok {
			if s.Code() == codes.Canceled && s.Message() == context.Canceled.Error() {
				return roachpb.BulkOpSummary{}, jobs.NewRetryJobError("node failure")
			}
		}

		// If the job was canceled, any of the distsql processors could have been
		// the first to encounter the .Progress error. This error's string is sent
		// through distsql back here, so we can't examine the err type in this case
		// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
		// job progress to coerce out the correct error type. If the update succeeds
		// then return the original error, otherwise return this error instead so
		// it can be cleaned up at a higher level.
		if err := job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			d := details.(*jobspb.Progress_Import).Import
			return d.Completed()
		}); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		return roachpb.BulkOpSummary{}, err
	}

	backupDesc := backupccl.BackupDescriptor{
		EndTime: hlc.Timestamp{WallTime: walltime},
	}
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		name := row[0].(*tree.DString)
		var counts roachpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[1].(*tree.DBytes)), &counts); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		backupDesc.EntryCounts.Add(counts)
		checksum := row[2].(*tree.DBytes)
		spanStart := row[3].(*tree.DBytes)
		spanEnd := row[4].(*tree.DBytes)
		backupDesc.Files = append(backupDesc.Files, backupccl.BackupDescriptor_File{
			Path: string(*name),
			Span: roachpb.Span{
				Key:    roachpb.Key(*spanStart),
				EndKey: roachpb.Key(*spanEnd),
			},
			Sha512: []byte(*checksum),
		})
	}

	if transformOnly == "" {
		return backupDesc.EntryCounts, nil
	}

	// The returned spans are from the SSTs themselves, and so don't perfectly
	// overlap. Sort the files so we can fix the spans to be correctly
	// overlapping. This is needed because RESTORE splits at both the start
	// and end of each SST, and so there are tiny ranges (like {NULL-/0/0} at
	// the start) that get created. During non-transform IMPORT this isn't a
	// problem because it only splits on the end key. Replicate that behavior
	// here by copying the end key from each span to the start key of the next.
	sort.Slice(backupDesc.Files, func(i, j int) bool {
		return backupDesc.Files[i].Span.Key.Compare(backupDesc.Files[j].Span.Key) < 0
	})

	var minTableSpan, maxTableSpan roachpb.Key
	for _, tableDesc := range tables {
		span := tableDesc.TableSpan()
		if minTableSpan == nil || span.Key.Compare(minTableSpan) < 0 {
			minTableSpan = span.Key
		}
		if maxTableSpan == nil || span.EndKey.Compare(maxTableSpan) > 0 {
			maxTableSpan = span.EndKey
		}
	}
	backupDesc.Files[0].Span.Key = minTableSpan
	for i := 1; i < len(backupDesc.Files); i++ {
		backupDesc.Files[i].Span.Key = backupDesc.Files[i-1].Span.EndKey
	}
	backupDesc.Files[len(backupDesc.Files)-1].Span.EndKey = maxTableSpan

	dest, err := storageccl.ExportStorageConfFromURI(transformOnly)
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	es, err := storageccl.MakeExportStorage(ctx, dest, p.ExecCfg().Settings)
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	defer es.Close()

	return backupDesc.EntryCounts, finalizeCSVBackup(ctx, &backupDesc, parentID, tables, es, p.ExecCfg())
}

type importResumer struct {
	settings *cluster.Settings
	res      roachpb.BulkOpSummary
}

func (r *importResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := job.Details().(jobspb.ImportDetails)
	p := phs.(sql.PlanHookState)

	// TODO(dt): consider looking at the legacy fields used in 2.0.

	walltime := details.Walltime
	transform := details.BackupPath
	files := details.URIs
	parentID := details.ParentID
	sstSize := details.SSTSize
	format := details.Format
	oversample := details.Oversample

	if sstSize == 0 {
		// The distributed importer will correctly chunk up large ranges into
		// multiple ssts that can be imported. In order to reduce the number of
		// ranges and increase the average range size after import, set a target of
		// some arbitrary multiple larger than the maximum sst size. Without this
		// the range sizes were somewhere between 1MB and > 64MB. Targeting a much
		// higher size should cause many ranges to be somewhere around the max range
		// size. This should also cause the distsql plan and range router to be much
		// smaller since there are fewer overall ranges.
		sstSize = storageccl.MaxImportBatchSize(r.settings) * 5
	}

	tables := make(map[string]*sqlbase.TableDescriptor, len(details.Tables))
	if details.Tables != nil {
		for _, i := range details.Tables {
			if i.Name != "" {
				tables[i.Name] = i.Desc
			} else if i.Desc != nil {
				tables[i.Desc.Name] = i.Desc
			} else {
				return errors.Errorf("invalid table specification")
			}
		}
	}

	{
		// Disable merging for the table IDs being imported into. We don't want the
		// merge queue undoing the splits performed during IMPORT.
		tableIDs := make([]uint32, 0, len(tables))
		for _, t := range tables {
			tableIDs = append(tableIDs, uint32(t.ID))
		}
		disableCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		gossipccl.DisableMerges(disableCtx, p.ExecCfg().Gossip, tableIDs)
	}

	res, err := doDistributedCSVTransform(
		ctx, job, files, p, parentID, tables, transform, format, walltime, sstSize, oversample,
	)
	if err != nil {
		return err
	}
	r.res = res
	return nil
}

// OnFailOrCancel removes KV data that has been committed from a import that
// has failed or been canceled. It does this by adding the table descriptors
// in DROP state, which causes the schema change stuff to delete the keys
// in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	details := job.Details().(jobspb.ImportDetails)
	if details.BackupPath != "" {
		return nil
	}

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	b := txn.NewBatch()
	for _, tbl := range details.Tables {
		tableDesc := tbl.Desc
		tableDesc.State = sqlbase.TableDescriptor_DROP
		// If the DropTime if set, a table uses RangeClear for fast data removal. This
		// operation starts at DropTime + the GC TTL. If we used now() here, it would
		// not clean up data until the TTL from the time of the error. Instead, use 1
		// (that is, 1ns past the epoch) to allow this to be cleaned up as soon as
		// possible. This is safe since the table data was never visible to users,
		// and so we don't need to preserve MVCC semantics.
		tableDesc.DropTime = 1
		b.CPut(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc), nil)
	}
	return txn.Run(ctx, b)
}

func (r *importResumer) OnSuccess(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	log.Event(ctx, "making tables live")
	details := job.Details().(jobspb.ImportDetails)

	if details.BackupPath != "" {
		return nil
	}

	toWrite := make([]*sqlbase.TableDescriptor, len(details.Tables))
	var seqs []roachpb.KeyValue
	for i := range details.Tables {
		toWrite[i] = details.Tables[i].Desc
		toWrite[i].ParentID = details.ParentID
		if d := details.Tables[i]; d.SeqVal != 0 {
			key, val, err := sql.MakeSequenceKeyVal(d.Desc, d.SeqVal, false)
			if err != nil {
				return err
			}
			kv := roachpb.KeyValue{Key: key}
			kv.Value.SetInt(val)
			seqs = append(seqs, kv)
		}
	}

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// imported data.
	if err := backupccl.WriteTableDescs(ctx, txn, nil, toWrite, job.Payload().Username, r.settings, seqs); err != nil {
		return errors.Wrapf(err, "creating tables")
	}

	return nil
}

func (r *importResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	details := job.Details().(jobspb.ImportDetails)

	if transform := details.BackupPath; transform != "" {
		transformStorage, err := storageccl.ExportStorageFromURI(ctx, transform, r.settings)
		if err != nil {
			log.Warningf(ctx, "unable to create storage: %+v", err)
		} else {
			// Always attempt to cleanup the checkpoint even if the import failed.
			if err := transformStorage.Delete(ctx, backupccl.BackupDescriptorCheckpointName); err != nil {
				log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
			}
			transformStorage.Close()
		}
	}

	if status == jobs.StatusSucceeded {
		telemetry.CountBucketed("import.rows", r.res.Rows)
		const mb = 1 << 20
		telemetry.CountBucketed("import.size-mb", r.res.DataSize/mb)

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(r.res.Rows)),
			tree.NewDInt(tree.DInt(r.res.IndexEntries)),
			tree.NewDInt(tree.DInt(r.res.SystemRecords)),
			tree.NewDInt(tree.DInt(r.res.DataSize)),
		}
	}
}

var _ jobs.Resumer = &importResumer{}

func importResumeHook(typ jobspb.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeImport {
		return nil
	}

	return &importResumer{
		settings: settings,
	}
}

func init() {
	sql.AddPlanHook(importPlanHook)
	jobs.AddResumeHook(importResumeHook)
}
