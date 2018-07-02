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
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	pgCopyDelimiter = "delimiter"
	pgCopyNull      = "nullif"

	pgMaxRowSize = "max_row_size"
)

var importOptionExpectValues = map[string]bool{
	csvDelimiter: true,
	csvComment:   true,
	csvNullIf:    true,
	csvSkip:      true,

	mysqlOutfileRowSep:   true,
	mysqlOutfileFieldSep: true,
	mysqlOutfileEnclose:  true,
	mysqlOutfileEscape:   true,

	importOptionTransform:  true,
	importOptionSSTSize:    true,
	importOptionDecompress: true,

	pgMaxRowSize: true,
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

// MakeSimpleTableDescriptor creates a TableDescriptor from a CreateTable parse
// node without the full machinery. Many parts of the syntax are unsupported
// (see the implementation and TestMakeSimpleTableDescriptorErrors for details),
// but this is enough for our csv IMPORT and for some unit tests.
func MakeSimpleTableDescriptor(
	ctx context.Context,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID,
	tableID sqlbase.ID,
	walltime int64,
) (*sqlbase.TableDescriptor, error) {
	sql.HoistConstraints(create)
	if create.IfNotExists {
		return nil, errors.New("unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, errors.New("interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, errors.New("CREATE AS not supported")
	}
	for _, def := range create.Defs {
		switch def := def.(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return nil, errors.Errorf("computed columns not supported: %s", tree.AsString(def))
			}
		case *tree.ForeignKeyConstraintTableDef:
			return nil, errors.Errorf("foreign keys not supported: %s", tree.AsString(def))
		default:
			return nil, errors.Errorf("unsupported table definition: %s", tree.AsString(def))
		}
	}
	semaCtx := tree.SemaContext{}
	evalCtx := tree.EvalContext{CtxProvider: ctxProvider{ctx}}
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vt */
		st,
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		return nil, err
	}

	return &tableDesc, nil
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
		if importOptionExpectValues[k] {
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

		var table *tree.TableName
		if importStmt.Table.TableNameReference != nil {
			// Normalize must be called regardles of whether there is a
			// transform because it prepares a TableName with the right
			// structure and stores it back into the statement AST, which we
			// need later when computing the job title.
			table, err = importStmt.Table.Normalize()
			if err != nil {
				return errors.Wrap(err, "normalize create table")
			}
		}

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
			dbDesc, err := sql.ResolveDatabase(ctx, p, p.SessionData().Database, true /*required*/)
			if err != nil {
				return errors.Wrap(err, "could not resolve current database")
			}
			parentID = dbDesc.ID
		}

		format := roachpb.IOFileFormat{}
		switch importStmt.FileFormat {
		case "CSV":
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
			format.Format = roachpb.IOFileFormat_Mysqldump
		case "PGCOPY":
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
			return errors.Errorf("unsupported import format: %q", importStmt.FileFormat)
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
				return errors.Errorf("unsupported compression value: %q", override)
			}
		}

		var tableDescs []*sqlbase.TableDescriptor
		var jobDesc string
		var names []string
		if importStmt.Bundle {
			store, err := storageccl.ExportStorageFromURI(ctx, files[0], p.ExecCfg().Settings)
			if err != nil {
				return err
			}
			defer store.Close()
			reader, err := store.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer reader.Close()

			var match string
			if table != nil {
				match = table.TableName.String()
			}
			switch format.Format {
			case roachpb.IOFileFormat_Mysqldump:
			case roachpb.IOFileFormat_PgDump:
				evalCtx := &p.ExtendedEvalContext().EvalContext
				tableDescs, err = readPostgresCreateTable(reader, evalCtx, p.ExecCfg().Settings, match, parentID, walltime, int(format.PgDump.MaxRowSize))
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
				create = &tree.CreateTable{Table: importStmt.Table, Defs: importStmt.CreateDefs}
			} else {
				filename, err := createFileFn()
				if err != nil {
					return err
				}
				create, err = readCreateTableFromStore(ctx, filename, p.ExecCfg().Settings)
				if err != nil {
					return err
				}

				if parsed, err := create.Table.Normalize(); err != nil {
					return errors.Wrap(err, "normalize create table")
				} else if table.TableName != parsed.TableName {
					return errors.Errorf("importing table %s, but file specifies a schema for table %s", table.TableName, parsed.TableName)
				}
			}

			tbl, err := MakeSimpleTableDescriptor(
				ctx, p.ExecCfg().Settings, create, parentID, defaultCSVTableID, walltime)
			if err != nil {
				return err
			}
			tableDescs = []*sqlbase.TableDescriptor{tbl}
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
			for _, tableDesc := range tableDescs {
				tableDesc.ID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
				if err != nil {
					return err
				}
			}
		}

		tableDetails := make([]jobspb.ImportDetails_Table, 0, len(tableDescs))
		for _, tbl := range tableDescs {
			tableDetails = append(tableDetails, jobspb.ImportDetails_Table{Desc: tbl})
		}
		for _, name := range names {
			tableDetails = append(tableDetails, jobspb.ImportDetails_Table{Name: name})
		}

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
				Walltime:   walltime,
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
) error {
	evalCtx := p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		{SemanticType: sqlbase.ColumnType_INT},
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
		func(descs map[sqlbase.ID]*sqlbase.TableDescriptor) (sql.KeyRewriter, error) {
			return storageccl.MakeKeyRewriter(descs)
		},
	); err != nil {
		// Check if this was a context canceled error and restart if it was.
		if s, ok := status.FromError(errors.Cause(err)); ok {
			if s.Code() == codes.Canceled && s.Message() == context.Canceled.Error() {
				return jobs.NewRetryJobError("node failure")
			}
		}

		// If the job was canceled, any of the distsql processors could have been
		// the first to encounter the .Progress error. This error's string is sent
		// through distsql back here, so we can't examine the err type in this case
		// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
		// job progress to coerce out the correct error type. If the update succeeds
		// then return the original error, otherwise return this error instead so
		// it can be cleaned up at a higher level.
		if err := job.Progressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			d := details.(*jobspb.Progress_Import).Import
			return d.Completed()
		}); err != nil {
			return err
		}
		return err
	}
	if transformOnly == "" {
		return nil
	}

	backupDesc := backupccl.BackupDescriptor{
		EndTime: hlc.Timestamp{WallTime: walltime},
	}
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		name := row[0].(*tree.DString)
		size := row[1].(*tree.DInt)
		checksum := row[2].(*tree.DBytes)
		spanStart := row[3].(*tree.DBytes)
		spanEnd := row[4].(*tree.DBytes)
		backupDesc.EntryCounts.DataSize += int64(*size)
		backupDesc.Files = append(backupDesc.Files, backupccl.BackupDescriptor_File{
			Path: string(*name),
			Span: roachpb.Span{
				Key:    roachpb.Key(*spanStart),
				EndKey: roachpb.Key(*spanEnd),
			},
			Sha512: []byte(*checksum),
		})
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
		return err
	}
	es, err := storageccl.MakeExportStorage(ctx, dest, p.ExecCfg().Settings)
	if err != nil {
		return err
	}
	defer es.Close()

	return finalizeCSVBackup(ctx, &backupDesc, parentID, tables, es, p.ExecCfg())
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

	return doDistributedCSVTransform(
		ctx, job, files, p, parentID, tables, transform, format, walltime, sstSize,
	)
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
	for i := range details.Tables {
		toWrite[i] = details.Tables[i].Desc
		toWrite[i].ParentID = details.ParentID
	}

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// imported data.
	if err := backupccl.WriteTableDescs(ctx, txn, nil, toWrite, job.Payload().Username, r.settings); err != nil {
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
