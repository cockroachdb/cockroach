// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	importOptionSSTSize    = "sstsize"
	importOptionDecompress = "decompress"
	importOptionOversample = "oversample"
	importOptionSkipFKs    = "skip_foreign_keys"

	importOptionDirectIngest = "experimental_direct_ingestion"

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

	importOptionSSTSize:    sql.KVStringOptRequireValue,
	importOptionDecompress: sql.KVStringOptRequireValue,
	importOptionOversample: sql.KVStringOptRequireValue,

	importOptionSkipFKs: sql.KVStringOptRequireNoValue,

	importOptionDirectIngest: sql.KVStringOptRequireNoValue,

	pgMaxRowSize: sql.KVStringOptRequireValue,
}

func importJobDescription(
	p sql.PlanHookState,
	orig *tree.Import,
	defs tree.TableDefs,
	files []string,
	opts map[string]string,
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
		opt := tree.KVOption{Key: tree.Name(k)}
		val := importOptionExpectValues[k] == sql.KVStringOptRequireValue
		val = val || (importOptionExpectValues[k] == sql.KVStringOptAny && len(v) > 0)
		if val {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(&stmt, ann), nil
}

// importPlanHook implements sql.PlanHookFn.
func importPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, nil, false, nil
	}

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var createFileFn func() (string, error)
	if !importStmt.Bundle && !importStmt.Into && importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	optsFn, err := p.TypeAsStringOpts(importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
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

		var parentID sqlbase.ID
		if table != nil {
			// We have a target table, so it might specify a DB in its name.
			found, descI, err := table.ResolveTarget(ctx,
				p, p.SessionData().Database, p.SessionData().SearchPath)
			if err != nil {
				return pgerror.Wrap(err, pgcode.UndefinedTable,
					"resolving target import name")
			}
			if !found {
				// Check if database exists right now. It might not after the import is done,
				// but it's better to fail fast than wait until restore.
				return pgerror.Newf(pgcode.UndefinedObject,
					"database does not exist: %q", table)
			}
			parentID = descI.(*sqlbase.DatabaseDescriptor).ID
		} else {
			// No target table means we're importing whatever we find into the session
			// database, so it must exist.
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, p.SessionData().Database, true /*required*/)
			if err != nil {
				return pgerror.Wrap(err, pgcode.UndefinedObject,
					"could not resolve current database")
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
					return pgerror.Wrap(err, pgcode.Syntax, "invalid comma value")
				}
				format.Csv.Comma = comma
			}

			if override, ok := opts[csvComment]; ok {
				comment, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrap(err, pgcode.Syntax, "invalid comment value")
				}
				format.Csv.Comment = comment
			}

			if override, ok := opts[csvNullIf]; ok {
				format.Csv.NullEncoding = &override
			}

			if override, ok := opts[csvSkip]; ok {
				skip, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %s value", csvSkip)
				}
				if skip < 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be >= 0", csvSkip)
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
					return pgerror.Wrapf(err, pgcode.Syntax,
						"invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.RowSeparator = c
			}

			if override, ok := opts[mysqlOutfileFieldSep]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", mysqlOutfileFieldSep)
				}
				format.MysqlOut.FieldSeparator = c
			}

			if override, ok := opts[mysqlOutfileEnclose]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.Enclose = roachpb.MySQLOutfileOptions_Always
				format.MysqlOut.Encloser = c
			}

			if override, ok := opts[mysqlOutfileEscape]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", mysqlOutfileRowSep)
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
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", pgCopyDelimiter)
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
			return unimplemented.Newf("import.format", "unsupported import format: %q", importStmt.FileFormat)
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
				return unimplemented.Newf("import.compression", "unsupported compression value: %q", override)
			}
		}

		_, ingestDirectly := opts[importOptionDirectIngest]
		if ingestDirectly {
			if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionDirectImport) {
				return errors.Errorf("Using %q requires all nodes to be upgraded to %s",
					importOptionDirectIngest, cluster.VersionByKey(cluster.VersionDirectImport))
			}
		}

		var tableDetails []jobspb.ImportDetails_Table
		jobDesc, err := importJobDescription(p, importStmt, nil, files, opts)
		if err != nil {
			return err
		}

		if importStmt.Into {
			// TODO(dt): this is a prototype for incremental import but there are many
			// TODOs remaining before it is ready to graduate to prime-time. Some of
			// them are captured in specific TODOs below, but some of the big, scary
			// things to do are:
			// - review planner vs txn use very carefully. We should try to get to a
			//   single txn used to plan the job and create it. Using the planner's
			//   txn today is very wrong since it will not commit until after the job
			//   has run, so starting a job based on reads it returned is very wrong.
			// - audit every place that we resolve/lease/read table descs to be sure
			//   that the IMPORTING state is handled correctly. SQL lease acquisition
			//   is probably the easy one here since it has single read path -- the
			//   things that read directly like the queues or background jobs are the
			//   ones we'll need to really carefully look though.
			// - Look at if/how cleanup/rollback works. Reconsider the cpu from the
			//   desc version (perhaps we should be re-reading instead?).
			// - Write _a lot_ of tests.
			found, err := p.ResolveMutableTableDescriptor(ctx, table, true, sql.ResolveRequireTableDesc)
			if err != nil {
				return err
			}

			if len(found.Mutations) > 0 {
				return errors.Errorf("cannot IMPORT INTO a table with schema changes in progress -- try again later (pending mutation %s)", found.Mutations[0].String())
			}
			if err := p.CheckPrivilege(ctx, found, privilege.CREATE); err != nil {
				return err
			}
			// TODO(dt): Ensure no other schema changes can start during ingest.
			importing := found.TableDescriptor
			importing.Version++
			// Take the table offline for import.
			// TODO(dt): audit everywhere we get table descs (leases or otherwise) to
			// ensure that filtering by state handles IMPORTING correctly.
			importing.State = sqlbase.TableDescriptor_IMPORTING
			// TODO(dt): de-validate all the FKs.

			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				return errors.Wrap(
					txn.CPut(ctx, sqlbase.MakeDescMetadataKey(found.TableDescriptor.ID),
						sqlbase.WrapDescriptor(&importing), sqlbase.WrapDescriptor(&found.TableDescriptor),
					), "another operation is currently operating on the table")
			}); err != nil {
				return err
			}
			// NB: we need to wait for the schema change to show up before it is safe
			// to ingest, but rather than do that here, we'll wait for this schema
			// change in the job's Resume hook, before running the ingest phase. That
			// will hopefully let it get a head start on propagating, plus the more we
			// do in the job, the more that has automatic cleanup on rollback.

			// TODO(dt): configure target cols from ImportStmt.IntoCols
			tableDetails = []jobspb.ImportDetails_Table{{Desc: &importing, IsNew: false}}
		} else {
			var tableDescs []*sqlbase.TableDescriptor
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
					return errors.Errorf("table definition not found for %q", table.TableName.String())
				}
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
				descStr, err := importJobDescription(p, importStmt, create.Defs, files, opts)
				if err != nil {
					return err
				}
				jobDesc = descStr
			}

			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				for _, tableDesc := range tableDescs {
					if err := backupccl.CheckTableExists(ctx, txn, parentID, tableDesc.Name); err != nil {
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
					if v, ok := seqVals[tableDesc.ID]; ok {
						newSeqVals[id] = v
					}
				}
				seqVals = newSeqVals
				if err := backupccl.RewriteTableDescs(tableDescs, tableRewrites, ""); err != nil {
					return err
				}

				for i := range tableDescs {
					tableDescs[i].State = sqlbase.TableDescriptor_IMPORTING
				}

				seqValKVs := make([]roachpb.KeyValue, 0, len(seqVals))
				for i := range tableDescs {
					if v, ok := seqVals[tableDescs[i].ID]; ok && v != 0 {
						key, val, err := sql.MakeSequenceKeyVal(tableDescs[i], v, false)
						if err != nil {
							return err
						}
						kv := roachpb.KeyValue{Key: key}
						kv.Value.SetInt(val)
						seqValKVs = append(seqValKVs, kv)
					}
				}

				// Write the new TableDescriptors and flip the namespace entries over to
				// them. After this call, any queries on a table will be served by the newly
				// imported data.
				if err := backupccl.WriteTableDescs(ctx, txn, nil, tableDescs, p.User(), p.ExecCfg().Settings, seqValKVs); err != nil {
					return errors.Wrapf(err, "creating tables")
				}

				// TODO(dt): we should be creating the job with this txn too. Once a job
				// is created, the contract is it does its own, explicit cleanup on
				// failure (i.e. not just txn rollback) but everything up to and including
				// the creation of the job *should* be a single atomic txn. As-is, if we
				// fail to creat the job after committing this txn, we've leaving broken
				// descs and namespace records.

				return nil
			}); err != nil {
				return err
			}

			tableDetails = make([]jobspb.ImportDetails_Table, len(tableDescs))
			for i := range tableDescs {
				tableDetails[i] = jobspb.ImportDetails_Table{Desc: tableDescs[i], SeqVal: seqVals[tableDescs[i].ID], IsNew: true}
			}
		}

		telemetry.CountBucketed("import.files", int64(len(files)))

		_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details: jobspb.ImportDetails{
				URIs:           files,
				Format:         format,
				ParentID:       parentID,
				Tables:         tableDetails,
				SSTSize:        sstSize,
				Oversample:     oversample,
				Walltime:       walltime,
				SkipFKs:        skipFKs,
				IngestDirectly: ingestDirectly,
			},
			Progress: jobspb.ImportProgress{},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, backupccl.RestoreHeader, nil, false, nil
}

func doDistributedCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	files []string,
	p sql.PlanHookState,
	parentID sqlbase.ID,
	tables map[string]*sqlbase.TableDescriptor,
	format roachpb.IOFileFormat,
	walltime int64,
	sstSize int64,
	oversample int64,
	ingestDirectly bool,
) (roachpb.BulkOpSummary, error) {
	if ingestDirectly {
		return sql.DistIngest(ctx, p, job, tables, files, format, walltime)
		// TODO(dt): check for errors in job records as is done below.
	}

	evalCtx := p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]types.T{
		*types.String,
		*types.Bytes,
		*types.Bytes,
		*types.Bytes,
		*types.Bytes,
	})
	rows := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
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
		format,
		walltime,
		sstSize,
		oversample,
		func(descs map[sqlbase.ID]*sqlbase.TableDescriptor) (sql.KeyRewriter, error) {
			return storageccl.MakeKeyRewriter(descs)
		},
	); err != nil {

		// Check if this was a context canceled error and restart if it was.
		if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
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

	var res roachpb.BulkOpSummary
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		var counts roachpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[1].(*tree.DBytes)), &counts); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		res.Add(counts)
	}

	return res, nil
}

type importResumer struct {
	job            *jobs.Job
	settings       *cluster.Settings
	res            roachpb.BulkOpSummary
	statsRefresher *stats.Refresher
}

// Resume is part of the jobs.Resumer interface.
func (r *importResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	p := phs.(sql.PlanHookState)

	if details.BackupPath != "" {
		return errors.Errorf("transform is no longer supported")
	}

	walltime := details.Walltime
	files := details.URIs
	parentID := details.ParentID
	sstSize := details.SSTSize
	format := details.Format
	oversample := details.Oversample
	ingestDirectly := details.IngestDirectly

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
	requiresSchemaChangeDelay := false
	if details.Tables != nil {
		for _, i := range details.Tables {
			if i.Name != "" {
				tables[i.Name] = i.Desc
			} else if i.Desc != nil {
				tables[i.Desc.Name] = i.Desc
			} else {
				return errors.Errorf("invalid table specification")
			}
			if !i.IsNew {
				requiresSchemaChangeDelay = true
			}
		}
	}

	if requiresSchemaChangeDelay {
		// TODO(dt): update job status to mention waiting for tables to go offline.
		for _, i := range details.Tables {
			if _, err := p.ExecCfg().LeaseManager.WaitForOneVersion(ctx, i.Desc.ID, retry.Options{}); err != nil {
				return err
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
		p.ExecCfg().Gossip.DisableMerges(disableCtx, tableIDs)
	}

	res, err := doDistributedCSVTransform(
		ctx, r.job, files, p, parentID, tables, format, walltime, sstSize, oversample, ingestDirectly,
	)
	if err != nil {
		return err
	}
	r.res = res
	r.statsRefresher = p.ExecCfg().StatsRefresher
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes data that has
// been committed from a import that has failed or been canceled. It does this
// by adding the table descriptors in DROP state, which causes the schema change
// stuff to delete the keys in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn) error {
	details := r.job.Details().(jobspb.ImportDetails)

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	b := txn.NewBatch()
	for _, tbl := range details.Tables {
		tableDesc := *tbl.Desc
		tableDesc.Version++
		if tbl.IsNew {
			tableDesc.State = sqlbase.TableDescriptor_DROP
			// If the DropTime if set, a table uses RangeClear for fast data removal. This
			// operation starts at DropTime + the GC TTL. If we used now() here, it would
			// not clean up data until the TTL from the time of the error. Instead, use 1
			// (that is, 1ns past the epoch) to allow this to be cleaned up as soon as
			// possible. This is safe since the table data was never visible to users,
			// and so we don't need to preserve MVCC semantics.
			tableDesc.DropTime = 1
			b.CPut(sqlbase.MakeNameMetadataKey(tableDesc.ParentID, tableDesc.Name), nil, tableDesc.ID)
		} else {
			// IMPORT did not create this table, so we should not drop it.
			// TODO(dt): consider trying to delete whatever was ingested before
			// returning the table to public. Unfortunately the ingestion isn't
			// transactional, so there is no clean way to just rollback our changes,
			// but we could iterate by time to delete before returning to public.
			// TODO(dt): re-validate any FKs?
			tableDesc.Version++
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
		}
		b.CPut(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(&tableDesc), sqlbase.WrapDescriptor(tbl.Desc))
	}
	return errors.Wrap(txn.Run(ctx, b), "rolling back tables")
}

// OnSuccess is part of the jobs.Resumer interface.
func (r *importResumer) OnSuccess(ctx context.Context, txn *client.Txn) error {
	log.Event(ctx, "making tables live")
	details := r.job.Details().(jobspb.ImportDetails)

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	b := txn.NewBatch()
	for _, tbl := range details.Tables {
		tableDesc := *tbl.Desc
		tableDesc.Version++
		tableDesc.State = sqlbase.TableDescriptor_PUBLIC
		// TODO(dt): re-validate any FKs?
		b.CPut(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(&tableDesc), sqlbase.WrapDescriptor(tbl.Desc))
	}
	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "publishing tables")
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range details.Tables {
		r.statsRefresher.NotifyMutation(details.Tables[i].Desc.ID, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

// OnTerminal is part of the jobs.Resumer interface.
func (r *importResumer) OnTerminal(
	ctx context.Context, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	if status == jobs.StatusSucceeded {
		telemetry.CountBucketed("import.rows", r.res.Rows)
		const mb = 1 << 20
		telemetry.CountBucketed("import.size-mb", r.res.DataSize/mb)

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*r.job.ID())),
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

func init() {
	sql.AddPlanHook(importPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeImport,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &importResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}
