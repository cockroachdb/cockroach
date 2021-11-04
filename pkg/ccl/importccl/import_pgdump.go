// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func getImportTempDBName(jobID jobspb.JobID) string {
	importTempPgdumpDB := "crdb_temp_pgdump_import"
	return fmt.Sprintf("%s_%d", importTempPgdumpDB, jobID)
}

// createTempImportDatabase creates a temporary database where we will create
// all the PGDUMP objects during the import.
func createTempImportDatabase(
	ctx context.Context, p sql.JobExecContext, jobID jobspb.JobID,
) (descpb.ID, error) {
	id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return 0, err
	}
	// TODO(adityamaru): Figure out how to create the database descriptor with
	// privileges for only the node user. Currently, root and admin have ALL
	// privileges on the database descriptor. This is enforced by
	// `ValidateSuperuserPrivileges` when writing the descriptor to store.
	// I tried moving the database descriptor to OFFLINE instead of mucking with
	// privileges, but this prevents us from running any DDL statements on this
	// database.
	tempDBDesc := dbdesc.NewInitial(id, getImportTempDBName(jobID), security.NodeUserName())
	return tempDBDesc.GetID(), sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn,
		col *descs.Collection) error {
		b := txn.NewBatch()
		if err := col.WriteDescToBatch(
			ctx, false /* kvTrace */, tempDBDesc, b,
		); err != nil {
			return err
		}
		b.CPut(catalogkeys.MakeDatabaseNameKey(p.ExecCfg().Codec, getImportTempDBName(jobID)), tempDBDesc.GetID(), nil)
		return txn.Run(ctx, b)
	})
}

type postgresDDLHandler struct {
	jobID            jobspb.JobID
	targetTableName  string
	dumpDatabaseName string
	// TODO(adityamaru): Maybe memory monitor?
	bufferedDDLStmts      []string
	unsupportedStmtLogger *unsupportedStmtLogger
}

func useNewDumpSchemaParsing(format roachpb.IOFileFormat, p sql.JobExecContext) bool {
	return format.Format == roachpb.IOFileFormat_PgDump &&
		featurePgdumpEnabled.Get(&p.ExecCfg().Settings.SV)
}

func formatPostgresStatement(n tree.NodeFormatter) string {
	f := tree.NewFmtCtx(
		// TODO(adityamaru): should this be serializable?
		tree.FmtParsable,
	)
	f.FormatNode(n)
	return f.CloseAndGetString()
}

func rewritePostgresStatementTableName(
	n tree.NodeFormatter, dumpDatabaseName string, jobID jobspb.JobID,
) (string, error) {
	var err error
	f := tree.NewFmtCtx(
		// TODO(adityamaru): should this be serializable?
		tree.FmtParsable,
		tree.FmtReformatTableNames(func(ctx *tree.FmtCtx, tn *tree.TableName) {
			// If the node has an explicit catalog name then we must replace it with
			// the temporary database we are importing into.
			if tn.CatalogName != "" {
				if tn.CatalogName != tree.Name(dumpDatabaseName) {
					err = errors.AssertionFailedf("catalog name %s does not match dump target database name %s",
						tn.CatalogName, dumpDatabaseName)
					return
				}
				tn.CatalogName = tree.Name(getImportTempDBName(jobID))
			}
			// TODO (adityamaru): Is it possible for dump files to have db.object names?
			// In that case, if the node has an explicit schema name, then this could
			// be a schema name or catalog name. If the SchemaName matches the target
			// database the dump file specified via a CREATE DATABASE statement, then
			// we replace it with the temporary database we are importing into.
			// Otherwise we leave it as is since it is a schema name. What if we have a
			// schema with the same name as the target database?
			// I'm not sure this is an issue so leaving it as a TODO for now.
			ctx.WithReformatTableNames(nil, func() {
				ctx.FormatNode(tn)
			})
		}),
	)
	if err != nil {
		return "", err
	}
	f.FormatNode(n)
	return f.CloseAndGetString(), nil
}

func bufferDDLPostgresStatement(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	postgresStmt interface{},
	p sql.JobExecContext,
	parentID descpb.ID,
	handler *postgresDDLHandler,
) error {
	doesMatchTargetTable := func(schemaQualifiedName schemaAndTableName) bool {
		return handler.targetTableName == "" || handler.targetTableName == schemaQualifiedName.String()
	}
	ignoreUnsupportedStmts := handler.unsupportedStmtLogger.ignoreUnsupported
	switch stmt := postgresStmt.(type) {
	case *tree.CreateDatabase:
		// If we have previously seen a `CREATE DATABASE` statement then we error
		// out.
		if handler.dumpDatabaseName != "" {
			return errors.Newf("encountered more than one `CREATE DATABASE` statement when importing PGDUMP file")
		}
		handler.dumpDatabaseName = string(stmt.Name)
	case *tree.CreateSchema:
		// If a target table is specified we do not want to create any user defined
		// schemas. This is because we only allow specifying target table's in the
		// public schema.
		if handler.targetTableName != "" {
			return nil
		}
		// If the schema specifies an explicit database name, replace it with the
		// temporary pgdump database being imported into.
		if stmt.Schema.ExplicitCatalog {
			if stmt.Schema.CatalogName != tree.Name(handler.dumpDatabaseName) {
				return errors.AssertionFailedf("catalog name %s does not match dump target database name %s",
					stmt.Schema.CatalogName, handler.dumpDatabaseName)
			}
			stmt.Schema.CatalogName = tree.Name(getImportTempDBName(handler.jobID))
		}
		handler.bufferedDDLStmts = append(handler.bufferedDDLStmts, formatPostgresStatement(stmt))
	case *tree.CreateTable:
		schemaQualifiedName, err := getSchemaAndTableName(&stmt.Table)
		if err != nil {
			return err
		}
		if !doesMatchTargetTable(schemaQualifiedName) {
			return nil
		}
		// If the `CREATE TABLE` specifies an explicit database name, replace it
		// with the temporary pgdump database being imported into.
		s, err := rewritePostgresStatementTableName(stmt, handler.dumpDatabaseName, handler.jobID)
		if err != nil {
			return err
		}
		handler.bufferedDDLStmts = append(handler.bufferedDDLStmts, s)
	case *tree.AlterTable:
		schemaQualifiedName, err := getSchemaAndTableName2(stmt.Table)
		if err != nil {
			return err
		}
		if !doesMatchTargetTable(schemaQualifiedName) {
			return nil
		}

		// If the `ALTER TABLE` statement has an explicit database name, replace it
		// with the temporary pgdump database being imported into.
		if stmt.Table.HasExplicitCatalog() {
			if stmt.Table.Parts[2] != handler.dumpDatabaseName {
				return errors.AssertionFailedf("catalog name %s does not match dump target database name %s",
					stmt.Table.Parts[2], handler.dumpDatabaseName)
			}
			stmt.Table.Parts[2] = getImportTempDBName(handler.jobID)
		}
		for _, cmd := range stmt.Cmds {
			switch cmd := cmd.(type) {
			case *tree.AlterTableAddConstraint:
				switch con := cmd.ConstraintDef.(type) {
				case *tree.ForeignKeyConstraintTableDef:
					// TODO(adityamaru): handle FKs and fk skip option.
					if con.Table.ExplicitCatalog {
						con.Table.CatalogName = tree.Name(getImportTempDBName(handler.jobID))
					}
				default:
					// TODO(adityamaru): confirm that not other constraint can have a
					// qualified table name.
				}
			case *tree.AlterTableSetDefault:
			case *tree.AlterTableSetVisible:
			case *tree.AlterTableAddColumn:
				if cmd.IfNotExists {
					if ignoreUnsupportedStmts {
						err := handler.unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
						if err != nil {
							return err
						}
						continue
					}
					return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
				}
			case *tree.AlterTableSetNotNull:
			default:
				if ignoreUnsupportedStmts {
					err := handler.unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
			}
		}
		handler.bufferedDDLStmts = append(handler.bufferedDDLStmts, formatPostgresStatement(stmt))
	case *tree.CreateIndex:
		schemaQualifiedTableName, err := getSchemaAndTableName(&stmt.Table)
		if err != nil {
			return err
		}
		if !doesMatchTargetTable(schemaQualifiedTableName) {
			return nil
		}

		// If the `CREATE INDEX` specifies an explicit database name, replace it
		// with the temporary pgdump database being imported into.
		s, err := rewritePostgresStatementTableName(stmt, handler.dumpDatabaseName, handler.jobID)
		if err != nil {
			return err
		}
		handler.bufferedDDLStmts = append(handler.bufferedDDLStmts, s)
	case *tree.AlterSchema:
		if ignoreUnsupportedStmts {
			return handler.unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
	case *tree.CreateSequence:
		schemaQualifiedTableName, err := getSchemaAndTableName(&stmt.Name)
		if err != nil {
			return err
		}
		if !doesMatchTargetTable(schemaQualifiedTableName) {
			return nil
		}

		s, err := rewritePostgresStatementTableName(stmt, handler.dumpDatabaseName, handler.jobID)
		if err != nil {
			return err
		}
		handler.bufferedDDLStmts = append(handler.bufferedDDLStmts, s)
	case *tree.AlterTableOwner:
		if ignoreUnsupportedStmts {
			return handler.unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
	case *tree.AlterSequence:
		if ignoreUnsupportedStmts {
			return handler.unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	// Some SELECT statements mutate schema. Search for those here.
	case *tree.Select:
		switch sel := stmt.Select.(type) {
		case *tree.SelectClause:
			for _, selExpr := range sel.Exprs {
				switch expr := selExpr.Expr.(type) {
				case *tree.FuncExpr:
					// Look for function calls that mutate schema (this is actually a thing).
					semaCtx := tree.MakeSemaContext()
					if _, err := expr.TypeCheck(ctx, &semaCtx, nil /* desired */); err != nil {
						// If the expression does not type check, it may be a case of using
						// a column that does not exist yet in a setval call (as is the case
						// of PGDUMP output from ogr2ogr). We're not interested in setval
						// calls during schema reading so it is safe to ignore this for now.
						if f := expr.Func.String(); pgerror.GetPGCode(err) == pgcode.UndefinedColumn && f == "setval" {
							continue
						}
						return err
					}
					ov := expr.ResolvedOverload()
					// Search for a SQLFn, which returns a SQL string to execute.
					fn := ov.SQLFn
					if fn == nil {
						err := errors.Errorf("unsupported function call: %s in stmt: %s",
							expr.Func.String(), stmt.String())
						if ignoreUnsupportedStmts {
							if err := handler.unsupportedStmtLogger.log(err.Error(), false /* isParseError */); err != nil {
								return err
							}
							continue
						}
						return wrapErrorWithUnsupportedHint(err)
					}
					// Attempt to convert all func exprs to datums.
					datums := make(tree.Datums, len(expr.Exprs))
					for i, ex := range expr.Exprs {
						d, ok := ex.(tree.Datum)
						if !ok {
							// We got something that wasn't a datum so we can't call the
							// overload. Since this is a SQLFn and the user would have
							// expected us to execute it, we have to error.
							return errors.Errorf("unsupported statement: %s", stmt)
						}
						datums[i] = d
					}
					// Now that we have all of the datums, we can execute the overload.
					fnSQL, err := fn(evalCtx, datums)
					if err != nil {
						return err
					}
					// We have some sql. Parse and process it.
					fnStmts, err := parser.Parse(fnSQL)
					if err != nil {
						return err
					}
					for _, fnStmt := range fnStmts {
						switch ast := fnStmt.AST.(type) {
						case *tree.AlterTable:
							alterTableHandler := &postgresDDLHandler{
								jobID:                 handler.jobID,
								targetTableName:       handler.targetTableName,
								dumpDatabaseName:      handler.dumpDatabaseName,
								unsupportedStmtLogger: handler.unsupportedStmtLogger,
							}
							err := bufferDDLPostgresStatement(ctx, evalCtx, ast, p, parentID, alterTableHandler)
							if err != nil {
								return err
							}
							handler.bufferedDDLStmts = append(handler.bufferedDDLStmts, alterTableHandler.bufferedDDLStmts[0])
						default:
							// We only support ALTER statements returned from a SQLFn.
							return errors.Errorf("unsupported statement: %s", stmt)
						}
					}
				default:
					err := errors.Errorf("unsupported %T SELECT expr: %s", expr, expr)
					if ignoreUnsupportedStmts {
						err := handler.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
						if err != nil {
							return err
						}
						continue
					}
					return wrapErrorWithUnsupportedHint(err)
				}
			}
		default:
			err := errors.Errorf("unsupported %T SELECT %s", sel, sel)
			if ignoreUnsupportedStmts {
				err := handler.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
				if err != nil {
					return err
				}
				return nil
			}
			return wrapErrorWithUnsupportedHint(err)
		}
	case *tree.DropTable:
		names := stmt.Names

		// If we find a table with the same name in the target DB we are importing
		// into and same public schema, then we throw an error telling the user to
		// drop the conflicting existing table to proceed.
		// Otherwise, we silently ignore the drop statement and continue with the import.
		for _, name := range names {
			tableName := name.ToUnresolvedObjectName().String()
			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				err := catalogkv.CheckObjectCollision(
					ctx,
					txn,
					p.ExecCfg().Codec,
					parentID,
					keys.PublicSchemaID,
					tree.NewUnqualifiedTableName(tree.Name(tableName)),
				)
				if err != nil {
					return errors.Wrapf(err, `drop table "%s" and then retry the import`, tableName)
				}
				return nil
			}); err != nil {
				return err
			}
		}
	case *tree.BeginTransaction, *tree.CommitTransaction:
	case *tree.Insert, *tree.CopyFrom, *tree.Delete, copyData:
		// handled during the data ingestion pass.
	case *tree.CreateExtension, *tree.CommentOnDatabase, *tree.CommentOnTable,
		*tree.CommentOnIndex, *tree.CommentOnConstraint, *tree.CommentOnColumn, *tree.SetVar, *tree.Analyze,
		*tree.CommentOnSchema:
		// These are the statements that can be parsed by CRDB but are not
		// supported, or are not required to be processed, during an IMPORT.
		// - ignore txns.
		// - ignore SETs and DMLs.
		// - ANALYZE is syntactic sugar for CreateStatistics. It can be ignored
		// because the auto stats stuff will pick up the changes and run if needed.
		if ignoreUnsupportedStmts {
			return handler.unsupportedStmtLogger.log(fmt.Sprintf("%s", stmt), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	case *tree.CreateType:
		return errors.New("IMPORT PGDUMP does not support user defined types; please" +
			" remove all CREATE TYPE statements and their usages from the dump file")
	case error:
		if !errors.Is(stmt, errCopyDone) {
			return stmt
		}
	default:
		if ignoreUnsupportedStmts {
			return handler.unsupportedStmtLogger.log(fmt.Sprintf("%s", stmt), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	}
	return nil
}

// parseDDLStatementsFromDumpFile parses the DDL statements from the dump file,
// and replaces all qualified object names to point to the temporary database
// being imported into.
// This method returns a postgresDDLHandler that contains the buffered DDL
// statements along with other relevant metadata.
func parseDDLStatementsFromDumpFile(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	p sql.JobExecContext,
	jobID jobspb.JobID,
	dumpFile string,
	format roachpb.IOFileFormat,
	maxRowSize int,
	parentID descpb.ID,
	targetTableName string,
) (postgresDDLHandler, error) {
	handler := postgresDDLHandler{
		jobID: jobID, bufferedDDLStmts: make([]string, 0), targetTableName: targetTableName}
	// Open the dump file.
	store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, dumpFile, p.User())
	if err != nil {
		return handler, err
	}
	defer store.Close()

	raw, err := store.ReadFile(ctx, "")
	if err != nil {
		return handler, err
	}
	defer raw.Close()
	reader, err := decompressingReader(raw, dumpFile, format.Compression)
	if err != nil {
		return handler, err
	}
	defer reader.Close()

	// Setup a logger to handle unsupported DDL statements in the PGDUMP file.
	unsupportedStmtLogger := makeUnsupportedStmtLogger(ctx, p.User(), int64(jobID),
		format.PgDump.IgnoreUnsupported, format.PgDump.IgnoreUnsupportedLog, schemaParsing,
		p.ExecCfg().DistSQLSrv.ExternalStorage)
	handler.unsupportedStmtLogger = unsupportedStmtLogger

	// Start reading postgres statements.
	ps := newPostgreStream(ctx, reader, maxRowSize, unsupportedStmtLogger)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return handler, errors.Wrap(err, "postgres parse error")
		}
		if err := bufferDDLPostgresStatement(ctx, evalCtx, stmt, p, parentID, &handler); err != nil {
			return handler, err
		}
	}

	// Flush the unsupported statement log.
	logErr := unsupportedStmtLogger.flush()
	if logErr != nil {
		return handler, logErr
	}

	return handler, nil
}

func runDDLStatementsFromDumpFile(
	ctx context.Context, p sql.JobExecContext, h postgresDDLHandler,
) error {
	for _, stmt := range h.bufferedDDLStmts {
		_, err := p.ExecCfg().InternalExecutor.ExecEx(ctx, "import-pgdump-ddl", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName(),
				Database: getImportTempDBName(h.jobID)}, stmt)
		if err != nil {
			return errors.Wrapf(err, "executing %s", stmt)
		}
	}
	return nil
}

func moveObjectsInTempDatabaseToState(
	ctx context.Context,
	p sql.JobExecContext,
	tempDatabaseID descpb.ID,
	state descpb.DescriptorState,
	dumpHasDatabase bool,
	parentID descpb.ID,
) (
	map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite,
	map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite,
	[]*tabledesc.Mutable,
	[]*schemadesc.Mutable,
	error,
) {
	tableRewrites := make(map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite)
	schemaRewrites := make(map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite)
	importedTables := make([]*tabledesc.Mutable, 0)
	importedSchemas := make([]*schemadesc.Mutable, 0)
	err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		b := txn.NewBatch()
		tableDescs, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, tempDatabaseID,
			tree.CommonLookupFlags{
				AvoidCached:    false,
				IncludeOffline: true,
			})
		if err != nil {
			return err
		}

		for _, desc := range tableDescs {
			mutTableDesc := tabledesc.NewBuilder(desc.TableDesc()).BuildExistingMutableTable()
			mutTableDesc.State = state
			if state == descpb.DescriptorState_OFFLINE {
				mutTableDesc.OfflineReason = "importing"
			}
			// If the dump file did not specify a database, we must queue a rewrite
			// for the descriptor to point it to the target database that was resolved
			// during import planning.
			if !dumpHasDatabase {
				tableRewrites[desc.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{
					ID:       desc.GetID(),
					ParentID: parentID,
				}
			}
			importedTables = append(importedTables, mutTableDesc)
			if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, mutTableDesc, b); err != nil {
				return err
			}
		}

		schemaDescs, err := descsCol.GetAllSchemaDescriptorsInDatabase(ctx, txn, tempDatabaseID, tree.CommonLookupFlags{
			AvoidCached:    false,
			IncludeOffline: true,
		})
		if err != nil {
			return err
		}

		for _, desc := range schemaDescs {
			mutSchemaDesc := schemadesc.NewBuilder(desc.SchemaDesc()).BuildCreatedMutableSchema()
			mutSchemaDesc.State = state
			if state == descpb.DescriptorState_OFFLINE {
				mutSchemaDesc.OfflineReason = "importing"
			}
			// If the dump file did not specify a database, we must queue a rewrite
			// for the descriptor to point it to the target database that was resolved
			// during import planning.
			if !dumpHasDatabase {
				schemaRewrites[desc.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{
					ID:       desc.GetID(),
					ParentID: parentID,
				}
			}
			importedSchemas = append(importedSchemas, mutSchemaDesc)
			if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, mutSchemaDesc, b); err != nil {
				return err
			}
		}

		// TODO(adityamaru): When we add UDT support to IMPORT PGDUMP we should
		// probably set those to offline too.
		return txn.Run(ctx, b)
	})
	return tableRewrites, schemaRewrites, importedTables, importedSchemas, err
}

// TODO(adityamaru): Figure out job resumption semantics.
// TODO(adityamaru): Update job status to reflect stage.
func processDDLStatements(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	p sql.JobExecContext,
	details jobspb.ImportDetails,
	jobID jobspb.JobID,
	dumpFile string,
	format roachpb.IOFileFormat,
	maxRowSize int,
	parentID descpb.ID,
) (
	map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite,
	map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite,
	[]*tabledesc.Mutable,
	[]*schemadesc.Mutable,
	error,
) {
	// A single table entry in the import job details when importing a bundle format
	// indicates that we are performing a single table import.
	// This info is populated during the planning phase.
	var tableName string
	if len(details.Tables) > 0 {
		tableName = details.Tables[0].Name
	}

	// Create a temporary database that we will run DDL statements against.
	tempDescDBID, err := createTempImportDatabase(ctx, p, jobID)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "creating temporary import database")
	}

	// Parse DDL statements in the dump file, and replace all qualified object
	// names to point to the temporary database we created above.
	h, err := parseDDLStatementsFromDumpFile(ctx, evalCtx, p, jobID, dumpFile, format, maxRowSize, parentID, tableName)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "parsing DDL statements from dump file")
	}

	// Run the buffered DDL statements.
	if err := runDDLStatementsFromDumpFile(ctx, p, h); err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "running DDL statements from dump file")
	}

	// If the dump file had a `CREATE DATABASE` stmt, we rename the temp database
	// to the provided name.
	dumpHasDatabase := h.dumpDatabaseName != ""
	if dumpHasDatabase {
		_, err := p.ExecCfg().InternalExecutor.ExecEx(ctx, "rename-temp-import-db", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName(), Database: getImportTempDBName(h.jobID)},
			fmt.Sprintf(`ALTER DATABASE %s RENAME TO %s`, getImportTempDBName(h.jobID), h.dumpDatabaseName))
		if err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "renaming temp import database")
		}
	}

	// Move all tables, schemas, sequences in the temporary database to an OFFLINE
	// state.
	tableRewrites, schemaRewrites, tableDescs, schemaDescs, err := moveObjectsInTempDatabaseToState(ctx, p, tempDescDBID,
		descpb.DescriptorState_OFFLINE, dumpHasDatabase, parentID)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "moving objects in temp database to offline state")
	}

	// TODO(adityamaru): Now it is safe to run the grants since the objects are
	// all offline.

	return tableRewrites, schemaRewrites, tableDescs, schemaDescs, nil
}
