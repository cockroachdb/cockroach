// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

const (
	// ImportTempPgdumpDB is the default name of the temp database for newly
	// created schema from DDL stmts in the dump file.
	ImportTempPgdumpDB = "crdb_temp_pgdump_import"
)

// createTempImportDatabase creates a temporary database where we will create
// all the PGDUMP objects during the import.
func createTempImportDatabase(ctx context.Context, p sql.JobExecContext) (descpb.ID, error) {
	id, err := p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
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
	tempDBDesc := dbdesc.NewInitial(id, ImportTempPgdumpDB, username.NodeUserName())
	return tempDBDesc.GetID(), p.ExecCfg().InternalExecutorFactory.DescsTxn(ctx, p.ExecCfg().DB, func(
		ctx context.Context, txn *kv.Txn, col *descs.Collection,
	) error {
		b := txn.NewBatch()
		if err := col.WriteDescToBatch(
			ctx, true /* kvTrace */, tempDBDesc, b,
		); err != nil {
			return err
		}
		b.CPut(catalogkeys.MakeDatabaseNameKey(p.ExecCfg().Codec, ImportTempPgdumpDB), tempDBDesc.GetID(), nil)
		return txn.Run(ctx, b)
	})
}

// PostgresDDLHandler stored DDL stmts and the temp db to store the new schemas.
type PostgresDDLHandler struct {
	dumpDatabaseName string
	// TODO(adityamaru): Maybe memory monitor?
	BufferedDDLStmts []string
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
	n tree.NodeFormatter, dumpDatabaseName string,
) (string, error) {
	var err error
	f := tree.NewFmtCtx(
		// TODO(adityamaru): should this be serializable?
		tree.FmtParsable,
		tree.FmtReformatTableNames(func(fmtctx *tree.FmtCtx, tn *tree.TableName) {
			// If the node has an explicit catalog name then we must replace it with
			// the temporary database we are importing into.
			// We only allow "db.schema.table" or "schema.table" form.
			if tn.CatalogName != "" {
				if checkCatNameErr := checkCatalogName(tn.CatalogName, tree.Name(dumpDatabaseName)); checkCatNameErr != nil {
					err = checkCatNameErr
					return
				}
				tn.CatalogName = ImportTempPgdumpDB
			}

			fmtctx.WithReformatTableNames(nil, func() {
				fmtctx.FormatNode(tn)
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
	evalCtx *eval.Context,
	postgresStmt interface{},
	p sql.JobExecContext,
	parentID descpb.ID,
	handler *PostgresDDLHandler,
) error {
	switch stmt := postgresStmt.(type) {
	case *tree.CreateDatabase:
		// If we have previously seen a `CREATE DATABASE` statement then we error
		// out.
		if handler.dumpDatabaseName != "" {
			return errors.Newf("encountered more than one `CREATE DATABASE` statement when importing PGDUMP file")
		}
		handler.dumpDatabaseName = string(stmt.Name)
	case *tree.CreateSchema:
		// If the schema specifies an explicit database name, replace it with the
		// temporary pgdump database being imported into.
		if stmt.Schema.ExplicitCatalog {
			if err := checkCatalogName(stmt.Schema.CatalogName, tree.Name(handler.dumpDatabaseName)); err != nil {
				return err
			}
			stmt.Schema.CatalogName = ImportTempPgdumpDB
		}
		handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, formatPostgresStatement(stmt))
	case *tree.CreateTable:
		// If the `CREATE TABLE` specifies an explicit database name, replace it
		// with the temporary pgdump database being imported into.
		s, err := rewritePostgresStatementTableName(stmt, handler.dumpDatabaseName)
		if err != nil {
			return err
		}
		handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, s)
	case *tree.AlterTable:
		// If the `ALTER TABLE` statement has an explicit database name, replace it
		// with the temporary pgdump database being imported into.
		if stmt.Table.HasExplicitCatalog() {
			if err := checkCatalogName(
				tree.Name(stmt.Table.Parts[2]),
				tree.Name(handler.dumpDatabaseName),
			); err != nil {
				return err
			}
			stmt.Table.Parts[2] = ImportTempPgdumpDB
		}
		for _, cmd := range stmt.Cmds {
			switch cmd := cmd.(type) {
			case *tree.AlterTableAddConstraint:
				switch con := cmd.ConstraintDef.(type) {
				case *tree.ForeignKeyConstraintTableDef:
					// TODO(adityamaru): handle FKs and fk skip option.
					if con.Table.ExplicitCatalog {
						con.Table.CatalogName = ImportTempPgdumpDB
					}
				default:
					// TODO(adityamaru): confirm that not other constraint can have a
					// qualified table name.
				}
			case *tree.AlterTableSetDefault:
			case *tree.AlterTableSetVisible:
			case *tree.AlterTableAddColumn:
				if cmd.IfNotExists {
					return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
				}
			case *tree.AlterTableSetNotNull:
			default:
				return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
			}
		}
		handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, formatPostgresStatement(stmt))
	case *tree.CreateIndex:
		// If the `CREATE INDEX` specifies an explicit database name, replace it
		// with the temporary pgdump database being imported into.
		s, err := rewritePostgresStatementTableName(stmt, handler.dumpDatabaseName)
		if err != nil {
			return err
		}
		handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, s)
	case *tree.AlterSchema:
		// If the schema specifies an explicit database name, replace it with the
		// temporary pgdump database being imported into.
		if stmt.Schema.ExplicitCatalog {
			if err := checkCatalogName(stmt.Schema.CatalogName, tree.Name(handler.dumpDatabaseName)); err != nil {
				return err
			}
			stmt.Schema.CatalogName = ImportTempPgdumpDB
		}
		handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, formatPostgresStatement(stmt))
	case *tree.CreateSequence:
		s, err := rewritePostgresStatementTableName(stmt, handler.dumpDatabaseName)
		if err != nil {
			return err
		}
		handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, s)
	case *tree.AlterTableOwner:
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	case *tree.AlterSequence:
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
					fn, ok := ov.SQLFn.(eval.SQLFnOverload)
					if !ok {
						err := errors.Errorf("unsupported function call: %s in stmt: %s",
							expr.Func.String(), stmt.String())
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
					fnSQL, err := fn(ctx, evalCtx, datums)
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
							alterTableHandler := &PostgresDDLHandler{}
							err := bufferDDLPostgresStatement(ctx, evalCtx, ast, p, parentID, alterTableHandler)
							if err != nil {
								return err
							}
							handler.BufferedDDLStmts = append(handler.BufferedDDLStmts, alterTableHandler.BufferedDDLStmts[0])
						default:
							// We only support ALTER statements returned from a SQLFn.
							return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
						}
					}
				default:
					return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T SELECT expr: %s", expr, expr))
				}
			}
		default:
			return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T SELECT %s", sel, sel))
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
				descsCol := p.ExtendedEvalContext().Descs
				err := descsCol.Direct().CheckObjectCollision(
					ctx,
					txn,
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
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	case *tree.CreateType:
		return errors.New("IMPORT PGDUMP does not support user defined types; please" +
			" remove all CREATE TYPE statements and their usages from the dump file")
	case error:
		if !errors.Is(stmt, errCopyDone) {
			return stmt
		}
	default:
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	}
	return nil
}

// ParseDDLStatementsFromDumpFile parses the DDL statements from the dump file,
// and replaces all qualified object names to point to the temporary database
// being imported into.
// This method returns a PostgresDDLHandler that contains the buffered DDL
// statements along with other relevant metadata.
func ParseDDLStatementsFromDumpFile(
	ctx context.Context,
	evalCtx *eval.Context,
	p sql.JobExecContext,
	dumpFile string,
	format roachpb.IOFileFormat,
	maxRowSize int,
	parentID descpb.ID,
) (PostgresDDLHandler, error) {
	handler := PostgresDDLHandler{BufferedDDLStmts: make([]string, 0)}
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
	defer raw.Close(ctx)
	reader, err := decompressingReader(ioctx.ReaderCtxAdapter(ctx, raw), dumpFile, format.Compression)
	if err != nil {
		return handler, err
	}
	defer reader.Close()

	// Start reading postgres statements.
	ps := newPostgreStream(ctx, reader, maxRowSize, &unsupportedStmtLogger{} /* unsupportedStmtLogger */)
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
	return handler, nil
}

func runDDLStmtsFromDumpFile(
	ctx context.Context, stmts []string, txn *kv.Txn, ie sqlutil.InternalExecutor,
) error {
	for _, stmt := range stmts {
		_, err := ie.ExecEx(ctx, "import-pgdump-ddl", txn,
			sessiondata.InternalExecutorOverride{User: username.NodeUserName(), Database: ImportTempPgdumpDB}, stmt)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "executing %s", stmt)
			fnStmts, parseErr := parser.Parse(stmt)
			if parseErr != nil {
				return parseErr
			}
			for _, fnStmt := range fnStmts {
				switch ast := fnStmt.AST.(type) {
				case *tree.CreateTable, *tree.CreateIndex, *tree.CreateSequence:
					return errors.WithHintf(wrappedErr, "is the target of the db.target form? "+
						"please use db.schema.target or schema.target format for target in %q", ast.String())
				}
			}
			return wrappedErr
		}
	}
	return nil
}

func MoveObjectsInTempDatabaseToState(
	ctx context.Context,
	tempDatabaseID descpb.ID,
	state descpb.DescriptorState,
	txn *kv.Txn,
	descsCol *descs.Collection,
) ([]*tabledesc.Mutable, []*schemadesc.Mutable, error) {
	importedTables := make([]*tabledesc.Mutable, 0)
	importedSchemas := make([]*schemadesc.Mutable, 0)

	ok, db, err := descsCol.GetImmutableDatabaseByID(
		ctx, txn, tempDatabaseID, tree.CommonLookupFlags{
			IncludeOffline: true,
		},
	)
	if !ok {
		if err != nil {
			return nil, nil, err
		} else {
			return nil, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", tempDatabaseID))
		}
	}
	tableDescs, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, db)
	if err != nil {
		return nil, nil, err
	}

	for _, desc := range tableDescs {
		mutTableDesc, err := descsCol.GetMutableTableByID(ctx, txn, desc.GetID(), tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				IncludeOffline: true,
			},
		})
		if err != nil {
			return nil, nil, err
		}
		mutTableDesc.State = state
		if state == descpb.DescriptorState_OFFLINE {
			mutTableDesc.OfflineReason = "importing"
		}
		importedTables = append(importedTables, mutTableDesc)
		if err := descsCol.WriteDesc(ctx, false /* kvTrace */, mutTableDesc, txn); err != nil {
			return nil, nil, err
		}
	}

	schemaDescs, err := descsCol.GetAllSchemaDescriptorsInDatabase(ctx, txn, db)
	if err != nil {
		return nil, nil, err
	}

	for _, desc := range schemaDescs {
		mutSchemaDesc, err := descsCol.GetMutableSchemaByID(ctx, txn, desc.GetID(), tree.SchemaLookupFlags{
			IncludeOffline: true,
		})
		if err != nil {
			return nil, nil, err
		}
		mutSchemaDesc.State = state
		if state == descpb.DescriptorState_OFFLINE {
			mutSchemaDesc.OfflineReason = "importing"
		}
		importedSchemas = append(importedSchemas, mutSchemaDesc)
		if err := descsCol.WriteDesc(ctx, false /* kvTrace */, mutSchemaDesc, txn); err != nil {
			return nil, nil, err
		}
	}

	return importedTables, importedSchemas, err
}

// ProcessDDLStatements is to create the schema specified with the ddl stmts
// in the dump file. Specifically, it creates a temporary db, parse the ddl
// stmts in the dump file and push them to the handler, and use an internal
// executor to run all these ddls within a single txn.
// TODO(janexing): Figure out job resumption semantics, via
// ImportDetail.PgdumpDDLProgress.
// TODO(janexing): Update job status to reflect stage.
func ProcessDDLStatements(
	ctx context.Context,
	evalCtx *eval.Context,
	p sql.JobExecContext,
	dumpFile string,
	format roachpb.IOFileFormat,
	maxRowSize int,
	parentID descpb.ID,
) (tableDescs []*tabledesc.Mutable, schemaDescs []*schemadesc.Mutable, err error) {
	// Create a temporary database that we will run DDL statements against.
	// This database will be in an OFFLINE state thereby remaining invisible to
	// the user for the duration of the IMPORT.
	tempDescDBID, err := createTempImportDatabase(ctx, p)
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating temporary import database")
	}

	// Parse DDL statements in the dump file, and replace all qualified object
	// names to point to the temporary database we created above.
	h, err := ParseDDLStatementsFromDumpFile(ctx, evalCtx, p, dumpFile, format, maxRowSize, parentID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "parsing DDL statements from dump file")
	}

	if err := p.ExecCfg().InternalExecutorFactory.DescsTxnWithExecutor(ctx, p.ExecCfg().DB, nil /* sessionData */, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, ie sqlutil.InternalExecutor,
	) error {
		// Run the buffered DDL statements.
		if err := runDDLStmtsFromDumpFile(ctx, h.BufferedDDLStmts, txn, ie); err != nil {
			return errors.Wrap(err, "running DDL statements from dump file")
		}

		// Moved all tables, schemas, sequences in the temporary database to an
		// OFFLINE state.
		tableDescs, schemaDescs, err = MoveObjectsInTempDatabaseToState(ctx, tempDescDBID, descpb.DescriptorState_OFFLINE, txn, descsCol)
		if err != nil {
			return errors.Wrap(err, "moving objects in temp database to offline state")
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}

	return tableDescs, schemaDescs, nil
}

// checkCatalogName check if a schema's explicit catalog is the database that's
// created in the dump file. If not, returns an error.
func checkCatalogName(givenDbName tree.Name, expectedDbName tree.Name) error {
	if givenDbName != expectedDbName {
		return errors.Newf("catalog name %s does not match dump target database name %s",
			givenDbName, expectedDbName)
	}
	return nil
}
