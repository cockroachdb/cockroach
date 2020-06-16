// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

type renameDatabaseNode struct {
	dbDesc  *sqlbase.ImmutableDatabaseDescriptor
	newName string
}

// RenameDatabase renames the database.
// Privileges: superuser, DROP on source database.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(ctx context.Context, n *tree.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("RENAME DATABASE on current database")
	}

	if err := p.RequireAdminRole(ctx, "ALTER DATABASE ... RENAME"); err != nil {
		return nil, err
	}

	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), true /*required*/)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if n.Name == n.NewName {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	return &renameDatabaseNode{
		dbDesc:  dbDesc,
		newName: string(n.NewName),
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameDatabaseNode) ReadingOwnWrites() {}

func (n *renameDatabaseNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	dbDesc := n.dbDesc

	// Check if any other tables depend on tables in the database.
	// Because our views and sequence defaults are currently just stored as
	// strings, they (may) explicitly specify the database name.
	// Rather than trying to rewrite them with the changed DB name, we
	// simply disallow such renames for now.
	// See #34416.
	phyAccessor := p.PhysicalSchemaAccessor()
	lookupFlags := p.CommonLookupFlags(true /*required*/)
	// DDL statements bypass the cache.
	lookupFlags.AvoidCached = true
	schemas, err := p.Tables().GetSchemasForDatabase(ctx, p.txn, dbDesc.GetID())
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		tbNames, err := phyAccessor.GetObjectNames(
			ctx,
			p.txn,
			p.ExecCfg().Codec,
			dbDesc,
			schema,
			tree.DatabaseListFlags{
				CommonLookupFlags: lookupFlags,
				ExplicitPrefix:    true,
			},
		)
		if err != nil {
			return err
		}
		lookupFlags.Required = false
		for i := range tbNames {
			objDesc, err := phyAccessor.GetObjectDesc(
				ctx,
				p.txn,
				p.ExecCfg().Settings,
				p.ExecCfg().Codec,
				tbNames[i].Catalog(),
				tbNames[i].Schema(),
				tbNames[i].Table(),
				tree.ObjectLookupFlags{CommonLookupFlags: lookupFlags},
			)
			if err != nil {
				return err
			}
			if objDesc == nil {
				continue
			}
			tbDesc := objDesc.TableDesc()
			for _, dependedOn := range tbDesc.DependedOnBy {
				dependentDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, p.ExecCfg().Codec, dependedOn.ID)
				if err != nil {
					return err
				}

				isAllowed, referencedCol, err := isAllowedDependentDescInRenameDatabase(
					ctx,
					dependedOn,
					tbDesc,
					dependentDesc,
					dbDesc.GetName(),
				)
				if err != nil {
					return err
				}
				if isAllowed {
					continue
				}

				tbTableName := tree.MakeTableNameWithSchema(
					tree.Name(dbDesc.GetName()),
					tree.Name(schema),
					tree.Name(tbDesc.Name),
				)
				var dependentDescQualifiedString string
				if dbDesc.GetID() != dependentDesc.ParentID || tbDesc.GetParentSchemaID() != dependentDesc.GetParentSchemaID() {
					var err error
					dependentDescQualifiedString, err = p.getQualifiedTableName(ctx, dependentDesc)
					if err != nil {
						log.Warningf(
							ctx,
							"unable to retrieve fully-qualified name of %s (id: %d): %v",
							tbTableName.String(),
							dependentDesc.ID,
							err,
						)
						return sqlbase.NewDependentObjectErrorf(
							"cannot rename database because a relation depends on relation %q",
							tbTableName.String())
					}
				} else {
					dependentDescTableName := tree.MakeTableNameWithSchema(
						tree.Name(dbDesc.GetName()),
						tree.Name(schema),
						tree.Name(dependentDesc.Name),
					)
					dependentDescQualifiedString = dependentDescTableName.String()
				}
				depErr := sqlbase.NewDependentObjectErrorf(
					"cannot rename database because relation %q depends on relation %q",
					dependentDescQualifiedString,
					tbTableName.String(),
				)

				// We can have a more specific error message for sequences.
				if tbDesc.IsSequence() {
					hint := fmt.Sprintf(
						"you can drop the column default %q of %q referencing %q",
						referencedCol,
						tbTableName.String(),
						dependentDescQualifiedString,
					)
					if dependentDesc.GetParentID() == dbDesc.GetID() {
						hint += fmt.Sprintf(
							" or modify the default to not reference the database name %q",
							dbDesc.GetName(),
						)
					}
					return errors.WithHint(depErr, hint)
				}

				// Otherwise, we default to the view error message.
				return errors.WithHintf(depErr,
					"you can drop %q instead", dependentDescQualifiedString)
			}
		}
	}

	return p.renameDatabase(ctx, dbDesc, n.newName)
}

// isAllowedDependentDescInRename determines when rename database is allowed with
// a given {tbDesc, dependentDesc} with the relationship dependedOn on a db named dbName.
// Returns a bool representing whether it's allowed, a string indicating the column name
// found to contain the database (if it exists), and an error if any.
// This is a workaround for #45411 until #34416 is resolved.
func isAllowedDependentDescInRenameDatabase(
	ctx context.Context,
	dependedOn sqlbase.TableDescriptor_Reference,
	tbDesc *sqlbase.TableDescriptor,
	dependentDesc *sqlbase.TableDescriptor,
	dbName string,
) (bool, string, error) {
	// If it is a sequence, and it does not contain the database name, then we have
	// no reason to block it's deletion.
	if !tbDesc.IsSequence() {
		return false, "", nil
	}

	colIDs := util.MakeFastIntSet()
	for _, colID := range dependedOn.ColumnIDs {
		colIDs.Add(int(colID))
	}

	for _, column := range dependentDesc.Columns {
		if !colIDs.Contains(int(column.ID)) {
			continue
		}
		colIDs.Remove(int(column.ID))

		if column.DefaultExpr == nil {
			return false, "", errors.AssertionFailedf(
				"rename_database: expected column id %d in table id %d to have a default expr",
				dependedOn.ID,
				dependentDesc.ID,
			)
		}
		// Try parse the default expression and find the table name direct reference.
		parsedExpr, err := parser.ParseExpr(*column.DefaultExpr)
		if err != nil {
			return false, "", err
		}
		typedExpr, err := tree.TypeCheck(ctx, parsedExpr, nil, column.Type)
		if err != nil {
			return false, "", err
		}
		seqNames, err := sequence.GetUsedSequenceNames(typedExpr)
		if err != nil {
			return false, "", err
		}
		for _, seqName := range seqNames {
			parsedSeqName, err := parser.ParseTableName(seqName)
			if err != nil {
				return false, "", err
			}
			// There must be at least two parts for this to work.
			if parsedSeqName.NumParts >= 2 {
				// We only don't allow this if the database name is in there.
				// This is always the last argument.
				if tree.Name(parsedSeqName.Parts[parsedSeqName.NumParts-1]).Normalize() == tree.Name(dbName).Normalize() {
					return false, column.Name, nil
				}
			}
		}
	}
	if colIDs.Len() > 0 {
		return false, "", errors.AssertionFailedf(
			"expected to find column ids %s in table id %d",
			colIDs.String(),
			dependentDesc.ID,
		)
	}
	return true, "", nil
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
