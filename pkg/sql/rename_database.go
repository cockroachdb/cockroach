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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

type renameDatabaseNode struct {
	n       *tree.RenameDatabase
	dbDesc  *dbdesc.Mutable
	newName string
}

// RenameDatabase renames the database.
// Privileges: superuser + DROP or ownership + CREATEDB privileges
//   Notes: mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(ctx context.Context, n *tree.RenameDatabase) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("RENAME DATABASE on current database")
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}

	if hasAdmin {
		// The user must have DROP privileges on the database. This prevents a
		// superuser from renaming, e.g., the system database.
		if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
			return nil, err
		}
	} else {
		// Non-superusers must be the owner and have the CREATEDB privilege.
		hasOwnership, err := p.HasOwnership(ctx, dbDesc)
		if err != nil {
			return nil, err
		}
		if !hasOwnership {
			return nil, pgerror.Newf(
				pgcode.InsufficientPrivilege, "must be owner of database %s", n.Name)
		}
		hasCreateDB, err := p.HasRoleOption(ctx, roleoption.CREATEDB)
		if err != nil {
			return nil, err
		}
		if !hasCreateDB {
			return nil, pgerror.New(
				pgcode.InsufficientPrivilege, "permission denied to rename database")
		}
	}

	if n.Name == n.NewName {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	return &renameDatabaseNode{
		n:       n,
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
	lookupFlags := p.CommonLookupFlags(true /*required*/)
	// DDL statements bypass the cache.
	lookupFlags.AvoidCached = true
	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, dbDesc.GetID())
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		tbNames, _, err := p.Descriptors().GetObjectNamesAndIDs(
			ctx,
			p.txn,
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
		// TODO(ajwerner): Make this do something better than one-at-a-time lookups
		// followed by catalogkv reads on each dependency.
		for i := range tbNames {
			found, tbDesc, err := p.Descriptors().GetImmutableTableByName(
				ctx, p.txn, &tbNames[i], tree.ObjectLookupFlags{CommonLookupFlags: lookupFlags},
			)
			if err != nil {
				return err
			}
			if !found {
				continue
			}

			if err := tbDesc.ForeachDependedOnBy(func(dependedOn *descpb.TableDescriptor_Reference) error {
				dependentDesc, err := catalogkv.MustGetTableDescByID(ctx, p.txn, p.ExecCfg().Codec, dependedOn.ID)
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
					return nil
				}

				tbTableName := tree.MakeTableNameWithSchema(
					tree.Name(dbDesc.GetName()),
					tree.Name(schema),
					tree.Name(tbDesc.GetName()),
				)
				var dependentDescQualifiedString string
				if dbDesc.GetID() != dependentDesc.GetParentID() || tbDesc.GetParentSchemaID() != dependentDesc.GetParentSchemaID() {
					descFQName, err := p.getQualifiedTableName(ctx, dependentDesc)
					if err != nil {
						log.Warningf(
							ctx,
							"unable to retrieve fully-qualified name of %s (id: %d): %v",
							tbTableName.String(),
							dependentDesc.GetID(),
							err,
						)
						return sqlerrors.NewDependentObjectErrorf(
							"cannot rename database because a relation depends on relation %q",
							tbTableName.String())
					}
					dependentDescQualifiedString = descFQName.FQString()
				} else {
					dependentDescTableName := tree.MakeTableNameWithSchema(
						tree.Name(dbDesc.GetName()),
						tree.Name(schema),
						tree.Name(dependentDesc.GetName()),
					)
					dependentDescQualifiedString = dependentDescTableName.String()
				}
				depErr := sqlerrors.NewDependentObjectErrorf(
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
			}); err != nil {
				return err
			}
		}
	}

	if err := p.renameDatabase(ctx, dbDesc, n.newName, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
		return err
	}

	// Log Rename Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		n.dbDesc.GetID(),
		&eventpb.RenameDatabase{
			DatabaseName:    n.n.Name.String(),
			NewDatabaseName: n.newName,
		})
}

// isAllowedDependentDescInRename determines when rename database is allowed with
// a given {tbDesc, dependentDesc} with the relationship dependedOn on a db named dbName.
// Returns a bool representing whether it's allowed, a string indicating the column name
// found to contain the database (if it exists), and an error if any.
// This is a workaround for #45411 until #34416 is resolved.
func isAllowedDependentDescInRenameDatabase(
	ctx context.Context,
	dependedOn *descpb.TableDescriptor_Reference,
	tbDesc catalog.TableDescriptor,
	dependentDesc catalog.TableDescriptor,
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

	for _, column := range dependentDesc.PublicColumns() {
		if !colIDs.Contains(int(column.GetID())) {
			continue
		}
		colIDs.Remove(int(column.GetID()))

		if !column.HasDefault() {
			return false, "", errors.AssertionFailedf(
				"rename_database: expected column id %d in table id %d to have a default expr",
				dependedOn.ID,
				dependentDesc.GetID(),
			)
		}
		// Try parse the default expression and find the table name direct reference.
		parsedExpr, err := parser.ParseExpr(column.GetDefaultExpr())
		if err != nil {
			return false, "", err
		}
		typedExpr, err := tree.TypeCheck(ctx, parsedExpr, nil, column.GetType())
		if err != nil {
			return false, "", err
		}
		seqIdentifiers, err := sequence.GetUsedSequences(typedExpr)
		if err != nil {
			return false, "", err
		}
		for _, seqIdentifier := range seqIdentifiers {
			if seqIdentifier.IsByID() {
				continue
			}
			parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
			if err != nil {
				return false, "", err
			}
			// There must be at least two parts for this to work.
			if parsedSeqName.NumParts >= 2 {
				// We only don't allow this if the database name is in there.
				// This is always the last argument.
				if tree.Name(parsedSeqName.Parts[parsedSeqName.NumParts-1]).Normalize() == tree.Name(dbName).Normalize() {
					return false, column.GetName(), nil
				}
			}
		}
	}
	if colIDs.Len() > 0 {
		return false, "", errors.AssertionFailedf(
			"expected to find column ids %s in table id %d",
			colIDs.String(),
			dependentDesc.GetID(),
		)
	}
	return true, "", nil
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
