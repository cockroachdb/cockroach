// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
)

var _ resolver.SchemaResolver = &planner{}

type resolveFlags struct {
	skipCache         bool
	contextDatabaseID descpb.ID
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind,
) (prefix catalog.ResolvedObjectPrefix, table *tabledesc.Mutable, err error) {
	prefix, desc, err := resolver.ResolveMutableExistingTableObject(ctx, p, tn, required, requiredType)
	if err != nil {
		return prefix, nil, err
	}

	// Ensure that the current user can access the target schema.
	if desc != nil {
		if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, desc); err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
	}

	return prefix, desc, nil
}

func (p *planner) ResolveTargetObject(
	ctx context.Context, un *tree.UnresolvedObjectName,
) (
	db catalog.DatabaseDescriptor,
	schema catalog.SchemaDescriptor,
	namePrefix tree.ObjectNamePrefix,
	err error,
) {
	var prefix catalog.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, namePrefix, err = resolver.ResolveTargetObject(ctx, p, un)
	})
	if err != nil {
		return nil, nil, namePrefix, err
	}
	return prefix.Database, prefix.Schema, namePrefix, err
}

// GetSchemasForDB gets all the schemas for a database.
func (p *planner) GetSchemasForDB(
	ctx context.Context, dbName string,
) (map[descpb.ID]string, error) {
	dbDesc, err := p.Descriptors().ByName(p.txn).Get().Database(ctx, dbName)
	if err != nil {
		return nil, err
	}

	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.Txn(), dbDesc)
	if err != nil {
		return nil, err
	}

	return schemas, nil
}

// SchemaExists implements the eval.DatabaseCatalog interface.
func (p *planner) SchemaExists(ctx context.Context, dbName, scName string) (found bool, err error) {
	found, _, err = p.LookupSchema(ctx, dbName, scName)
	return found, err
}

// HasAnyPrivilegeForSpecifier is part of the eval.DatabaseCatalog interface.
func (p *planner) HasAnyPrivilegeForSpecifier(
	ctx context.Context,
	specifier eval.HasPrivilegeSpecifier,
	user username.SQLUsername,
	privs []privilege.Privilege,
) (eval.HasAnyPrivilegeResult, error) {
	desc, err := p.ResolveDescriptorForPrivilegeSpecifier(
		ctx,
		specifier,
	)
	if err != nil {
		return eval.HasNoPrivilege, err
	}
	if desc == nil {
		return eval.ObjectNotFound, nil
	}

	for _, priv := range privs {
		// RULE was only added for compatibility with Postgres, and Postgres
		// never allows RULE to be granted, even if the user has ALL privileges.
		// See https://www.postgresql.org/docs/8.1/sql-grant.html
		// and https://www.postgresql.org/docs/release/8.2.0/.
		if priv.Kind == privilege.RULE {
			continue
		}

		if ok, err := p.HasPrivilege(ctx, desc, priv.Kind, user); err != nil {
			return eval.HasNoPrivilege, err
		} else if !ok {
			continue
		}

		if priv.GrantOption {
			isGrantable, err := p.CheckGrantOptionsForUser(
				ctx, desc.GetPrivileges(), desc, []privilege.Kind{priv.Kind}, user,
			)
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			if !isGrantable {
				continue
			}
		}
		return eval.HasPrivilege, nil
	}

	return eval.HasNoPrivilege, nil
}

// ResolveDescriptorForPrivilegeSpecifier resolves a tree.HasPrivilegeSpecifier
// and returns the descriptor for the given object.
func (p *planner) ResolveDescriptorForPrivilegeSpecifier(
	ctx context.Context, specifier eval.HasPrivilegeSpecifier,
) (catalog.Descriptor, error) {
	if specifier.DatabaseName != nil {
		return p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, *specifier.DatabaseName)
	} else if specifier.DatabaseOID != nil {
		database, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Database(ctx, descpb.ID(*specifier.DatabaseOID))
		// When a DatabaseOID is specified and the database is not found,
		// we return NULL.
		if err != nil && sqlerrors.IsUndefinedDatabaseError(err) {
			// nolint:returnerrcheck
			return nil, nil
		}
		return database, err
	} else if specifier.SchemaName != nil {
		database, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, *specifier.SchemaDatabaseName)
		if err != nil {
			return nil, err
		}
		b := p.Descriptors().ByNameWithLeased(p.txn)
		if *specifier.SchemaIsRequired {
			return b.Get().Schema(ctx, database, *specifier.SchemaName)
		}
		return b.MaybeGet().Schema(ctx, database, *specifier.SchemaName)
	} else if specifier.TableName != nil || specifier.TableOID != nil {
		var table catalog.TableDescriptor
		var err error
		if specifier.TableName != nil {
			var tn *tree.TableName
			tn, err = parser.ParseQualifiedTableName(*specifier.TableName)
			if err != nil {
				return nil, err
			}
			if _, err = p.ResolveTableName(ctx, tn); err != nil {
				return nil, err
			}

			if p.SessionData().Database != "" && p.SessionData().Database != string(tn.CatalogName) {
				// Postgres does not allow cross-database references in these
				// functions, so we don't either.
				return nil, pgerror.Newf(pgcode.FeatureNotSupported,
					"cross-database references are not implemented: %s", tn)
			}
			_, table, err = descs.PrefixAndTable(ctx, p.Descriptors().ByNameWithLeased(p.txn).MaybeGet(), tn)
		} else {
			table, err = p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(*specifier.TableOID))
			// When a TableOID is specified and the relation is not found, we return NULL.
			if err != nil && sqlerrors.IsUndefinedRelationError(err) {
				// nolint:returnerrcheck
				return nil, nil
			}
		}
		if err != nil {
			return nil, err
		}
		if *specifier.IsSequence {
			// has_table_privilege works with sequences, but has_sequence_privilege does not work with tables
			if !table.IsSequence() {
				return nil, pgerror.Newf(pgcode.WrongObjectType,
					"\"%s\" is not a sequence", table.GetName())
			}
		} else {
			if err := validateColumnForHasPrivilegeSpecifier(
				table,
				specifier,
			); err != nil {
				return nil, err
			}
		}
		return table, nil
	} else if specifier.FunctionOID != nil {
		fnID := funcdesc.UserDefinedFunctionOIDToID(*specifier.FunctionOID)
		return p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Function(ctx, fnID)
	}
	return nil, errors.AssertionFailedf("invalid HasPrivilegeSpecifier")
}

func validateColumnForHasPrivilegeSpecifier(
	table catalog.TableDescriptor, specifier eval.HasPrivilegeSpecifier,
) error {
	if specifier.ColumnName != nil {
		_, err := catalog.MustFindColumnByTreeName(table, *specifier.ColumnName)
		return err
	}
	if specifier.ColumnAttNum != nil {
		for _, col := range table.PublicColumns() {
			if uint32(col.GetPGAttributeNum()) == *specifier.ColumnAttNum {
				return nil
			}
		}
		return pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %d of relation %s does not exist",
			*specifier.ColumnAttNum,
			tree.Name(table.GetName()),
		)

	}
	return nil
}

// DescriptorWithObjectType wraps a descriptor with the corresponding
// privilege object type.
type DescriptorWithObjectType struct {
	descriptor catalog.Descriptor
	objectType privilege.ObjectType
}

// getDescriptorsFromTargetListForPrivilegeChange fetches the descriptors
// for the targets. Each descriptor is marked along with the corresponding
// object type.
func (p *planner) getDescriptorsFromTargetListForPrivilegeChange(
	ctx context.Context, targets tree.GrantTargetList,
) ([]DescriptorWithObjectType, error) {
	const required = true
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, sqlerrors.ErrNoDatabase
		}
		descs := make([]DescriptorWithObjectType, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(database))
			if err != nil {
				return nil, err
			}
			descs = append(descs, DescriptorWithObjectType{
				descriptor: descriptor,
				objectType: privilege.Database,
			})
		}
		if len(descs) == 0 {
			return nil, sqlerrors.ErrNoMatch
		}
		return descs, nil
	}

	if targets.Types != nil {
		if len(targets.Types) == 0 {
			return nil, sqlerrors.ErrNoType
		}
		descs := make([]DescriptorWithObjectType, 0, len(targets.Types))
		for _, typ := range targets.Types {
			_, descriptor, err := p.ResolveMutableTypeDescriptor(ctx, typ, required)
			if err != nil {
				return nil, err
			}

			descs = append(descs, DescriptorWithObjectType{
				descriptor: descriptor,
				objectType: privilege.Type,
			})
		}

		if len(descs) == 0 {
			return nil, sqlerrors.ErrNoMatch
		}
		return descs, nil
	}

	if targets.Functions != nil || targets.Procedures != nil {
		targetRoutines := targets.Functions
		isFuncs := true
		routineType := tree.UDFRoutine
		if targets.Functions == nil {
			targetRoutines = targets.Procedures
			isFuncs = false
			routineType = tree.ProcedureRoutine
		}
		if len(targetRoutines) == 0 {
			return nil, sqlerrors.ErrNoFunction
		}
		descs := make([]DescriptorWithObjectType, 0, len(targetRoutines))
		fnResolved := catalog.DescriptorIDSet{}
		for _, f := range targetRoutines {
			overload, err := p.matchRoutine(
				ctx, &f, true /* required */, routineType, false, /* inDropContext */
			)
			if err != nil {
				return nil, err
			}
			fnID := funcdesc.UserDefinedFunctionOIDToID(overload.Oid)
			if fnResolved.Contains(fnID) {
				continue
			}
			fnResolved.Add(fnID)
			fnDesc, err := p.Descriptors().MutableByID(p.txn).Function(ctx, fnID)
			if err != nil {
				return nil, err
			}
			if isFuncs == fnDesc.IsProcedure() {
				arg := "function"
				if fnDesc.IsProcedure() {
					arg = "procedure"
				}
				return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a %s",
					fnDesc.Name, arg)
			}
			descs = append(descs, DescriptorWithObjectType{
				descriptor: fnDesc,
				objectType: privilege.Routine,
			})
		}
		return descs, nil
	}

	if targets.Schemas != nil {
		if len(targets.Schemas) == 0 {
			return nil, sqlerrors.ErrNoSchema
		}
		if targets.AllTablesInSchema || targets.AllSequencesInSchema {
			// Get all the descriptors for the tables in the specified schemas.
			var descs []DescriptorWithObjectType
			for _, scName := range targets.Schemas {
				dbName := p.CurrentDatabase()
				if scName.ExplicitCatalog {
					dbName = scName.Catalog()
				}
				db, err := p.Descriptors().MutableByName(p.txn).Database(ctx, dbName)
				if err != nil {
					return nil, err
				}
				sc, err := p.byNameGetterBuilder().Get().Schema(ctx, db, scName.Schema())
				if err != nil {
					return nil, err
				}
				_, objectIDs, err := p.GetObjectNamesAndIDs(ctx, db, sc)
				if err != nil {
					return nil, err
				}
				muts, err := p.Descriptors().MutableByID(p.txn).Descs(ctx, objectIDs)
				if err != nil {
					return nil, err
				}
				for _, mut := range muts {
					if targets.AllTablesInSchema {
						if mut != nil {
							if mut.DescriptorType() == catalog.Table {
								descs = append(
									descs,
									DescriptorWithObjectType{
										descriptor: mut,
										objectType: mut.GetObjectType(),
									})
							}
						}
					} else if targets.AllSequencesInSchema {
						if mut.DescriptorType() == catalog.Table {
							tableDesc, err := catalog.AsTableDescriptor(mut)
							if err != nil {
								return nil, err
							}
							if tableDesc.IsSequence() {
								descs = append(
									descs,
									DescriptorWithObjectType{
										descriptor: mut,
										objectType: privilege.Sequence,
									},
								)
							}
						}
					}
				}
			}

			return descs, nil
		} else if targets.AllFunctionsInSchema || targets.AllProceduresInSchema {
			isProcs := true
			if targets.AllFunctionsInSchema {
				isProcs = false
			}
			var descs []DescriptorWithObjectType
			for _, scName := range targets.Schemas {
				dbName := p.CurrentDatabase()
				if scName.ExplicitCatalog {
					dbName = scName.Catalog()
				}
				db, err := p.Descriptors().MutableByName(p.txn).Database(ctx, dbName)
				if err != nil {
					return nil, err
				}
				sc, err := p.Descriptors().ByName(p.txn).Get().Schema(ctx, db, scName.Schema())
				if err != nil {
					return nil, err
				}
				err = sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
					fn, err := p.Descriptors().MutableByID(p.txn).Function(ctx, sig.ID)
					if err != nil {
						return err
					}
					if isProcs != fn.IsProcedure() {
						// Skip functions if ALL PROCEDURES was specified, and
						// skip procedures if ALL FUNCTIONS was specified.
						return nil
					}
					descs = append(descs, DescriptorWithObjectType{
						descriptor: fn,
						objectType: privilege.Routine,
					})
					return nil
				})
				if err != nil {
					return nil, err
				}
			}
			return descs, nil
		}

		descs := make([]DescriptorWithObjectType, 0, len(targets.Schemas))

		// Resolve the databases being changed
		type schemaWithDBDesc struct {
			schema string
			dbDesc *dbdesc.Mutable
		}
		var targetSchemas []schemaWithDBDesc
		for _, sc := range targets.Schemas {
			dbName := p.CurrentDatabase()
			if sc.ExplicitCatalog {
				dbName = sc.Catalog()
			}
			db, err := p.Descriptors().MutableByName(p.txn).Database(ctx, dbName)
			if err != nil {
				return nil, err
			}
			targetSchemas = append(
				targetSchemas,
				schemaWithDBDesc{schema: sc.Schema(), dbDesc: db},
			)
		}

		for _, sc := range targetSchemas {
			resSchema, err := p.byNameGetterBuilder().Get().Schema(ctx, sc.dbDesc, sc.schema)
			if err != nil {
				return nil, err
			}
			switch resSchema.SchemaKind() {
			case catalog.SchemaUserDefined:
				mutSchema, err := p.Descriptors().MutableByID(p.txn).Schema(ctx, resSchema.GetID())
				if err != nil {
					return nil, err
				}
				descs = append(descs, DescriptorWithObjectType{
					descriptor: mutSchema,
					objectType: privilege.Schema,
				})
			default:
				return nil, pgerror.Newf(pgcode.InvalidSchemaName,
					"cannot change privileges on schema %q", resSchema.GetName())
			}
		}
		return descs, nil
	}

	if len(targets.Tables.TablePatterns) == 0 {
		return nil, sqlerrors.ErrNoTable
	}
	descs := make([]DescriptorWithObjectType, 0, len(targets.Tables.TablePatterns))
	for _, tableTarget := range targets.Tables.TablePatterns {
		tableGlob, err := tableTarget.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		_, objectIDs, err := p.ExpandTableGlob(ctx, tableGlob)
		if err != nil {
			return nil, err
		}
		muts, err := p.Descriptors().MutableByID(p.txn).Descs(ctx, objectIDs)
		if err != nil {
			return nil, err
		}
		for _, mut := range muts {
			if mut != nil && mut.DescriptorType() == catalog.Table {
				tableDesc, err := catalog.AsTableDescriptor(mut)
				if err != nil {
					return nil, err
				}
				if tableDesc.IsSequence() {
					descs = append(
						descs,
						DescriptorWithObjectType{
							descriptor: mut,
							objectType: privilege.Sequence,
						},
					)
				} else if targets.Tables.SequenceOnly {
					return nil, pgerror.Newf(pgcode.WrongObjectType, "%s is not a sequence", tableDesc.GetName())
				} else {
					descs = append(
						descs,
						DescriptorWithObjectType{
							descriptor: mut,
							objectType: privilege.Table,
						},
					)
				}
			}
		}
	}
	if len(descs) == 0 {
		return nil, sqlerrors.ErrNoMatch
	}
	return descs, nil
}

// getFullyQualifiedNamesFromIDs resolves a list of table IDs to their
// fully qualified names.
func (p *planner) getFullyQualifiedNamesFromIDs(
	ctx context.Context, ids []descpb.ID,
) (fullyQualifiedNames []string, _ error) {
	for _, id := range ids {
		desc, err := p.Descriptors().ByIDWithoutLeased(p.txn).Get().Desc(ctx, id)
		if err != nil {
			return nil, err
		}
		switch t := desc.(type) {
		case catalog.TableDescriptor:
			tbName, err := p.getQualifiedTableName(ctx, t)
			if err != nil {
				return nil, err
			}
			fullyQualifiedNames = append(fullyQualifiedNames, tbName.FQString())
		case catalog.FunctionDescriptor:
			fName, err := p.getQualifiedFunctionName(ctx, t)
			if err != nil {
				return nil, err
			}
			fullyQualifiedNames = append(fullyQualifiedNames, fName.FQString())
		}
	}
	return fullyQualifiedNames, nil
}

// getQualifiedSchemaName returns the database-qualified name of the
// schema represented by the provided descriptor.
func (p *planner) getQualifiedSchemaName(
	ctx context.Context, desc catalog.SchemaDescriptor,
) (*tree.ObjectNamePrefix, error) {
	dbDesc, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Database(ctx, desc.GetParentID())
	if err != nil {
		return nil, err
	}
	return &tree.ObjectNamePrefix{
		CatalogName:     tree.Name(dbDesc.GetName()),
		SchemaName:      tree.Name(desc.GetName()),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}, nil
}

// getQualifiedTypeName returns the database-qualified name of the type
// represented by the provided descriptor.
func (p *planner) getQualifiedTypeName(
	ctx context.Context, desc catalog.TypeDescriptor,
) (*tree.TypeName, error) {
	dbDesc, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Database(ctx, desc.GetParentID())
	if err != nil {
		return nil, err
	}

	schemaID := desc.GetParentSchemaID()
	scDesc, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Schema(ctx, schemaID)
	if err != nil {
		return nil, err
	}

	typeName := tree.MakeQualifiedTypeName(
		dbDesc.GetName(),
		scDesc.GetName(),
		desc.GetName(),
	)

	return &typeName, nil
}

// expandMutableIndexName ensures that the index name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandMutableIndexName(
	ctx context.Context, p *planner, index *tree.TableIndexName, requireTable bool,
) (tn *tree.TableName, desc *tabledesc.Mutable, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandIndexName(ctx, p, index, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context, p *planner, index *tree.TableIndexName, requireTable bool,
) (tn *tree.TableName, desc *tabledesc.Mutable, err error) {
	tn = &index.Table
	if tn.Object() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		_, desc, err = resolver.ResolveMutableExistingTableObject(ctx, p, tn, requireTable, tree.ResolveRequireTableOrViewDesc)
		if err != nil {
			return nil, nil, err
		}
		if desc != nil && desc.IsView() && !desc.MaterializedView() {
			return nil, nil, pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table or materialized view", tn.Table())
		}
		return tn, desc, nil
	}

	found, resolvedPrefix, tbl, _, err := resolver.ResolveIndex(
		ctx,
		p,
		index,
		tree.IndexLookupFlags{
			Required:              requireTable,
			IncludeNonActiveIndex: true,
		},
	)
	if err != nil {
		return nil, nil, err
	}
	// if err is nil, that means either:
	// (1) require==false, index is either found or not found
	// (2) require==true, index is found
	if !found {
		return nil, nil, nil
	}
	tableName := tree.MakeTableNameFromPrefix(resolvedPrefix.NamePrefix(), tree.Name(tbl.GetName()))
	// Expand the tableName explicitly.
	tableName.ExplicitSchema = true
	tableName.ExplicitCatalog = true
	// Memoize the table name that was found. tn is a reference to the table name
	// stored in index.Table.
	*tn = tableName
	tblMutable, err := p.Descriptors().MutableByID(p.Txn()).Table(ctx, tbl.GetID())
	if err != nil {
		return nil, nil, err
	}
	return &tableName, tblMutable, nil
}

// getTableAndIndex returns the table and index descriptors for a
// TableIndexName.
//
// It can return indexes that are being rolled out.
func (p *planner) getTableAndIndex(
	ctx context.Context,
	tableWithIndex *tree.TableIndexName,
	privilege privilege.Kind,
	skipCache bool,
) (prefix catalog.ResolvedObjectPrefix, mut *tabledesc.Mutable, idx catalog.Index, err error) {
	p.runWithOptions(resolveFlags{skipCache: skipCache}, func() {
		prefix, mut, idx, err = p.getTableAndIndexImpl(ctx, tableWithIndex, privilege)
	})
	return prefix, mut, idx, err
}

func (p *planner) getTableAndIndexImpl(
	ctx context.Context, tableWithIndex *tree.TableIndexName, privilege privilege.Kind,
) (catalog.ResolvedObjectPrefix, *tabledesc.Mutable, catalog.Index, error) {
	_, resolvedPrefix, tbl, idx, err := resolver.ResolveIndex(
		ctx, p, tableWithIndex,
		tree.IndexLookupFlags{
			Required:              true,
			IncludeNonActiveIndex: false,
		},
	)
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, nil, err
	}
	if err := p.canResolveDescUnderSchema(ctx, resolvedPrefix.Schema, tbl); err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, nil, err
	}
	if err := p.CheckPrivilege(ctx, tbl, privilege); err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, nil, err
	}

	// Set the object name for logging if it's missing.
	if tableWithIndex.Table.ObjectName == "" {
		tableWithIndex.Table = tree.MakeTableNameFromPrefix(
			resolvedPrefix.NamePrefix(), tree.Name(tbl.GetName()),
		)
	}

	// Use the descriptor collection to get a proper handle to the mutable
	// descriptor for the relevant table and use that mutable object to
	// get a handle to the corresponding index.
	mut, err := p.Descriptors().MutableByID(p.Txn()).Table(ctx, tbl.GetID())
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to re-resolve table %d for index %s", tbl.GetID(), tableWithIndex)
	}
	retIdx, err := catalog.MustFindIndexByID(mut, idx.GetID())
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"retrieving index %s (%d) from table which was known to already exist for table %d",
			tableWithIndex, idx.GetID(), tbl.GetID())
	}
	return resolvedPrefix, mut, retIdx, nil
}

// ExpandTableGlob expands pattern into a list of objects represented
// as a tree.TableNames and corresponding descriptor IDs.
func (p *planner) ExpandTableGlob(
	ctx context.Context, pattern tree.TablePattern,
) (tree.TableNames, descpb.IDs, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	return cat.ExpandDataSourceGlob(ctx, &catalog, cat.Flags{}, pattern)
}

// fkSelfResolver is a SchemaResolver that inserts itself between a
// user of name resolution and another SchemaResolver, and will answer
// lookups of the new table being created. This is needed in the case
// of CREATE TABLE with a foreign key self-reference: the target of
// the FK definition is a table that does not exist yet.
type fkSelfResolver struct {
	resolver.SchemaResolver
	prefix       catalog.ResolvedObjectPrefix
	newTableName *tree.TableName
	newTableDesc *tabledesc.Mutable
}

var _ resolver.SchemaResolver = &fkSelfResolver{}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *fkSelfResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, objMeta catalog.Descriptor, err error) {
	if dbName == r.prefix.Database.GetName() &&
		scName == r.prefix.Schema.GetName() &&
		obName == r.newTableName.Table() {
		table := r.newTableDesc
		if flags.RequireMutable {
			return true, r.prefix, table, nil
		}
		return true, r.prefix, table.ImmutableCopy(), nil
	}
	flags.IncludeOffline = false
	return r.SchemaResolver.LookupObject(ctx, flags, dbName, scName, obName)
}

// internalLookupCtx can be used in contexts where all descriptors
// have been recently read, to accelerate the lookup of
// inter-descriptor relationships.
//
// This is used mainly in the generators for virtual tables,
// aliased as tableLookupFn below.
//
// It only reveals physical descriptors (not virtual descriptors).
//
// TODO(ajwerner): remove in 21.2 or whenever cross-database references are
// fully removed.
type internalLookupCtx struct {
	dbNames     map[descpb.ID]string
	dbIDs       []descpb.ID
	dbDescs     map[descpb.ID]catalog.DatabaseDescriptor
	schemaDescs map[descpb.ID]catalog.SchemaDescriptor
	schemaNames map[descpb.ID]string
	schemaIDs   []descpb.ID
	tbDescs     map[descpb.ID]catalog.TableDescriptor
	tbIDs       []descpb.ID
	typDescs    map[descpb.ID]catalog.TypeDescriptor
	typIDs      []descpb.ID
	fnDescs     map[descpb.ID]catalog.FunctionDescriptor
	fnIDs       []descpb.ID
}

// GetSchemaName looks up a schema with the given id in the LookupContext.
func (l *internalLookupCtx) GetSchemaName(
	ctx context.Context, id, parentDBID descpb.ID, version clusterversion.Handle,
) (string, bool, error) {
	dbDesc, err := l.getDatabaseByID(parentDBID)
	if err != nil {
		return "", false, err
	}

	// If a db does not have a public schema backed by a descriptor, we can
	// assume that its public schema ID is 29. This is valid since we cannot
	// drop the public schema in v21.2 or v22.1.
	if !dbDesc.HasPublicSchemaWithDescriptor() {
		if id == keys.PublicSchemaID {
			return catconstants.PublicSchemaName, true, nil
		}
	}

	if parentDBID == keys.SystemDatabaseID {
		if id == keys.SystemPublicSchemaID {
			return catconstants.PublicSchemaName, true, nil
		}
	}

	schemaName, found := l.schemaNames[id]
	return schemaName, found, nil
}

var metamorphicDefaultUseIndexLookupForDescriptorsInDatabase = metamorphic.ConstantWithTestBool(
	`use-index-lookup-for-descriptors-in-database`, true,
)

// useIndexLookupForDescriptorsInDatabase controls whether an index against the
// namespace table should be used to fetch the set of descriptors needed to
// materialize most system tables.
var useIndexLookupForDescriptorsInDatabase = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.catalog.virtual_tables.use_index_lookup_for_descriptors_in_database.enabled",
	"if enabled, virtual tables will do a lookup against the namespace table to"+
		" find the descriptors in a database instead of scanning all descriptors",
	metamorphicDefaultUseIndexLookupForDescriptorsInDatabase,
)

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

// newInternalLookupCtx provides cached access to a set of descriptors for use
// in virtual tables.
func newInternalLookupCtx(
	descs []catalog.Descriptor, prefix catalog.DatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[descpb.ID]string)
	dbDescs := make(map[descpb.ID]catalog.DatabaseDescriptor)
	schemaDescs := make(map[descpb.ID]catalog.SchemaDescriptor)
	schemaNames := make(map[descpb.ID]string)

	tbDescs := make(map[descpb.ID]catalog.TableDescriptor)
	typDescs := make(map[descpb.ID]catalog.TypeDescriptor)
	fnDescs := make(map[descpb.ID]catalog.FunctionDescriptor)

	var tbIDs, typIDs, dbIDs, schemaIDs, fnIDs []descpb.ID

	// Record descriptors for name lookups.
	for i := range descs {
		switch desc := descs[i].(type) {
		case catalog.DatabaseDescriptor:
			dbNames[desc.GetID()] = desc.GetName()
			dbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetID() {
				// Only make the database visible for iteration if the prefix was included.
				dbIDs = append(dbIDs, desc.GetID())
			}
		case catalog.TableDescriptor:
			tbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, desc.GetID())
			}
		case catalog.TypeDescriptor:
			typDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the type visible for iteration if the prefix was included.
				typIDs = append(typIDs, desc.GetID())
			}
		case catalog.SchemaDescriptor:
			schemaDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the schema visible for iteration if the prefix was included.
				schemaIDs = append(schemaIDs, desc.GetID())
				schemaNames[desc.GetID()] = desc.GetName()
			}
		case catalog.FunctionDescriptor:
			fnDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				fnIDs = append(fnIDs, desc.GetID())
			}
		}
	}

	return &internalLookupCtx{
		dbNames:     dbNames,
		dbDescs:     dbDescs,
		schemaDescs: schemaDescs,
		schemaNames: schemaNames,
		schemaIDs:   schemaIDs,
		tbDescs:     tbDescs,
		typDescs:    typDescs,
		fnDescs:     fnDescs,
		tbIDs:       tbIDs,
		dbIDs:       dbIDs,
		typIDs:      typIDs,
		fnIDs:       fnIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(id descpb.ID) (catalog.DatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getTableByID(id descpb.ID) (catalog.TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb, nil
}

func (l *internalLookupCtx) getTypeByID(id descpb.ID) (catalog.TypeDescriptor, error) {
	typ, ok := l.typDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedTypeError(
			tree.NewUnqualifiedTypeName(fmt.Sprintf("[%d]", id)))
	}
	return typ, nil
}

func (l *internalLookupCtx) getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error) {
	sc, ok := l.schemaDescs[id]
	if !ok {
		if id == keys.SystemPublicSchemaID {
			return schemadesc.GetPublicSchema(), nil
		}
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

// getSchemaNameByID returns the schema name given an ID for a schema.
func (l *internalLookupCtx) getSchemaNameByID(id descpb.ID) (string, error) {
	// TODO(richardjcai): Remove this in 22.2, once it is guaranteed that
	//    public schemas are regular UDS.
	if id == keys.PublicSchemaID {
		return catconstants.PublicSchemaName, nil
	}
	schema, err := l.getSchemaByID(id)
	if err != nil {
		return "", err
	}
	return schema.GetName(), nil
}

func (l *internalLookupCtx) getDatabaseName(table catalog.Descriptor) string {
	parentName := l.dbNames[table.GetParentID()]
	if parentName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this table before the dropped table descriptors are
		// effectively deleted.
		parentName = fmt.Sprintf("[%d]", table.GetParentID())
	}
	return parentName
}

func (l *internalLookupCtx) getSchemaName(table catalog.TableDescriptor) string {
	schemaName := l.schemaNames[table.GetParentSchemaID()]
	if schemaName == "" {
		// The parent schema was deleted. This is possible e.g. when
		// a schema is dropped with CASCADE, and someone queries
		// this table before the dropped table descriptors are
		// effectively deleted.
		schemaName = fmt.Sprintf("[%d]", table.GetParentSchemaID())
	}
	return schemaName
}

// getTableNameFromTableDescriptor returns a TableName object for a given
// TableDescriptor.
func getTableNameFromTableDescriptor(
	l simpleSchemaResolver, table catalog.TableDescriptor, dbPrefix string,
) (tree.TableName, error) {
	var tableName tree.TableName
	tableDbDesc, err := l.getDatabaseByID(table.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	var parentSchemaName tree.Name
	// TODO(richardjcai): Remove this in 22.2.
	if table.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = catconstants.PublicSchemaName
	} else {
		parentSchema, err := l.getSchemaByID(table.GetParentSchemaID())
		if err != nil {
			return tree.TableName{}, err
		}
		parentSchemaName = tree.Name(parentSchema.GetName())
	}
	tableName = tree.MakeTableNameWithSchema(tree.Name(tableDbDesc.GetName()),
		parentSchemaName, tree.Name(table.GetName()))
	tableName.ExplicitCatalog = tableDbDesc.GetName() != dbPrefix
	return tableName, nil
}

// getTypeNameFromTypeDescriptor returns a TypeName object for a given
// TableDescriptor.
func getTypeNameFromTypeDescriptor(
	l simpleSchemaResolver, typ catalog.TypeDescriptor,
) (tree.TypeName, error) {
	var typeName tree.TypeName
	tableDbDesc, err := l.getDatabaseByID(typ.GetParentID())
	if err != nil {
		return typeName, err
	}
	var parentSchemaName string
	// TODO(richardjcai): Remove this in 22.2.
	if typ.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = catconstants.PublicSchemaName
	} else {
		parentSchema, err := l.getSchemaByID(typ.GetParentSchemaID())
		if err != nil {
			return typeName, err
		}
		parentSchemaName = parentSchema.GetName()
	}
	typeName = tree.MakeQualifiedTypeName(tableDbDesc.GetName(),
		parentSchemaName, typ.GetName())
	return typeName, nil
}

func getSchemaNameFromSchemaDescriptor(
	l simpleSchemaResolver, sc catalog.SchemaDescriptor,
) (tree.ObjectNamePrefix, error) {
	var scName tree.ObjectNamePrefix
	db, err := l.getDatabaseByID(sc.GetParentID())
	if err != nil {
		return scName, err
	}
	return tree.ObjectNamePrefix{
		CatalogName:     tree.Name(db.GetName()),
		SchemaName:      tree.Name(sc.GetName()),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}, nil
}

func getFunctionNameFromFunctionDescriptor(
	l simpleSchemaResolver, fn catalog.FunctionDescriptor,
) (tree.RoutineName, error) {
	var fnName tree.RoutineName
	db, err := l.getDatabaseByID(fn.GetParentID())
	if err != nil {
		return fnName, err
	}
	var scName string
	// TODO(richardjcai): Remove this in 22.2.
	if fn.GetParentSchemaID() == keys.PublicSchemaID {
		scName = catconstants.PublicSchemaName
	} else {
		sc, err := l.getSchemaByID(fn.GetParentSchemaID())
		if err != nil {
			return fnName, err
		}
		scName = sc.GetName()
	}
	return tree.MakeQualifiedRoutineName(db.GetName(), scName, fn.GetName()), nil
}

// ResolveMutableTypeDescriptor resolves a type descriptor for mutable access.
func (p *planner) ResolveMutableTypeDescriptor(
	ctx context.Context, name *tree.UnresolvedObjectName, required bool,
) (catalog.ResolvedObjectPrefix, *typedesc.Mutable, error) {
	prefix, desc, err := resolver.ResolveMutableType(ctx, p, name, required)
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}

	if desc != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, desc); err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
		tn := tree.MakeTypeNameWithPrefix(prefix.NamePrefix(), desc.GetName())
		name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	}

	return prefix, desc, nil
}

// The versions below are part of the work for #34240.
// TODO(radu): clean these up when everything is switched over.

// See ResolveMutableTableDescriptor.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (catalog.ResolvedObjectPrefix, *tabledesc.Mutable, error) {
	tn := name.ToTableName()
	prefix, table, err := resolver.ResolveMutableExistingTableObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)

	if table != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
	}

	return prefix, table, nil
}

// ResolveMutableTableDescriptorExAllowNoPrimaryKey performs the
// same logic as ResolveMutableTableDescriptorEx but allows for
// the resolved table to not have a primary key.
func (p *planner) ResolveMutableTableDescriptorExAllowNoPrimaryKey(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (catalog.ResolvedObjectPrefix, *tabledesc.Mutable, error) {
	lookupFlags := tree.ObjectLookupFlags{
		Required:               required,
		RequireMutable:         true,
		AllowWithoutPrimaryKey: true,
		DesiredObjectKind:      tree.TableObject,
		DesiredTableDescKind:   requiredType,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil || desc == nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix.NamePrefix(), tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	table := desc.(*tabledesc.Mutable)

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}

	return prefix, table, err
}

// See ResolveUncachedTableDescriptor.
func (p *planner) ResolveUncachedTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (table catalog.TableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = p.ResolveExistingObjectEx(ctx, name, required, requiredType)
	})
	return table, err
}

// See ResolveExistingTableObject.
func (p *planner) ResolveExistingObjectEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (res catalog.TableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		Required:             required,
		AssertNotLeased:      p.skipDescriptorCache,
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil || desc == nil {
		return nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix.NamePrefix(), tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	table := desc.(catalog.TableDescriptor)

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
		return nil, err
	}

	return table, nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) tree.ObjectName {
	return u.Resolved(&p.semaCtx.Annotations)
}

type simpleSchemaResolver interface {
	getDatabaseByID(id descpb.ID) (catalog.DatabaseDescriptor, error)
	getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error)
	getTableByID(id descpb.ID) (catalog.TableDescriptor, error)
}
