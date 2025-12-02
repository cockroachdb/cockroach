// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupresolver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ResolveTargets performs name resolution on a set of targets and returns
// the resulting descriptors using Collection APIs to avoid loading all
// descriptors into memory.
//
// This implementation uses Collection methods to load only the necessary
// descriptors, making it O(targets) instead of O(all_descriptors).
//
// Returns:
//   - []catalog.Descriptor: All resolved descriptors
//   - []descpb.ID: completeDBs - database IDs where all objects are included
//   - []catalog.DatabaseDescriptor: requestedDBs - explicitly requested DATABASE targets
//   - map[tree.TablePattern]catalog.Descriptor: descsByTablePattern - TABLE patterns to descriptors
func ResolveTargets(
	ctx context.Context, p sql.PlanHookState, endTime hlc.Timestamp, targets *tree.BackupTargetList,
) (
	[]catalog.Descriptor,
	[]descpb.ID,
	[]catalog.DatabaseDescriptor,
	map[tree.TablePattern]catalog.Descriptor,
	error,
) {
	var result []catalog.Descriptor
	var completeDBs []descpb.ID
	var requestedDBs []catalog.DatabaseDescriptor
	descsByTablePattern := make(map[tree.TablePattern]catalog.Descriptor)
	seenDescs := make(map[descpb.ID]struct{})

	// Helper to add descriptor if not already seen.
	addDesc := func(desc catalog.Descriptor) {
		if desc == nil {
			return
		}
		if _, seen := seenDescs[desc.GetID()]; !seen {
			result = append(result, desc)
			seenDescs[desc.GetID()] = struct{}{}
		}
	}

	err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		if err := txn.KV().SetFixedTimestamp(ctx, endTime); err != nil {
			return err
		}

		currentDatabase := p.CurrentDatabase()
		searchPath := p.CurrentSearchPath()

		// Create a resolver for name resolution using Collection APIs.
		r := &simpleResolver{col: col, txn: txn.KV()}

		// Helper to get type descriptors referenced by a table.
		getReferencedTypes := func(table catalog.TableDescriptor, db catalog.DatabaseDescriptor) error {
			getTypeByID := func(id descpb.ID) (catalog.TypeDescriptor, error) {
				typ, err := col.ByIDWithLeased(txn.KV()).Get().Type(ctx, id)
				if err != nil {
					return nil, err
				}
				return typ, nil
			}

			typeIDs, _, err := table.GetAllReferencedTypeIDs(db, getTypeByID)
			if err != nil {
				return err
			}
			for _, typeID := range typeIDs {
				typ, err := col.ByIDWithLeased(txn.KV()).Get().Type(ctx, typeID)
				if err != nil {
					return err
				}
				addDesc(typ)
			}
			return nil
		}

		// Helper to get function descriptors referenced by a table.
		getReferencedFunctions := func(table catalog.TableDescriptor) error {
			fnIDs, err := table.GetAllReferencedFunctionIDs()
			if err != nil {
				return err
			}
			for _, fnID := range fnIDs.Ordered() {
				fn, err := col.ByIDWithLeased(txn.KV()).Get().Function(ctx, fnID)
				if err != nil {
					return err
				}
				addDesc(fn)
			}
			return nil
		}

		// Helper to add schema descriptor.
		addSchemaDesc := func(schemaID descpb.ID) error {
			// Skip public schema placeholders.
			if schemaID == keys.PublicSchemaIDForBackup || schemaID == keys.PublicSchemaID {
				return nil
			}
			schema, err := col.ByIDWithLeased(txn.KV()).Get().Schema(ctx, schemaID)
			if err != nil {
				// Schema might not exist or be dropped, skip it.
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					return nil
				}
				return err
			}
			if !schema.Dropped() {
				addDesc(schema)
			}
			return nil
		}

		// Helper to check if a schema is virtual/system (should be skipped).
		isVirtualSchema := func(schema catalog.SchemaDescriptor) bool {
			if schema == nil {
				return false
			}
			return schema.SchemaKind() == catalog.SchemaVirtual
		}

		// Process DATABASE targets.
		for _, dbName := range targets.Databases {
			db, err := col.ByNameWithLeased(txn.KV()).Get().Database(ctx, string(dbName))
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					return errors.Errorf("database %q does not exist", dbName)
				}
				return err
			}
			if !db.Public() {
				return errors.Errorf("database %q does not exist", dbName)
			}
			addDesc(db)

			// Track completeDBs and requestedDBs for DATABASE targets.
			completeDBs = append(completeDBs, db.GetID())
			requestedDBs = append(requestedDBs, db)

			// For database backup, get all objects in the database.
			allInDB, err := col.GetAllInDatabase(ctx, txn.KV(), db)
			if err != nil {
				return err
			}

			err = allInDB.ForEachDescriptor(func(desc catalog.Descriptor) error {
				if desc.Dropped() || (!desc.Public() && !desc.Offline()) {
					return nil
				}

				// Skip virtual tables.
				if table, ok := desc.(catalog.TableDescriptor); ok {
					if table.IsVirtualTable() {
						return nil
					}
					if err := getReferencedTypes(table, db); err != nil {
						return err
					}
					if err := getReferencedFunctions(table); err != nil {
						return err
					}
					if err := addSchemaDesc(table.GetParentSchemaID()); err != nil {
						return err
					}
					addDesc(table)
					return nil
				}

				// Skip virtual schemas.
				if schema, ok := desc.(catalog.SchemaDescriptor); ok {
					if isVirtualSchema(schema) {
						return nil
					}
				}

				addDesc(desc)
				return nil
			})
			if err != nil {
				return err
			}
		}

		// Process TABLE targets.
		for _, origPattern := range targets.Tables.TablePatterns {
			pattern, err := origPattern.NormalizeTablePattern()
			if err != nil {
				return err
			}

			switch p := pattern.(type) {
			case *tree.TableName:
				// Resolve individual table.
				un := p.ToUnresolvedObjectName()

				found, prefix, desc, err := resolver.ResolveExisting(
					ctx, un, r, tree.ObjectLookupFlags{}, currentDatabase, searchPath,
				)
				if err != nil {
					return err
				}
				if !found {
					return errors.Errorf("table %q does not exist", tree.ErrString(p))
				}

				table, ok := desc.(catalog.TableDescriptor)
				if !ok || !table.Public() {
					return errors.Errorf("table %q does not exist", tree.ErrString(p))
				}

				// Add the table and its parent database.
				addDesc(table)
				addDesc(prefix.Database)

				// Add the schema if it's user-defined.
				if err := addSchemaDesc(table.GetParentSchemaID()); err != nil {
					return err
				}

				// Add referenced types and functions.
				if err := getReferencedTypes(table, prefix.Database); err != nil {
					return err
				}
				if err := getReferencedFunctions(table); err != nil {
					return err
				}

				// Track descsByTablePattern for TABLE patterns.
				// Use the original pattern as the key, not the normalized one, to match
				// the behavior of DescriptorsMatchingTargets.
				descsByTablePattern[origPattern] = table

			case *tree.AllTablesSelector:
				// Resolve pattern like db.* or db.schema.*
				// For 2-part patterns like "db.*", the parser treats the first part as a schema name.
				// We need to check if it's actually a database name and handle accordingly.
				var db catalog.DatabaseDescriptor
				var targetSchema catalog.SchemaDescriptor
				isDBWildcard := false

				if p.ExplicitSchema && !p.ExplicitCatalog {
					// This is a 2-part pattern like "my_db.*", but the parser always puts
					// the first part in SchemaName. Check if it's actually a database name.
					potentialDBName := p.ObjectNamePrefix.Schema()
					maybeDB, err := col.ByNameWithLeased(txn.KV()).MaybeGet().Database(ctx, potentialDBName)
					if err != nil {
						return err
					}
					if maybeDB != nil {
						// We found a matching database name, so treat as database wildcard.
						db = maybeDB
						isDBWildcard = true
					}
				}

				if !isDBWildcard {
					// Normal resolution treating it as a schema name.
					found, prefix, err := resolver.ResolveObjectNamePrefix(
						ctx, r, currentDatabase, searchPath, &p.ObjectNamePrefix,
					)
					if err != nil {
						return err
					}
					if !found {
						return sqlerrors.NewInvalidWildcardError(tree.ErrString(p))
					}
					db = prefix.Database
					targetSchema = prefix.Schema
				}

				// Add the database.
				addDesc(db)

				// Track completeDBs for db.* wildcards (all objects in DB are included).
				if isDBWildcard {
					completeDBs = append(completeDBs, db.GetID())
				}

				// Determine if we're scoped to a specific schema.
				hasSchemaScope := p.ExplicitSchema && p.ExplicitCatalog
				// For db.* patterns that we identified as database wildcards, don't scope to schema.
				if isDBWildcard {
					hasSchemaScope = false
				}

				if hasSchemaScope && targetSchema != nil {
					// Skip virtual schemas.
					if isVirtualSchema(targetSchema) {
						continue
					}

					// Get all objects in the specific schema.
					objects, err := col.GetAllObjectsInSchema(ctx, txn.KV(), db, targetSchema)
					if err != nil {
						return err
					}

					// Add the schema itself.
					addDesc(targetSchema)

					err = objects.ForEachDescriptor(func(desc catalog.Descriptor) error {
						if desc.Dropped() || (!desc.Public() && !desc.Offline()) {
							return nil
						}
						// Skip virtual tables.
						if table, ok := desc.(catalog.TableDescriptor); ok {
							if table.IsVirtualTable() {
								return nil
							}
							if err := getReferencedTypes(table, db); err != nil {
								return err
							}
							if err := getReferencedFunctions(table); err != nil {
								return err
							}
							addDesc(table)
							return nil
						}
						addDesc(desc)
						return nil
					})
					if err != nil {
						return err
					}
				} else {
					// Get all objects in the database.
					allInDB, err := col.GetAllInDatabase(ctx, txn.KV(), db)
					if err != nil {
						return err
					}

					err = allInDB.ForEachDescriptor(func(desc catalog.Descriptor) error {
						if desc.Dropped() || (!desc.Public() && !desc.Offline()) {
							return nil
						}
						// Skip virtual tables.
						if table, ok := desc.(catalog.TableDescriptor); ok {
							if table.IsVirtualTable() {
								return nil
							}
							if err := getReferencedTypes(table, db); err != nil {
								return err
							}
							if err := getReferencedFunctions(table); err != nil {
								return err
							}
							if err := addSchemaDesc(table.GetParentSchemaID()); err != nil {
								return err
							}
							addDesc(table)
							return nil
						}
						// Skip virtual schemas.
						if schema, ok := desc.(catalog.SchemaDescriptor); ok {
							if isVirtualSchema(schema) {
								return nil
							}
						}
						addDesc(desc)
						return nil
					})
					if err != nil {
						return err
					}
				}

			default:
				return errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
			}
		}

		return nil
	})

	return result, completeDBs, requestedDBs, descsByTablePattern, err
}

// simpleResolver implements the resolver interfaces needed for name resolution
// using the Collection APIs.
type simpleResolver struct {
	col *descs.Collection
	txn *kv.Txn
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (r *simpleResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, catalog.ResolvedObjectPrefix, error) {
	db, err := r.col.ByNameWithLeased(r.txn).MaybeGet().Database(ctx, dbName)
	if err != nil || db == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}

	schema, err := r.col.ByNameWithLeased(r.txn).MaybeGet().Schema(ctx, db, scName)
	if err != nil || schema == nil {
		// Check for public schema.
		if scName == catconstants.PublicSchemaName {
			if !db.HasPublicSchemaWithDescriptor() {
				// Return a synthetic public schema.
				return true, catalog.ResolvedObjectPrefix{
					Database: db,
					Schema:   schemadesc.GetPublicSchema(),
				}, nil
			}
		}
		return false, catalog.ResolvedObjectPrefix{}, err
	}

	return true, catalog.ResolvedObjectPrefix{
		Database: db,
		Schema:   schema,
	}, nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *simpleResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (bool, catalog.ResolvedObjectPrefix, catalog.Descriptor, error) {
	resolvedPrefix := catalog.ResolvedObjectPrefix{}

	db, err := r.col.ByNameWithLeased(r.txn).MaybeGet().Database(ctx, dbName)
	if err != nil || db == nil {
		return false, resolvedPrefix, nil, err
	}
	resolvedPrefix.Database = db

	schema, err := r.col.ByNameWithLeased(r.txn).MaybeGet().Schema(ctx, db, scName)
	if err != nil {
		return false, resolvedPrefix, nil, err
	}

	// Handle public schema.
	if schema == nil && scName == catconstants.PublicSchemaName {
		if !db.HasPublicSchemaWithDescriptor() {
			schema = schemadesc.GetPublicSchema()
		}
	}

	if schema == nil {
		return false, resolvedPrefix, nil, nil
	}
	resolvedPrefix.Schema = schema

	// Try to find the object (table or type).
	table, err := r.col.ByNameWithLeased(r.txn).MaybeGet().Table(ctx, db, schema, obName)
	if err != nil {
		return false, resolvedPrefix, nil, err
	}
	if table != nil {
		return true, resolvedPrefix, table, nil
	}

	// Try type if not a table.
	typ, err := r.col.ByNameWithLeased(r.txn).MaybeGet().Type(ctx, db, schema, obName)
	if err != nil {
		return false, resolvedPrefix, nil, err
	}
	if typ != nil {
		return true, resolvedPrefix, typ, nil
	}

	return false, resolvedPrefix, nil, nil
}
