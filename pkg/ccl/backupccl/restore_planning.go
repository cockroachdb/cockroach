// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"go/constant"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DescRewriteMap maps old descriptor IDs to new descriptor and parent IDs.
type DescRewriteMap map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite

const (
	restoreOptIntoDB                    = "into_db"
	restoreOptSkipMissingFKs            = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences      = "skip_missing_sequences"
	restoreOptSkipMissingSequenceOwners = "skip_missing_sequence_owners"
	restoreOptSkipMissingViews          = "skip_missing_views"
	restoreOptSkipLocalitiesCheck       = "skip_localities_check"

	// The temporary database system tables will be restored into for full
	// cluster backups.
	restoreTempSystemDB = "crdb_temp_system"
)

// featureRestoreEnabled is used to enable and disable the RESTORE feature.
var featureRestoreEnabled = settings.RegisterBoolSetting(
	"feature.restore.enabled",
	"set to true to enable restore, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

// rewriteViewQueryDBNames rewrites the passed table's ViewQuery replacing all
// non-empty db qualifiers with `newDB`.
//
// TODO: this AST traversal misses tables named in strings (#24556).
func rewriteViewQueryDBNames(table *tabledesc.Mutable, newDB string) error {
	stmt, err := parser.ParseOne(table.ViewQuery)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.Syntax,
			"failed to parse underlying query from view %q", table.Name)
	}
	// Re-format to change all DB names to `newDB`.
	f := tree.NewFmtCtx(
		tree.FmtParsable,
		tree.FmtReformatTableNames(func(ctx *tree.FmtCtx, tn *tree.TableName) {
			// empty catalog e.g. ``"".information_schema.tables` should stay empty.
			if tn.CatalogName != "" {
				tn.CatalogName = tree.Name(newDB)
			}
			ctx.WithReformatTableNames(nil, func() {
				ctx.FormatNode(tn)
			})
		}),
	)
	f.FormatNode(stmt.AST)
	table.ViewQuery = f.CloseAndGetString()
	return nil
}

// rewriteTypesInExpr rewrites all explicit ID type references in the input
// expression string according to rewrites.
func rewriteTypesInExpr(expr string, rewrites DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	ctx := tree.NewFmtCtx(
		tree.FmtSerializable,
		tree.FmtIndexedTypeFormat(func(ctx *tree.FmtCtx, ref *tree.OIDTypeReference) {
			newRef := ref
			var id descpb.ID
			id, err = typedesc.UserDefinedTypeOIDToID(ref.OID)
			if err != nil {
				return
			}
			if rw, ok := rewrites[id]; ok {
				newRef = &tree.OIDTypeReference{OID: typedesc.TypeIDToOID(rw.ID)}
			}
			ctx.WriteString(newRef.SQLString())
		}),
	)
	if err != nil {
		return "", err
	}
	ctx.FormatNode(parsed)
	return ctx.CloseAndGetString(), nil
}

// rewriteSequencesInExpr rewrites all sequence IDs in the input expression
// string according to rewrites.
func rewriteSequencesInExpr(expr string, rewrites DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}
	rewriteFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		id, ok := schemaexpr.GetSeqIDFromExpr(expr)
		if !ok {
			return true, expr, nil
		}
		annotateTypeExpr, ok := expr.(*tree.AnnotateTypeExpr)
		if !ok {
			return true, expr, nil
		}

		rewrite, ok := rewrites[descpb.ID(id)]
		if !ok {
			return true, expr, nil
		}
		annotateTypeExpr.Expr = tree.NewNumVal(
			constant.MakeInt64(int64(rewrite.ID)),
			strconv.Itoa(int(rewrite.ID)),
			false, /* negative */
		)
		return false, annotateTypeExpr, nil
	}

	newExpr, err := tree.SimpleVisit(parsed, rewriteFunc)
	if err != nil {
		return "", err
	}
	return newExpr.String(), nil
}

// rewriteSequencesInView walks the given viewQuery and
// rewrites all sequence IDs in it according to rewrites.
func rewriteSequencesInView(viewQuery string, rewrites DescRewriteMap) (string, error) {
	rewriteFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		id, ok := schemaexpr.GetSeqIDFromExpr(expr)
		if !ok {
			return true, expr, nil
		}
		annotateTypeExpr, ok := expr.(*tree.AnnotateTypeExpr)
		if !ok {
			return true, expr, nil
		}
		rewrite, ok := rewrites[descpb.ID(id)]
		if !ok {
			return true, expr, nil
		}
		annotateTypeExpr.Expr = tree.NewNumVal(
			constant.MakeInt64(int64(rewrite.ID)),
			strconv.Itoa(int(rewrite.ID)),
			false, /* negative */
		)
		return false, annotateTypeExpr, nil
	}

	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return "", err
	}
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, rewriteFunc)
	if err != nil {
		return "", err
	}
	return newStmt.String(), nil
}

// maybeFilterMissingViews filters the set of tables to restore to exclude views
// whose dependencies are either missing or are themselves unrestorable due to
// missing dependencies, and returns the resulting set of tables. If the
// skipMissingViews option is not set, an error is returned if any
// unrestorable views are found.
func maybeFilterMissingViews(
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	skipMissingViews bool,
) (map[descpb.ID]*tabledesc.Mutable, error) {
	// Function that recursively determines whether a given table, if it is a
	// view, has valid dependencies. Dependencies are looked up in tablesByID.
	var hasValidViewDependencies func(desc *tabledesc.Mutable) bool
	hasValidViewDependencies = func(desc *tabledesc.Mutable) bool {
		if !desc.IsView() {
			return true
		}
		for _, id := range desc.DependsOn {
			if depDesc, ok := tablesByID[id]; !ok || !hasValidViewDependencies(depDesc) {
				return false
			}
		}
		for _, id := range desc.DependsOnTypes {
			if _, ok := typesByID[id]; !ok {
				return false
			}
		}
		return true
	}

	filteredTablesByID := make(map[descpb.ID]*tabledesc.Mutable)
	for id, table := range tablesByID {
		if hasValidViewDependencies(table) {
			filteredTablesByID[id] = table
		} else {
			if !skipMissingViews {
				return nil, errors.Errorf(
					"cannot restore view %q without restoring referenced table (or %q option)",
					table.Name, restoreOptSkipMissingViews,
				)
			}
		}
	}
	return filteredTablesByID, nil
}

func synthesizePGTempSchema(
	ctx context.Context, p sql.PlanHookState, schemaName string,
) (descpb.ID, descpb.ID, error) {
	var synthesizedSchemaID descpb.ID
	var defaultDBID descpb.ID
	err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		defaultDBID, err = lookupDatabaseID(ctx, txn, p.ExecCfg().Codec,
			catalogkeys.DefaultDatabaseName)
		if err != nil {
			return err
		}

		sKey := catalogkeys.NewNameKeyComponents(defaultDBID, keys.RootNamespaceID, schemaName)
		schemaID, err := catalogkv.GetDescriptorID(ctx, txn, p.ExecCfg().Codec, sKey)
		if err != nil {
			return err
		}
		if schemaID != descpb.InvalidID {
			return errors.Newf("attempted to synthesize temp schema during RESTORE but found"+
				" another schema already using the same schema key %s", sKey.GetName())
		}
		synthesizedSchemaID, err = catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return err
		}
		return p.CreateSchemaNamespaceEntry(ctx, catalogkeys.EncodeNameKey(p.ExecCfg().Codec, sKey), synthesizedSchemaID)
	})

	return synthesizedSchemaID, defaultDBID, err
}

// dbSchemaKey is used when generating fake pg_temp schemas for the purpose of
// restoring temporary objects. Detailed comments can be found where it is being
// used.
type dbSchemaKey struct {
	parentID descpb.ID
	schemaID descpb.ID
}

// allocateDescriptorRewrites determines the new ID and parentID (a "DescriptorRewrite")
// for each table in sqlDescs and returns a mapping from old ID to said
// DescriptorRewrite. It first validates that the provided sqlDescs can be restored
// into their original database (or the database specified in opts) to avoid
// leaking table IDs if we can be sure the restore would fail.
func allocateDescriptorRewrites(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	schemasByID map[descpb.ID]*schemadesc.Mutable,
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	restoreDBs []catalog.DatabaseDescriptor,
	descriptorCoverage tree.DescriptorCoverage,
	opts tree.RestoreOptions,
	intoDB string,
) (DescRewriteMap, error) {
	descriptorRewrites := make(DescRewriteMap)
	var overrideDB string
	var renaming bool
	if opts.IntoDB != nil {
		overrideDB = intoDB
		renaming = true
	}

	restoreDBNames := make(map[string]catalog.DatabaseDescriptor, len(restoreDBs))
	for _, db := range restoreDBs {
		restoreDBNames[db.GetName()] = db
	}

	if len(restoreDBNames) > 0 && renaming {
		return nil, errors.Errorf("cannot use %q option when restoring database(s)", restoreOptIntoDB)
	}

	// The logic at the end of this function leaks table IDs, so fail fast if
	// we can be certain the restore will fail.

	// Fail fast if the tables to restore are incompatible with the specified
	// options.
	maxDescIDInBackup := int64(keys.MinNonPredefinedUserDescID)
	for _, table := range tablesByID {
		if int64(table.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(table.ID)
		}
		// Check that foreign key targets exist.
		for i := range table.OutboundFKs {
			fk := &table.OutboundFKs[i]
			if _, ok := tablesByID[fk.ReferencedTableID]; !ok {
				if !opts.SkipMissingFKs {
					return nil, errors.Errorf(
						"cannot restore table %q without referenced table %d (or %q option)",
						table.Name, fk.ReferencedTableID, restoreOptSkipMissingFKs,
					)
				}
			}
		}

		// Check that referenced sequences exist.
		for i := range table.Columns {
			col := &table.Columns[i]
			// Ensure that all referenced types are present.
			if col.Type.UserDefined() {
				// TODO (rohany): This can be turned into an option later.
				id, err := typedesc.GetUserDefinedTypeDescID(col.Type)
				if err != nil {
					return nil, err
				}
				if _, ok := typesByID[id]; !ok {
					return nil, errors.Errorf(
						"cannot restore table %q without referenced type %d",
						table.Name,
						id,
					)
				}
			}
			for _, seqID := range col.UsesSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if !opts.SkipMissingSequences {
						return nil, errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequences,
						)
					}
				}
			}
			for _, seqID := range col.OwnsSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if !opts.SkipMissingSequenceOwners {
						return nil, errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequenceOwners)
					}
				}
			}
		}

		// Handle sequence ownership dependencies.
		if table.IsSequence() && table.SequenceOpts.HasOwner() {
			if _, ok := tablesByID[table.SequenceOpts.SequenceOwner.OwnerTableID]; !ok {
				if !opts.SkipMissingSequenceOwners {
					return nil, errors.Errorf(
						"cannot restore sequence %q without referenced owner table %d (or %q option)",
						table.Name,
						table.SequenceOpts.SequenceOwner.OwnerTableID,
						restoreOptSkipMissingSequenceOwners,
					)
				}
			}
		}
	}

	// Include the database descriptors when calculating the max ID.
	for _, database := range databasesByID {
		if int64(database.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(database.ID)
		}
	}

	// Include the type descriptors when calculating the max ID.
	for _, typ := range typesByID {
		if int64(typ.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(typ.ID)
		}
	}

	// Include the schema descriptors when calculating the max ID.
	for _, sc := range schemasByID {
		if int64(sc.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(sc.ID)
		}
	}

	needsNewParentIDs := make(map[string][]descpb.ID)

	// Increment the DescIDSequenceKey so that it is higher than the max desc ID
	// in the backup. This generator keeps produced the next descriptor ID.
	var tempSysDBID descpb.ID
	if descriptorCoverage == tree.AllDescriptors {
		var err error
		// Restore the key which generates descriptor IDs.
		if err = p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			// N.B. This key is usually mutated using the Inc command. That
			// command warns that if the key was every Put directly, Inc will
			// return an error. This is only to ensure that the type of the key
			// doesn't change. Here we just need to be very careful that we only
			// write int64 values.
			// The generator's value should be set to the value of the next ID
			// to generate.
			b.Put(p.ExecCfg().Codec.DescIDSequenceKey(), maxDescIDInBackup+1)
			return txn.Run(ctx, b)
		}); err != nil {
			return nil, err
		}
		tempSysDBID, err = catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		// Remap all of the descriptor belonging to system tables to the temp system
		// DB.
		descriptorRewrites[tempSysDBID] = &jobspb.RestoreDetails_DescriptorRewrite{ID: tempSysDBID}
		for _, table := range tablesByID {
			if table.GetParentID() == systemschema.SystemDB.GetID() {
				descriptorRewrites[table.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: tempSysDBID}
			}
		}
		for _, sc := range typesByID {
			if sc.GetParentID() == systemschema.SystemDB.GetID() {
				descriptorRewrites[sc.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: tempSysDBID}
			}
		}
		for _, typ := range typesByID {
			if typ.GetParentID() == systemschema.SystemDB.GetID() {
				descriptorRewrites[typ.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: tempSysDBID}
			}
		}

		// When restoring a temporary object, the parent schema which the descriptor
		// is originally pointing to is never part of the BACKUP. This is because
		// the "pg_temp" schema in which temporary objects are created is not
		// represented as a descriptor and thus is not picked up during a full
		// cluster BACKUP.
		// To overcome this orphaned schema pointer problem, when restoring a
		// temporary object we create a "fake" pg_temp schema in defaultdb and add
		// it to the namespace table. We then remap the temporary object descriptors
		// to point to this schema. This allows us to piggy back on the temporary
		// reconciliation job which looks for "pg_temp" schemas linked to temporary
		// sessions and properly cleans up the temporary objects in it.
		haveSynthesizedTempSchema := make(map[dbSchemaKey]bool)
		var defaultDBID descpb.ID
		var synthesizedTempSchemaCount int
		for _, table := range tablesByID {
			if table.IsTemporary() {
				// We generate a "fake" temporary schema for every unique
				// <dbID,schemaID> tuple of the backed-up temporary table descriptors.
				// This is important because post rewrite all the "fake" schemas and
				// consequently temp table objects are going to be in defaultdb. Placing
				// them under different "fake" schemas prevents name collisions if the
				// backed up tables had the same names but were in different temp
				// schemas/databases in the cluster which was backed up.
				dbSchemaIDKey := dbSchemaKey{parentID: table.GetParentID(),
					schemaID: table.GetParentSchemaID()}
				if _, ok := haveSynthesizedTempSchema[dbSchemaIDKey]; !ok {
					var synthesizedSchemaID descpb.ID
					var err error
					// NB: TemporarySchemaNameForRestorePrefix is a special value that has
					// been chosen to trick the reconciliation job into performing our
					// cleanup for us. The reconciliation job strips the "pg_temp" prefix
					// and parses the remainder of the string into a session ID. It then
					// checks the session ID against its active sessions, and performs
					// cleanup on the inactive ones.
					// We reserve the high bit to be 0 so as to never collide with an
					// actual session ID as normally the high bit is the hlc.Timestamp at
					// which the cluster was started.
					schemaName := sql.TemporarySchemaNameForRestorePrefix +
						strconv.Itoa(synthesizedTempSchemaCount)
					synthesizedSchemaID, defaultDBID, err = synthesizePGTempSchema(ctx, p, schemaName)
					if err != nil {
						return nil, err
					}
					// Write a schema descriptor rewrite so that we can remap all
					// temporary table descs which were under the original session
					// specific pg_temp schema to point to this synthesized schema when we
					// are performing the table rewrites.
					descriptorRewrites[table.GetParentSchemaID()] = &jobspb.RestoreDetails_DescriptorRewrite{ID: synthesizedSchemaID}
					haveSynthesizedTempSchema[dbSchemaIDKey] = true
					synthesizedTempSchemaCount++
				}

				// Remap the temp table descriptors to belong to the defaultdb where we
				// have synthesized the temp schema.
				descriptorRewrites[table.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: defaultDBID}
			}
		}
	}

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			found, _, err := catalogkv.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, name)
			if err != nil {
				return err
			}
			if found {
				return errors.Errorf("database %q already exists", name)
			}
		}

		// TODO (rohany, pbardea): These checks really need to be refactored.
		// Construct rewrites for any user defined schemas.
		for _, sc := range schemasByID {
			if _, ok := descriptorRewrites[sc.ID]; ok {
				continue
			}

			targetDB, err := resolveTargetDB(ctx, txn, p, databasesByID, renaming, overrideDB,
				descriptorCoverage, sc)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], sc.ID)
			} else {
				// Look up the parent database's ID.
				found, parentID, err := catalogkv.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, targetDB)
				if err != nil {
					return err
				}
				if !found {
					return errors.Errorf("a database named %q needs to exist to restore schema %q",
						targetDB, sc.Name)
				}
				// Check privileges on the parent DB.
				parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, p.ExecCfg().Codec, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}
				if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
					return err
				}

				// See if there is an existing schema with the same name.
				found, id, err := catalogkv.LookupObjectID(ctx, txn, p.ExecCfg().Codec, parentID, keys.RootNamespaceID, sc.Name)
				if err != nil {
					return err
				}
				if !found {
					// If we didn't find a matching schema, then we'll restore this schema.
					descriptorRewrites[sc.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}
				} else {
					// If we found an existing schema, then we need to remap all references
					// to this schema to the existing one.
					desc, err := catalogkv.MustGetSchemaDescByID(ctx, txn, p.ExecCfg().Codec, id)
					if err != nil {
						return err
					}
					descriptorRewrites[sc.ID] = &jobspb.RestoreDetails_DescriptorRewrite{
						ParentID:   desc.GetParentID(),
						ID:         desc.GetID(),
						ToExisting: true,
					}
				}
			}
		}

		for _, table := range tablesByID {
			// If a descriptor has already been assigned a rewrite, then move on.
			if _, ok := descriptorRewrites[table.ID]; ok {
				continue
			}

			targetDB, err := resolveTargetDB(ctx, txn, p, databasesByID, renaming, overrideDB,
				descriptorCoverage, table)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], table.ID)
			} else if descriptorCoverage == tree.AllDescriptors {
				// Set the remapped ID to the original parent ID, except for system tables which
				// should be RESTOREd to the temporary system database.
				if targetDB != restoreTempSystemDB {
					descriptorRewrites[table.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: table.ParentID}
				}
			} else {
				var parentID descpb.ID
				{
					found, newParentID, err := catalogkv.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, targetDB)
					if err != nil {
						return err
					}
					if !found {
						return errors.Errorf("a database named %q needs to exist to restore table %q",
							targetDB, table.Name)
					}
					parentID = newParentID
				}
				// Check that the table name is _not_ in use.
				// This would fail the CPut later anyway, but this yields a prettier error.
				tableName := tree.NewUnqualifiedTableName(tree.Name(table.GetName()))
				err := catalogkv.CheckObjectCollision(ctx, txn, p.ExecCfg().Codec, parentID, table.GetParentSchemaID(), tableName)
				if err != nil {
					return err
				}

				// Check privileges.
				parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, p.ExecCfg().Codec, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}
				if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
					return err
				}

				if parentDB.IsMultiRegion() && table.GetLocalityConfig() != nil {
					// We're restoring a table and not its parent database. We may block
					// restoring multi-region tables to multi-region databases since regions
					// may mismatch.
					if err := checkMultiRegionCompatible(ctx, txn, p.ExecCfg().Codec, table, parentDB); err != nil {
						return pgerror.WithCandidateCode(err, pgcode.FeatureNotSupported)
					}
				}
				// Create the table rewrite with the new parent ID. We've done all the
				// up-front validation that we can.
				descriptorRewrites[table.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}
			}
		}

		// Iterate through typesByID to construct a remapping entry for each type.
		for _, typ := range typesByID {
			// If a descriptor has already been assigned a rewrite, then move on.
			if _, ok := descriptorRewrites[typ.ID]; ok {
				continue
			}

			targetDB, err := resolveTargetDB(ctx, txn, p, databasesByID, renaming, overrideDB,
				descriptorCoverage, typ)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], typ.ID)
			} else {
				// The remapping logic for a type will perform the remapping for a type's
				// array type, so don't perform this logic for the array type itself.
				if typ.Kind == descpb.TypeDescriptor_ALIAS {
					continue
				}

				// Look up the parent database's ID.
				found, parentID, err := catalogkv.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, targetDB)
				if err != nil {
					return err
				}
				if !found {
					return errors.Errorf("a database named %q needs to exist to restore type %q",
						targetDB, typ.Name)
				}
				// Check privileges on the parent DB.
				parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, p.ExecCfg().Codec, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}

				// See if there is an existing type with the same name.
				desc, err := catalogkv.GetDescriptorCollidingWithObject(
					ctx,
					txn,
					p.ExecCfg().Codec,
					parentID,
					typ.GetParentSchemaID(),
					typ.Name,
				)
				if err != nil {
					return err
				}
				if desc == nil {
					// If we didn't find a type with the same name, then mark that we
					// need to create the type.

					// Ensure that the user has the correct privilege to create types.
					if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
						return err
					}

					// Create a rewrite entry for the type.
					descriptorRewrites[typ.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}

					// Ensure that there isn't a collision with the array type name.
					arrTyp := typesByID[typ.ArrayTypeID]
					typeName := tree.NewUnqualifiedTypeName(arrTyp.GetName())
					err := catalogkv.CheckObjectCollision(ctx, txn, p.ExecCfg().Codec, parentID, typ.GetParentSchemaID(), typeName)
					if err != nil {
						return errors.Wrapf(err, "name collision for %q's array type", typ.Name)
					}
					// Create the rewrite entry for the array type as well.
					descriptorRewrites[arrTyp.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}
				} else {
					// If there was a name collision, we'll try to see if we can remap
					// this type to the type existing in the cluster.

					// If the collided object isn't a type, then error out.
					existingType, isType := desc.(catalog.TypeDescriptor)
					if !isType {
						return sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), typ.Name)
					}

					// Check if the collided type is compatible to be remapped to.
					if err := typ.IsCompatibleWith(existingType); err != nil {
						return errors.Wrapf(
							err,
							"%q is not compatible with type %q existing in cluster",
							existingType.GetName(),
							existingType.GetName(),
						)
					}

					// Remap both the type and its array type since they are compatible
					// with the type existing in the cluster.
					descriptorRewrites[typ.ID] = &jobspb.RestoreDetails_DescriptorRewrite{
						ParentID:   existingType.GetParentID(),
						ID:         existingType.GetID(),
						ToExisting: true,
					}
					descriptorRewrites[typ.ArrayTypeID] = &jobspb.RestoreDetails_DescriptorRewrite{
						ParentID:   existingType.GetParentID(),
						ID:         existingType.GetArrayTypeID(),
						ToExisting: true,
					}
				}
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	// Allocate new IDs for each database and table.
	//
	// NB: we do this in a standalone transaction, not one that covers the
	// entire restore since restarts would be terrible (and our bulk import
	// primitive are non-transactional), but this does mean if something fails
	// during restore we've "leaked" the IDs, in that the generator will have
	// been incremented.
	//
	// NB: The ordering of the new IDs must be the same as the old ones,
	// otherwise the keys may sort differently after they're rekeyed. We could
	// handle this by chunking the AddSSTable calls more finely in Import, but
	// it would be a big performance hit.

	for _, db := range restoreDBs {
		var newID descpb.ID
		var err error
		if descriptorCoverage == tree.AllDescriptors {
			newID = db.GetID()
		} else {
			newID, err = catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
			if err != nil {
				return nil, err
			}
		}

		descriptorRewrites[db.GetID()] = &jobspb.RestoreDetails_DescriptorRewrite{ID: newID}
		for _, tableID := range needsNewParentIDs[db.GetName()] {
			descriptorRewrites[tableID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: newID}
		}
	}

	// descriptorsToRemap usually contains all tables that are being restored. In a
	// full cluster restore this should only include the system tables that need
	// to be remapped to the temporary table. All other tables in a full cluster
	// backup should have the same ID as they do in the backup.
	descriptorsToRemap := make([]catalog.Descriptor, 0, len(tablesByID))
	for _, table := range tablesByID {
		if descriptorCoverage == tree.AllDescriptors {
			if table.ParentID == systemschema.SystemDB.GetID() {
				// This is a system table that should be marked for descriptor creation.
				descriptorsToRemap = append(descriptorsToRemap, table)
			} else {
				// This table does not need to be remapped.
				descriptorRewrites[table.ID].ID = table.ID
			}
		} else {
			descriptorsToRemap = append(descriptorsToRemap, table)
		}
	}

	// Update the remapping information for type descriptors.
	for _, typ := range typesByID {
		if descriptorCoverage == tree.AllDescriptors {
			// The type doesn't need to be remapped.
			descriptorRewrites[typ.ID].ID = typ.ID
		} else {
			// If the type is marked to be remapped to an existing type in the
			// cluster, then we don't want to generate an ID for it.
			if !descriptorRewrites[typ.ID].ToExisting {
				descriptorsToRemap = append(descriptorsToRemap, typ)
			}
		}
	}

	// Update remapping information for schema descriptors.
	for _, sc := range schemasByID {
		if descriptorCoverage == tree.AllDescriptors {
			// The schema doesn't need to be remapped.
			descriptorRewrites[sc.ID].ID = sc.ID
		} else {
			// If this schema isn't being remapped to an existing schema, then
			// request to generate an ID for it.
			if !descriptorRewrites[sc.ID].ToExisting {
				descriptorsToRemap = append(descriptorsToRemap, sc)
			}
		}
	}

	sort.Sort(catalog.Descriptors(descriptorsToRemap))

	// Generate new IDs for the tables that need to be remapped.
	for _, desc := range descriptorsToRemap {
		newTableID, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		descriptorRewrites[desc.GetID()].ID = newTableID
	}

	return descriptorRewrites, nil
}

func resolveTargetDB(
	ctx context.Context,
	txn *kv.Txn,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	renaming bool,
	overrideDB string,
	descriptorCoverage tree.DescriptorCoverage,
	descriptor catalog.Descriptor,
) (string, error) {
	if renaming {
		return overrideDB, nil
	}

	if descriptorCoverage == tree.AllDescriptors && descriptor.GetParentID() < catalogkeys.MaxDefaultDescriptorID {
		// This is a table that is in a database that already existed at
		// cluster creation time.
		defaultDBID, err := lookupDatabaseID(ctx, txn, p.ExecCfg().Codec, catalogkeys.DefaultDatabaseName)
		if err != nil {
			return "", err
		}
		postgresDBID, err := lookupDatabaseID(ctx, txn, p.ExecCfg().Codec, catalogkeys.PgDatabaseName)
		if err != nil {
			return "", err
		}

		var targetDB string
		if descriptor.GetParentID() == systemschema.SystemDB.GetID() {
			// For full cluster backups, put the system tables in the temporary
			// system table.
			targetDB = restoreTempSystemDB
		} else if descriptor.GetParentID() == defaultDBID {
			targetDB = catalogkeys.DefaultDatabaseName
		} else if descriptor.GetParentID() == postgresDBID {
			targetDB = catalogkeys.PgDatabaseName
		}
		return targetDB, nil
	}

	database, ok := databasesByID[descriptor.GetParentID()]
	if !ok {
		return "", errors.Errorf("no database with ID %d in backup for object %q (%d)",
			descriptor.GetParentID(), descriptor.GetName(), descriptor.GetID())
	}
	return database.Name, nil
}

// maybeUpgradeDescriptors performs post-deserialization upgrades on the
// descriptors.
//
// This is done, for instance, to use the newer 19.2-style foreign key
// representation, if they are not already upgraded.
//
// if skipFKsWithNoMatchingTable is set, FKs whose "other" table is missing from
// the set provided are omitted during the upgrade, instead of causing an error
// to be returned.
func maybeUpgradeDescriptors(
	ctx context.Context, descs []catalog.Descriptor, skipFKsWithNoMatchingTable bool,
) error {
	descGetter := catalog.MakeMapDescGetter()

	// Populate the catalog.DescGetter with all table descriptors in the backup.
	for _, desc := range descs {
		descGetter.Descriptors[desc.GetID()] = desc
	}

	for j, desc := range descs {
		var b catalog.DescriptorBuilder
		if tableDesc, isTable := desc.(catalog.TableDescriptor); isTable {
			b = tabledesc.NewBuilderForFKUpgrade(tableDesc.TableDesc(), skipFKsWithNoMatchingTable)
		} else {
			b = catalogkv.NewBuilder(desc.DescriptorProto())
		}
		err := b.RunPostDeserializationChanges(ctx, descGetter)
		if err != nil {
			return err
		}
		descs[j] = b.BuildExistingMutable()
	}
	return nil
}

// maybeUpgradeDescriptorsInBackupManifests updates the descriptors in the
// manifests. This is done in particular to use the newer 19.2-style foreign
// key representation, if they are not already upgraded.
// This requires resolving cross-table FK references, which is done by looking
// up all table descriptors across all backup descriptors provided.
// If skipFKsWithNoMatchingTable is set, FKs whose
// "other" table is missing from the set provided are omitted during the
// upgrade, instead of causing an error to be returned.
func maybeUpgradeDescriptorsInBackupManifests(
	ctx context.Context, backupManifests []BackupManifest, skipFKsWithNoMatchingTable bool,
) error {
	if len(backupManifests) == 0 {
		return nil
	}
	descs := make([]catalog.Descriptor, 0, len(backupManifests[0].Descriptors))
	for _, backupManifest := range backupManifests {
		for _, pb := range backupManifest.Descriptors {
			descs = append(descs, catalogkv.NewBuilder(&pb).BuildExistingMutable())
		}
	}

	err := maybeUpgradeDescriptors(ctx, descs, skipFKsWithNoMatchingTable)
	if err != nil {
		return err
	}

	k := 0
	for i := range backupManifests {
		manifest := &backupManifests[i]
		for j := range manifest.Descriptors {
			manifest.Descriptors[j] = *descs[k].DescriptorProto()
			k++
		}
	}
	return nil
}

// rewriteDatabaseDescs rewrites all ID's in the input slice of
// DatabaseDescriptors using the input ID rewrite mapping.
func rewriteDatabaseDescs(databases []*dbdesc.Mutable, descriptorRewrites DescRewriteMap) error {
	for _, db := range databases {
		rewrite, ok := descriptorRewrites[db.ID]
		if !ok {
			return errors.Errorf("missing rewrite for database %d", db.ID)
		}
		db.ID = rewrite.ID

		db.Version = 1
		db.ModificationTime = hlc.Timestamp{}

		// Rewrite the name-to-ID mapping for the database's child schemas.
		newSchemas := make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
		for schemaName, schemaInfo := range db.Schemas {
			if schemaInfo.Dropped {
				continue
			}
			rewrite, ok := descriptorRewrites[schemaInfo.ID]
			if !ok {
				return errors.Errorf("missing rewrite for schema %d", db.ID)
			}
			newSchemas[schemaName] = descpb.DatabaseDescriptor_SchemaInfo{ID: rewrite.ID}
		}
		db.Schemas = newSchemas
	}
	return nil
}

// rewriteIDsInTypesT rewrites all ID's in the input types.T using the input
// ID rewrite mapping.
func rewriteIDsInTypesT(typ *types.T, descriptorRewrites DescRewriteMap) error {
	if !typ.UserDefined() {
		return nil
	}
	tid, err := typedesc.GetUserDefinedTypeDescID(typ)
	if err != nil {
		return err
	}
	// Collect potential new OID values.
	var newOID, newArrayOID oid.Oid
	if rw, ok := descriptorRewrites[tid]; ok {
		newOID = typedesc.TypeIDToOID(rw.ID)
	}
	if typ.Family() != types.ArrayFamily {
		tid, err = typedesc.GetUserDefinedArrayTypeDescID(typ)
		if err != nil {
			return err
		}
		if rw, ok := descriptorRewrites[tid]; ok {
			newArrayOID = typedesc.TypeIDToOID(rw.ID)
		}
	}
	types.RemapUserDefinedTypeOIDs(typ, newOID, newArrayOID)
	// If the type is an array, then we need to rewrite the element type as well.
	if typ.Family() == types.ArrayFamily {
		if err := rewriteIDsInTypesT(typ.ArrayContents(), descriptorRewrites); err != nil {
			return err
		}
	}

	return nil
}

// rewriteTypeDescs rewrites all ID's in the input slice of TypeDescriptors
// using the input ID rewrite mapping.
func rewriteTypeDescs(types []*typedesc.Mutable, descriptorRewrites DescRewriteMap) error {
	for _, typ := range types {
		rewrite, ok := descriptorRewrites[typ.ID]
		if !ok {
			return errors.Errorf("missing rewrite for type %d", typ.ID)
		}
		// Reset the version and modification time on this new descriptor.
		typ.Version = 1
		typ.ModificationTime = hlc.Timestamp{}

		typ.ID = rewrite.ID
		typ.ParentSchemaID = maybeRewriteSchemaID(typ.ParentSchemaID, descriptorRewrites,
			false /* isTemporaryDesc */)
		typ.ParentID = rewrite.ParentID
		for i := range typ.ReferencingDescriptorIDs {
			id := typ.ReferencingDescriptorIDs[i]
			if rw, ok := descriptorRewrites[id]; ok {
				typ.ReferencingDescriptorIDs[i] = rw.ID
			}
		}
		switch t := typ.Kind; t {
		case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
			if rw, ok := descriptorRewrites[typ.ArrayTypeID]; ok {
				typ.ArrayTypeID = rw.ID
			}
		case descpb.TypeDescriptor_ALIAS:
			// We need to rewrite any ID's present in the aliased types.T.
			if err := rewriteIDsInTypesT(typ.Alias, descriptorRewrites); err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unknown type kind %s", t.String())
		}
	}
	return nil
}

// rewriteSchemaDescs rewrites all ID's in the input slice of SchemaDescriptors
// using the input ID rewrite mapping.
func rewriteSchemaDescs(schemas []*schemadesc.Mutable, descriptorRewrites DescRewriteMap) error {
	for _, sc := range schemas {
		rewrite, ok := descriptorRewrites[sc.ID]
		if !ok {
			return errors.Errorf("missing rewrite for schema %d", sc.ID)
		}
		// Reset the version and modification time on this new descriptor.
		sc.Version = 1
		sc.ModificationTime = hlc.Timestamp{}

		sc.ID = rewrite.ID
		sc.ParentID = rewrite.ParentID
	}
	return nil
}

func maybeRewriteSchemaID(
	curSchemaID descpb.ID, descriptorRewrites DescRewriteMap, isTemporaryDesc bool,
) descpb.ID {
	// If the current schema is the public schema, then don't attempt to
	// do any rewriting.
	if curSchemaID == keys.PublicSchemaID && !isTemporaryDesc {
		return curSchemaID
	}
	rw, ok := descriptorRewrites[curSchemaID]
	if !ok {
		return curSchemaID
	}
	return rw.ID
}

// RewriteTableDescs mutates tables to match the ID and privilege specified
// in descriptorRewrites, as well as adjusting cross-table references to use the
// new IDs. overrideDB can be specified to set database names in views.
func RewriteTableDescs(
	tables []*tabledesc.Mutable, descriptorRewrites DescRewriteMap, overrideDB string,
) error {
	for _, table := range tables {
		tableRewrite, ok := descriptorRewrites[table.ID]
		if !ok {
			return errors.Errorf("missing table rewrite for table %d", table.ID)
		}
		// Reset the version and modification time on this new descriptor.
		table.Version = 1
		table.ModificationTime = hlc.Timestamp{}

		if table.IsView() && overrideDB != "" {
			// restore checks that all dependencies are also being restored, but if
			// the restore is overriding the destination database, qualifiers in the
			// view query string may be wrong. Since the destination override is
			// applied to everything being restored, anything the view query
			// references will be in the override DB post-restore, so all database
			// qualifiers in the view query should be replaced with overrideDB.
			if err := rewriteViewQueryDBNames(table, overrideDB); err != nil {
				return err
			}
		}

		table.ID = tableRewrite.ID
		table.UnexposedParentSchemaID = maybeRewriteSchemaID(table.GetParentSchemaID(),
			descriptorRewrites, table.IsTemporary())
		table.ParentID = tableRewrite.ParentID

		// Remap type IDs and sequence IDs in all serialized expressions within the TableDescriptor.
		// TODO (rohany): This needs tests once partial indexes are ready.
		if err := tabledesc.ForEachExprStringInTableDesc(table, func(expr *string) error {
			newExpr, err := rewriteTypesInExpr(*expr, descriptorRewrites)
			if err != nil {
				return err
			}
			*expr = newExpr

			newExpr, err = rewriteSequencesInExpr(*expr, descriptorRewrites)
			if err != nil {
				return err
			}
			*expr = newExpr
			return nil
		}); err != nil {
			return err
		}

		// Walk view query and remap sequence IDs.
		if table.IsView() {
			viewQuery, err := rewriteSequencesInView(table.ViewQuery, descriptorRewrites)
			if err != nil {
				return err
			}
			table.ViewQuery = viewQuery
		}

		if err := catalog.ForEachNonDropIndex(table, func(indexI catalog.Index) error {
			index := indexI.IndexDesc()
			// Verify that for any interleaved index being restored, the interleave
			// parent is also being restored. Otherwise, the interleave entries in the
			// restored IndexDescriptors won't have anything to point to.
			// TODO(dan): It seems like this restriction could be lifted by restoring
			// stub TableDescriptors for the missing interleave parents.
			for j, a := range index.Interleave.Ancestors {
				ancestorRewrite, ok := descriptorRewrites[a.TableID]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave parent %d", table.Name, a.TableID,
					)
				}
				index.Interleave.Ancestors[j].TableID = ancestorRewrite.ID
			}
			for j, c := range index.InterleavedBy {
				childRewrite, ok := descriptorRewrites[c.Table]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave child table %d", table.Name, c.Table,
					)
				}
				index.InterleavedBy[j].Table = childRewrite.ID
			}
			return nil
		}); err != nil {
			return err
		}

		// TODO(lucy): deal with outbound foreign key mutations here as well.
		origFKs := table.OutboundFKs
		table.OutboundFKs = nil
		for i := range origFKs {
			fk := &origFKs[i]
			to := fk.ReferencedTableID
			if indexRewrite, ok := descriptorRewrites[to]; ok {
				fk.ReferencedTableID = indexRewrite.ID
				fk.OriginTableID = tableRewrite.ID
			} else {
				// If indexRewrite doesn't exist, the user has specified
				// restoreOptSkipMissingFKs. Error checking in the case the user hasn't has
				// already been done in allocateDescriptorRewrites.
				continue
			}

			// TODO(dt): if there is an existing (i.e. non-restoring) table with
			// a db and name matching the one the FK pointed to at backup, should
			// we update the FK to point to it?
			table.OutboundFKs = append(table.OutboundFKs, *fk)
		}

		origInboundFks := table.InboundFKs
		table.InboundFKs = nil
		for i := range origInboundFks {
			ref := &origInboundFks[i]
			if refRewrite, ok := descriptorRewrites[ref.OriginTableID]; ok {
				ref.ReferencedTableID = tableRewrite.ID
				ref.OriginTableID = refRewrite.ID
				table.InboundFKs = append(table.InboundFKs, *ref)
			}
		}

		for i, dest := range table.DependsOn {
			if depRewrite, ok := descriptorRewrites[dest]; ok {
				table.DependsOn[i] = depRewrite.ID
			} else {
				// Views with missing dependencies should have been filtered out
				// or have caused an error in maybeFilterMissingViews().
				return errors.AssertionFailedf(
					"cannot restore %q because referenced table %d was not found",
					table.Name, dest)
			}
		}
		for i, dest := range table.DependsOnTypes {
			if depRewrite, ok := descriptorRewrites[dest]; ok {
				table.DependsOnTypes[i] = depRewrite.ID
			} else {
				// Views with missing dependencies should have been filtered out
				// or have caused an error in maybeFilterMissingViews().
				return errors.AssertionFailedf(
					"cannot restore %q because referenced type %d was not found",
					table.Name, dest)
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if refRewrite, ok := descriptorRewrites[ref.ID]; ok {
				ref.ID = refRewrite.ID
				table.DependedOnBy = append(table.DependedOnBy, ref)
			}
		}

		if table.IsSequence() && table.SequenceOpts.HasOwner() {
			if ownerRewrite, ok := descriptorRewrites[table.SequenceOpts.SequenceOwner.OwnerTableID]; ok {
				table.SequenceOpts.SequenceOwner.OwnerTableID = ownerRewrite.ID
			} else {
				// The sequence's owner table is not being restored, thus we simply
				// remove the ownership dependency. To get here, the user must have
				// specified 'skip_missing_sequence_owners', otherwise we would have
				// errored out in allocateDescriptorRewrites.
				table.SequenceOpts.SequenceOwner = descpb.TableDescriptor_SequenceOpts_SequenceOwner{}
			}
		}

		// rewriteCol is a closure that performs the ID rewrite logic on a column.
		rewriteCol := func(col *descpb.ColumnDescriptor) error {
			// Rewrite the types.T's IDs present in the column.
			if err := rewriteIDsInTypesT(col.Type, descriptorRewrites); err != nil {
				return err
			}
			var newUsedSeqRefs []descpb.ID
			for _, seqID := range col.UsesSequenceIds {
				if rewrite, ok := descriptorRewrites[seqID]; ok {
					newUsedSeqRefs = append(newUsedSeqRefs, rewrite.ID)
				} else {
					// The referenced sequence isn't being restored.
					// Strip the DEFAULT expression and sequence references.
					// To get here, the user must have specified 'skip_missing_sequences' --
					// otherwise, would have errored out in allocateDescriptorRewrites.
					newUsedSeqRefs = []descpb.ID{}
					col.DefaultExpr = nil
					break
				}
			}
			col.UsesSequenceIds = newUsedSeqRefs

			var newOwnedSeqRefs []descpb.ID
			for _, seqID := range col.OwnsSequenceIds {
				// We only add the sequence ownership dependency if the owned sequence
				// is being restored.
				// If the owned sequence is not being restored, the user must have
				// specified 'skip_missing_sequence_owners' to get here, otherwise
				// we would have errored out in allocateDescriptorRewrites.
				if rewrite, ok := descriptorRewrites[seqID]; ok {
					newOwnedSeqRefs = append(newOwnedSeqRefs, rewrite.ID)
				}
			}
			col.OwnsSequenceIds = newOwnedSeqRefs

			return nil
		}

		// Rewrite sequence and type references in column descriptors.
		for idx := range table.Columns {
			if err := rewriteCol(&table.Columns[idx]); err != nil {
				return err
			}
		}
		for idx := range table.Mutations {
			if col := table.Mutations[idx].GetColumn(); col != nil {
				if err := rewriteCol(col); err != nil {
					return err
				}
			}
		}

		// since this is a "new" table in eyes of new cluster, any leftover change
		// lease is obviously bogus (plus the nodeID is relative to backup cluster).
		table.Lease = nil
	}
	return nil
}

func errOnMissingRange(span covering.Range, start, end hlc.Timestamp) error {
	return errors.Errorf(
		"no backup covers time [%s,%s) for range [%s,%s) (or backups out of order)",
		start, end, roachpb.Key(span.Start), roachpb.Key(span.End),
	)
}

func getUserDescriptorNames(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) ([]string, error) {
	allDescs, err := catalogkv.GetAllDescriptors(ctx, txn, codec, true /* shouldRunPostDeserializationChanges */)
	if err != nil {
		return nil, err
	}

	var allNames = make([]string, 0, len(allDescs))
	for _, desc := range allDescs {
		if !catalogkeys.IsDefaultCreatedDescriptor(desc.GetID()) {
			allNames = append(allNames, desc.GetName())
		}
	}

	return allNames, nil
}

func resolveOptionsForRestoreJobDescription(
	opts tree.RestoreOptions, intoDB string, kmsURIs []string,
) (tree.RestoreOptions, error) {
	if opts.IsDefault() {
		return opts, nil
	}

	newOpts := tree.RestoreOptions{
		SkipMissingFKs:            opts.SkipMissingFKs,
		SkipMissingSequences:      opts.SkipMissingSequences,
		SkipMissingSequenceOwners: opts.SkipMissingSequenceOwners,
		SkipMissingViews:          opts.SkipMissingViews,
		Detached:                  opts.Detached,
	}

	if opts.EncryptionPassphrase != nil {
		newOpts.EncryptionPassphrase = tree.NewDString("redacted")
	}

	if opts.IntoDB != nil {
		newOpts.IntoDB = tree.NewDString(intoDB)
	}

	for _, uri := range kmsURIs {
		redactedURI, err := cloud.RedactKMSURI(uri)
		if err != nil {
			return tree.RestoreOptions{}, err
		}
		newOpts.DecryptionKMSURI = append(newOpts.DecryptionKMSURI, tree.NewDString(redactedURI))
	}

	return newOpts, nil
}

func restoreJobDescription(
	p sql.PlanHookState,
	restore *tree.Restore,
	from [][]string,
	opts tree.RestoreOptions,
	intoDB string,
	kmsURIs []string,
) (string, error) {
	r := &tree.Restore{
		DescriptorCoverage: restore.DescriptorCoverage,
		AsOf:               restore.AsOf,
		Targets:            restore.Targets,
		From:               make([]tree.StringOrPlaceholderOptList, len(restore.From)),
	}

	var options tree.RestoreOptions
	var err error
	if options, err = resolveOptionsForRestoreJobDescription(opts, intoDB, kmsURIs); err != nil {
		return "", err
	}
	r.Options = options

	for i, backup := range from {
		r.From[i] = make(tree.StringOrPlaceholderOptList, len(backup))
		for j, uri := range backup {
			sf, err := cloud.SanitizeExternalStorageURI(uri, nil /* extraParams */)
			if err != nil {
				return "", err
			}
			r.From[i][j] = tree.NewDString(sf)
		}
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(r, ann), nil
}

// restorePlanHook implements sql.PlanHookFn.
func restorePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	restoreStmt, ok := stmt.(*tree.Restore)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureRestoreEnabled,
		"RESTORE",
	); err != nil {
		return nil, nil, nil, false, err
	}

	fromFns := make([]func() ([]string, error), len(restoreStmt.From))
	for i := range restoreStmt.From {
		fromFn, err := p.TypeAsStringArray(ctx, tree.Exprs(restoreStmt.From[i]), "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
		fromFns[i] = fromFn
	}

	var pwFn func() (string, error)
	var err error
	if restoreStmt.Options.EncryptionPassphrase != nil {
		pwFn, err = p.TypeAsString(ctx, restoreStmt.Options.EncryptionPassphrase, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var kmsFn func() ([]string, error)
	if restoreStmt.Options.DecryptionKMSURI != nil {
		if restoreStmt.Options.EncryptionPassphrase != nil {
			return nil, nil, nil, false, errors.New("cannot have both encryption_passphrase and kms option set")
		}
		kmsFn, err = p.TypeAsStringArray(ctx, tree.Exprs(restoreStmt.Options.DecryptionKMSURI),
			"RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var intoDBFn func() (string, error)
	if restoreStmt.Options.IntoDB != nil {
		intoDBFn, err = p.TypeAsString(ctx, restoreStmt.Options.IntoDB, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	subdirFn := func() (string, error) { return "", nil }
	if restoreStmt.Subdir != nil {
		subdirFn, err = p.TypeAsString(ctx, restoreStmt.Subdir, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnImplicit || restoreStmt.Options.Detached) {
			return errors.Errorf("RESTORE cannot be used inside a transaction without DETACHED option")
		}

		subdir, err := subdirFn()
		if err != nil {
			return err
		}

		from := make([][]string, len(fromFns))
		for i := range fromFns {
			from[i], err = fromFns[i]()
			if err != nil {
				return err
			}
		}
		if subdir != "" {
			if len(from) != 1 {
				return errors.Errorf("RESTORE FROM ... IN can only by used against a single collection path (per-locality)")
			}
			for i := range from[0] {
				parsed, err := url.Parse(from[0][i])
				if err != nil {
					return err
				}
				parsed.Path = path.Join(parsed.Path, subdir)
				from[0][i] = parsed.String()
			}
		}

		if err := checkPrivilegesForRestore(ctx, restoreStmt, p, from); err != nil {
			return err
		}

		var endTime hlc.Timestamp
		if restoreStmt.AsOf.Expr != nil {
			var err error
			endTime, err = p.EvalAsOfTimestamp(ctx, restoreStmt.AsOf)
			if err != nil {
				return err
			}
		}

		var passphrase string
		if pwFn != nil {
			passphrase, err = pwFn()
			if err != nil {
				return err
			}
		}

		var kms []string
		if kmsFn != nil {
			kms, err = kmsFn()
			if err != nil {
				return err
			}
		}

		var intoDB string
		if intoDBFn != nil {
			intoDB, err = intoDBFn()
			if err != nil {
				return err
			}
		}

		return doRestorePlan(ctx, restoreStmt, p, from, passphrase, kms, intoDB, endTime, resultsCh)
	}

	if restoreStmt.Options.Detached {
		return fn, utilccl.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, utilccl.BulkJobExecutionResultHeader, nil, false, nil
}

func checkPrivilegesForRestore(
	ctx context.Context, restoreStmt *tree.Restore, p sql.PlanHookState, from [][]string,
) error {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}
	// Do not allow full cluster restores.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to restore full cluster backups")
	}
	// Do not allow tenant restores.
	if restoreStmt.Targets.Tenant != (roachpb.TenantID{}) {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role can perform RESTORE TENANT")
	}
	// Database restores require the CREATEDB privileges.
	if len(restoreStmt.Targets.Databases) > 0 {
		hasCreateDB, err := p.HasRoleOption(ctx, roleoption.CREATEDB)
		if err != nil {
			return err
		}
		if !hasCreateDB {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the CREATEDB privilege can restore databases")
		}
	}
	knobs := p.ExecCfg().BackupRestoreTestingKnobs
	if knobs != nil && knobs.AllowImplicitAccess {
		return nil
	}
	// Check that none of the sources rely on implicit access.
	for i := range from {
		for j := range from[i] {
			conf, err := cloud.ExternalStorageConfFromURI(from[i][j], p.User())
			if err != nil {
				return err
			}
			if !conf.AccessIsWithExplicitAuth() {
				return pgerror.Newf(
					pgcode.InsufficientPrivilege,
					"only users with the admin role are allowed to RESTORE from the specified %s URI",
					conf.Provider.String())
			}
		}
	}
	return nil
}

func findNodeOfRegion(nodes []statuspb.NodeStatus, region descpb.RegionName) bool {
	constraint := zonepb.Constraint{
		Type:  zonepb.Constraint_REQUIRED,
		Key:   "region",
		Value: string(region),
	}
	for _, n := range nodes {
		for _, store := range n.StoreStatuses {
			if zonepb.StoreMatchesConstraint(store.Desc, constraint) {
				return true
			}
		}
	}
	return false
}

func checkClusterRegions(
	ctx context.Context,
	typesByID map[descpb.ID]*typedesc.Mutable,
	ss serverpb.OptionalNodesStatusServer,
	codec keys.SQLCodec,
) error {

	regionSet := make(map[descpb.RegionName]struct{})
	for _, typ := range typesByID {
		typeDesc := typedesc.NewBuilder(typ.TypeDesc()).BuildImmutableType()
		if typeDesc.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM {
			regionNames, err := typeDesc.RegionNames()
			if err != nil {
				return err
			}
			for _, region := range regionNames {
				if _, ok := regionSet[region]; !ok {
					regionSet[region] = struct{}{}
				}
			}
		}
	}

	if len(regionSet) == 0 {
		return nil
	}

	var nodeStatusServer serverpb.NodesStatusServer
	var err error
	if nodeStatusServer, err = ss.OptionalNodesStatusServer(sql.MultitenancyZoneCfgIssueNo); err != nil {
		if !codec.ForSystemTenant() {
			hintMsg := fmt.Sprintf("only the system tenant supports localities check for restore, otherwise option %q is required", restoreOptSkipLocalitiesCheck)
			return errors.WithHint(err, hintMsg)
		}
		return err
	}

	var nodesResponse *serverpb.NodesResponse
	nodesResponse, err = nodeStatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}

	missingRegions := make([]string, 0)
	for region := range regionSet {
		if nodeFound := findNodeOfRegion(nodesResponse.Nodes, region); !nodeFound {
			missingRegions = append(missingRegions, string(region))
		}
	}

	if len(missingRegions) > 0 {
		// Missing regions are sorted for predictable outputs in tests.
		sort.Strings(missingRegions)
		mismatchErr := errors.Newf("detected a mismatch in regions between the restore cluster and the backup cluster, "+
			"missing regions detected: %s.", strings.Join(missingRegions, ", "))
		hintsMsg := fmt.Sprintf("there are two ways you can resolve this issue: "+
			"1) update the cluster to which you're restoring to ensure that the regions present on the nodes' "+
			"--locality flags match those present in the backup image, or "+
			"2) restore with the %q option", restoreOptSkipLocalitiesCheck)
		return errors.WithHint(mismatchErr, hintsMsg)
	}

	return nil
}

func doRestorePlan(
	ctx context.Context,
	restoreStmt *tree.Restore,
	p sql.PlanHookState,
	from [][]string,
	passphrase string,
	kms []string,
	intoDB string,
	endTime hlc.Timestamp,
	resultsCh chan<- tree.Datums,
) error {
	if len(from) < 1 || len(from[0]) < 1 {
		return errors.New("invalid base backup specified")
	}
	baseStores := make([]cloud.ExternalStorage, len(from[0]))
	for i := range from[0] {
		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, from[0][i], p.User())
		if err != nil {
			return errors.Wrapf(err, "failed to open backup storage location")
		}
		defer store.Close()
		baseStores[i] = store
	}

	var encryption *jobspb.BackupEncryptionOptions
	if restoreStmt.Options.EncryptionPassphrase != nil {
		opts, err := readEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}
		encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts.Salt)
		encryption = &jobspb.BackupEncryptionOptions{Mode: jobspb.EncryptionMode_Passphrase,
			Key: encryptionKey}
	} else if restoreStmt.Options.DecryptionKMSURI != nil {
		opts, err := readEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}
		ioConf := baseStores[0].ExternalIOConf()
		defaultKMSInfo, err := validateKMSURIsAgainstFullBackup(kms,
			newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), &backupKMSEnv{
				baseStores[0].Settings(),
				&ioConf,
			})
		if err != nil {
			return err
		}
		encryption = &jobspb.BackupEncryptionOptions{
			Mode:    jobspb.EncryptionMode_KMS,
			KMSInfo: defaultKMSInfo}
	}

	defaultURIs, mainBackupManifests, localityInfo, err := resolveBackupManifests(
		ctx, baseStores, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, from, endTime, encryption,
		p.User(),
	)
	if err != nil {
		return err
	}

	currentVersion := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	for i := range mainBackupManifests {
		if v := mainBackupManifests[i].ClusterVersion; v.Major != 0 {
			// This is the "cluster" version that does not change between patches but
			// rather just tracks migrations run. If the backup is more migrated than
			// this cluster, then this cluster isn't ready to restore this backup.
			if currentVersion.Less(v) {
				return errors.Errorf("backup from version %s is newer than current version %s", v, currentVersion)
			}
		}
	}

	// Validate that the table coverage of the backup matches that of the restore.
	// This prevents FULL CLUSTER backups to be restored as anything but full
	// cluster restores and vice-versa.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors && mainBackupManifests[0].DescriptorCoverage == tree.RequestedDescriptors {
		return errors.Errorf("full cluster RESTORE can only be used on full cluster BACKUP files")
	}

	// Ensure that no user table descriptors exist for a full cluster restore.
	txn := p.ExecCfg().DB.NewTxn(ctx, "count-user-descs")
	descCount, err := catalogkv.CountUserDescriptors(ctx, txn, p.ExecCfg().Codec)
	if err != nil {
		return errors.Wrap(err, "looking up user descriptors during restore")
	}
	if descCount != 0 && restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		var userDescriptorNames []string
		userDescriptorNames, err := getUserDescriptorNames(ctx, txn, p.ExecCfg().Codec)
		if err != nil {
			// We're already returning an error, and we're just trying to make the
			// error message more helpful. If we fail to do that, let's just log.
			log.Errorf(ctx, "fetching user descriptor names: %+v", err)
		}
		return errors.Errorf(
			"full cluster restore can only be run on a cluster with no tables or databases but found %d descriptors: %s",
			descCount, userDescriptorNames,
		)
	}

	// wasOffline tracks which tables were in an offline or adding state at some
	// point in the incremental chain, meaning their spans would be seeing
	// non-transactional bulk-writes. If that backup exported those spans, then it
	// can't be trusted for that table/index since those bulk-writes can fail to
	// be caught by backups.
	wasOffline := make(map[tableAndIndex]hlc.Timestamp)

	for _, m := range mainBackupManifests {
		spans := roachpb.Spans(m.Spans)
		for i := range m.Descriptors {
			table, _, _, _ := descpb.FromDescriptor(&m.Descriptors[i])
			if table == nil {
				continue
			}
			if err := catalog.ForEachNonDropIndex(
				tabledesc.NewBuilder(table).BuildImmutable().(catalog.TableDescriptor),
				func(index catalog.Index) error {
					if index.Adding() && spans.ContainsKey(keys.TODOSQLCodec.IndexPrefix(uint32(table.ID), uint32(index.GetID()))) {
						k := tableAndIndex{tableID: table.ID, indexID: index.GetID()}
						if _, ok := wasOffline[k]; !ok {
							wasOffline[k] = m.EndTime
						}
					}
					return nil
				}); err != nil {
				return err
			}
		}
	}

	sqlDescs, restoreDBs, tenants, err := selectTargets(ctx, p, mainBackupManifests, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime)
	if err != nil {
		return errors.Wrap(err,
			"failed to resolve targets in the BACKUP location specified by the RESTORE stmt, "+
				"use SHOW BACKUP to find correct targets")
	}

	var revalidateIndexes []jobspb.RestoreDetails_RevalidateIndex
	for _, desc := range sqlDescs {
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		for _, idx := range tbl.ActiveIndexes() {
			if _, ok := wasOffline[tableAndIndex{tableID: desc.GetID(), indexID: idx.GetID()}]; ok {
				revalidateIndexes = append(revalidateIndexes, jobspb.RestoreDetails_RevalidateIndex{
					TableID: desc.GetID(), IndexID: idx.GetID(),
				})
			}
		}
	}

	if err := maybeUpgradeDescriptors(ctx, sqlDescs, restoreStmt.Options.SkipMissingFKs); err != nil {
		return err
	}

	if len(tenants) > 0 {
		if !p.ExecCfg().Codec.ForSystemTenant() {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can restore other tenants")
		}
		for _, i := range tenants {
			res, err := p.ExecCfg().InternalExecutor.QueryRow(
				ctx, "restore-lookup-tenant", p.ExtendedEvalContext().Txn,
				`SELECT active FROM system.tenants WHERE id = $1`, i.ID,
			)
			if err != nil {
				return err
			}
			if res != nil {
				return errors.Errorf("tenant %d already exists", i.ID)
			}
		}
	}

	databasesByID := make(map[descpb.ID]*dbdesc.Mutable)
	schemasByID := make(map[descpb.ID]*schemadesc.Mutable)
	tablesByID := make(map[descpb.ID]*tabledesc.Mutable)
	typesByID := make(map[descpb.ID]*typedesc.Mutable)

	for _, desc := range sqlDescs {
		switch desc := desc.(type) {
		case *dbdesc.Mutable:
			databasesByID[desc.GetID()] = desc
		case *schemadesc.Mutable:
			schemasByID[desc.ID] = desc
		case *tabledesc.Mutable:
			tablesByID[desc.ID] = desc
		case *typedesc.Mutable:
			typesByID[desc.ID] = desc
		}
	}

	if !restoreStmt.Options.SkipLocalitiesCheck {
		if err := checkClusterRegions(ctx, typesByID, p.ExecCfg().NodesStatusServer, p.ExecCfg().Codec); err != nil {
			return err
		}
	}

	filteredTablesByID, err := maybeFilterMissingViews(
		tablesByID,
		typesByID,
		restoreStmt.Options.SkipMissingViews)
	if err != nil {
		return err
	}
	descriptorRewrites, err := allocateDescriptorRewrites(
		ctx,
		p,
		databasesByID,
		schemasByID,
		filteredTablesByID,
		typesByID,
		restoreDBs,
		restoreStmt.DescriptorCoverage,
		restoreStmt.Options,
		intoDB,
	)
	if err != nil {
		return err
	}
	description, err := restoreJobDescription(p, restoreStmt, from, restoreStmt.Options, intoDB, kms)
	if err != nil {
		return err
	}

	var databases []*dbdesc.Mutable
	for i := range databasesByID {
		if _, ok := descriptorRewrites[i]; ok {
			databases = append(databases, databasesByID[i])
		}
	}
	var schemas []*schemadesc.Mutable
	for i := range schemasByID {
		schemas = append(schemas, schemasByID[i])
	}
	var tables []*tabledesc.Mutable
	for _, desc := range filteredTablesByID {
		tables = append(tables, desc)
	}
	var types []*typedesc.Mutable
	for _, desc := range typesByID {
		types = append(types, desc)
	}

	// We attempt to rewrite ID's in the collected type and table descriptors
	// to catch errors during this process here, rather than in the job itself.
	if err := RewriteTableDescs(tables, descriptorRewrites, intoDB); err != nil {
		return err
	}
	if err := rewriteDatabaseDescs(databases, descriptorRewrites); err != nil {
		return err
	}
	if err := rewriteSchemaDescs(schemas, descriptorRewrites); err != nil {
		return err
	}
	if err := rewriteTypeDescs(types, descriptorRewrites); err != nil {
		return err
	}
	for i := range revalidateIndexes {
		revalidateIndexes[i].TableID = descriptorRewrites[revalidateIndexes[i].TableID].ID
	}

	// Collect telemetry.
	collectTelemetry := func() {
		telemetry.Count("restore.total.started")
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
			telemetry.Count("restore.full-cluster")
		}
	}

	encodedTables := make([]*descpb.TableDescriptor, len(tables))
	for i, table := range tables {
		encodedTables[i] = table.TableDesc()
	}
	jr := jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
			for _, tableRewrite := range descriptorRewrites {
				sqlDescIDs = append(sqlDescIDs, tableRewrite.ID)
			}
			return sqlDescIDs
		}(),
		Details: jobspb.RestoreDetails{
			EndTime:            endTime,
			DescriptorRewrites: descriptorRewrites,
			URIs:               defaultURIs,
			BackupLocalityInfo: localityInfo,
			TableDescs:         encodedTables,
			Tenants:            tenants,
			OverrideDB:         intoDB,
			DescriptorCoverage: restoreStmt.DescriptorCoverage,
			Encryption:         encryption,
			RevalidateIndexes:  revalidateIndexes,
		},
		Progress: jobspb.RestoreProgress{},
	}

	if restoreStmt.Options.Detached {
		// When running in detached mode, we simply create the job record.
		// We do not wait for the job to finish.
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
			ctx, jr, jobID, p.ExtendedEvalContext().Txn)
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
		collectTelemetry()
		return nil
	}

	// We create the job record in the planner's transaction to ensure that
	// the job record creation happens transactionally.
	plannerTxn := p.ExtendedEvalContext().Txn

	// Construct the job and commit the transaction. Perform this work in a
	// closure to ensure that the job is cleaned up if an error occurs.
	var sj *jobs.StartableJob
	if err := func() (err error) {
		defer func() {
			if err == nil || sj == nil {
				return
			}
			if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Errorf(ctx, "failed to cleanup job: %v", cleanupErr)
			}
		}()
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, plannerTxn, jr); err != nil {
			return err
		}

		// We commit the transaction here so that the job can be started. This is
		// safe because we're in an implicit transaction. If we were in an explicit
		// transaction the job would have to be created with the detached option and
		// would have been handled above.
		return plannerTxn.Commit(ctx)
	}(); err != nil {
		return err
	}

	collectTelemetry()
	if err := sj.Start(ctx); err != nil {
		return err
	}
	if err := sj.AwaitCompletion(ctx); err != nil {
		return err
	}
	return sj.ReportExecutionResults(ctx, resultsCh)
}

func init() {
	sql.AddPlanHook(restorePlanHook)
}
