// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

func Truncate(b BuildCtx, stmt *tree.Truncate) {
	// If we detect concurrent schema change errors, those will be converted to
	// not implemented errors, since truncate is supposed to be able to finalize
	// mutations.
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				if scerrors.ConcurrentSchemaChangeDescID(err) != descpb.InvalidID {
					panic(scerrors.NotImplementedErrorf(stmt, "concurrent schema change detected"))
				}
			}
			panic(r)
		}
	}()

	tablesToTruncate := catalog.DescriptorIDSet{}
	// Resolve all tables that need to be truncated.
	for i := range stmt.Tables {
		tblName := &stmt.Tables[i]
		elts := b.ResolveTable(tblName.ToUnresolvedObjectName(), ResolveParams{
			RequiredPrivilege: privilege.DROP,
		})
		tbl := elts.FilterTable().MustGetOneElement()
		tblName.ObjectNamePrefix = b.NamePrefix(tbl)
		tablesToTruncate.Add(tbl.TableID)
	}

	truncatesToProcess := tablesToTruncate.Ordered()
	addFkCascade := func(tableID catid.DescID, referencingTableID catid.DescID) {
		refElts := b.QueryByID(referencingTableID)
		if stmt.DropBehavior != tree.DropCascade {
			name := b.QueryByID(tableID).FilterNamespace().MustGetOneElement()
			refName := refElts.FilterNamespace().MustGetZeroOrOneElement()
			// The error code returned here matches PGSQL, even though the CASCADE
			// operation is supported there with TRUNCATE as well.
			panic(errors.WithHint(pgerror.Newf(pgcode.FeatureNotSupported,
				"%q is %s table %q", name.Name,
				"referenced by foreign key from",
				refName.Name),
				"truncate dependent tables at the same time or specify the CASCADE option"))
		}
		if !tablesToTruncate.Contains(referencingTableID) {
			tablesToTruncate.Add(referencingTableID)
			// Queue this table to process dependencies.
			truncatesToProcess = append(truncatesToProcess, referencingTableID)
		}
		// Validate we have permission to drop this table.
		if err := b.CheckPrivilege(refElts.FilterTable().MustGetOneElement(), privilege.DROP); err != nil {
			panic(err)
		}
	}
	for len(truncatesToProcess) > 0 {
		// Pop the first element and process it.
		id := truncatesToProcess[0]
		truncatesToProcess = truncatesToProcess[1:]
		// Process any foreign keys that reference in this table.
		backRefs := b.BackReferences(id)
		backRefs.FilterForeignKeyConstraint().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraint) {
			if !tablesToTruncate.Contains(e.TableID) && e.ReferencedTableID == id {
				addFkCascade(e.ReferencedTableID, e.TableID)
			}
		})
		backRefs.FilterForeignKeyConstraintUnvalidated().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraintUnvalidated) {
			if !tablesToTruncate.Contains(e.TableID) && e.ReferencedTableID == id {
				addFkCascade(e.ReferencedTableID, e.TableID)
			}
		})
	}

	// Finally, truncate the union of all tables that need truncation.
	tablesToTruncate.ForEach(func(id descpb.ID) {
		truncateTable(b, stmt, b.QueryByID(id))
	})

}

func truncateTable(b BuildCtx, n *tree.Truncate, elts ElementResultSet) {
	tbl := elts.FilterTable().MustGetOneElement()
	// Fall back to legacy schema changer if we need to rewrite index references.
	backRefs := b.BackReferences(tbl.TableID)
	backRefs.FilterView().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.View) {
		for _, ref := range e.ForwardReferences {
			if ref.ToID == tbl.TableID && ref.IndexID > 0 {
				panic(scerrors.NotImplementedErrorf(n, "index reference requiring a rewrite"))
			}
		}
	})
	backRefs.FilterTriggerDeps().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.TriggerDeps) {
		for _, ref := range e.UsesRelations {
			if ref.ID == tbl.TableID && ref.IndexID > 0 {
				panic(scerrors.NotImplementedErrorf(n, "index reference requiring a rewrite"))
			}
		}
	})
	backRefs.FilterFunctionBody().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.FunctionBody) {
		for _, ref := range e.UsesTables {
			if ref.TableID == tbl.TableID && ref.IndexID > 0 {
				panic(scerrors.NotImplementedErrorf(n, "index reference requiring a rewrite"))
			}
		}
	})
	// Recreate all primary indexes.
	var newPrimaryIndexID descpb.IndexID
	elts.FilterPrimaryIndex().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex) {
		// We don't support complex transactions that modify the primary key via
		// ADD COLUMN / DROP COLUMN / ALTER PRIMARY KEY. For these we will need to
		// resolve the final primary key for truncate.
		if newPrimaryIndexID != 0 {
			panic(scerrors.NotImplementedErrorf(n, "multiple primary indexes"))
		}
		if current != scpb.Status_PUBLIC || target != scpb.ToPublic {
			return
		}
		oldSpec := makeIndexSpec(b, e.TableID, e.IndexID)
		newSpec := oldSpec.clone()
		newIndexID := b.NextTableIndexID(e.TableID)
		newPrimaryIndexID = newIndexID
		newSpec.primary.ConstraintID = b.NextTableConstraintID(e.TableID)
		newSpec.apply(func(e scpb.Element) {
			_ = screl.WalkIndexIDs(e, func(id *catid.IndexID) error {
				*id = newIndexID
				return nil
			})
		})
		// Indicate this table needs no backfill.
		newSpec.primary.TemporaryIndexID = 0
		// SourceIndexID is used to ensure that a swap is done
		// atomically.
		newSpec.primary.SourceIndexID = oldSpec.indexID()
		newSpec.temporary = nil
		oldSpec.apply(b.Drop)
		newSpec.apply(b.Add)
		// Update the index for partitioning.
		if err := configureZoneConfigForReplacementIndexPartitioning(
			b,
			tbl.TableID,
			oldSpec.indexID(),
			newSpec.indexID(),
		); err != nil {
			panic(err)
		}
		// Attach a truncate event on the primary index.
		namePrefix := b.NamePrefix(tbl)
		namespace := elts.FilterNamespace().MustGetOneElement()
		tblName := tree.MakeTableNameFromPrefix(namePrefix, tree.Name(namespace.Name))
		b.LogEventForExistingPayload(newSpec.primary, &eventpb.TruncateTable{
			TableName: tblName.FQString(),
		})
	})

	// Recreate all secondary indexes.
	elts.FilterSecondaryIndex().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.SecondaryIndex) {
		if current != scpb.Status_PUBLIC || target != scpb.ToPublic {
			return
		}
		oldSpec := makeIndexSpec(b, e.TableID, e.IndexID)
		newSpec := oldSpec.clone()
		newIndexID := b.NextTableIndexID(e.TableID)
		newSpec.secondary.ConstraintID = b.NextTableConstraintID(e.TableID)
		newSpec.secondary.TemporaryIndexID = 0
		newSpec.temporary = nil
		newSpec.apply(func(e scpb.Element) {
			_ = screl.WalkIndexIDs(e, func(id *catid.IndexID) error {
				*id = newIndexID
				return nil
			})
		})
		// Indicate this table needs no backfill.
		newSpec.secondary.TemporaryIndexID = 0
		newSpec.secondary.SourceIndexID = 0
		// Setup so that this becomes public with the new primary index.
		// Note RecreateSourceIndexID is used to ensure this is swapped
		// with the old index.
		newSpec.secondary.RecreateTargetIndexID = newPrimaryIndexID
		newSpec.secondary.RecreateSourceIndexID = oldSpec.indexID()
		newSpec.secondary.HideForPrimaryKeyRecreated = true
		oldSpec.apply(b.Drop)
		newSpec.apply(b.Add)
		// Update the index for partitioning.
		if err := configureZoneConfigForReplacementIndexPartitioning(
			b,
			tbl.TableID,
			oldSpec.indexID(),
			newSpec.indexID(),
		); err != nil {
			panic(err)
		}
	})
}
