// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	tablesToTruncates := catalog.DescriptorIDSet{}
	tablesLeftToTruncate := catalog.DescriptorIDSet{}
	// Resolve all tables that need to be truncated.
	for i := range stmt.Tables {
		tblName := &stmt.Tables[i]
		elts := b.ResolveTable(tblName.ToUnresolvedObjectName(), ResolveParams{
			RequiredPrivilege: privilege.DROP,
		})
		tbl := elts.FilterTable().MustGetOneElement()
		tblName.ObjectNamePrefix = b.NamePrefix(tbl)
		tablesToTruncates.Add(tbl.TableID)
	}

	addFkCascade := func(tableID catid.DescID, referencingTableID catid.DescID) {
		if stmt.DropBehavior != tree.DropCascade {
			name := b.QueryByID(tableID).FilterNamespace().MustGetOneElement()
			refName := b.QueryByID(referencingTableID).FilterNamespace().MustGetZeroOrOneElement()
			panic(errors.Errorf("%q is %s table %q", name.Name, "referenced by foreign key from", refName.Name))
		}
		tablesLeftToTruncate.Add(referencingTableID)

	}
	// Detect if any tables have foreign keys that need truncation.
	tablesToTruncates.ForEach(func(id descpb.ID) {
		backRefs := b.BackReferences(id)
		backRefs.FilterForeignKeyConstraint().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraint) {
			if !tablesToTruncates.Contains(e.TableID) && e.ReferencedTableID == id {
				addFkCascade(e.ReferencedTableID, e.TableID)
			}
		})
		backRefs.FilterForeignKeyConstraintUnvalidated().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraintUnvalidated) {
			if !tablesToTruncates.Contains(e.TableID) && e.ReferencedTableID == id {
				addFkCascade(e.ReferencedTableID, e.TableID)
			}
		})
	})
	// Finally, truncate the union of all tables that need truncation.
	tablesToTruncates.Union(tablesLeftToTruncate).ForEach(func(id descpb.ID) {
		truncateTable(b, stmt, b.QueryByID(id))
	})

}

func truncateTable(b BuildCtx, n *tree.Truncate, elts ElementResultSet) {
	tbl := elts.FilterTable().MustGetOneElement()
	// Fall back to legacy schema changer if we need to rewrite index references.
	backRefs := b.BackReferences(tbl.TableID)
	backRefs.FilterView().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.View) {
		for _, ref := range e.ForwardReferences {
			if ref.ToID == tbl.TableID {
				panic(scerrors.NotImplementedErrorf(n, "index reference requiring a rewrite"))
			}
		}
	})
	backRefs.FilterTriggerDeps().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.TriggerDeps) {
		for _, ref := range e.UsesRelations {
			if ref.ID == tbl.TableID {
				panic(scerrors.NotImplementedErrorf(n, "index reference requiring a rewrite"))
			}
		}
	})
	// Recreate all primary indexes.
	elts.FilterPrimaryIndex().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.PrimaryIndex) {
		if current != scpb.Status_PUBLIC || target != scpb.ToPublic {
			return
		}
		oldSpec := makeIndexSpec(b, e.TableID, e.IndexID)
		newSpec := oldSpec.clone()
		newIndexID := b.NextTableIndexID(e.TableID)
		newSpec.primary.ConstraintID = b.NextTableConstraintID(e.TableID)

		newSpec.apply(func(e scpb.Element) {
			_ = screl.WalkIndexIDs(e, func(id *catid.IndexID) error {
				*id = newIndexID
				return nil
			})
		})
		// Indicate this table needs no backfill.
		newSpec.primary.TemporaryIndexID = 0
		newSpec.primary.SourceIndexID = 0
		newSpec.temporary = nil
		oldSpec.apply(b.Drop)
		newSpec.apply(b.Add)
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
			// Indicate this table needs no backfill.
			newSpec.secondary.TemporaryIndexID = 0
			newSpec.secondary.SourceIndexID = 0
		})
		oldSpec.apply(b.Drop)
		newSpec.apply(b.Add)
	})
}
