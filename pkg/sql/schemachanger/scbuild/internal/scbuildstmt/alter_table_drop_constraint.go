// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func alterTableDropConstraint(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableDropConstraint,
) {
	constraintElems := b.ResolveConstraint(tbl.TableID, t.Constraint, ResolveParams{
		IsExistenceOptional: t.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	if constraintElems == nil {
		// Send a notice to user if constraint not found but `IF EXISTS` is set.
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b, pgnotice.Newf(
			"constraint %q of relation %q does not exist, skipping", t.Constraint, tn.Table()))
		return
	}

	// Dropping PK constraint: Fall back to legacy schema changer unless a new
	// primary key is being added in the same statement. If there is an ADD
	// PRIMARY KEY, we proceed with dropping the old PK elements here, and the
	// ADD PRIMARY KEY command will detect this and handle the full PK swap.
	if dropPrimaryKeyConstraint(b, constraintElems, stmt, t) {
		return
	}
	// Dropping UNIQUE constraint backed by an index.
	if dropUniqueIndexBackedConstraint(b, tn, tbl, constraintElems, stmt, t) {
		return
	}

	_, _, constraintNameElem := scpb.FindConstraintWithoutIndexName(constraintElems)
	constraintID := constraintNameElem.ConstraintID

	// Disallow dropping a constraint used to lookup values for the region column
	// in a REGIONAL BY ROW table.
	checkRegionalByRowConstraintConflict(b, tbl, constraintID, t.Constraint)

	constraintElems.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		b.Drop(e)
	})

	// UniqueWithoutIndex constraints can serve inbound FKs, and hence we might
	// need to drop those dependent FKs if cascade.
	maybeDropAdditionallyForUniqueWithoutIndexConstraint(b, tbl.TableID, constraintID,
		constraintNameElem.Name, t.DropBehavior)
}

func maybeDropAdditionallyForUniqueWithoutIndexConstraint(
	b BuildCtx,
	tableID catid.DescID,
	maybeUWIConstraintID catid.ConstraintID,
	constraintName string,
	behavior tree.DropBehavior,
) {
	uwiElem := retrieveUniqueWithoutIndexConstraintElem(b, tableID, maybeUWIConstraintID)
	if uwiElem == nil {
		return
	}
	maybeDropDependentFKConstraints(b, tableID, uwiElem.ConstraintID, constraintName, behavior,
		func(fkReferencedColIDs []catid.ColumnID) bool {
			return uwiElem.Predicate == nil &&
				descpb.ColumnIDs(uwiElem.ColumnIDs).PermutationOf(fkReferencedColIDs)
		})
}

// dropPrimaryKeyConstraint handles dropping a primary key constraint. If the
// constraint is not a PK, it returns false and the caller continues with other
// constraint types. If it is a PK:
//   - If there's an ADD PRIMARY KEY in the same statement (a PK swap), it marks
//     the old PK's IndexName element as ToAbsent and returns true. The ADD
//     PRIMARY KEY (or ADD CONSTRAINT) command will appear later and cause the
//     schema changer to do the full PK swap.
//   - If there's no ADD PRIMARY KEY, it panics with NotImplementedError to fall
//     back to the legacy schema changer.
func dropPrimaryKeyConstraint(
	b BuildCtx,
	constraintElems ElementResultSet,
	stmt tree.Statement,
	t *tree.AlterTableDropConstraint,
) bool {
	if constraintElems.FilterPrimaryIndex().IsEmpty() {
		// We are not dropping a primary key constraint.
		return false
	}
	// Check if there's an ADD PRIMARY KEY command in the same ALTER TABLE
	// statement. If so, the declarative schema changer can handle this case
	// as a PK swap. We mark the old PK's IndexName as ToAbsent here, and the
	// ADD PRIMARY KEY command will detect this and handle the full swap.
	if alterTable, ok := stmt.(*tree.AlterTable); ok {
		for _, cmd := range alterTable.Cmds {
			if addConstraint, ok := cmd.(*tree.AlterTableAddConstraint); ok {
				if uniqueDef, ok := addConstraint.ConstraintDef.(*tree.UniqueConstraintTableDef); ok {
					if uniqueDef.PrimaryKey {
						// This is a PK swap. Mark the old PK's IndexName as ToAbsent.
						// The PrimaryIndex and other elements are handled by the
						// ADD PRIMARY KEY command via alterPrimaryKey, which
						// performs the full PK swap including creating the new
						// primary index and converting the old one appropriately.
						constraintElems.FilterIndexName().ForEach(
							func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName) {
								b.Drop(e)
							})
						return true
					}
				}
			}
		}
	}
	// Dropping a primary key constraint without adding a new one is not
	// implemented in the declarative schema changer yet. The legacy schema
	// changer will handle this since it allows the PK to be re-added by a
	// subsequent statement in the same transaction.
	panic(scerrors.NotImplementedError(t))
}

// dropUniqueIndexBackedConstraint handles dropping a unique constraint that is
// backed by a secondary index. Returns true if the constraint was handled,
// false if not a unique index-backed constraint.
func dropUniqueIndexBackedConstraint(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	constraintElems ElementResultSet,
	stmt tree.Statement,
	t *tree.AlterTableDropConstraint,
) bool {
	_, _, sie := scpb.FindSecondaryIndex(constraintElems)
	if sie == nil {
		return false
	}
	if !sie.IsUnique {
		panic(errors.AssertionFailedf("dropping an index-backed constraint but the index is not unique"))
	}

	// Dropping a unique constraint via ALTER TABLE ... DROP CONSTRAINT was added
	// in v26.2. In mixed-version clusters, fall back to the legacy schema changer
	// to avoid inconsistent behavior across nodes.
	if !b.EvalCtx().Settings.Version.ActiveVersion(b).IsActive(clusterversion.V26_2) {
		panic(scerrors.NotImplementedErrorf(t,
			"DROP CONSTRAINT for unique constraints is not supported before v26.2"))
	}

	// Since this will drop the index as well, guard this behind the
	// sql_safe_updates flag.
	failIfSafeUpdates(b, t)

	panicIfRegionChangeUnderwayOnRBRTable(b, "ALTER TABLE DROP CONSTRAINT", sie.TableID)

	// Build index name for error messages and dependency handling.
	_, _, indexNameElem := scpb.FindIndexName(constraintElems)
	indexName := &tree.TableIndexName{
		Table: *tn,
		Index: tree.UnrestrictedName(indexNameElem.Name),
	}

	// Reuse the DROP INDEX logic which handles all dependencies:
	// - Dependent views
	// - Dependent functions
	// - Dependent FK constraints
	// - Sharded index cleanup
	// - Expression index cleanup
	// - Dependent triggers
	dropSecondaryIndex(b, indexName, t.DropBehavior, sie, stmt)

	return true
}

func checkRegionalByRowConstraintConflict(
	b BuildCtx, tbl *scpb.Table, constraintID catid.ConstraintID, constraintName tree.Name,
) {
	var usingConstraint *scpb.TableLocalityRegionalByRowUsingConstraint
	scpb.ForEachTableLocalityRegionalByRowUsingConstraint(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.TableLocalityRegionalByRowUsingConstraint,
	) {
		usingConstraint = e
	})
	if usingConstraint == nil {
		return
	}
	if usingConstraint.ConstraintID == constraintID {
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot drop constraint %s as it is used to determine the region in a REGIONAL BY ROW table",
				constraintName,
			),
			"You must reset the storage param \"%s\" before dropping this constraint",
			catpb.RBRUsingConstraintTableSettingName,
		))
	}
}
