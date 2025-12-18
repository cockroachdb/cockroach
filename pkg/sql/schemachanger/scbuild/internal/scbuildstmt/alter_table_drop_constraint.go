// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
	// primary key is being added in the same statement. In that case, the ADD
	// PRIMARY KEY command will handle the full PK swap, so we return early.
	if skipIfPrimaryKeySwap(constraintElems, stmt, t) {
		return
	}
	// Dropping UNIQUE constraint: error out as not implemented.
	droppingUniqueConstraintNotImplemented(constraintElems, t)

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

// skipIfPrimaryKeySwap checks if we're dropping a primary key constraint. If
// we are dropping a PK and also adding a new PK in the same statement (a "PK
// swap"), it returns true indicating the caller should return early (the ADD
// PRIMARY KEY command will handle the full PK swap). Otherwise, if dropping a
// PK without adding a new one, it panics with NotImplementedError to fall back
// to the legacy schema changer.
func skipIfPrimaryKeySwap(
	constraintElems ElementResultSet, stmt tree.Statement, t *tree.AlterTableDropConstraint,
) bool {
	if constraintElems.FilterPrimaryIndex().IsEmpty() {
		// We are not dropping a primary key constraint, so proceed as normal.
		return false
	}
	// Check if there's an ADD PRIMARY KEY command in the same ALTER TABLE
	// statement. If so, the declarative schema changer can handle this case.
	// The ADD PRIMARY KEY command will handle the full PK swap, so the caller
	// should return early.
	if alterTable, ok := stmt.(*tree.AlterTable); ok {
		for _, cmd := range alterTable.Cmds {
			if addConstraint, ok := cmd.(*tree.AlterTableAddConstraint); ok {
				if uniqueDef, ok := addConstraint.ConstraintDef.(*tree.UniqueConstraintTableDef); ok {
					if uniqueDef.PrimaryKey {
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

func droppingUniqueConstraintNotImplemented(
	constraintElems ElementResultSet, t *tree.AlterTableDropConstraint,
) {
	_, _, sie := scpb.FindSecondaryIndex(constraintElems)
	if sie != nil {
		if sie.IsUnique {
			panic(unimplemented.NewWithIssueDetailf(42840, "drop-constraint-unique",
				"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
				tree.ErrNameString(string(t.Constraint))))
		} else {
			panic(errors.AssertionFailedf("dropping an index-backed constraint but the " +
				"index is not unique"))
		}
	}
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
