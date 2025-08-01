// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

	// Dropping PK constraint: Fall back to legacy schema changer.
	// CRDB only allows dropping PK constraint if it's immediately followed by a
	// add PK constraint command in the same transaction. Declarative schema changer
	// is not mature enough to deal with DDLs in transaction; we will fall back
	// until it is.
	fallBackIfDroppingPrimaryKey(constraintElems, t)
	// Dropping UNIQUE constraint: error out as not implemented.
	droppingUniqueConstraintNotImplemented(constraintElems, t)

	_, _, constraintNameElem := scpb.FindConstraintWithoutIndexName(constraintElems)
	constraintID := constraintNameElem.ConstraintID
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

func fallBackIfDroppingPrimaryKey(
	constraintElems ElementResultSet, t *tree.AlterTableDropConstraint,
) {
	_, _, pie := scpb.FindPrimaryIndex(constraintElems)
	if pie != nil {
		panic(scerrors.NotImplementedError(t))
	}
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
