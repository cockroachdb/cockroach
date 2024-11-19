// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableValidateConstraint(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableValidateConstraint,
) {
	constraintElems := b.ResolveConstraint(tbl.TableID, t.Constraint, ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})

	// 1. We can only validate non-index-backed constraints. Panic if not.
	_, _, constraintNameElem := scpb.FindConstraintWithoutIndexName(constraintElems)
	if constraintNameElem == nil {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"constraint %q of relation %q is not a foreign key, check, or unique without index"+
				" constraint", tree.ErrString(&t.Constraint), tree.ErrString(tn)))
	}

	// 2. Return if the constraint is already validated. Or panic if the
	// constraint is being dropped with a "constraint does not exist" error.
	constraintID := constraintNameElem.ConstraintID
	if skip, err := shouldSkipValidatingConstraint(b, tbl.TableID, constraintID); err != nil {
		panic(err)
	} else if skip {
		return
	}

	// 3. Drop the not-valid constraint and old constraint name element
	//    Add a new sibling constraint and a new constraint name element.
	validateConstraint(b, tbl.TableID, validateConstraintSpec{
		constraintNameElem: constraintNameElem,
		ckNotValidElem:     retrieveCheckConstraintUnvalidatedElem(b, tbl.TableID, constraintID),
		uwiNotValidElem:    retrieveUniqueWithoutIndexConstraintUnvalidatedElem(b, tbl.TableID, constraintID),
		fkNotValidElem:     retrieveForeignKeyConstraintUnvalidatedElem(b, tbl.TableID, constraintID),
	})
}

func retrieveCheckConstraintUnvalidatedElem(
	b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID,
) (CheckConstraintUnvalidatedElem *scpb.CheckConstraintUnvalidated) {
	scpb.ForEachCheckConstraintUnvalidated(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.CheckConstraintUnvalidated,
	) {
		if e.ConstraintID == constraintID {
			CheckConstraintUnvalidatedElem = e
		}
	})
	return CheckConstraintUnvalidatedElem
}

func retrieveUniqueWithoutIndexConstraintUnvalidatedElem(
	b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID,
) (UniqueWithoutIndexConstraintUnvalidatedElem *scpb.UniqueWithoutIndexConstraintUnvalidated) {
	scpb.ForEachUniqueWithoutIndexConstraintUnvalidated(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.UniqueWithoutIndexConstraintUnvalidated,
	) {
		if e.ConstraintID == constraintID {
			UniqueWithoutIndexConstraintUnvalidatedElem = e
		}
	})
	return UniqueWithoutIndexConstraintUnvalidatedElem
}

func retrieveUniqueWithoutIndexConstraintElem(
	b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID,
) (UniqueWithoutIndexConstraintElem *scpb.UniqueWithoutIndexConstraint) {
	scpb.ForEachUniqueWithoutIndexConstraint(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.UniqueWithoutIndexConstraint,
	) {
		if e.ConstraintID == constraintID {
			UniqueWithoutIndexConstraintElem = e
		}
	})
	return UniqueWithoutIndexConstraintElem
}

func retrieveForeignKeyConstraintUnvalidatedElem(
	b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID,
) (ForeignKeyConstraintUnvalidatedElem *scpb.ForeignKeyConstraintUnvalidated) {
	scpb.ForEachForeignKeyConstraintUnvalidated(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ForeignKeyConstraintUnvalidated,
	) {
		if e.ConstraintID == constraintID {
			ForeignKeyConstraintUnvalidatedElem = e
		}
	})
	return ForeignKeyConstraintUnvalidatedElem
}

type validateConstraintSpec struct {
	constraintNameElem *scpb.ConstraintWithoutIndexName
	ckNotValidElem     *scpb.CheckConstraintUnvalidated
	uwiNotValidElem    *scpb.UniqueWithoutIndexConstraintUnvalidated
	fkNotValidElem     *scpb.ForeignKeyConstraintUnvalidated
}

func validateConstraint(b BuildCtx, tableID catid.DescID, spec validateConstraintSpec) {
	nextConstraintID := b.NextTableConstraintID(tableID)
	if spec.ckNotValidElem != nil {
		b.Drop(spec.ckNotValidElem)
		b.Add(&scpb.CheckConstraint{
			TableID:               tableID,
			ConstraintID:          nextConstraintID,
			ColumnIDs:             spec.ckNotValidElem.ColumnIDs,
			Expression:            spec.ckNotValidElem.Expression,
			FromHashShardedColumn: false,
			IndexIDForValidation:  getIndexIDForValidationForConstraint(b, tableID),
		})
	}
	if spec.uwiNotValidElem != nil {
		b.Drop(spec.uwiNotValidElem)
		b.Add(&scpb.UniqueWithoutIndexConstraint{
			TableID:      tableID,
			ConstraintID: nextConstraintID,
			ColumnIDs:    spec.uwiNotValidElem.ColumnIDs,
			Predicate:    spec.uwiNotValidElem.Predicate,
		})
	}
	if spec.fkNotValidElem != nil {
		b.Drop(spec.fkNotValidElem)
		b.Add(&scpb.ForeignKeyConstraint{
			TableID:                 tableID,
			ConstraintID:            nextConstraintID,
			ColumnIDs:               spec.fkNotValidElem.ColumnIDs,
			ReferencedTableID:       spec.fkNotValidElem.ReferencedTableID,
			ReferencedColumnIDs:     spec.fkNotValidElem.ReferencedColumnIDs,
			OnUpdateAction:          spec.fkNotValidElem.OnUpdateAction,
			OnDeleteAction:          spec.fkNotValidElem.OnDeleteAction,
			CompositeKeyMatchMethod: spec.fkNotValidElem.CompositeKeyMatchMethod,
			IndexIDForValidation:    getIndexIDForValidationForConstraint(b, tableID),
		})
	}
	b.Drop(spec.constraintNameElem)
	b.Add(&scpb.ConstraintWithoutIndexName{
		TableID:      tableID,
		ConstraintID: nextConstraintID,
		Name:         spec.constraintNameElem.Name,
	})
}
