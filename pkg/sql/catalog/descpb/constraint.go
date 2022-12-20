// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// EquivFKMatchMethod allows us to easily convert between various types of the same concept: FK reference match method.
// Unfortunately, for legacy reasons, we end up with those three types for the same concept. This struct
// is thus created to allow easy conversion between them when needed.
type EquivFKMatchMethod struct {
	AsTree   tree.CompositeKeyMatchMethod
	AsDescpb ForeignKeyReference_Match
	AsScpb   scpb.ForeignKeyConstraint_Match
}

// BuildEquivFKMatchMethod builds a struct that stores the same foreign key reference match method as `match` in
// three types: `tree.CompositeKeyMatchMethod`, `ForeignKeyReference_Match`, and `scpb.ForeignKeyConstraint_Match`.
// The input `match` must also be one of the three types.
func BuildEquivFKMatchMethod(match interface{}) EquivFKMatchMethod {
	switch m := match.(type) {
	case tree.CompositeKeyMatchMethod:
		return EquivFKMatchMethod{
			AsTree:   m,
			AsDescpb: fkMatchMethodTreeToDescpb[m],
			AsScpb:   fkMatchMethodTreeToScpb[m],
		}
	case ForeignKeyReference_Match:
		return EquivFKMatchMethod{
			AsTree:   fkMatchMethodDescpbToTree[m],
			AsDescpb: m,
			AsScpb:   fkMatchMethodTreeToScpb[fkMatchMethodDescpbToTree[m]],
		}
	case scpb.ForeignKeyConstraint_Match:
		return EquivFKMatchMethod{
			AsTree:   fkMatchMethodScpbToTree[m],
			AsDescpb: fkMatchMethodTreeToDescpb[fkMatchMethodScpbToTree[m]],
			AsScpb:   m,
		}
	default:
		panic(errors.AssertionFailedf("unknown foreign key reference match type %T", match))
	}
}

// fkMatchMethodTreeToDescpb allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var fkMatchMethodTreeToDescpb = [...]ForeignKeyReference_Match{
	tree.MatchSimple:  ForeignKeyReference_SIMPLE,
	tree.MatchFull:    ForeignKeyReference_FULL,
	tree.MatchPartial: ForeignKeyReference_PARTIAL,
}

// fkMatchMethodDescpbToTree allows the conversion from a
// ForeignKeyReference_Match to a tree.ReferenceCompositeKeyMatchMethod.
// This should match fkMatchMethodTreeToDescpb.
var fkMatchMethodDescpbToTree = [...]tree.CompositeKeyMatchMethod{
	ForeignKeyReference_SIMPLE:  tree.MatchSimple,
	ForeignKeyReference_FULL:    tree.MatchFull,
	ForeignKeyReference_PARTIAL: tree.MatchPartial,
}

// fkMatchMethodScpbToTree allows the conversion from a
// scpb.ForeignKeyConstraint_Match to a tree.ReferenceCompositeKeyMatchMethod.
var fkMatchMethodScpbToTree = [...]tree.CompositeKeyMatchMethod{
	scpb.ForeignKeyConstraint_SIMPLE:  tree.MatchSimple,
	scpb.ForeignKeyConstraint_FULL:    tree.MatchFull,
	scpb.ForeignKeyConstraint_PARTIAL: tree.MatchPartial,
}

// fkMatchMethodTreeToScpb allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a scpb.ForeignKeyConstraint_Match.
var fkMatchMethodTreeToScpb = [...]scpb.ForeignKeyConstraint_Match{
	tree.MatchSimple:  scpb.ForeignKeyConstraint_SIMPLE,
	tree.MatchFull:    scpb.ForeignKeyConstraint_FULL,
	tree.MatchPartial: scpb.ForeignKeyConstraint_PARTIAL,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Match) String() string {
	switch x {
	case ForeignKeyReference_SIMPLE:
		return "MATCH SIMPLE"
	case ForeignKeyReference_FULL:
		return "MATCH FULL"
	case ForeignKeyReference_PARTIAL:
		return "MATCH PARTIAL"
	default:
		return strconv.Itoa(int(x))
	}
}

// EquivFKAction allows us to easily convert between various types of the same concept: FK reference action.
// Unfortunately, for legacy reasons, we end up with those three types for the same concept. This struct
// is thus created to allow easy conversion between them when needed.
type EquivFKAction struct {
	AsTree  tree.ReferenceAction
	AsCatpb catpb.ForeignKeyAction
	AsScpb  scpb.ForeignKeyConstraint_Action
}

// BuildEquivFKAction builds a struct that stores the same foreign key reference action as `action` in three types:
// `tree.ReferenceAction`, `catpb.ForeignKeyAction`, and `scpb.ForeignKeyConstraint_Action`.
// The input `action` must also be one of the three types.
func BuildEquivFKAction(action interface{}) EquivFKAction {
	switch a := action.(type) {
	case tree.ReferenceAction:
		return EquivFKAction{
			AsTree:  a,
			AsCatpb: fkActionTreeToCatpb[a],
			AsScpb:  fkActionTreeToScpb[a],
		}
	case catpb.ForeignKeyAction:
		return EquivFKAction{
			AsTree:  fkActionCatpbToTree[a],
			AsCatpb: a,
			AsScpb:  fkActionTreeToScpb[fkActionCatpbToTree[a]],
		}
	case scpb.ForeignKeyConstraint_Action:
		return EquivFKAction{
			AsTree:  fkActionScpbToTree[a],
			AsCatpb: fkActionTreeToCatpb[fkActionScpbToTree[a]],
			AsScpb:  a,
		}
	default:
		panic(errors.AssertionFailedf("unknown foreign key reference action type %T", action))
	}
}

// fkActionCatpbToTree allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var fkActionCatpbToTree = [...]tree.ReferenceAction{
	catpb.ForeignKeyAction_NO_ACTION:   tree.NoAction,
	catpb.ForeignKeyAction_RESTRICT:    tree.Restrict,
	catpb.ForeignKeyAction_SET_DEFAULT: tree.SetDefault,
	catpb.ForeignKeyAction_SET_NULL:    tree.SetNull,
	catpb.ForeignKeyAction_CASCADE:     tree.Cascade,
}

// fkActionTreeToCatpb allows the conversion between a
// catpb.ForeignKeyAction_Action and a tree.ReferenceAction.
var fkActionTreeToCatpb = [...]catpb.ForeignKeyAction{
	tree.NoAction:   catpb.ForeignKeyAction_NO_ACTION,
	tree.Restrict:   catpb.ForeignKeyAction_RESTRICT,
	tree.SetDefault: catpb.ForeignKeyAction_SET_DEFAULT,
	tree.SetNull:    catpb.ForeignKeyAction_SET_NULL,
	tree.Cascade:    catpb.ForeignKeyAction_CASCADE,
}

// fkActionScpbToTree allows the conversion between a
// scpb.ForeignKeyConstraint_Action and a tree.ReferenceAction.
var fkActionScpbToTree = [...]tree.ReferenceAction{
	scpb.ForeignKeyConstraint_NO_ACTION:   tree.NoAction,
	scpb.ForeignKeyConstraint_RESTRICT:    tree.Restrict,
	scpb.ForeignKeyConstraint_SET_DEFAULT: tree.SetDefault,
	scpb.ForeignKeyConstraint_SET_NULL:    tree.SetNull,
	scpb.ForeignKeyConstraint_CASCADE:     tree.Cascade,
}

// fkActionTreeToScpb allows the conversion between a
// tree.ReferenceAction and a scpb.ForeignKeyConstraint_Action.
var fkActionTreeToScpb = [...]scpb.ForeignKeyConstraint_Action{
	tree.NoAction:   scpb.ForeignKeyConstraint_NO_ACTION,
	tree.Restrict:   scpb.ForeignKeyConstraint_RESTRICT,
	tree.SetDefault: scpb.ForeignKeyConstraint_SET_DEFAULT,
	tree.SetNull:    scpb.ForeignKeyConstraint_SET_NULL,
	tree.Cascade:    scpb.ForeignKeyConstraint_CASCADE,
}

// ConstraintType is used to identify the type of a constraint.
type ConstraintType string

const (
	// ConstraintTypePK identifies a PRIMARY KEY constraint.
	ConstraintTypePK ConstraintType = "PRIMARY KEY"
	// ConstraintTypeFK identifies a FOREIGN KEY constraint.
	ConstraintTypeFK ConstraintType = "FOREIGN KEY"
	// ConstraintTypeUnique identifies a UNIQUE constraint.
	ConstraintTypeUnique ConstraintType = "UNIQUE"
	// ConstraintTypeCheck identifies a CHECK constraint.
	ConstraintTypeCheck ConstraintType = "CHECK"
)
