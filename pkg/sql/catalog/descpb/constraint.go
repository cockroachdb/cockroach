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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CompositeKeyMatchMethodValue allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var CompositeKeyMatchMethodValue = [...]ForeignKeyReference_Match{
	tree.MatchSimple:  ForeignKeyReference_SIMPLE,
	tree.MatchFull:    ForeignKeyReference_FULL,
	tree.MatchPartial: ForeignKeyReference_PARTIAL,
}

// ForeignKeyReferenceMatchValue allows the conversion from a
// ForeignKeyReference_Match to a tree.ReferenceCompositeKeyMatchMethod.
// This should match CompositeKeyMatchMethodValue.
var ForeignKeyReferenceMatchValue = [...]tree.CompositeKeyMatchMethod{
	ForeignKeyReference_SIMPLE:  tree.MatchSimple,
	ForeignKeyReference_FULL:    tree.MatchFull,
	ForeignKeyReference_PARTIAL: tree.MatchPartial,
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

// ForeignKeyReferenceActionType allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var ForeignKeyReferenceActionType = [...]tree.ReferenceAction{
	catpb.ForeignKeyAction_NO_ACTION:   tree.NoAction,
	catpb.ForeignKeyAction_RESTRICT:    tree.Restrict,
	catpb.ForeignKeyAction_SET_DEFAULT: tree.SetDefault,
	catpb.ForeignKeyAction_SET_NULL:    tree.SetNull,
	catpb.ForeignKeyAction_CASCADE:     tree.Cascade,
}

// ForeignKeyReferenceActionValue allows the conversion between a
// catpb.ForeignKeyAction_Action and a tree.ReferenceAction.
var ForeignKeyReferenceActionValue = [...]catpb.ForeignKeyAction{
	tree.NoAction:   catpb.ForeignKeyAction_NO_ACTION,
	tree.Restrict:   catpb.ForeignKeyAction_RESTRICT,
	tree.SetDefault: catpb.ForeignKeyAction_SET_DEFAULT,
	tree.SetNull:    catpb.ForeignKeyAction_SET_NULL,
	tree.Cascade:    catpb.ForeignKeyAction_CASCADE,
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

// ConstraintDetail describes a constraint.
//
// TODO(ajwerner): Lift this up a level of abstraction next to the
// Immutable and have it store those for the ReferencedTable.
type ConstraintDetail struct {
	Kind         ConstraintType
	ConstraintID ConstraintID
	Columns      []string
	Details      string
	Unvalidated  bool

	// Only populated for PK and Unique Constraints with an index.
	Index *IndexDescriptor

	// Only populated for Unique Constraints without an index.
	UniqueWithoutIndexConstraint *UniqueWithoutIndexConstraint

	// Only populated for FK Constraints.
	FK              *ForeignKeyConstraint
	ReferencedTable *TableDescriptor

	// Only populated for Check Constraints.
	CheckConstraint *TableDescriptor_CheckConstraint
}
