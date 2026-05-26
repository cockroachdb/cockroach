// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
)

// ReferenceAction is the method used to maintain referential integrity through
// foreign keys.
type ReferenceAction semenumpb.ForeignKeyAction

// The values for ReferenceAction. It has a one-to-one mapping to semenumpb.ForeignKeyAction.
const (
	NoAction ReferenceAction = iota
	Restrict
	SetNull
	SetDefault
	Cascade
)

// ReferenceActions contains the actions specified to maintain referential
// integrity through foreign keys for different operations.
type ReferenceActions struct {
	Delete ReferenceAction
	Update ReferenceAction
}

// Format implements the NodeFormatter interface.
func (node *ReferenceActions) Format(ctx *FmtCtx) {
	if node.Delete != NoAction {
		ctx.WriteString(" ON DELETE ")
		ctx.WriteString(node.Delete.String())
	}
	if node.Update != NoAction {
		ctx.WriteString(" ON UPDATE ")
		ctx.WriteString(node.Update.String())
	}
}

// HasUpdateAction returns true if any update action is set.
func (node *ReferenceActions) HasUpdateAction() bool {
	// NoAction and Restrict are currently equivalent.
	return node.Update != NoAction && node.Update != Restrict
}

// HasDisallowedActionForComputedFKCol return true if an action is set that
// isn't compatible with an FK over computed columns.
func (node *ReferenceActions) HasDisallowedActionForComputedFKCol() bool {
	// We disallow any actions that modify column values. NoAction and Restrict
	// are equivalent and always allowed. 'ON DELETE CASCADE' is also allowed
	// since it removes the entire row instead of modifying the computed column.
	return node.HasUpdateAction() ||
		(node.Delete != NoAction && node.Delete != Restrict && node.Delete != Cascade)
}

// ForeignKeyReferenceActionType allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var ForeignKeyReferenceActionType = [...]ReferenceAction{
	semenumpb.ForeignKeyAction_NO_ACTION:   NoAction,
	semenumpb.ForeignKeyAction_RESTRICT:    Restrict,
	semenumpb.ForeignKeyAction_SET_DEFAULT: SetDefault,
	semenumpb.ForeignKeyAction_SET_NULL:    SetNull,
	semenumpb.ForeignKeyAction_CASCADE:     Cascade,
}

// ForeignKeyReferenceActionValue allows the conversion between a
// semenumpb.ForeignKeyAction_Action and a tree.ReferenceAction.
var ForeignKeyReferenceActionValue = [...]semenumpb.ForeignKeyAction{
	NoAction:   semenumpb.ForeignKeyAction_NO_ACTION,
	Restrict:   semenumpb.ForeignKeyAction_RESTRICT,
	SetDefault: semenumpb.ForeignKeyAction_SET_DEFAULT,
	SetNull:    semenumpb.ForeignKeyAction_SET_NULL,
	Cascade:    semenumpb.ForeignKeyAction_CASCADE,
}

// String implements the fmt.Stringer interface.
func (x ReferenceAction) String() string {
	switch x {
	case Restrict:
		return "RESTRICT"
	case SetDefault:
		return "SET DEFAULT"
	case SetNull:
		return "SET NULL"
	case Cascade:
		return "CASCADE"
	default:
		return strconv.Itoa(int(x))
	}
}

// CompositeKeyMatchMethod is the algorithm use when matching composite keys.
// See https://github.com/cockroachdb/cockroach/issues/20305 or
// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
// different composite foreign key matching methods.
type CompositeKeyMatchMethod semenumpb.Match

// The values for CompositeKeyMatchMethod.
const (
	MatchSimple CompositeKeyMatchMethod = iota
	MatchFull
	MatchPartial // Note: PARTIAL not actually supported at this point.
)

// CompositeKeyMatchMethodType allows the conversion from a
// ForeignKeyReference_Match to a tree.ReferenceCompositeKeyMatchMethod.
// This should match CompositeKeyMatchMethodValue.
var CompositeKeyMatchMethodType = [...]CompositeKeyMatchMethod{
	semenumpb.Match_SIMPLE:  MatchSimple,
	semenumpb.Match_FULL:    MatchFull,
	semenumpb.Match_PARTIAL: MatchPartial,
}

// CompositeKeyMatchMethodValue allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var CompositeKeyMatchMethodValue = [...]semenumpb.Match{
	MatchSimple:  semenumpb.Match_SIMPLE,
	MatchFull:    semenumpb.Match_FULL,
	MatchPartial: semenumpb.Match_PARTIAL,
}

// String implements the fmt.Stringer interface.
func (x CompositeKeyMatchMethod) String() string {
	switch x {
	case MatchSimple:
		return "MATCH SIMPLE"
	case MatchFull:
		return "MATCH FULL"
	case MatchPartial:
		return "MATCH PARTIAL"
	default:
		return strconv.Itoa(int(x))
	}
}
