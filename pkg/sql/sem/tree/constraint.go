// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catenumpb"
)

// ReferenceAction is the method used to maintain referential integrity through
// foreign keys.
type ReferenceAction catenumpb.ForeignKeyAction

// The values for ReferenceAction. It has a one-to-one mapping to catenumpb.ForeignKeyAction.
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

// ForeignKeyReferenceActionType allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var ForeignKeyReferenceActionType = [...]ReferenceAction{
	catenumpb.ForeignKeyAction_NO_ACTION:   NoAction,
	catenumpb.ForeignKeyAction_RESTRICT:    Restrict,
	catenumpb.ForeignKeyAction_SET_DEFAULT: SetDefault,
	catenumpb.ForeignKeyAction_SET_NULL:    SetNull,
	catenumpb.ForeignKeyAction_CASCADE:     Cascade,
}

// ForeignKeyReferenceActionValue allows the conversion between a
// catenumpb.ForeignKeyAction_Action and a tree.ReferenceAction.
var ForeignKeyReferenceActionValue = [...]catenumpb.ForeignKeyAction{
	NoAction:   catenumpb.ForeignKeyAction_NO_ACTION,
	Restrict:   catenumpb.ForeignKeyAction_RESTRICT,
	SetDefault: catenumpb.ForeignKeyAction_SET_DEFAULT,
	SetNull:    catenumpb.ForeignKeyAction_SET_NULL,
	Cascade:    catenumpb.ForeignKeyAction_CASCADE,
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
type CompositeKeyMatchMethod catenumpb.Match

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
	catenumpb.Match_SIMPLE:  MatchSimple,
	catenumpb.Match_FULL:    MatchFull,
	catenumpb.Match_PARTIAL: MatchPartial,
}

// CompositeKeyMatchMethodValue allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var CompositeKeyMatchMethodValue = [...]catenumpb.Match{
	MatchSimple:  catenumpb.Match_SIMPLE,
	MatchFull:    catenumpb.Match_FULL,
	MatchPartial: catenumpb.Match_PARTIAL,
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
