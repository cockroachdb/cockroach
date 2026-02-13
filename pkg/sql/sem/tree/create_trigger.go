// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"

type CreateTrigger struct {
	Replace     bool
	Name        Name
	ActionTime  TriggerActionTime
	Events      []*TriggerEvent
	TableName   *UnresolvedObjectName
	Transitions []*TriggerTransition
	ForEach     TriggerForEach
	When        Expr
	FuncName    *UnresolvedName
	FuncArgs    []string

	// FuncBody is the body of the trigger function with fully-qualified names.
	// TODO(#128536): Pass this information through `memo.CreateTriggerExpr`
	// instead.
	FuncBody string

	// FuncBodyOverride, if set, is used by the optbuilder instead of reading the
	// function body from the catalog. This is set by the declarative schema
	// changer during CREATE OR REPLACE FUNCTION on a trigger function, so that
	// the new function body can be analyzed in each dependent trigger's table
	// context to capture the correct dependencies.
	FuncBodyOverride string
}

var _ Statement = &CreateTrigger{}

// Format implements the NodeFormatter interface.
func (node *CreateTrigger) Format(ctx *FmtCtx) {
	if node.Replace {
		ctx.WriteString("CREATE OR REPLACE TRIGGER ")
	} else {
		ctx.WriteString("CREATE TRIGGER ")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" ")
	node.ActionTime.Format(ctx)
	ctx.WriteString(" ")
	for i := range node.Events {
		if i > 0 {
			ctx.WriteString(" OR ")
		}
		node.Events[i].Format(ctx)
	}
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.TableName)
	if len(node.Transitions) > 0 {
		ctx.WriteString(" REFERENCING ")
		for i := range node.Transitions {
			if i > 0 {
				ctx.WriteString(" ")
			}
			node.Transitions[i].Format(ctx)
		}
	}
	ctx.WriteString(" ")
	node.ForEach.Format(ctx)
	if node.When != nil {
		ctx.WriteString(" WHEN ")
		ctx.FormatNode(node.When)
	}
	ctx.WriteString(" EXECUTE FUNCTION ")
	ctx.FormatNode(node.FuncName)
	ctx.WriteString("(")
	for i, arg := range node.FuncArgs {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatStringConstant(arg)
	}
	ctx.WriteString(")")
}

type TriggerActionTime uint8

const (
	TriggerActionTimeUnknown TriggerActionTime = iota
	TriggerActionTimeBefore
	TriggerActionTimeAfter
	TriggerActionTimeInsteadOf
)

// TriggerActionTimeFromTree allows the conversion from a
// semenumpb.TriggerActionTime to a tree.TriggerActionTime.
var TriggerActionTimeFromTree = [...]semenumpb.TriggerActionTime{
	TriggerActionTimeUnknown:   semenumpb.TriggerActionTime_ACTION_UNKNOWN,
	TriggerActionTimeBefore:    semenumpb.TriggerActionTime_BEFORE,
	TriggerActionTimeAfter:     semenumpb.TriggerActionTime_AFTER,
	TriggerActionTimeInsteadOf: semenumpb.TriggerActionTime_INSTEAD_OF,
}

// TriggerActionTimeToTree allows the conversion from a
// tree.TriggerActionTime to a semenumpb.TriggerActionTime.
var TriggerActionTimeToTree = [...]TriggerActionTime{
	semenumpb.TriggerActionTime_ACTION_UNKNOWN: TriggerActionTimeUnknown,
	semenumpb.TriggerActionTime_BEFORE:         TriggerActionTimeBefore,
	semenumpb.TriggerActionTime_AFTER:          TriggerActionTimeAfter,
	semenumpb.TriggerActionTime_INSTEAD_OF:     TriggerActionTimeInsteadOf,
}

// Format implements the NodeFormatter interface.
func (node *TriggerActionTime) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerActionTimeUnknown:
		ctx.WriteString("UNKNOWN")
	case TriggerActionTimeBefore:
		ctx.WriteString("BEFORE")
	case TriggerActionTimeAfter:
		ctx.WriteString("AFTER")
	case TriggerActionTimeInsteadOf:
		ctx.WriteString("INSTEAD OF")
	}
}

type TriggerEventType uint8

const (
	TriggerEventTypeUnknown TriggerEventType = iota
	TriggerEventInsert
	TriggerEventUpdate
	TriggerEventDelete
	TriggerEventTruncate
	TriggerEventTypeMax = iota - 1
)

// TriggerEventTypeFromTree allows the conversion from a
// tree.TriggerEventType to a semenumpb.TriggerEventType.
var TriggerEventTypeFromTree = [...]semenumpb.TriggerEventType{
	TriggerEventTypeUnknown: semenumpb.TriggerEventType_EVENT_UNKNOWN,
	TriggerEventInsert:      semenumpb.TriggerEventType_INSERT,
	TriggerEventUpdate:      semenumpb.TriggerEventType_UPDATE,
	TriggerEventDelete:      semenumpb.TriggerEventType_DELETE,
	TriggerEventTruncate:    semenumpb.TriggerEventType_TRUNCATE,
}

// TriggerEventTypeToTree allows the conversion from a
// semenumpb.TriggerEventType to a tree.TriggerEventType.
var TriggerEventTypeToTree = [...]TriggerEventType{
	semenumpb.TriggerEventType_EVENT_UNKNOWN: TriggerEventTypeUnknown,
	semenumpb.TriggerEventType_INSERT:        TriggerEventInsert,
	semenumpb.TriggerEventType_UPDATE:        TriggerEventUpdate,
	semenumpb.TriggerEventType_DELETE:        TriggerEventDelete,
	semenumpb.TriggerEventType_TRUNCATE:      TriggerEventTruncate,
}

// Format implements the NodeFormatter interface.
func (node *TriggerEventType) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerEventTypeUnknown:
		ctx.WriteString("UNKNOWN")
	case TriggerEventInsert:
		ctx.WriteString("INSERT")
	case TriggerEventUpdate:
		ctx.WriteString("UPDATE")
	case TriggerEventDelete:
		ctx.WriteString("DELETE")
	case TriggerEventTruncate:
		ctx.WriteString("TRUNCATE")
	}
}

// String implements the Stringer interface.
func (node *TriggerEventType) String() string {
	return AsString(node)
}

// TriggerEventTypeSet is a set of TriggerEventType values, used to conveniently
// check for the presence of a given event type in a trigger definition.
type TriggerEventTypeSet uint8

// Ensure that TriggerEventTypeSet can contain all TriggerEventTypes.
const _ = TriggerEventTypeSet(1) << TriggerEventTypeMax

// MakeTriggerEventTypeSet creates a TriggerEventTypeSet from a list of
// TriggerEventType values.
func MakeTriggerEventTypeSet(types ...TriggerEventType) TriggerEventTypeSet {
	var s TriggerEventTypeSet
	for _, t := range types {
		s.Add(t)
	}
	return s
}

// Add adds a TriggerEventType value to the set.
func (s *TriggerEventTypeSet) Add(t TriggerEventType) {
	*s |= 1 << t
}

// Contains returns true if the set contains the given TriggerEventType value.
func (s TriggerEventTypeSet) Contains(t TriggerEventType) bool {
	return s&(1<<t) != 0
}

type TriggerEvent struct {
	EventType TriggerEventType
	Columns   NameList
}

// Format implements the NodeFormatter interface.
func (node *TriggerEvent) Format(ctx *FmtCtx) {
	node.EventType.Format(ctx)
	if len(node.Columns) > 0 {
		ctx.WriteString(" OF ")
		ctx.FormatNode(&node.Columns)
	}
}

type TriggerTransition struct {
	Name  Name
	IsNew bool
	IsRow bool
}

// Format implements the NodeFormatter interface.
func (node *TriggerTransition) Format(ctx *FmtCtx) {
	if node.IsNew {
		ctx.WriteString("NEW")
	} else {
		ctx.WriteString("OLD")
	}
	if node.IsRow {
		ctx.WriteString(" ROW")
	} else {
		ctx.WriteString(" TABLE")
	}
	ctx.WriteString(" AS ")
	node.Name.Format(ctx)
}

type TriggerForEach uint8

const (
	TriggerForEachStatement TriggerForEach = iota
	TriggerForEachRow
)

// Format implements the NodeFormatter interface.
func (node *TriggerForEach) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerForEachStatement:
		ctx.WriteString("FOR EACH STATEMENT")
	case TriggerForEachRow:
		ctx.WriteString("FOR EACH ROW")
	}
}

// DropTrigger represents a DROP TRIGGER statement.
type DropTrigger struct {
	IfExists     bool
	Trigger      Name
	Table        *UnresolvedObjectName
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP TRIGGER ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatName(string(node.Trigger))
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.Table)
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}
