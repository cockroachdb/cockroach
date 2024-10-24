// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CreateTrigger handles the CREATE TRIGGER statement.
func (tc *Catalog) CreateTrigger(n *tree.CreateTrigger) {
	ctx := context.Background()
	tableName := n.TableName.ToTableName()
	ds, _, err := tc.ResolveDataSource(ctx, cat.Flags{}, &tableName)
	if err != nil {
		panic(err)
	}
	triggerFunc, ok := tc.udfs[n.FuncName.String()]
	if !ok {
		panic(errors.Newf("unknown function: %s", n.FuncName))
	}
	if len(triggerFunc.Overloads) != 1 {
		panic(errors.Newf("trigger function %s must have exactly one overload", n.FuncName))
	}
	ol := triggerFunc.Overloads[0]
	funcReturn := ol.ReturnType(nil /* args */)
	if !funcReturn.Identical(types.Trigger) {
		panic(errors.Newf("trigger function %s must return type trigger", n.FuncName))
	}
	var allEventTypes tree.TriggerEventTypeSet
	for i := range n.Events {
		allEventTypes.Add(n.Events[i].EventType)
	}
	if err = cat.ValidateCreateTrigger(n, ds, allEventTypes); err != nil {
		panic(err)
	}
	var newTransitionAlias, oldTransitionAlias tree.Name
	for _, transition := range n.Transitions {
		if transition.IsNew {
			newTransitionAlias = transition.Name
		} else {
			oldTransitionAlias = transition.Name
		}
	}
	funcArgs := make(tree.Datums, len(n.FuncArgs))
	for i, arg := range n.FuncArgs {
		funcArgs[i] = tree.NewDString(arg)
	}
	var whenExpr string
	if n.When != nil {
		whenExpr = tree.AsStringWithFlags(n.When, tree.FmtSerializable)
	}
	trigger := Trigger{
		TriggerName:               n.Name,
		TriggerActionTime:         n.ActionTime,
		TriggerEvents:             n.Events,
		TriggerTableID:            ds.ID(),
		TriggerNewTransitionAlias: newTransitionAlias,
		TriggerOldTransitionAlias: oldTransitionAlias,
		TriggerForEachRow:         n.ForEach == tree.TriggerForEachRow,
		TriggerWhenExpr:           whenExpr,
		TriggerFuncID:             cat.StableID(catid.UserDefinedOIDToID(ol.Oid)),
		TriggerFuncArgs:           funcArgs,
		TriggerFuncBody:           ol.Body,
		TriggerEnabled:            true,
	}
	switch t := ds.(type) {
	case *View:
		t.Triggers = append(t.Triggers, trigger)
	case *Table:
		t.Triggers = append(t.Triggers, trigger)
	default:
		panic(errors.Newf("triggers can only be created on a table or view"))
	}
}

// DropTrigger handles the DROP TRIGGER statement.
func (tc *Catalog) DropTrigger(n *tree.DropTrigger) {
	if n.DropBehavior != tree.DropDefault {
		panic(errors.Newf("test catalog does not support DROP TRIGGER with CASCADE or RESTRICT"))
	}
	ctx := context.Background()
	tableName := n.Table.ToTableName()
	ds, _, err := tc.ResolveDataSource(ctx, cat.Flags{}, &tableName)
	if err != nil {
		panic(err)
	}
	dropTrigger := func(triggers []Trigger) []Trigger {
		for i := range triggers {
			if triggers[i].Name() == n.Trigger {
				return append(triggers[:i], triggers[i+1:]...)
			}
		}
		if !n.IfExists {
			panic(errors.Newf("trigger %s does not exist", n.Trigger))
		}
		return triggers
	}
	switch t := ds.(type) {
	case *View:
		t.Triggers = dropTrigger(t.Triggers)
	case *Table:
		t.Triggers = dropTrigger(t.Triggers)
	default:
		panic(errors.Newf("triggers can only be created on a table or view"))
	}
}
