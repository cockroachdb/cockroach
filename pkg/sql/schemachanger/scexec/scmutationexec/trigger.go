// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) AddTrigger(ctx context.Context, op scop.AddTrigger) error {
	tbl, err := i.checkOutTable(ctx, op.Trigger.TableID)
	if err != nil {
		return err
	}
	if op.Trigger.TriggerID >= tbl.NextTriggerID {
		tbl.NextTriggerID = op.Trigger.TriggerID + 1
	}
	tbl.Triggers = append(tbl.Triggers, descpb.TriggerDescriptor{
		ID: op.Trigger.TriggerID,
	})
	return nil
}

func (i *immediateVisitor) SetTriggerName(ctx context.Context, op scop.SetTriggerName) error {
	trigger, err := i.checkOutTrigger(ctx, op.Name.TableID, op.Name.TriggerID)
	if err != nil {
		return err
	}
	trigger.Name = op.Name.Name
	return nil
}

func (i *immediateVisitor) SetTriggerEnabled(ctx context.Context, op scop.SetTriggerEnabled) error {
	trigger, err := i.checkOutTrigger(ctx, op.Enabled.TableID, op.Enabled.TriggerID)
	if err != nil {
		return err
	}
	trigger.Enabled = op.Enabled.Enabled
	return nil
}

func (i *immediateVisitor) SetTriggerTiming(ctx context.Context, op scop.SetTriggerTiming) error {
	trigger, err := i.checkOutTrigger(ctx, op.Timing.TableID, op.Timing.TriggerID)
	if err != nil {
		return err
	}
	trigger.ActionTime = op.Timing.ActionTime
	trigger.ForEachRow = op.Timing.ForEachRow
	return nil
}

func (i *immediateVisitor) SetTriggerEvents(ctx context.Context, op scop.SetTriggerEvents) error {
	trigger, err := i.checkOutTrigger(ctx, op.Events.TableID, op.Events.TriggerID)
	if err != nil {
		return err
	}
	for _, ev := range op.Events.Events {
		trigger.Events = append(trigger.Events, &descpb.TriggerDescriptor_Event{
			Type:        ev.Type,
			ColumnNames: ev.ColumnNames,
		})
	}
	return nil
}

func (i *immediateVisitor) SetTriggerTransition(
	ctx context.Context, op scop.SetTriggerTransition,
) error {
	trigger, err := i.checkOutTrigger(ctx, op.Transition.TableID, op.Transition.TriggerID)
	if err != nil {
		return err
	}
	trigger.NewTransitionAlias = op.Transition.NewTransitionAlias
	trigger.OldTransitionAlias = op.Transition.OldTransitionAlias
	return nil
}

func (i *immediateVisitor) SetTriggerWhen(ctx context.Context, op scop.SetTriggerWhen) error {
	trigger, err := i.checkOutTrigger(ctx, op.When.TableID, op.When.TriggerID)
	if err != nil {
		return err
	}
	trigger.WhenExpr = op.When.WhenExpr
	return nil
}

func (i *immediateVisitor) SetTriggerFunctionCall(
	ctx context.Context, op scop.SetTriggerFunctionCall,
) error {
	trigger, err := i.checkOutTrigger(ctx, op.FunctionCall.TableID, op.FunctionCall.TriggerID)
	if err != nil {
		return err
	}
	trigger.FuncID = op.FunctionCall.FuncID
	trigger.FuncArgs = op.FunctionCall.FuncArgs
	trigger.FuncBody = op.FunctionCall.FuncBody
	return nil
}

func (i *immediateVisitor) SetTriggerForwardReferences(
	ctx context.Context, op scop.SetTriggerForwardReferences,
) error {
	trigger, err := i.checkOutTrigger(ctx, op.Deps.TableID, op.Deps.TriggerID)
	if err != nil {
		return err
	}
	trigger.DependsOn = op.Deps.UsesRelationIDs
	trigger.DependsOnTypes = op.Deps.UsesTypeIDs
	trigger.DependsOnRoutines = op.Deps.UsesRoutineIDs
	return nil
}

func (i *immediateVisitor) RemoveTrigger(ctx context.Context, op scop.RemoveTrigger) error {
	// NOTE: we must still remove the trigger even if the table is being dropped,
	// in order to ensure that the back-references are cleaned up later.
	tbl, err := i.checkOutTable(ctx, op.Trigger.TableID)
	if err != nil {
		return err
	}
	var found bool
	for idx := range tbl.Triggers {
		if tbl.Triggers[idx].ID == op.Trigger.TriggerID {
			tbl.Triggers = append(tbl.Triggers[:idx], tbl.Triggers[idx+1:]...)
			found = true
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("failed to find trigger with ID %d in table %q (%d)",
			op.Trigger.TriggerID, tbl.GetName(), tbl.GetID())
	}
	return nil
}
