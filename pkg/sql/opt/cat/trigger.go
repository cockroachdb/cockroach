// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// Trigger is an interface to a trigger on a table or view, which executes a
// trigger function in response to a pre-defined mutation of the table.
type Trigger interface {
	// Name is the name of the trigger. It is unique within a given table, and
	// cannot be qualified.
	Name() tree.Name

	// ActionTime defines whether the trigger should be fired before, after, or
	// instead of the triggering event.
	ActionTime() tree.TriggerActionTime

	// EventCount returns the number of events that the trigger is configured to
	// fire on.
	EventCount() int

	// Event returns the ith event that the trigger is configured to fire on.
	Event(i int) tree.TriggerEvent

	// NewTransitionAlias is the name to be used for the NEW transition table.
	// If no alias was specified, the result is the empty string.
	NewTransitionAlias() tree.Name

	// OldTransitionAlias is the name to be used for the OLD transition table.
	// If no alias was specified, the result is the empty string.
	OldTransitionAlias() tree.Name

	// ForEachRow returns true if the trigger function should be invoked once for
	// each row affected by the triggering event. If false, the trigger function
	// is invoked once per statement.
	ForEachRow() bool

	// WhenExpr is the optional filter expression that determines whether the
	// trigger function should be invoked. If no WHEN clause was specified, the
	// result is the empty string.
	WhenExpr() string

	// FuncID is the ID of the function that will be called when the trigger
	// fires.
	FuncID() StableID

	// FuncArgs is a list of constant string arguments for the trigger function.
	FuncArgs() tree.Datums

	// FuncBody is the set of body statements for the trigger function with
	// fully-qualified names, resolved when the trigger was created.
	FuncBody() string

	// Enabled is true if the trigger is currently enabled.
	Enabled() bool
}

// ValidateCreateTrigger checks that the CREATE TRIGGER statement is valid. It
// performs only validation that can be performed without access to the
// database schema, and without actually building the component expressions.
func ValidateCreateTrigger(
	ct *tree.CreateTrigger, ds DataSource, allEventTypes tree.TriggerEventTypeSet,
) error {
	var hasTargetCols bool
	for i := range ct.Events {
		if len(ct.Events[i].Columns) > 0 {
			hasTargetCols = true
			break
		}
	}

	// Check that the target table/view is valid.
	switch t := ds.(type) {
	case Table:
		if t.IsSystemTable() || t.IsVirtualTable() {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"permission denied: \"%s\" is a system catalog", t.Name())
		}
		if t.IsMaterializedView() {
			return errors.WithDetail(pgerror.Newf(pgcode.WrongObjectType,
				"relation \"%s\" cannot have triggers", t.Name()),
				"This operation is not supported for materialized views.")
		}
		if ct.ActionTime == tree.TriggerActionTimeInsteadOf {
			return errors.WithDetail(pgerror.Newf(pgcode.WrongObjectType,
				"\"%s\" is a table", t.Name()),
				"Tables cannot have INSTEAD OF triggers.")
		}
		if !ct.Replace {
			for i := 0; i < t.TriggerCount(); i++ {
				if t.Trigger(i).Name() == ct.Name {
					return pgerror.Newf(pgcode.DuplicateObject,
						"trigger \"%s\" for relation \"%s\" already exists", ct.Name, t.Name())
				}
			}
		}
	case View:
		if t.IsSystemView() {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"permission denied: \"%s\" is a system catalog", t.Name())
		}
		// Views can only use row-level INSTEAD OF, or statement-level BEFORE or
		// AFTER timing. The former is checked below.
		if ct.ActionTime != tree.TriggerActionTimeInsteadOf && ct.ForEach == tree.TriggerForEachRow {
			return errors.WithDetail(pgerror.Newf(pgcode.WrongObjectType,
				"\"%s\" is a view", t.Name()),
				"Views cannot have row-level BEFORE or AFTER triggers.")
		}
		if allEventTypes.Contains(tree.TriggerEventTruncate) {
			return errors.WithDetail(pgerror.Newf(pgcode.WrongObjectType,
				"\"%s\" is a view", t.Name()),
				"Views cannot have TRUNCATE triggers.")
		}
		if !ct.Replace {
			for i := 0; i < t.TriggerCount(); i++ {
				if t.Trigger(i).Name() == ct.Name {
					return pgerror.Newf(pgcode.DuplicateObject,
						"trigger \"%s\" for relation \"%s\" already exists", ct.Name, t.Name())
				}
			}
		}
	default:
		return pgerror.Newf(pgcode.WrongObjectType, "relation \"%s\" cannot have triggers", t.Name())
	}

	// TRUNCATE is not compatible with FOR EACH ROW.
	if ct.ForEach == tree.TriggerForEachRow && allEventTypes.Contains(tree.TriggerEventTruncate) {
		return pgerror.New(pgcode.FeatureNotSupported,
			"TRUNCATE FOR EACH ROW triggers are not supported")
	}

	// Validate usage of INSTEAD OF timing.
	if ct.ActionTime == tree.TriggerActionTimeInsteadOf {
		if ct.ForEach != tree.TriggerForEachRow {
			return pgerror.New(pgcode.FeatureNotSupported,
				"INSTEAD OF triggers must be FOR EACH ROW")
		}
		if ct.When != nil {
			return pgerror.New(pgcode.FeatureNotSupported,
				"INSTEAD OF triggers cannot have WHEN conditions")
		}
		if hasTargetCols {
			return pgerror.New(pgcode.FeatureNotSupported,
				"INSTEAD OF triggers cannot have column lists")
		}
	}

	// Validate usage of transition tables.
	if len(ct.Transitions) > 0 {
		if ct.ActionTime != tree.TriggerActionTimeAfter {
			return pgerror.New(pgcode.InvalidObjectDefinition,
				"transition table name can only be specified for an AFTER trigger")
		}
		if allEventTypes.Contains(tree.TriggerEventTruncate) {
			return pgerror.New(pgcode.InvalidObjectDefinition,
				"TRUNCATE triggers cannot specify transition tables")
		}
		if len(ct.Events) > 1 {
			return pgerror.New(pgcode.FeatureNotSupported,
				"transition tables cannot be specified for triggers with more than one event")
		}
		if hasTargetCols {
			return pgerror.New(pgcode.FeatureNotSupported,
				"transition tables cannot be specified for triggers with column lists")
		}
	}
	if len(ct.Transitions) == 2 && ct.Transitions[0].Name == ct.Transitions[1].Name {
		return pgerror.Newf(pgcode.InvalidObjectDefinition,
			"OLD TABLE name and NEW TABLE name cannot be the same")
	}
	var sawOld, sawNew bool
	for i := range ct.Transitions {
		if ct.Transitions[i].IsNew {
			if !allEventTypes.Contains(tree.TriggerEventInsert) &&
				!allEventTypes.Contains(tree.TriggerEventUpdate) {
				return pgerror.New(pgcode.InvalidObjectDefinition,
					"NEW TABLE can only be specified for an INSERT or UPDATE trigger")
			}
			if sawNew {
				return pgerror.Newf(pgcode.Syntax, "cannot specify NEW more than once")
			}
			sawNew = true
		} else {
			if !allEventTypes.Contains(tree.TriggerEventDelete) &&
				!allEventTypes.Contains(tree.TriggerEventUpdate) {
				return pgerror.New(pgcode.InvalidObjectDefinition,
					"OLD TABLE can only be specified for a DELETE or UPDATE trigger")
			}
			if sawOld {
				return pgerror.Newf(pgcode.Syntax, "cannot specify OLD more than once")
			}
			sawOld = true
		}
		if ct.Transitions[i].IsRow {
			// NOTE: Postgres also returns an "unimplemented" error here.
			return errors.WithHint(pgerror.New(pgcode.FeatureNotSupported,
				"ROW variable naming in the REFERENCING clause is not supported"),
				"Use OLD TABLE or NEW TABLE for naming transition tables.")
		}
	}
	return nil
}

// HasRowLevelTriggers returns true if the given table has any row-level
// triggers that match the given action time and event type.
func HasRowLevelTriggers(
	tab Table, actionTime tree.TriggerActionTime, eventToMatch tree.TriggerEventType,
) bool {
	for i := 0; i < tab.TriggerCount(); i++ {
		trigger := tab.Trigger(i)
		if !trigger.Enabled() || !trigger.ForEachRow() ||
			trigger.ActionTime() != actionTime {
			continue
		}
		for j := 0; j < trigger.EventCount(); j++ {
			if eventToMatch == trigger.Event(j).EventType {
				return true
			}
		}
	}
	return false
}

// GetRowLevelTriggers returns the set of row-level triggers for the given
// table and given trigger event type and timing. The triggers are returned in
// the order in which they should be executed.
func GetRowLevelTriggers(
	tab Table, actionTime tree.TriggerActionTime, eventsToMatch tree.TriggerEventTypeSet,
) []Trigger {
	var neededTriggers intsets.Fast
	for i := 0; i < tab.TriggerCount(); i++ {
		trigger := tab.Trigger(i)
		if !trigger.Enabled() || !trigger.ForEachRow() ||
			trigger.ActionTime() != actionTime {
			continue
		}
		for j := 0; j < trigger.EventCount(); j++ {
			if eventsToMatch.Contains(trigger.Event(j).EventType) {
				// The conditions have been met for this trigger to fire.
				neededTriggers.Add(i)
				break
			}
		}
	}
	if neededTriggers.Empty() {
		return nil
	}
	triggers := make([]Trigger, 0, neededTriggers.Len())
	for i, ok := neededTriggers.Next(0); ok; i, ok = neededTriggers.Next(i + 1) {
		triggers = append(triggers, tab.Trigger(i))
	}
	// Triggers fire in alphabetical order of the name. The names are always
	// unique within a given table, so a stable sort is not necessary.
	less := func(i, j int) bool {
		return triggers[i].Name() < triggers[j].Name()
	}
	sort.Slice(triggers, less)
	return triggers
}
