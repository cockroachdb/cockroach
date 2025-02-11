// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// CreateTrigger creates a new trigger on a table in the declarative schema
// changer. It expects that the CREATE TRIGGER statement has already been
// validated, except for cross-DB references.
func CreateTrigger(b BuildCtx, n *tree.CreateTrigger) {
	if n.Replace {
		panic(unimplemented.NewWithIssue(128422, "CREATE OR REPLACE TRIGGER is not supported"))
	}
	b.IncrementSchemaChangeCreateCounter("trigger")

	refProvider := b.BuildReferenceProvider(n)
	relationElements := b.ResolveRelation(n.TableName, ResolveParams{
		RequiredPrivilege: privilege.TRIGGER,
	})
	panicIfSchemaChangeIsDisallowed(relationElements, n)

	// Check for cross-DB references.
	_, _, namespace := scpb.FindNamespace(relationElements)
	validateTypeReferences(b, refProvider, namespace.DatabaseID)
	validateFunctionRelationReferences(b, refProvider, namespace.DatabaseID)
	validateFunctionToFunctionReferences(b, refProvider, namespace.DatabaseID)

	_, _, tbl := scpb.FindTable(relationElements)
	tableID, triggerID := tbl.TableID, b.NextTableTriggerID(tbl.TableID)

	trigger := &scpb.Trigger{
		TableID:   tableID,
		TriggerID: triggerID,
	}
	b.Add(trigger)
	b.Add(&scpb.TriggerName{
		TableID:   tableID,
		TriggerID: triggerID,
		Name:      string(n.Name),
	})
	b.Add(&scpb.TriggerEnabled{
		TableID:   tableID,
		TriggerID: triggerID,
		Enabled:   true, /* triggers are enabled by default */
	})
	b.Add(&scpb.TriggerTiming{
		TableID:    tableID,
		TriggerID:  triggerID,
		ActionTime: tree.TriggerActionTimeFromTree[n.ActionTime],
		ForEachRow: n.ForEach == tree.TriggerForEachRow,
	})
	events := make([]*scpb.TriggerEvent, 0, len(n.Events))
	for _, event := range n.Events {
		columnNames := make([]string, len(event.Columns))
		for i := range event.Columns {
			columnNames[i] = string(event.Columns[i])
		}
		events = append(events, &scpb.TriggerEvent{
			Type:        tree.TriggerEventTypeFromTree[event.EventType],
			ColumnNames: columnNames,
		})
	}
	b.Add(&scpb.TriggerEvents{
		TableID:   tableID,
		TriggerID: triggerID,
		Events:    events,
	})
	var newTransitionAlias, oldTransitionAlias string
	for i := range n.Transitions {
		if n.Transitions[i].IsNew {
			newTransitionAlias = string(n.Transitions[i].Name)
		} else {
			oldTransitionAlias = string(n.Transitions[i].Name)
		}
	}
	if newTransitionAlias != "" || oldTransitionAlias != "" {
		b.Add(&scpb.TriggerTransition{
			TableID:            tableID,
			TriggerID:          triggerID,
			NewTransitionAlias: newTransitionAlias,
			OldTransitionAlias: oldTransitionAlias,
		})
	}
	if n.When != nil {
		when := b.WrapExpression(tableID, n.When)
		b.Add(&scpb.TriggerWhen{
			TableID:   tableID,
			TriggerID: triggerID,
			WhenExpr:  string(when.Expr),
		})
	}
	routineName, err := n.FuncName.ToRoutineName()
	if err != nil {
		panic(err)
	}
	fnElements := b.ResolveRoutine(
		&tree.RoutineObj{FuncName: routineName},
		ResolveParams{RequiredPrivilege: privilege.EXECUTE},
		tree.UDFRoutine,
	)
	var fn *scpb.Function
	fnElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, elem scpb.Element) {
		switch e := elem.(type) {
		case *scpb.Function:
			fn = e
		}
	})
	if fn == nil {
		panic(errors.AssertionFailedf("expected function %v to be resolved", routineName))
	}
	if n.FuncBody == "" {
		panic(errors.AssertionFailedf("expected non-empty function body"))
	}
	b.Add(&scpb.TriggerFunctionCall{
		TableID:   tableID,
		TriggerID: triggerID,
		FuncID:    fn.FunctionID,
		FuncBody:  b.ReplaceSeqTypeNamesInStatements(n.FuncBody, catpb.Function_PLPGSQL),
		FuncArgs:  n.FuncArgs,
	})
	// It is possible for the trigger function to reference the table on which the
	// trigger is defined. In that case, we need to remove the table from the list
	// of referenced relations to avoid a circular dependency.
	relIDs := refProvider.ReferencedRelationIDs().Ordered()
	for i, id := range relIDs {
		if id == tableID {
			relIDs = append(relIDs[:i], relIDs[i+1:]...)
			break
		}
	}
	b.Add(&scpb.TriggerDeps{
		TableID:         tableID,
		TriggerID:       triggerID,
		UsesRelationIDs: relIDs,
		UsesTypeIDs:     refProvider.ReferencedTypes().Ordered(),
		UsesRoutineIDs:  refProvider.ReferencedRoutines().Ordered(),
	})
	b.LogEventForExistingTarget(trigger)
}
