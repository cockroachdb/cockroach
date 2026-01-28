// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

func alterTableSetTrigger(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	_ tree.Statement,
	t *tree.AlterTableSetTrigger,
) {
	// Require table ownership (same as DROP TRIGGER).
	_ = b.ResolveTable(tn.ToUnresolvedObjectName(), ResolveParams{
		RequireOwnership: true,
	})

	tableElts := b.QueryByID(tbl.TableID)

	switch t.Scope {
	case tree.TriggerScopeName:
		// Handle a specific named trigger.
		setTriggerEnabled(b, tbl.TableID, t.Name, tn.Object(), t.Enable)
	case tree.TriggerScopeAll, tree.TriggerScopeUser:
		// Handle ALL/USER - iterate all triggers on the table.
		// Collect trigger names first to avoid modifying while iterating.
		var triggerNames []string
		tableElts.ForEach(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			if tn, ok := e.(*scpb.TriggerName); ok {
				// Only consider triggers that exist or are being created.
				if current == scpb.Status_PUBLIC || target == scpb.ToPublic {
					triggerNames = append(triggerNames, tn.Name)
				}
			}
		})
		// Update each trigger.
		for _, name := range triggerNames {
			setTriggerEnabled(b, tbl.TableID, tree.Name(name), tn.Object(), t.Enable)
		}
	}
}

// setTriggerEnabled enables or disables a single trigger by name.
func setTriggerEnabled(
	b BuildCtx,
	tableID catid.DescID,
	triggerName tree.Name,
	tableName string,
	enable bool,
) {
	// Find the trigger by name.
	triggerElts := b.ResolveTrigger(tableID, triggerName, ResolveParams{
		IsExistenceOptional: false,
	})
	if triggerElts == nil {
		panic(sqlerrors.NewUndefinedTriggerError(string(triggerName), tableName))
	}

	// Find the TriggerEnabled element for this trigger.
	var origElem *scpb.TriggerEnabled
	triggerElts.ForEach(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if te, ok := e.(*scpb.TriggerEnabled); ok && target != scpb.ToAbsent {
			origElem = te
		}
	})

	if origElem == nil {
		// This shouldn't happen for valid triggers - every trigger has a TriggerEnabled element.
		panic(sqlerrors.NewUndefinedTriggerError(string(triggerName), tableName))
	}

	// If the trigger is already in the desired state, no-op.
	if origElem.Enabled == enable {
		return
	}

	// Use the SeqNum pattern (like RowLevelTTL) to update the enabled state.
	// Drop the old element and add a new one with an incremented SeqNum.
	// This allows the new element to transition ABSENT -> PUBLIC, which emits
	// the SetTriggerEnabled operation with the new enabled value.
	newElem := &scpb.TriggerEnabled{
		TableID:   origElem.TableID,
		TriggerID: origElem.TriggerID,
		Enabled:   enable,
		SeqNum:    origElem.SeqNum + 1,
	}

	b.Drop(origElem)
	b.Add(newElem)
}
