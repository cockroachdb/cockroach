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
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, _ tree.Statement, t *tree.AlterTableSetTrigger,
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
	b BuildCtx, tableID catid.DescID, triggerName tree.Name, tableName string, enable bool,
) {
	// Find the trigger by name.
	triggerElts := b.ResolveTrigger(tableID, triggerName, ResolveParams{
		IsExistenceOptional: false,
	})
	if triggerElts == nil {
		panic(sqlerrors.NewUndefinedTriggerError(string(triggerName), tableName))
	}

	// Find the Trigger element to get the trigger ID.
	var trigger *scpb.Trigger
	triggerElts.ForEach(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if t, ok := e.(*scpb.Trigger); ok && target != scpb.ToAbsent {
			trigger = t
		}
	})

	if trigger == nil {
		panic(sqlerrors.NewUndefinedTriggerError(string(triggerName), tableName))
	}

	elem := &scpb.TriggerEnabled{
		TableID:   trigger.TableID,
		TriggerID: trigger.TriggerID,
	}

	// Presence of TriggerEnabled means enabled, absence means disabled.
	if enable {
		b.Add(elem)
	} else {
		b.Drop(elem)
	}
}
