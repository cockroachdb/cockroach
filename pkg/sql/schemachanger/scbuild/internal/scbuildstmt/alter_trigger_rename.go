// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// AlterTriggerRename implements ALTER TRIGGER ... RENAME TO ... for the
// declarative schema changer. It requires ownership of the trigger's table,
// matching the privilege check used by DROP TRIGGER. PostgreSQL does not
// accept IF EXISTS here; CockroachDB supports it as an extension so migration
// scripts can be idempotent.
func AlterTriggerRename(b BuildCtx, n *tree.AlterTriggerRename) {
	noticeSender := b.EvalCtx().ClientNoticeSender

	tableElems := b.ResolveTable(n.Table, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequireOwnership:    true,
	})
	if tableElems == nil {
		// IF EXISTS was true and the table was not found.
		noticeSender.BufferClientNotice(b,
			pgnotice.Newf("relation \"%v\" does not exist, skipping", n.Table))
		return
	}
	tbl := tableElems.FilterTable().MustGetOneElement()
	defer checkTableSchemaChangePrerequisites(b, tableElems, n)()

	triggerElems := b.ResolveTrigger(tbl.TableID, n.Trigger, ResolveParams{
		IsExistenceOptional: n.IfExists,
	})
	if triggerElems == nil {
		// IF EXISTS was true and the trigger was not found.
		noticeSender.BufferClientNotice(b,
			pgnotice.Newf("trigger \"%v\" for relation \"%v\" does not exist, skipping",
				n.Trigger, n.Table))
		return
	}
	oldName := triggerElems.FilterTriggerName().MustGetOneElement()

	// No-op when the new name matches the existing one. PostgreSQL silently
	// short-circuits the same case.
	if oldName.Name == string(n.NewName) {
		return
	}

	// Reject collisions with another trigger on the same table.
	conflictingElems := b.ResolveTrigger(tbl.TableID, n.NewName, ResolveParams{
		IsExistenceOptional: true,
	})
	if conflictingElems != nil {
		conflicting := conflictingElems.FilterTriggerName().MustGetZeroOrOneElement()
		if conflicting != nil && conflicting.TriggerID != oldName.TriggerID {
			tableNS := tableElems.FilterNamespace().MustGetOneElement()
			panic(pgerror.Newf(pgcode.DuplicateObject,
				"trigger %q for relation %q already exists", n.NewName, tableNS.Name))
		}
	}

	newName := &scpb.TriggerName{
		TableID:   oldName.TableID,
		TriggerID: oldName.TriggerID,
		Name:      string(n.NewName),
	}
	b.Drop(oldName)
	b.Add(newName)

	tableNS := tableElems.FilterNamespace().MustGetOneElement()
	fqTableName := tree.MakeTableNameFromPrefix(b.NamePrefix(tbl), tree.Name(tableNS.Name))
	b.LogEventForExistingPayload(newName, &eventpb.RenameTrigger{
		TableName:      fqTableName.FQString(),
		TriggerName:    oldName.Name,
		NewTriggerName: string(n.NewName),
	})
}
