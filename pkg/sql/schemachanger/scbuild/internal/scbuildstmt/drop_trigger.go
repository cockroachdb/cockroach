// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func DropTrigger(b BuildCtx, n *tree.DropTrigger) {
	noticeSender := b.EvalCtx().ClientNoticeSender
	if n.DropBehavior == tree.DropCascade {
		panic(unimplemented.NewWithIssue(128151, "cascade dropping triggers"))
	}

	// NOTE: DROP TRIGGER requires the user to have ownership of the table.
	tableElems := b.ResolveTable(n.Table, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequireOwnership:    true,
	})
	_, _, tbl := scpb.FindTable(tableElems)
	if tbl == nil {
		// IF EXISTS was true and the table was not found.
		noticeSender.BufferClientNotice(b,
			pgnotice.Newf("relation \"%v\" does not exist, skipping", n.Table))
		return
	}

	triggerElems := b.ResolveTrigger(tbl.TableID, n.Trigger, ResolveParams{
		IsExistenceOptional: n.IfExists,
	})
	_, _, trigger := scpb.FindTrigger(triggerElems)
	if trigger == nil {
		// IF EXISTS was true and the trigger was not found.
		noticeSender.BufferClientNotice(b,
			pgnotice.Newf("trigger \"%v\" for relation \"%v\" does not exist, skipping",
				n.Trigger, n.Table))
		return
	}

	triggerElems.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.Trigger, *scpb.TriggerDeps:
			// Dropping is a no-op for other element types.
			b.Drop(e)
		}
	})
	b.LogEventForExistingTarget(trigger)
}
