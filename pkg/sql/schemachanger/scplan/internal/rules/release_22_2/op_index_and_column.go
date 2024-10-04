// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// Skill all IndexColumn removal ops for indexes which are also being removed.
func init() {

	ic := MkNodeVars("index-column")
	index := MkNodeVars("index")
	relationID, indexID := rel.Var("relation-id"), rel.Var("index-id")

	registerOpRule(
		"skip index-column removal ops on index removal",
		ic.Node,
		screl.MustQuery(
			ic.Type((*scpb.IndexColumn)(nil)),
			index.TypeFilter(rulesVersionKey, IsIndex),
			JoinOnIndexID(ic, index, relationID, indexID),
			ic.JoinTargetNode(),
			ic.TargetStatus(scpb.ToAbsent, scpb.Transient),
			ic.CurrentStatus(scpb.Status_PUBLIC, scpb.Status_TRANSIENT_PUBLIC),
			index.JoinTarget(),
			index.TargetStatus(scpb.ToAbsent, scpb.Transient),
		),
	)
}
