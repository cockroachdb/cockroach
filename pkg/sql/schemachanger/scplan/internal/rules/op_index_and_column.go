// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// Skill all IndexColumn removal ops for indexes which are also being removed.
func init() {

	ic := mkNodeVars("index-column")
	index := mkNodeVars("index")
	relationID, indexID := rel.Var("relation-id"), rel.Var("index-id")

	registerOpRule(
		"skip index-column removal ops on index removal",
		ic.node,
		screl.MustQuery(
			ic.Type((*scpb.IndexColumn)(nil)),
			index.typeFilter(isIndex),
			joinOnIndexID(ic, index, relationID, indexID),
			ic.joinTargetNode(),
			ic.targetStatus(scpb.ToAbsent, scpb.Transient),
			ic.currentStatus(scpb.Status_PUBLIC, scpb.Status_TRANSIENT_PUBLIC),
			index.joinTarget(),
			index.targetStatus(scpb.ToAbsent, scpb.Transient),
		),
	)
}
