// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// FromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL creates
// a clause which leads to the outer clause failing to unify
// if the passed element `from` is a Table, `to` is a RowLevelTTl,
// and there does not exist a Node with the same Target as
// `fromTarget` in PUBLIC status.
// It is used to suppress rule "descriptor drop right before dependent element removal"
// for the special case where we drop a rowLevelTTL table in mixed
// version state for forward compatibility (issue #86672).
var FromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL = screl.Schema.DefNotJoin3(
	"fromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL",
	"fromTarget", "fromEl", "toEl", func(fromTarget, fromEl, toEl rel.Var) rel.Clauses {
		n := rel.Var("n")
		return rel.Clauses{
			fromEl.Type((*scpb.Table)(nil)),
			toEl.Type((*scpb.RowLevelTTL)(nil)),
			n.Type((*screl.Node)(nil)),
			n.AttrEqVar(screl.Target, fromTarget),
			screl.Schema.DefNotJoin1("nodeHasNoPublicStatus", "n", func(n rel.Var) rel.Clauses {
				public := rel.Var("public")
				return rel.Clauses{
					public.Eq(scpb.Status_PUBLIC),
					n.AttrEqVar(screl.CurrentStatus, public),
				}
			})(n),
		}
	})
