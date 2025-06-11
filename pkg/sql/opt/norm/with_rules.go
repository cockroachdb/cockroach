// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// PushFilterIntoWithRule matches Select->With patterns and pushes
// applicable filters into the With's binding.
var PushFilterIntoWithRule = memo.NewRuleGetter(
	"PushFilterIntoWith",
	"Push filters into With expressions to improve query performance",
	func() Rule {
		return MakeRule(
			"PushFilterIntoWith",
			"Select(With)",
			pushFilterIntoWith,
		)
	},
)

// pushFilterIntoWith implements the PushFilterIntoWith rule.
func pushFilterIntoWith(f *Factory, e memo.RelExpr) {
	select_ := e.(*memo.SelectExpr)
	with, ok := select_.Input.(*memo.WithExpr)
	if !ok {
		return
	}

	if with.WithPrivate.Mtr == tree.CTEMaterializeAlways {
		return
	}

	// Use the PushFilterIntoWith custom function
	result := f.CustomFuncs().PushFilterIntoWith(
		with.Binding,
		with.Input,
		with.WithPrivate,
		select_.Filters,
	)

	f.Memo().ReplaceExpr(e, result)
}

// Register the rule in init
func init() {
	// Register the PushFilterIntoWith rule
	RegisterRuleClass(PushFilterIntoWithRule)
}
