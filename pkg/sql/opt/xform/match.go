// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package xform

// This file contains various helper functions that can be used to
// check properties of an ExprView.

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// MatchesTupleOfConstants returns true if the expression is a TupleOp with
// ConstValue children.
func MatchesTupleOfConstants(ev ExprView) bool {
	if ev.Operator() != opt.TupleOp {
		return false
	}
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		if !child.IsConstValue() {
			return false
		}
	}
	return true
}

// MatchesConstNull returns true if ev is a ConstOp with NULL value.
// TODO(radu): perhaps add a NullOp instead.
func MatchesConstNull(ev ExprView) bool {
	return ev.Operator() == opt.ConstOp && ev.Private() == tree.DNull
}
