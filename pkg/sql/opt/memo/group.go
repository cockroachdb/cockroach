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

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// exprGroup represents a group of relational query plans that are logically
// equivalent to on another. The group points to the first member of the group,
// and subsequent members can be accessed via calls to RelExpr.NextExpr. The
// group maintains the logical properties shared by members of the group, as
// well as the physical properties and cost of the best expression in the group
// once optimization is complete.
//
// See comments for Memo, RelExpr, Relational, and Physical for more details.
type exprGroup interface {
	// memo is the memo which contains the group.
	memo() *Memo

	// firstExpr points to the first member expression in the group. Other members
	// of the group can be accessed via calls to RelExpr.NextExpr.
	firstExpr() RelExpr

	// relational are the relational properties shared by members of the group.
	relational() *props.Relational

	// bestProps returns a per-group instance of bestProps. This is the zero
	// value until optimization is complete.
	bestProps() *bestProps
}

// bestProps contains the properties of the "best" expression in group. The best
// expression is the expression which is part of the lowest-cost tree for the
// overall query. It is well-defined because the lowest-cost tree does not
// contain multiple expressions from the same group.
//
// These are not properties of the group per se but they are stored within each
// group for efficiency.
type bestProps struct {
	// Required properties with respect to which the best expression was
	// optimized.
	required *physical.Required

	// Provided properties, which must be compatible with the required properties.
	//
	// We store these properties in-place because the structure is very small; if
	// that changes we will want to intern them, similar to the required
	// properties.
	provided physical.Provided

	// Cost of the best expression.
	cost Cost
}
