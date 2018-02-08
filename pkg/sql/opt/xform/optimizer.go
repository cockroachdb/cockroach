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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
)

// OptimizeSteps is passed to NewOptimizer, and specifies the maximum number
// of normalization and exploration transformations that will be applied by the
// optimizer. This can be used to effectively disable the optimizer (if set to
// zero), to debug the optimizer (by disabling optimizations past a certain
// point), or to limit the running time of the optimizer.
type OptimizeSteps int

const (
	// OptimizeNone instructs the optimizer to suppress all transformations.
	// The unaltered input expression tree will become the output expression
	// tree. This effectively disables the optimizer.
	OptimizeNone = OptimizeSteps(0)

	// OptimizeAll instructs the optimizer to continue applying transformations
	// until the best plan has been found. There is no limit to the number of
	// steps that the optimizer will take to get there.
	OptimizeAll = OptimizeSteps(^uint(0) >> 1)
)

// Optimizer transforms an input expression tree into the logically equivalent
// output expression tree with the lowest possible execution cost.
//
// To use the optimizer, construct an input expression tree by invoking
// construction methods on the Optimizer.Factory instance. The factory
// transforms the input expression into its canonical form as part of
// construction. Pass the root of the tree constructed by the factory to the
// Optimize method, along with a set of required physical properties that the
// expression must provide. The optimizer will return an ExprView over the
// output expression tree with the lowest cost.
type Optimizer struct {
	f   *factory
	mem *memo
}

// NewOptimizer constructs an instance of the optimizer. Input expressions can
// use metadata from the specified catalog. The maxSteps parameter limits the
// number of normalization and exploration transformations that will be applied
// by the optimizer. If maxSteps is zero, then the unaltered input expression
// tree becomes the output expression tree (because no transformations are
// applied).
func NewOptimizer(cat optbase.Catalog, maxSteps OptimizeSteps) *Optimizer {
	f := newFactory(cat, maxSteps)
	return &Optimizer{f: f, mem: f.mem}
}

// Factory returns a factory interface that the caller uses to construct an
// input expression tree. The root of the resulting tree can be passed to the
// Optimize method in order to find the lowest cost plan.
func (o *Optimizer) Factory() opt.Factory {
	return o.f
}

// Optimize returns the expression which satisfies the required physical
// properties at the lowest possible execution cost, but is still logically
// equivalent to the given expression. If there is a cost "tie", then any one
// of the qualifying lowest cost expressions may be selected by the optimizer.
// TODO(andyk): For now, the input tree becomes the output tree, with no
// transformations applied to it.
func (o *Optimizer) Optimize(root opt.GroupID, required *opt.PhysicalProps) ExprView {
	// TODO(andyk): Need to intern the physical props once there are actually
	// fields in the physical props.
	return makeExprView(o.mem, root, opt.MinPhysPropsID)
}
