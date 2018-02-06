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

package build

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (xform.ExprView).
type Builder struct {
	factory exec.Factory
	ev      xform.ExprView
	ivh     tree.IndexedVarHelper
}

// NewBuilder constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
func NewBuilder(factory exec.Factory, ev xform.ExprView) *Builder {
	container := (*metadataContainer)(ev.Metadata())
	ivh := tree.MakeIndexedVarHelper(container, ev.Metadata().NumColumns())
	return &Builder{factory: factory, ev: ev, ivh: ivh}
}

// Build constructs the execution node tree and returns its root node if no
// error occurred.
func (b *Builder) Build() (exec.Node, error) {
	return b.build(b.ev)
}

func (b *Builder) build(ev xform.ExprView) (exec.Node, error) {
	if ev.IsRelational() {
		return b.buildRelational(ev)
	}
	return b.buildScalar(ev), nil
}
