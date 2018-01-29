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

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Node represents a node in the execution tree (currently maps to a
// sql.planNode).
type Node interface{}

// Engine enables the creation and execution of plans. The engine's Factory
// constructs a tree of execution nodes, and the Execute method executes the
// root of that tree, returning results as a Datum table.
type Engine interface {
	// Factory is used to construct the tree of execution nodes. The tree is
	// always built bottom-up. The Construct methods either construct leaf
	// nodes, or they take other nodes previously constructed by this same
	// factory as children.
	Factory() Factory

	// Execute runs the given execution node and returns the results as a Datum
	// table.
	Execute(n Node) ([]tree.Datums, error)

	// Explain executes EXPLAIN (VERBOSE) on the given execution node and
	// returns the results as a Datum table.
	Explain(n Node) ([]tree.Datums, error)

	// Close cleans up any state associated with construction or execution of
	// nodes. It must always be called as the last step in using an engine
	// instance.
	Close()
}
