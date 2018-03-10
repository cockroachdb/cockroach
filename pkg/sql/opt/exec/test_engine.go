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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// TestEngine enables the creation and execution of plans for testing.
//
// An instance of TestEngine can be used for planing and running one query.
//
// Each instance of TestEngine has an associated Catalog and Factory.
type TestEngine interface {
	// Catalog returns the Catalog associated with this engine
	Catalog() opt.Catalog

	// Factory returns the execution Factory associated with this engine, which
	// can be used to create an execution plan.
	Factory() Factory

	// Columns returns the column information for the given execution node
	// (created through the factory).
	Columns(n Node) sqlbase.ResultColumns

	// Execute runs the given execution node (created through the Factory) and
	// returns the results as a Datum table.
	Execute(n Node) ([]tree.Datums, error)

	// Explain executes EXPLAIN (VERBOSE) on the given execution node (created
	// through the Factory) and returns the results as a Datum table.
	Explain(n Node) ([]tree.Datums, error)

	// Close cleans up any state associated with construction or execution of
	// nodes. It must always be called as the last step in using an engine
	// instance.
	Close()
}

// TestEngineFactory is an interface through which TestEngines can be created.
//
// It is implemented by the SQL executor and can be accessed from tests using
// TestServerInterface.Executor().(TestEngineFactory), without depending on the
// sql package.
type TestEngineFactory interface {
	// NewTestEngine creates an execution engine.
	NewTestEngine(defaultDatabase string) TestEngine
}
