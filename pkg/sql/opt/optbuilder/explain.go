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

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func (b *Builder) buildExplain(explain *tree.Explain, inScope *scope) (outScope *scope) {
	opts, err := explain.ParseOptions()
	if err != nil {
		panic(builderError{err})
	}

	// We don't allow the statement under Explain to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	stmtScope := b.buildStmt(explain.Statement, &scope{builder: b})
	// Calculate the presentation, since we will store it in the Explain op.
	stmtScope.setPresentation()

	outScope = inScope.push()

	if opts.Mode != tree.ExplainPlan {
		panic(errorf("only PLAN is supported for EXPLAIN"))
	}

	verboseColumns := opts.Flags.Contains(tree.ExplainFlagVerbose) ||
		opts.Flags.Contains(tree.ExplainFlagTypes)

	// Tree shows the node type with the tree structure.
	b.synthesizeColumn(outScope, "Tree", types.String, nil /* expr */, 0 /* group */)
	if verboseColumns {
		// Level is the depth of the node in the tree (hidden).
		c := b.synthesizeColumn(outScope, "Level", types.Int, nil /* expr */, 0 /* group */)
		c.hidden = true
		// Type is the node type (hidden).
		c = b.synthesizeColumn(outScope, "Type", types.String, nil /* expr */, 0 /* group */)
		c.hidden = true
	}
	// Field is the part of the node that a row of output pertains to.
	b.synthesizeColumn(outScope, "Field", types.String, nil /* expr */, 0 /* group */)
	// Description contains details about the field.
	b.synthesizeColumn(outScope, "Description", types.String, nil /* expr */, 0 /* group */)
	if verboseColumns {
		// Columns is the type signature of the data source.
		b.synthesizeColumn(outScope, "Columns", types.String, nil /* expr */, 0 /* group */)
		// Ordering indicates the known ordering of the data from this source.
		b.synthesizeColumn(outScope, "Ordering", types.String, nil /* expr */, 0 /* group */)
	}

	def := memo.ExplainOpDef{
		Options: opts,
		ColList: make(opt.ColList, len(outScope.cols)),
		Props:   stmtScope.physicalProps,
	}
	for i := range outScope.cols {
		def.ColList[i] = outScope.cols[i].id
	}
	outScope.group = b.factory.ConstructExplain(stmtScope.group, b.factory.InternExplainOpDef(&def))
	return outScope
}
