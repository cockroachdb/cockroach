// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/stretchr/testify/require"
)

// TestFactory is a general API test for Factory. It is not intended as an
// exhaustive test of all factory Construct methods.
func TestFactory(t *testing.T) {
	f := NewFactory(exec.StubFactory{})

	n, err := f.ConstructValues(
		[][]tree.TypedExpr{
			{tree.NewDInt(1), tree.NewDString("one")},
			{tree.NewDInt(2), tree.NewDString("two")},
			{tree.NewDInt(3), tree.NewDString("three")},
		},
		colinfo.ResultColumns{
			{Name: "number", Typ: types.Int},
			{Name: "word", Typ: types.String},
		},
	)
	require.NoError(t, err)
	f.AnnotateNode(n, exec.EstimatedStatsID, &exec.EstimatedStats{
		RowCount: 10.0,
		Cost:     1000.0,
	})

	n, err = f.ConstructFilter(n, tree.DBoolTrue, exec.OutputOrdering{{ColIdx: 0, Direction: encoding.Ascending}})
	require.NoError(t, err)
	f.AnnotateNode(n, exec.EstimatedStatsID, &exec.EstimatedStats{
		RowCount: 5.0,
		Cost:     1500.0,
	})

	plan, err := f.ConstructPlan(n, nil /* subqueries */, nil /* cascades */, nil /* checks */)
	require.NoError(t, err)
	p := plan.(*Plan)

	tp := treeprinter.New()
	printTree(p.Root, tp)
	exp := `
op with *explain.filterArgs
 ├── columns: (number int, word string)
 ├── ordering: +number
 ├── estimated row count: 5
 ├── estimated cost: 1500
 └── op with *explain.valuesArgs
      ├── columns: (number int, word string)
      ├── estimated row count: 10
      └── estimated cost: 1000
`
	require.Equal(t, strings.TrimLeft(exp, "\n"), tp.String())
}

func printTree(n *Node, tp treeprinter.Node) {
	tp = tp.Childf("op with %T", n.args)
	tp.Childf("columns: %s", n.Columns().String(true /* printTypes */, false /* showHidden */))
	if len(n.Ordering()) > 0 {
		tp.Childf("ordering: %v", n.Ordering().String(n.Columns()))
	}
	stats := n.annotations[exec.EstimatedStatsID].(*exec.EstimatedStats)
	tp.Childf("estimated row count: %v", stats.RowCount)
	tp.Childf("estimated cost: %v", stats.Cost)
	for i := 0; i < n.ChildCount(); i++ {
		printTree(n.Child(i), tp)
	}
}
