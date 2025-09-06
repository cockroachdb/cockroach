// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// TODO(drewk): as-is, this only allows for alternative operators, not different
// children, tables, indexes. It also doesn't handle recursion or shared nodes.
type Pheromone struct {
	alternates []opt.Operator
	gistOpName string
	table      cat.Table
	index      cat.Index
	children   []Pheromone
}

func (p *Pheromone) AddAlternates(ops ...opt.Operator) {
	p.alternates = append(p.alternates, ops...)
}

func (p *Pheromone) AddChild(child Pheromone) {
	p.children = append(p.children, child)
}

func (p *Pheromone) Format(tp treeprinter.Node) {
	var buf strings.Builder
	if len(p.alternates) == 0 {
		buf.WriteString(fmt.Sprintf("* (%s)", p.gistOpName))
	}
	for i, alt := range p.alternates {
		if i > 0 {
			buf.WriteString(" | ")
		}
		buf.WriteString(alt.String())
	}
	child := tp.Child(buf.String())
	if p.table != nil {
		child.AddLine(fmt.Sprintf(" table: %s", p.table.Name()))
	}
	if p.index != nil {
		child.AddLine(fmt.Sprintf(" index: %s", p.index.Name()))
	}
	for _, c := range p.children {
		c.Format(child)
	}
}

func DecompileToPheromone(node *Node) Pheromone {
	root := Pheromone{}
	switch node.op {
	case scanOp:
		args := node.args.(*scanArgs)
		root.table, root.index = args.Table, args.Index
		root.AddAlternates(opt.ScanOp, opt.PlaceholderScanOp)
	case indexJoinOp:
		root.table = node.args.(*indexJoinArgs).Table
		root.AddAlternates(opt.IndexJoinOp)
	case lookupJoinOp:
		args := node.args.(*lookupJoinArgs)
		root.table, root.index = args.Table, args.Index
		root.AddAlternates(opt.LookupJoinOp, opt.LockOp)
	case invertedJoinOp:
		args := node.args.(*invertedJoinArgs)
		root.table, root.index = args.Table, args.Index
		root.AddAlternates(opt.InvertedJoinOp)
	case hashJoinOp:
		args := node.args.(*hashJoinArgs)
		switch args.JoinType {
		case descpb.JoinType_INNER:
			root.AddAlternates(opt.InnerJoinOp)
		case descpb.JoinType_LEFT_OUTER:
			root.AddAlternates(opt.LeftJoinOp)
		case descpb.JoinType_RIGHT_OUTER:
			root.AddAlternates(opt.RightJoinOp)
		case descpb.JoinType_FULL_OUTER:
			root.AddAlternates(opt.FullJoinOp)
		case descpb.JoinType_LEFT_SEMI:
			root.AddAlternates(opt.SemiJoinOp)
		case descpb.JoinType_LEFT_ANTI:
			root.AddAlternates(opt.AntiJoinOp)
		default:
			// TODO(drewk): hash join can map to set operations.
			root.AddAlternates(opt.UnknownOp)
		}
	case mergeJoinOp:
		// TODO(drewk): ordering is probably important to specify.
		root.AddAlternates(opt.MergeJoinOp)
	case sortOp:
		// TODO(drewk): ordering is probably important to specify.
		root.AddAlternates(opt.SortOp)
	case topKOp:
		root.AddAlternates(opt.TopKOp)
	default:
		switch node.op {
		case scanOp:
			root.gistOpName = "scan"
		case valuesOp:
			root.gistOpName = "values"
		case groupByOp:
			// TODO(drew): ordering could be important here, may want to match this.
			root.gistOpName = "group by"
		case applyJoinOp:
			root.gistOpName = "apply"
		case hashSetOpOp:
			// Plan gists do not include the type of set operation.
			root.gistOpName = "hash set op"
		case streamingSetOpOp:
			root.gistOpName = "streaming set op"
		case opaqueOp:
			a := node.args.(*opaqueArgs)
			if a.Metadata == nil {
				root.gistOpName = "unknown opaque"
			} else {
				root.gistOpName = strings.ToLower(a.Metadata.String())
			}
		default:
			root.gistOpName = nodeNames[node.op]
		}
	}
	for _, child := range node.children {
		root.AddChild(DecompileToPheromone(child))
	}
	return root
}
