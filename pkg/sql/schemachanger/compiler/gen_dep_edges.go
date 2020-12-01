package compiler

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"

func generateTargetStateDepEdges(g *targetStateGraph, t targets.Target, s targets.State) (_ error) {
	switch t := t.(type) {
	case *targets.DropColumn:
		generateDropColumnDepEdges(g, t, s)
	case *targets.AddIndex:
		generateAddIndexDepEdges(g, t, s)
	case *targets.DropIndex:
		generateDropIndexDepEdges(g, t, s)
	}
	return nil
}

func generateDropIndexDepEdges(g *targetStateGraph, t *targets.DropIndex, s targets.State) {
	switch s {
	case targets.StateDeleteAndWriteOnly:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.AddIndex:
				if ot.IndexID == t.ReplacedBy {
					g.addDepEdge(t, s, ot, targets.StatePublic)
				}
			case *targets.DropColumn:
				if t.TableID != ot.TableID || !columnsContainsID(t.ColumnIDs, ot.ColumnID) {
					continue
				}
				g.addDepEdge(t, s, ot, targets.StateDeleteAndWriteOnly)
			}
		}
	}
}

func generateAddIndexDepEdges(g *targetStateGraph, t *targets.AddIndex, s targets.State) {
	// AddIndex in the Public state depends on any DropIndex it is replacing being
	// in the DeleteAndWriteOnly state
	switch s {
	case targets.StatePublic:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.DropIndex:
				if ot.IndexID == t.ReplacementFor {
					g.addDepEdge(t, s, ot, targets.StateDeleteAndWriteOnly)
				}
			}
		}
	}
}

func generateDropColumnDepEdges(g *targetStateGraph, t *targets.DropColumn, s targets.State) {
	switch s {
	case targets.StateDeleteAndWriteOnly:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.DropIndex:
				if t.TableID != ot.TableID || !columnsContainsID(ot.ColumnIDs, t.ColumnID) {
					continue
				}
				g.addDepEdge(t, s, ot, targets.StateDeleteAndWriteOnly)
			}
		}
	}
}

/*

- DropColumn:
  - StateDeleteAndWriteOnly
    - DropIndex:
       - <predicate over fields> -> DeleteAndWriteOnly
*/
