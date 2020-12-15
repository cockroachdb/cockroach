package compiler

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"

func generateTargetStateDepEdges(g *SchemaChange, t targets.Target, s targets.State) (_ error) {
	switch t := t.(type) {
	case *targets.AddColumn:
		generateAddColumnDepEdges(g, t, s)
	case *targets.DropColumn:
		generateDropColumnDepEdges(g, t, s)
	case *targets.AddIndex:
		generateAddIndexDepEdges(g, t, s)
	case *targets.DropIndex:
		generateDropIndexDepEdges(g, t, s)
	}
	return nil
}

func generateAddColumnDepEdges(g *SchemaChange, t *targets.AddColumn, s targets.State) {
	switch s {
	case targets.State_DELETE_AND_WRITE_ONLY, targets.State_PUBLIC:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.AddIndex:
				if t.TableID != ot.TableID ||
					(!columnsContainsID(ot.Index.ColumnIDs, t.Column.ID) &&
						!columnsContainsID(ot.Index.StoreColumnIDs, t.Column.ID) &&
						!columnsContainsID(ot.Index.ExtraColumnIDs, t.Column.ID)) {
					continue
				}
				g.addDepEdge(t, s, ot, s)
			}
		}
	}
}

func generateDropIndexDepEdges(g *SchemaChange, t *targets.DropIndex, s targets.State) {
	switch s {
	case targets.State_DELETE_AND_WRITE_ONLY:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.AddIndex:
				if ot.Index.ID == t.ReplacedBy {
					g.addDepEdge(t, s, ot, targets.State_PUBLIC)
				}
			case *targets.DropColumn:
				if t.TableID != ot.TableID || !columnsContainsID(t.ColumnIDs, ot.ColumnID) {
					continue
				}
				g.addDepEdge(t, s, ot, targets.State_DELETE_AND_WRITE_ONLY)
			}
		}
	}
}

func generateAddIndexDepEdges(g *SchemaChange, t *targets.AddIndex, s targets.State) {
	// AddIndex in the Public state depends on any DropIndex it is replacing being
	// in the DeleteAndWriteOnly state
	switch s {
	case targets.State_PUBLIC:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.DropIndex:
				if ot.IndexID == t.ReplacementFor {
					g.addDepEdge(t, s, ot, targets.State_DELETE_AND_WRITE_ONLY)
				}
			}
		}
	}
}

func generateDropColumnDepEdges(g *SchemaChange, t *targets.DropColumn, s targets.State) {
	switch s {
	case targets.State_DELETE_AND_WRITE_ONLY:
		for _, ot := range g.targets {
			switch ot := ot.(type) {
			case *targets.DropIndex:
				if t.TableID != ot.TableID || !columnsContainsID(ot.ColumnIDs, t.ColumnID) {
					continue
				}
				g.addDepEdge(t, s, ot, targets.State_DELETE_AND_WRITE_ONLY)
			}
		}
	}
}

/*

- DropColumn:
  - State_DELETE_AND_WRITE_ONLY
    - DropIndex:
       - <predicate over fields> -> DeleteAndWriteOnly
*/
