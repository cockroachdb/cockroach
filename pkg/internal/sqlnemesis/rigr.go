// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// RIGR: Referential Integrity Graph Randomizer
//
// Creates a random graph of tables connected by FK and trigger references.

// numTables must be >= 2
func RIGRSetup(
	ctx context.Context, rng *rand.Rand, prefix string, numTables int, opts []randgen.TableOption,
) []tree.Statement {
	var colID int
	generateColumnIndexSuffix := func() string {
		suffix := strconv.Itoa(colID)
		colID += 1
		return suffix
	}

	// Start with N random tables, which are the nodes in the graph.
	stmts := make([]tree.Statement, numTables)
	for i := range numTables {
		stmts[i] = randgen.RandCreateTableWithColumnIndexNumberGenerator(
			ctx, rng, prefix, i, opts, generateColumnIndexSuffix,
		)
	}

	// Randomly add edges between nodes. Each edge represents a relationship
	// between two tables that can be validated, such as a FK relationship. Adding
	// edges will also add columns to the tables and possibly DDL statements.
	numEdges := numTables - 1
	for range numEdges {
		// Pick tables I and J such that I != J.
		i := rng.Intn(numTables)
		j := rng.Intn(numTables - 1)
		if j >= i {
			j += 1
		}
		// Randomly pick a type of edge to add.
		switch rng.Intn(1) {
		case 0:
			stmts = addFKEdge(rng, stmts, i, j, generateColumnIndexSuffix)
		default:
			// TODO: trigger edge
			// TODO: duplicate column edge
			// TODO: double-entry ledger edge
			// TODO: mutual exclusion edge
		}
	}

	// Randomly add self-edges to some nodes, which represent either CHECK
	// constraints referencing multiple columns or multiple indexes.
	numSelfEdges := numTables - 1
	for range numSelfEdges {
		i := rng.Intn(numTables)
		// Randomly pick a type of self-edge to add.
		switch rng.Intn(2) {
		case 0:
			stmts = addIndexEdge(rng, stmts, i, generateColumnIndexSuffix)
		case 1:
			stmts = addFKEdge(rng, stmts, i, i, generateColumnIndexSuffix)
		default:
			// TODO: CHECK edge
			// TODO: self-duplicate-column edge
		}
	}

	// Return the DDL statements needed to create this graph.
	return stmts
}

// addFKEdge adds a FK relationship from table I (child) to table J (parent) by
// creating a new column in each table.
//
// NOTE: this is very similar to the foreignKeyMutator in randgen, should this
// be combined somehow? or should we be calling into sqlsmith to do this?
func addFKEdge(
	rng *rand.Rand, ddl []tree.Statement, i, j int, generateColumnIndexSuffix func() string,
) []tree.Statement {
	createI := ddl[i].(*tree.CreateTable)
	createJ := ddl[j].(*tree.CreateTable)

	var stmts []tree.Statement
	t := randgen.RandColumnType(rng)

	// ALTER TABLE ADD COLUMN to the child table
	childColName := tree.Name(fmt.Sprintf("col%d_%s_fk_child", i, generateColumnIndexSuffix()))
	childCol, err := tree.NewColumnTableDef(childColName, t, false /* isSerial */, nil)
	if err != nil {
		return ddl
	}
	childCol.Nullable.Nullability = tree.Null
	stmts = append(stmts, &tree.AlterTable{
		Table: createI.Table.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: childCol,
			},
		},
	})

	// CREATE INDEX on the child FK column
	stmts = append(stmts, &tree.CreateIndex{
		Table:   createI.Table,
		Type:    idxtype.FORWARD,
		Columns: tree.IndexElemList{tree.IndexElem{Column: childColName}},
	})

	// ALTER TABLE ADD COLUMN to the parent table
	parentColName := tree.Name(fmt.Sprintf("col%d_%s_fk_parent", j, generateColumnIndexSuffix()))
	parentCol, err := tree.NewColumnTableDef(parentColName, t, false /* isSerial */, nil)
	if err != nil {
		return ddl
	}
	parentCol.Nullable.Nullability = tree.Null
	stmts = append(stmts, &tree.AlterTable{
		Table: createJ.Table.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: parentCol,
			},
		},
	})

	// CREATE UNIQUE INDEX on the parent FK column
	stmts = append(stmts, &tree.CreateIndex{
		Table:   createJ.Table,
		Unique:  true,
		Type:    idxtype.FORWARD,
		Columns: tree.IndexElemList{tree.IndexElem{Column: parentColName}},
	})

	// TODO: multiple columns?

	var actions tree.ReferenceActions
	switch rng.Intn(4) {
	case 0:
		actions.Delete = tree.NoAction
	case 1:
		actions.Delete = tree.Restrict
	case 2:
		actions.Delete = tree.SetNull
	case 3:
		actions.Delete = tree.Cascade
	}
	switch rng.Intn(4) {
	case 0:
		actions.Update = tree.NoAction
	case 1:
		actions.Update = tree.Restrict
	case 2:
		actions.Update = tree.SetNull
	case 3:
		actions.Update = tree.Cascade
	}
	stmts = append(stmts, &tree.AlterTable{
		Table: createI.Table.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{&tree.AlterTableAddConstraint{
			ConstraintDef: &tree.ForeignKeyConstraintTableDef{
				Table:    createJ.Table,
				FromCols: tree.NameList{childColName},
				ToCols:   tree.NameList{parentColName},
				Actions:  actions,
			},
		}},
	})

	return append(ddl, stmts...)
}

// most of this is copied from makeCreateIndex. should we be calling into sqlsmith?
func addIndexEdge(
	rng *rand.Rand, ddl []tree.Statement, i int, generateColumnIndexSuffix func() string,
) []tree.Statement {
	createI := ddl[i].(*tree.CreateTable)
	var defs []*tree.ColumnTableDef
	for _, def := range createI.Defs {
		if colDef, ok := def.(*tree.ColumnTableDef); ok {
			defs = append(defs, colDef)
		}
	}
	unique := rng.Intn(2) > 0
	var cols tree.IndexElemList
	seen := map[tree.Name]bool{}
	indexType := idxtype.FORWARD
	for len(cols) < 1 || rng.Intn(2) > 0 {
		col := defs[rng.Intn(len(defs))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		// If this is the first column and it's invertible (i.e., JSONB), make an inverted index.
		if len(cols) == 0 &&
			colinfo.ColumnTypeIsOnlyInvertedIndexable(tree.MustBeStaticallyKnownType(col.Type)) {
			indexType = idxtype.INVERTED
			unique = false
			cols = append(cols, tree.IndexElem{
				Column: col.Name,
			})
			break
		}
		if colinfo.ColumnTypeIsIndexable(tree.MustBeStaticallyKnownType(col.Type)) {
			cols = append(cols, tree.IndexElem{
				Column:    col.Name,
				Direction: randDirection(rng),
			})
		}
	}
	var storing tree.NameList
	for indexType.SupportsStoring() && rng.Intn(2) > 0 {
		col := defs[rng.Intn(len(defs))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		storing = append(storing, col.Name)
	}

	invisibility := tree.IndexInvisibility{Value: 0.0}
	if notvisible := rng.Intn(6) == 1; notvisible {
		invisibility.Value = 1.0
		if rng.Intn(2) > 0 {
			invisibility.Value = rng.Float64() // [0.0, 1.0)
			invisibility.FloatProvided = true
		}
	}

	return append(ddl, &tree.CreateIndex{
		Table:        createI.Table,
		Unique:       unique,
		Columns:      cols,
		Storing:      storing,
		Type:         indexType,
		Invisibility: invisibility,
	})
}

var orderDirections = []tree.Direction{
	tree.DefaultDirection,
	tree.Ascending,
	tree.Descending,
}

func randDirection(rng *rand.Rand) tree.Direction {
	return orderDirections[rng.Intn(len(orderDirections))]
}
