// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mutations

import (
	"encoding/json"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var (
	// StatisticsMutator adds ALTER TABLE INJECT STATISTICS statements.
	StatisticsMutator MultiStatementMutation = statisticsMutator

	// ForeignKeyMutator adds ALTER TABLE ADD FOREIGN KEY statements.
	ForeignKeyMutator MultiStatementMutation = foreignKeyMutator

	// ColumnFamilyMutator modifies a CREATE TABLE statement without any FAMILY
	// definitions to have random FAMILY definitions.
	ColumnFamilyMutator StatementMutator = columnFamilyMutator
)

// StatementMutator defines a func that can change a statement.
type StatementMutator func(rng *rand.Rand, stmt tree.Statement) (changed bool)

// MultiStatementMutation defines a func that returns additional statements,
// but must not change any of the statements passed.
type MultiStatementMutation func(rng *rand.Rand, stmts []tree.Statement) (additional []tree.Statement)

// Mutator defines a method that can mutate or add SQL statements.
type Mutator interface {
	Mutate(rng *rand.Rand, stmts []tree.Statement) (mutated []tree.Statement, changed bool)
}

// Mutate implements the Mutator interface.
func (sm StatementMutator) Mutate(
	rng *rand.Rand, stmts []tree.Statement,
) (mutated []tree.Statement, changed bool) {
	for _, stmt := range stmts {
		sc := sm(rng, stmt)
		changed = changed || sc
	}
	return stmts, changed
}

// Mutate implements the Mutator interface.
func (msm MultiStatementMutation) Mutate(
	rng *rand.Rand, stmts []tree.Statement,
) (mutated []tree.Statement, changed bool) {
	additional := msm(rng, stmts)
	if len(additional) == 0 {
		return stmts, false
	}
	return append(stmts, additional...), true
}

// Apply executes all mutators on stmts. It returns the (possibly mutated and
// changed in place) statements and a boolean indicating whether any changes
// were made.
func Apply(
	rng *rand.Rand, stmts []tree.Statement, mutators ...Mutator,
) (mutated []tree.Statement, changed bool) {
	var mc bool
	for _, m := range mutators {
		stmts, mc = m.Mutate(rng, stmts)
		changed = changed || mc
	}
	return stmts, changed
}

// ApplyString executes all mutators on input.
func ApplyString(rng *rand.Rand, input string, mutators ...Mutator) (output string, changed bool) {
	parsed, err := parser.Parse(input)
	if err != nil {
		return input, false
	}

	stmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		stmts[i] = p.AST
	}

	stmts, changed = Apply(rng, stmts, mutators...)
	if !changed {
		return input, false
	}

	var sb strings.Builder
	for _, s := range stmts {
		sb.WriteString(s.String())
		sb.WriteString(";\n")
	}
	return sb.String(), true
}

// TODO(mjibson): This type is copied from sql/stats, but due to that package
// depending on sqlbase, which depends on this package, a cycle would be
// created. Refactor something such that we avoid the cycle. This probably
// means moving all of the rand table/datum stuff out of sqlbase.
type jsonStatistic struct {
	Name          string   `json:"name,omitempty"`
	CreatedAt     string   `json:"created_at"`
	Columns       []string `json:"columns"`
	RowCount      uint64   `json:"row_count"`
	DistinctCount uint64   `json:"distinct_count"`
	NullCount     uint64   `json:"null_count"`
}

func statisticsMutator(rng *rand.Rand, stmts []tree.Statement) (additional []tree.Statement) {
	for _, stmt := range stmts {
		create, ok := stmt.(*tree.CreateTable)
		if !ok {
			continue
		}
		alter := &tree.AlterTable{
			Table: create.Table.ToUnresolvedObjectName(),
		}
		// rowCount should be the same for all columns in a
		// table. Attempt to distribute it over powers of 10.
		var rowCount int64
		if n := rng.Intn(20); n == 0 {
			// ignore
		} else if n <= 10 {
			rowCount = rng.Int63n(10) + 1
			for i := 0; i < n; i++ {
				rowCount *= 10
			}
		} else {
			rowCount = rng.Int63()
		}
		var stats []jsonStatistic
		for _, def := range create.Defs {
			col, ok := def.(*tree.ColumnTableDef)
			if !ok {
				continue
			}
			var nullCount, distinctCount uint64
			if rowCount > 0 {
				if col.Nullable.Nullability != tree.NotNull {
					nullCount = uint64(rng.Int63n(rowCount))
				}
				distinctCount = uint64(rng.Int63n(rowCount))
			}
			// TODO(mjibson): Generate histograms for the first column of indexes.
			stats = append(stats, jsonStatistic{
				Name:          "__auto__",
				CreatedAt:     "2000-01-01 00:00:00+00:00",
				RowCount:      uint64(rowCount),
				Columns:       []string{col.Name.String()},
				DistinctCount: distinctCount,
				NullCount:     nullCount,
			})
		}
		if len(stats) > 0 {
			b, err := json.Marshal(stats)
			if err != nil {
				// Should not happen.
				panic(err)
			}
			alter.Cmds = append(alter.Cmds, &tree.AlterTableInjectStats{
				Stats: tree.NewDString(string(b)),
			})
			additional = append(additional, alter)
		}
	}
	return additional
}

func foreignKeyMutator(rng *rand.Rand, stmts []tree.Statement) (additional []tree.Statement) {
	// Find columns in the tables.
	cols := map[tree.TableName][]*tree.ColumnTableDef{}
	byName := map[tree.TableName]*tree.CreateTable{}

	// Keep track of referencing columns since we have a limitation that a
	// column can only be used by one FK.
	usedCols := map[tree.TableName]map[tree.Name]bool{}

	// Keep track of table dependencies to prevent circular dependencies.
	dependsOn := map[tree.TableName]map[tree.TableName]bool{}

	var tables []*tree.CreateTable
	for _, stmt := range stmts {
		table, ok := stmt.(*tree.CreateTable)
		if !ok {
			continue
		}
		tables = append(tables, table)
		byName[table.Table] = table
		usedCols[table.Table] = map[tree.Name]bool{}
		dependsOn[table.Table] = map[tree.TableName]bool{}
		for _, def := range table.Defs {
			switch def := def.(type) {
			case *tree.ColumnTableDef:
				cols[table.Table] = append(cols[table.Table], def)
			}
		}
	}

	toNames := func(cols []*tree.ColumnTableDef) tree.NameList {
		names := make(tree.NameList, len(cols))
		for i, c := range cols {
			names[i] = c.Name
		}
		return names
	}

	// We cannot mutate the table definitions themselves because 1) we
	// don't know the order of dependencies (i.e., table 1 could reference
	// table 4 which doesn't exist yet) and relatedly 2) we don't prevent
	// circular dependencies. Instead, add new ALTER TABLE commands to the
	// end of a list of statements.

	// Create some FKs.
	for rng.Intn(2) == 0 {
		// Choose a random table.
		table := tables[rng.Intn(len(tables))]
		// Choose a random column subset.
		var fkCols []*tree.ColumnTableDef
		for _, c := range cols[table.Table] {
			if usedCols[table.Table][c.Name] {
				continue
			}
			fkCols = append(fkCols, c)
		}
		if len(fkCols) == 0 {
			continue
		}
		rng.Shuffle(len(fkCols), func(i, j int) {
			fkCols[i], fkCols[j] = fkCols[j], fkCols[i]
		})
		// Pick some randomly short prefix. I'm sure there's a closed
		// form solution to this with a single call to rng.Intn but I'm
		// not sure what to search for.
		i := 1
		for len(fkCols) > i && rng.Intn(2) == 0 {
			i++
		}
		fkCols = fkCols[:i]

		// Check if a table has the needed column types.
	LoopTable:
		for refTable, refCols := range cols {
			// Prevent circular and self references because
			// generating valid INSERTs could become impossible or
			// difficult algorithmically.
			if refTable == table.Table || len(refCols) < len(fkCols) {
				continue
			}

			{
				// To prevent circular references, find all transitive
				// dependencies of refTable and make sure none of them
				// are table.
				stack := []tree.TableName{refTable}
				for i := 0; i < len(stack); i++ {
					curTable := stack[i]
					if curTable == table.Table {
						// table was trying to add a dependency
						// to refTable, but refTable already
						// depends on table (directly or
						// indirectly).
						continue LoopTable
					}
					for t := range dependsOn[curTable] {
						stack = append(stack, t)
					}
				}
			}

			// We found a table with enough columns. Check if it
			// has some columns that are needed types. In order
			// to not use columns multiple times, keep track of
			// available columns.
			availCols := append([]*tree.ColumnTableDef(nil), refCols...)
			var usingCols []*tree.ColumnTableDef
			for len(availCols) > 0 && len(usingCols) < len(fkCols) {
				fkCol := fkCols[len(usingCols)]
				found := false
				for refI, refCol := range availCols {
					if fkCol.Type.Equivalent(refCol.Type) {
						usingCols = append(usingCols, refCol)
						availCols = append(availCols[:refI], availCols[refI+1:]...)
						found = true
						break
					}
				}
				if !found {
					continue LoopTable
				}
			}
			// If we didn't find enough columns, try another table.
			if len(usingCols) != len(fkCols) {
				continue
			}

			// Found a suitable table.
			// TODO(mjibson): prevent the creation of unneeded
			// unique indexes. One may already exist with the
			// correct prefix.
			ref := byName[refTable]
			refColumns := make(tree.IndexElemList, len(usingCols))
			for i, c := range usingCols {
				refColumns[i].Column = c.Name
			}
			for _, c := range fkCols {
				usedCols[table.Table][c.Name] = true
			}
			dependsOn[table.Table][ref.Table] = true
			ref.Defs = append(ref.Defs, &tree.UniqueConstraintTableDef{
				IndexTableDef: tree.IndexTableDef{
					Columns: refColumns,
				},
			})

			additional = append(additional, &tree.AlterTable{
				Table: table.Table.ToUnresolvedObjectName(),
				Cmds: tree.AlterTableCmds{&tree.AlterTableAddConstraint{
					ConstraintDef: &tree.ForeignKeyConstraintTableDef{
						Table:    ref.Table,
						FromCols: toNames(fkCols),
						ToCols:   toNames(usingCols),
					},
				}},
			})
			break
		}
	}

	return additional
}

func columnFamilyMutator(rng *rand.Rand, stmt tree.Statement) (changed bool) {
	ast, ok := stmt.(*tree.CreateTable)
	if !ok {
		return false
	}

	var columns []tree.Name
	isPKCol := map[tree.Name]bool{}
	for _, def := range ast.Defs {
		switch def := def.(type) {
		case *tree.FamilyTableDef:
			return false
		case *tree.ColumnTableDef:
			if def.HasColumnFamily() {
				return false
			}
			// Primary keys must be in the first
			// column family, so don't add them to
			// the list.
			if def.PrimaryKey {
				continue
			}
			columns = append(columns, def.Name)
		case *tree.UniqueConstraintTableDef:
			// If there's an explicit PK index
			// definition, save the columns from it
			// and remove them later.
			if def.PrimaryKey {
				for _, col := range def.Columns {
					isPKCol[col.Column] = true
				}
			}
		}
	}

	if len(columns) <= 1 {
		return false
	}

	// Any columns not specified in column families
	// are auto assigned to the first family, so
	// there's no requirement to exhaust columns here.

	// Remove columns specified in PK index
	// definitions. We need to do this here because
	// index defs and columns can appear in any
	// order in the CREATE TABLE.
	{
		n := 0
		for _, x := range columns {
			if !isPKCol[x] {
				columns[n] = x
				n++
			}
		}
		columns = columns[:n]
	}
	rng.Shuffle(len(columns), func(i, j int) {
		columns[i], columns[j] = columns[j], columns[i]
	})
	fd := &tree.FamilyTableDef{}
	for {
		if len(columns) == 0 {
			if len(fd.Columns) > 0 {
				ast.Defs = append(ast.Defs, fd)
			}
			break
		}
		fd.Columns = append(fd.Columns, columns[0])
		columns = columns[1:]
		// 50% chance to make a new column family.
		if rng.Intn(2) != 0 {
			ast.Defs = append(ast.Defs, fd)
			fd = &tree.FamilyTableDef{}
		}
	}
	return true
}
