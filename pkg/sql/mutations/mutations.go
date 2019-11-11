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
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// StatementMutator defines a func that can change a statement.
type StatementMutator func(rng *rand.Rand, stmt tree.Statement) (changed bool)

// MultiStatementMutation defines a func that returns additional statements,
// but must not change any of the statements passed.
type MultiStatementMutation func(rng *rand.Rand, stmts []tree.Statement) (additional []tree.Statement)

// ForAllStatements executes stmtMutator on all SQL statements of input. The
// list of changed statements is returned.
func ForAllStatements(
	rng *rand.Rand, input string, stmtMutator StatementMutator,
) (output string, changed []string) {
	parsed, err := parser.Parse(input)
	if err != nil {
		return input, nil
	}

	var sb strings.Builder
	for _, p := range parsed {
		stmtChanged := stmtMutator(rng, p.AST)
		if !stmtChanged {
			sb.WriteString(p.SQL)
		} else {
			s := p.AST.String()
			sb.WriteString(s)
			changed = append(changed, s)
		}
		sb.WriteString(";\n")
	}
	return sb.String(), changed
}

// ForeignKeyMutator adds ALTER TABLE ADD FOREIGN KEY statements.
func ForeignKeyMutator(rng *rand.Rand, stmts []tree.Statement) (additional []tree.Statement) {
	// Find columns in the tables.
	cols := map[tree.TableName][]*tree.ColumnTableDef{}
	byName := map[tree.TableName]*tree.CreateTable{}
	var tables []*tree.CreateTable
	for _, stmt := range stmts {
		table, ok := stmt.(*tree.CreateTable)
		if !ok {
			continue
		}
		tables = append(tables, table)
		byName[table.Table] = table
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

	// Keep track of referencing columns since we have a limitation that a
	// column can only be used by one FK.
	usedCols := map[tree.TableName]map[tree.Name]bool{}

	// Create some FKs.
	for rng.Intn(2) == 0 {
		// Choose a random table.
		table := tables[rng.Intn(len(tables))]
		if _, ok := usedCols[table.Table]; !ok {
			usedCols[table.Table] = map[tree.Name]bool{}
		}
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
			// Prevent self references.
			// TODO(mjibson): Circular references are not
			// prevented, but it would be nice to detect and
			// prevent them.
			if refTable == table.Table || len(refCols) < len(fkCols) {
				continue
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

// ColumnFamilyMutator modifies a CREATE TABLE statement without any FAMILY
// definitions to have random FAMILY definitions.
func ColumnFamilyMutator(rng *rand.Rand, stmt tree.Statement) (changed bool) {
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
