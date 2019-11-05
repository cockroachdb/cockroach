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
type StatementMutator func(stmt tree.Statement) (changed bool)

// ForAllStatements executes stmtMutator on all SQL statements of input. The
// list of changed statements is returned.
func ForAllStatements(
	input string, stmtMutator StatementMutator,
) (output string, changed []string) {
	parsed, err := parser.Parse(input)
	if err != nil {
		return input, nil
	}

	var sb strings.Builder
	for _, p := range parsed {
		stmtChanged := stmtMutator(p.AST)
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

// ColumnFamilyMutator modifies a CREATE TABLE statement without any FAMILY
// definitions to have random FAMILY definitions.
func ColumnFamilyMutator(stmt tree.Statement) (changed bool) {
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
	rand.Shuffle(len(columns), func(i, j int) {
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
		if rand.Intn(2) != 0 {
			ast.Defs = append(ast.Defs, fd)
			fd = &tree.FamilyTableDef{}
		}
	}
	return true
}
