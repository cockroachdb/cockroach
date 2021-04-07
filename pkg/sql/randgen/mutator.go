// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Mutator defines a method that can mutate or add SQL statements. See the
// sql/mutations package. This interface is defined here to avoid cyclic
// dependencies.
type Mutator interface {
	Mutate(rng *rand.Rand, stmts []tree.Statement) (mutated []tree.Statement, changed bool)
}

// ColumnFamilyMutator is mutations.StatementMutator, but lives here to prevent
// dependency cycles with RandCreateTable.
func ColumnFamilyMutator(rng *rand.Rand, stmt tree.Statement) (changed bool) {
	ast, ok := stmt.(*tree.CreateTable)
	if !ok {
		return false
	}

	var columns []tree.Name
	for _, def := range ast.Defs {
		switch def := def.(type) {
		case *tree.FamilyTableDef:
			return false
		case *tree.ColumnTableDef:
			if def.HasColumnFamily() {
				return false
			}
			if !def.Computed.Virtual {
				columns = append(columns, def.Name)
			}
		}
	}

	if len(columns) <= 1 {
		return false
	}

	// Any columns not specified in column families
	// are auto assigned to the first family, so
	// there's no requirement to exhaust columns here.

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

// tableInfo is a helper struct that contains information necessary for mutating
// indexes. It is used by IndexStoringMutator and PartialIndexMutator.
type tableInfo struct {
	columnNames      []tree.Name
	columnsTableDefs []*tree.ColumnTableDef
	pkCols           []tree.Name
	refColsLists     [][]tree.Name
}

// getTableInfoFromDDLStatements collects tableInfo from every CreateTable
// and AlterTable statement in the given list of statements.
func getTableInfoFromDDLStatements(stmts []tree.Statement) map[tree.Name]tableInfo {
	tables := make(map[tree.Name]tableInfo)
	for _, stmt := range stmts {
		switch ast := stmt.(type) {
		case *tree.CreateTable:
			info := tableInfo{}
			for _, def := range ast.Defs {
				switch ast := def.(type) {
				case *tree.ColumnTableDef:
					info.columnNames = append(info.columnNames, ast.Name)
					info.columnsTableDefs = append(info.columnsTableDefs, ast)
					if ast.PrimaryKey.IsPrimaryKey {
						info.pkCols = []tree.Name{ast.Name}
					}
				case *tree.UniqueConstraintTableDef:
					if ast.PrimaryKey {
						for _, elem := range ast.Columns {
							info.pkCols = append(info.pkCols, elem.Column)
						}
					}
				case *tree.ForeignKeyConstraintTableDef:
					// The tableInfo must have already been created, since FK constraints
					// can only reference tables that already exist.
					if refTableInfo, ok := tables[ast.Table.ObjectName]; ok {
						refTableInfo.refColsLists = append(refTableInfo.refColsLists, ast.ToCols)
						tables[ast.Table.ObjectName] = refTableInfo
					}
				}
			}
			tables[ast.Table.ObjectName] = info
		case *tree.AlterTable:
			for _, cmd := range ast.Cmds {
				switch alterCmd := cmd.(type) {
				case *tree.AlterTableAddConstraint:
					switch constraintDef := alterCmd.ConstraintDef.(type) {
					case *tree.ForeignKeyConstraintTableDef:
						// The tableInfo must have already been created, since ALTER
						// statements come after CREATE statements.
						if info, ok := tables[constraintDef.Table.ObjectName]; ok {
							info.refColsLists = append(info.refColsLists, constraintDef.ToCols)
							tables[constraintDef.Table.ObjectName] = info
						}
					}
				}
			}
		}
	}
	return tables
}

// IndexStoringMutator is a mutations.MultiStatementMutator, but lives here to
// prevent dependency cycles with RandCreateTable.
func IndexStoringMutator(rng *rand.Rand, stmts []tree.Statement) ([]tree.Statement, bool) {
	changed := false
	tables := getTableInfoFromDDLStatements(stmts)
	mapFromIndexCols := func(cols []tree.Name) map[tree.Name]struct{} {
		colMap := map[tree.Name]struct{}{}
		for _, col := range cols {
			colMap[col] = struct{}{}
		}
		return colMap
	}
	generateStoringCols := func(rng *rand.Rand, tableInfo tableInfo, indexCols map[tree.Name]struct{}) []tree.Name {
		var storingCols []tree.Name
		for colOrdinal, col := range tableInfo.columnNames {
			if _, ok := indexCols[col]; ok {
				// Skip PK columns and columns already in the index.
				continue
			}
			if tableInfo.columnsTableDefs[colOrdinal].Computed.Virtual {
				// Virtual columns can't be stored.
				continue
			}
			if rng.Intn(2) == 0 {
				storingCols = append(storingCols, col)
			}
		}
		return storingCols
	}
	for _, stmt := range stmts {
		switch ast := stmt.(type) {
		case *tree.CreateIndex:
			if ast.Inverted {
				continue
			}
			info, ok := tables[ast.Table.ObjectName]
			if !ok {
				continue
			}
			// If we don't have a storing list, make one with 50% chance.
			if ast.Storing == nil && rng.Intn(2) == 0 {
				indexCols := mapFromIndexCols(info.pkCols)
				for _, elem := range ast.Columns {
					indexCols[elem.Column] = struct{}{}
				}
				ast.Storing = generateStoringCols(rng, info, indexCols)
				changed = true
			}
		case *tree.CreateTable:
			info, ok := tables[ast.Table.ObjectName]
			if !ok {
				panic("table info could not be found")
			}
			for _, def := range ast.Defs {
				var idx *tree.IndexTableDef
				switch defType := def.(type) {
				case *tree.IndexTableDef:
					idx = defType
				case *tree.UniqueConstraintTableDef:
					if !defType.PrimaryKey && !defType.WithoutIndex {
						idx = &defType.IndexTableDef
					}
				}
				if idx == nil || idx.Inverted {
					continue
				}
				// If we don't have a storing list, make one with 50% chance.
				if idx.Storing == nil && rng.Intn(2) == 0 {
					indexCols := mapFromIndexCols(info.pkCols)
					for _, elem := range idx.Columns {
						indexCols[elem.Column] = struct{}{}
					}
					idx.Storing = generateStoringCols(rng, info, indexCols)
					changed = true
				}
			}
		}
	}
	return stmts, changed
}

// PartialIndexMutator is a mutations.MultiStatementMutator, but lives here to
// prevent dependency cycles with RandCreateTable. This mutator adds random
// partial index predicate expressions to indexes.
func PartialIndexMutator(rng *rand.Rand, stmts []tree.Statement) ([]tree.Statement, bool) {
	changed := false
	tables := getTableInfoFromDDLStatements(stmts)
	for _, stmt := range stmts {
		switch ast := stmt.(type) {
		case *tree.CreateIndex:
			info, ok := tables[ast.Table.ObjectName]
			if !ok {
				continue
			}

			// If the index is not already a partial index, make it a partial index
			// with a 50% chance. Do not mutate an index that was created to satisfy a
			// FK constraint.
			if ast.Predicate == nil &&
				!hasReferencingConstraint(info, ast.Columns) &&
				rng.Intn(2) == 0 {
				tn := tree.MakeUnqualifiedTableName(ast.Table.ObjectName)
				ast.Predicate = randPartialIndexPredicateFromCols(rng, info.columnsTableDefs, &tn)
				changed = true
			}
		case *tree.CreateTable:
			info, ok := tables[ast.Table.ObjectName]
			if !ok {
				panic("table info could not be found")
			}
			for _, def := range ast.Defs {
				var idx *tree.IndexTableDef
				switch defType := def.(type) {
				case *tree.IndexTableDef:
					idx = defType
				case *tree.UniqueConstraintTableDef:
					if !defType.PrimaryKey && !defType.WithoutIndex {
						idx = &defType.IndexTableDef
					}
				}

				if idx == nil {
					continue
				}

				// If the index is not already a partial index, make it a partial
				// index with a 50% chance.
				if idx.Predicate == nil &&
					!hasReferencingConstraint(info, idx.Columns) &&
					rng.Intn(2) == 0 {
					tn := tree.MakeUnqualifiedTableName(ast.Table.ObjectName)
					idx.Predicate = randPartialIndexPredicateFromCols(rng, info.columnsTableDefs, &tn)
					changed = true
				}
			}
		}
	}
	return stmts, changed
}

// hasReferencingConstraint returns true if the tableInfo has any referencing
// columns that match idxColumns.
func hasReferencingConstraint(info tableInfo, idxColumns tree.IndexElemList) bool {
RefColsLoop:
	for _, refCols := range info.refColsLists {
		if len(refCols) != len(idxColumns) {
			continue RefColsLoop
		}
		for i := range refCols {
			if refCols[i] != idxColumns[i].Column {
				continue RefColsLoop
			}
		}
		return true
	}
	return false
}
