// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var (
	alters               = append(append(append(altersTableExistence, altersExistingTable...), altersTypeExistence...), altersExistingTypes...)
	altersTableExistence = []statementWeight{
		{10, makeCreateTable},
		{2, makeCreateSchema},
		{1, makeDropTable},
	}
	altersExistingTable = []statementWeight{
		{5, makeRenameTable},

		{10, makeAddColumn},
		{10, makeJSONComputedColumn},
		{10, makeAlterPrimaryKey},
		{1, makeDropColumn},
		{5, makeRenameColumn},
		{5, makeAlterColumnType},

		{10, makeCreateIndex},
		{1, makeDropIndex},
		{5, makeRenameIndex},
	}
	altersTypeExistence = []statementWeight{
		{5, makeCreateType},
	}
	altersExistingTypes = []statementWeight{
		{5, makeAlterTypeDropValue},
		{5, makeAlterTypeAddValue},
		{1, makeAlterTypeRenameValue},
		{1, makeAlterTypeRenameType},
	}
	alterTableMultiregion = []statementWeight{
		{10, makeAlterLocality},
	}
	alterDatabaseMultiregion = []statementWeight{
		{5, makeAlterDatabaseDropRegion},
		{5, makeAlterDatabaseAddRegion},
		{5, makeAlterSurvivalGoal},
		{5, makeAlterDatabasePlacement},
	}
	alterMultiregion = append(alterTableMultiregion, alterDatabaseMultiregion...)
)

func makeAlter(s *Smither) (tree.Statement, bool) {
	if s.canRecurse() {
		// Schema changes aren't visible immediately, so try to
		// sync the change from the last alter before trying the
		// next one. This is instead of running ReloadSchemas right
		// after the alter succeeds (which doesn't always pick
		// up the change). This has the added benefit of leaving
		// behind old column references for a bit, which should
		// test some additional logic.
		err := s.ReloadSchemas()
		if err != nil {
			// If we fail to load any schema information, then
			// the actual statement generation could panic, so
			// fail out here.
			return nil, false
		}

		for i := 0; i < retryCount; i++ {
			stmt, ok := s.alterSampler.Next()(s)
			if ok {
				return stmt, ok
			}
		}
	}
	return nil, false
}

func makeCreateSchema(s *Smither) (tree.Statement, bool) {
	return &tree.CreateSchema{
		Schema: tree.ObjectNamePrefix{
			SchemaName:     s.name("schema"),
			ExplicitSchema: true,
		},
	}, true
}

func makeCreateTable(s *Smither) (tree.Statement, bool) {
	table := randgen.RandCreateTable(s.rnd, "", 0)
	schemaOrd := s.rnd.Intn(len(s.schemas))
	schema := s.schemas[schemaOrd]
	table.Table = tree.MakeTableNameWithSchema(tree.Name(s.dbName), schema.SchemaName, s.name("tab"))
	return table, true
}

func makeDropTable(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}

	return &tree.DropTable{
		Names:        tree.TableNames{*tableRef.TableName},
		DropBehavior: s.randDropBehavior(),
	}, true
}

func makeRenameTable(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}

	newName, err := tree.NewUnresolvedObjectName(
		1 /* numParts */, [3]string{string(s.name("tab"))}, tree.NoAnnotation,
	)
	if err != nil {
		return nil, false
	}

	return &tree.RenameTable{
		Name:    tableRef.TableName.ToUnresolvedObjectName(),
		NewName: newName,
	}, true
}

func makeRenameColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]

	return &tree.RenameColumn{
		Table:   *tableRef.TableName,
		Name:    col.Name,
		NewName: s.name("col"),
	}, true
}

func makeAlterColumnType(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	typ := randgen.RandColumnType(s.rnd)
	col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAlterColumnType{
				Column: col.Name,
				ToType: typ,
			},
		},
	}, true
}

func makeAddColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, colRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	colRefs.stripTableName()
	t := randgen.RandColumnType(s.rnd)
	col, err := tree.NewColumnTableDef(s.name("col"), t, false /* isSerial */, nil)
	if err != nil {
		return nil, false
	}
	col.Nullable.Nullability = s.randNullability()
	if s.coin() {
		col.DefaultExpr.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, nil)}
	} else if s.coin() {
		col.Computed.Computed = true
		col.Computed.Expr = &tree.ParenExpr{Expr: makeScalar(s, t, colRefs)}
	}
	for s.coin() {
		col.CheckExprs = append(col.CheckExprs, tree.ColumnTableDefCheckExpr{
			Expr: makeBoolExpr(s, colRefs),
		})
	}

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: col,
			},
		},
	}, true
}

func makeJSONComputedColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, colRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	colRefs.stripTableName()
	// Shuffle columns and find the first one that's JSON.
	s.rnd.Shuffle(len(colRefs), func(i, j int) {
		colRefs[i], colRefs[j] = colRefs[j], colRefs[i]
	})
	var ref *colRef
	for _, c := range colRefs {
		if c.typ.Family() == types.JsonFamily {
			ref = c
			break
		}
	}
	// If we didn't find any JSON columns, return.
	if ref == nil {
		return nil, false
	}
	col, err := tree.NewColumnTableDef(s.name("col"), types.Jsonb, false /* isSerial */, nil)
	if err != nil {
		return nil, false
	}
	col.Computed.Computed = true
	col.Computed.Expr = tree.NewTypedBinaryExpr(
		treebin.MakeBinaryOperator(treebin.JSONFetchText),
		ref.typedExpr(),
		randgen.RandDatumSimple(s.rnd, types.String),
		types.String,
	)

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAddColumn{
				ColumnDef: col,
			},
		},
	}, true
}

func makeDropColumn(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]

	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableDropColumn{
				Column:       col.Name,
				DropBehavior: s.randDropBehavior(),
			},
		},
	}, true
}

func makeAlterPrimaryKey(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	// Collect all columns that are NOT NULL to be candidate new primary keys.
	var candidateColumns tree.IndexElemList
	for _, c := range tableRef.Columns {
		if c.Nullable.Nullability == tree.NotNull {
			candidateColumns = append(candidateColumns, tree.IndexElem{Column: c.Name})
		}
	}
	if len(candidateColumns) == 0 {
		return nil, false
	}
	s.rnd.Shuffle(len(candidateColumns), func(i, j int) {
		candidateColumns[i], candidateColumns[j] = candidateColumns[j], candidateColumns[i]
	})
	// Pick some randomly short prefix of the candidate columns as a potential new primary key.
	i := 1
	for len(candidateColumns) > i && s.rnd.Intn(2) == 0 {
		i++
	}
	candidateColumns = candidateColumns[:i]
	return &tree.AlterTable{
		Table: tableRef.TableName.ToUnresolvedObjectName(),
		Cmds: tree.AlterTableCmds{
			&tree.AlterTableAlterPrimaryKey{
				Columns: candidateColumns,
			},
		},
	}, true
}

func makeCreateIndex(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	var cols tree.IndexElemList
	seen := map[tree.Name]bool{}
	inverted := false
	unique := s.coin()
	for len(cols) < 1 || s.coin() {
		col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		// If this is the first column and it's invertible (i.e., JSONB), make an inverted index.
		if len(cols) == 0 &&
			colinfo.ColumnTypeIsInvertedIndexable(tree.MustBeStaticallyKnownType(col.Type)) {
			inverted = true
			unique = false
			cols = append(cols, tree.IndexElem{
				Column: col.Name,
			})
			break
		}
		if colinfo.ColumnTypeIsIndexable(tree.MustBeStaticallyKnownType(col.Type)) {
			cols = append(cols, tree.IndexElem{
				Column:    col.Name,
				Direction: s.randDirection(),
			})
		}
	}
	var storing tree.NameList
	for !inverted && s.coin() {
		col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		storing = append(storing, col.Name)
	}

	return &tree.CreateIndex{
		Name:         s.name("idx"),
		Table:        *tableRef.TableName,
		Unique:       unique,
		Columns:      cols,
		Storing:      storing,
		Inverted:     inverted,
		Concurrently: s.coin(),
	}, true
}

func makeDropIndex(s *Smither) (tree.Statement, bool) {
	tin, _, _, ok := s.getRandIndex()
	return &tree.DropIndex{
		IndexList:    tree.TableIndexNames{tin},
		DropBehavior: s.randDropBehavior(),
		Concurrently: s.coin(),
	}, ok
}

func makeRenameIndex(s *Smither) (tree.Statement, bool) {
	tin, _, _, ok := s.getRandIndex()
	return &tree.RenameIndex{
		Index:   tin,
		NewName: tree.UnrestrictedName(s.name("idx")),
	}, ok
}

func makeCreateType(s *Smither) (tree.Statement, bool) {
	name := s.name("typ")
	return randgen.RandCreateType(s.rnd, string(name), letters), true
}

func rowsToRegionList(rows *gosql.Rows) []string {
	// Don't add duplicate regions to the slice.
	regionsSet := make(map[string]struct{})
	var region, zone string
	for rows.Next() {
		if err := rows.Scan(&region, &zone); err != nil {
			panic(err)
		}
		regionsSet[region] = struct{}{}
	}

	var regions []string
	for region := range regionsSet {
		regions = append(regions, region)
	}
	return regions
}

func getClusterRegions(s *Smither) []string {
	rows, err := s.db.Query("SHOW REGIONS FROM CLUSTER")
	if err != nil {
		panic(err)
	}
	return rowsToRegionList(rows)
}

func getDatabaseRegions(s *Smither) []string {
	rows, err := s.db.Query("SHOW REGIONS FROM DATABASE defaultdb")
	if err != nil {
		panic(err)
	}
	return rowsToRegionList(rows)
}

func makeAlterLocality(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	regions := getClusterRegions(s)

	localityLevel := tree.LocalityLevel(rand.Intn(3))
	ast := &tree.AlterTableLocality{
		Name: tableRef.TableName.ToUnresolvedObjectName(),
		Locality: &tree.Locality{
			LocalityLevel: localityLevel,
		},
	}
	if localityLevel == tree.LocalityLevelTable {
		if len(regions) == 0 {
			return &tree.AlterDatabaseAddRegion{}, false
		}
		ast.Locality.TableRegion = tree.Name(regions[rand.Intn(len(regions))])
	}
	return ast, ok
}

func makeAlterDatabaseAddRegion(s *Smither) (tree.Statement, bool) {
	regions := getClusterRegions(s)

	if len(regions) == 0 {
		return &tree.AlterDatabaseAddRegion{}, false
	}

	ast := &tree.AlterDatabaseAddRegion{
		Region: tree.Name(regions[rand.Intn(len(regions))]),
		Name:   tree.Name("defaultdb"),
	}

	return ast, true
}

func makeAlterDatabaseDropRegion(s *Smither) (tree.Statement, bool) {
	regions := getDatabaseRegions(s)

	if len(regions) == 0 {
		return &tree.AlterDatabaseDropRegion{}, false
	}

	ast := &tree.AlterDatabaseDropRegion{
		Region: tree.Name(regions[rand.Intn(len(regions))]),
		Name:   tree.Name("defaultdb"),
	}

	return ast, true
}

func makeAlterSurvivalGoal(s *Smither) (tree.Statement, bool) {
	// Only SurvivalGoalRegionFailure and SurvivalGoalZoneFailure are valid
	// values for SurvivalGoal. SurvivalGoalDefault is not valid in an
	// AlterDatabaseSurvivalGoal AST node.
	survivalGoals := [...]tree.SurvivalGoal{
		tree.SurvivalGoalRegionFailure,
		tree.SurvivalGoalZoneFailure,
	}
	survivalGoal := survivalGoals[rand.Intn(len(survivalGoals))]

	ast := &tree.AlterDatabaseSurvivalGoal{
		Name:         tree.Name("defaultdb"),
		SurvivalGoal: survivalGoal,
	}
	return ast, true
}

func makeAlterDatabasePlacement(s *Smither) (tree.Statement, bool) {
	// Only DataPlacementDefault and DataPlacementRestricted are valid values
	// for Placement. DataPlacementUnspecified is not valid in an
	// AlterDatabasePlacement AST node.
	dataPlacements := [...]tree.DataPlacement{
		tree.DataPlacementDefault,
		tree.DataPlacementRestricted,
	}
	dataPlacement := dataPlacements[rand.Intn(len(dataPlacements))]

	ast := &tree.AlterDatabasePlacement{
		Name:      tree.Name("defaultdb"),
		Placement: dataPlacement,
	}

	return ast, true
}

func makeAlterTypeDropValue(s *Smither) (tree.Statement, bool) {
	enumVal, udtName, ok := s.getRandUserDefinedTypeLabel()
	if !ok {
		return nil, false
	}
	return &tree.AlterType{
		Type: udtName.ToUnresolvedObjectName(),
		Cmd: &tree.AlterTypeDropValue{
			Val: *enumVal,
		},
	}, ok
}

func makeAlterTypeAddValue(s *Smither) (tree.Statement, bool) {
	_, udtName, ok := s.getRandUserDefinedTypeLabel()
	if !ok {
		return nil, false
	}
	return &tree.AlterType{
		Type: udtName.ToUnresolvedObjectName(),
		Cmd: &tree.AlterTypeAddValue{
			NewVal:      tree.EnumValue(s.name("added_val")),
			IfNotExists: true,
		},
	}, true
}

func makeAlterTypeRenameValue(s *Smither) (tree.Statement, bool) {
	enumVal, udtName, ok := s.getRandUserDefinedTypeLabel()
	if !ok {
		return nil, false
	}
	return &tree.AlterType{
		Type: udtName.ToUnresolvedObjectName(),
		Cmd: &tree.AlterTypeRenameValue{
			OldVal: *enumVal,
			NewVal: tree.EnumValue(s.name("renamed_val")),
		},
	}, true
}

func makeAlterTypeRenameType(s *Smither) (tree.Statement, bool) {
	_, udtName, ok := s.getRandUserDefinedTypeLabel()
	if !ok {
		return nil, false
	}
	return &tree.AlterType{
		Type: udtName.ToUnresolvedObjectName(),
		Cmd: &tree.AlterTypeRename{
			NewName: s.name("typ"),
		},
	}, true
}
