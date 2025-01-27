// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	"context"
	gosql "database/sql"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var (
	alters = append(append(append(append(append(
		altersTableExistence,
		altersExistingTable...),
		altersTypeExistence...),
		altersExistingTypes...),
		altersFunctionExistence...),
		altersSequenceExistence...)
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
		{1, makeDropType},
	}
	altersExistingTypes = []statementWeight{
		{5, makeAlterTypeDropValue},
		{5, makeAlterTypeAddValue},
		{1, makeAlterTypeRenameValue},
		{1, makeAlterTypeRenameType},
	}
	altersFunctionExistence = []statementWeight{
		{10, makeCreateFunc},
		{1, makeDropFunc},
	}
	altersSequenceExistence = []statementWeight{
		{10, makeCreateSequence},
		{1, makeDropSequence},
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
	table := randgen.RandCreateTable(context.Background(), s.rnd, "", 0, randgen.TableOptNone)
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
		// Find a type that can be assignment-casted to the column's type.
		var defaultType *types.T
		for {
			defaultType = randgen.RandColumnType(s.rnd)
			if cast.ValidCast(defaultType, t, cast.ContextAssignment) {
				break
			}
		}
		col.DefaultExpr.Expr = &tree.ParenExpr{Expr: makeScalar(s, defaultType, nil)}
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
	// Pick a random column to drop while ignoring the system columns since
	// those cannot be dropped.
	var col *tree.ColumnTableDef
	for {
		col = tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
		var isSystemCol bool
		for _, systemColDesc := range colinfo.AllSystemColumnDescs {
			isSystemCol = isSystemCol || string(col.Name) == systemColDesc.Name
		}
		if !isSystemCol {
			break
		}
	}

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
	indexType := idxtype.FORWARD
	unique := s.coin()
	for len(cols) < 1 || s.coin() {
		col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
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
				Direction: s.randDirection(),
			})
		}
	}
	var storing tree.NameList
	for indexType.SupportsStoring() && s.coin() {
		col := tableRef.Columns[s.rnd.Intn(len(tableRef.Columns))]
		if seen[col.Name] {
			continue
		}
		seen[col.Name] = true
		storing = append(storing, col.Name)
	}

	invisibility := tree.IndexInvisibility{Value: 0.0}
	if notvisible := s.d6() == 1; notvisible {
		invisibility.Value = 1.0
		if s.coin() {
			invisibility.Value = s.rnd.Float64() // [0.0, 1.0)
			invisibility.FloatProvided = true
		}
	}

	return &tree.CreateIndex{
		Name:         s.name("idx"),
		Table:        *tableRef.TableName,
		Unique:       unique,
		Columns:      cols,
		Storing:      storing,
		Type:         indexType,
		Concurrently: s.coin(),
		Invisibility: invisibility,
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

	if s.coin() {
		return randgen.RandCreateEnumType(s.rnd, string(name), letters), true
	}

	return randgen.RandCreateCompositeType(s.rnd, string(name), letters), true
}

func makeDropType(s *Smither) (tree.Statement, bool) {
	var typNames []*tree.UnresolvedObjectName
	for len(typNames) < 1 || s.coin() {
		// It's ok if the same type is chosen multiple times.
		_, typName, ok := s.getRandUserDefinedType()
		if !ok {
			if len(typNames) == 0 {
				return nil, false
			}
			break
		}
		typNames = append(typNames, typName.ToUnresolvedObjectName())
	}
	return &tree.DropType{
		Names:    typNames,
		IfExists: s.d6() < 3,
		// TODO(#51480): use s.randDropBehavior() once DROP TYPE CASCADE is
		// implemented.
		DropBehavior: s.randDropBehaviorNoCascade(),
	}, true
}

func rowsToRegionList(rows *gosql.Rows) ([]string, error) {
	// Don't add duplicate regions to the slice.
	regionsSet := make(map[string]struct{})
	var region, zone string
	for rows.Next() {
		if err := rows.Scan(&region, &zone); err != nil {
			return nil, err
		}
		regionsSet[region] = struct{}{}
	}

	var regions []string
	for region := range regionsSet {
		regions = append(regions, region)
	}
	// Make deterministic. Note that we don't need to shuffle the regions since
	// the caller will be picking random ones from the slice.
	sort.Strings(regions)
	return regions, nil
}

func getClusterRegions(s *Smither) []string {
	rows, err := s.db.Query("SHOW REGIONS FROM CLUSTER")
	if err != nil {
		panic(err)
	}
	regions, err := rowsToRegionList(rows)
	if err != nil {
		panic(errors.Wrap(err, "Failed to scan SHOW REGIONS FROM CLUSTER into values"))
	}
	return regions
}

func getDatabaseRegions(s *Smither) []string {
	rows, err := s.db.Query("SELECT region, zones FROM [SHOW REGIONS FROM DATABASE defaultdb]")
	if err != nil {
		panic(err)
	}
	regions, err := rowsToRegionList(rows)
	if err != nil {
		panic(errors.Wrap(err, "Failed to scan SHOW REGIONS FROM DATABASE defaultdb into values"))
	}
	return regions
}

func makeAlterLocality(s *Smither) (tree.Statement, bool) {
	_, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, false
	}
	regions := getClusterRegions(s)

	localityLevel := tree.LocalityLevel(s.rnd.Intn(3))
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
		ast.Locality.TableRegion = tree.Name(regions[s.rnd.Intn(len(regions))])
	}
	return ast, ok
}

func makeAlterDatabaseAddRegion(s *Smither) (tree.Statement, bool) {
	regions := getClusterRegions(s)

	if len(regions) == 0 {
		return &tree.AlterDatabaseAddRegion{}, false
	}

	ast := &tree.AlterDatabaseAddRegion{
		Region: tree.Name(regions[s.rnd.Intn(len(regions))]),
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
		Region: tree.Name(regions[s.rnd.Intn(len(regions))]),
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
	survivalGoal := survivalGoals[s.rnd.Intn(len(survivalGoals))]

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
	dataPlacement := dataPlacements[s.rnd.Intn(len(dataPlacements))]

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

func makeCreateSequence(s *Smither) (tree.Statement, bool) {
	schema := s.schemas[s.rnd.Intn(len(s.schemas))]
	name := tree.MakeTableNameWithSchema(tree.Name(s.dbName), schema.SchemaName, s.name("seq"))
	return &tree.CreateSequence{
		IfNotExists: s.d6() < 3,
		Name:        name,
	}, true
}

func makeDropSequence(s *Smither) (tree.Statement, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.sequences) == 0 {
		return nil, false
	}
	var names tree.TableNames
	for len(names) < 1 || s.coin() {
		// It's ok if the same sequence is chosen multiple times.
		names = append(names, s.sequences[s.rnd.Intn(len(s.sequences))].SequenceName)
	}
	return &tree.DropSequence{
		Names:        names,
		IfExists:     s.coin(),
		DropBehavior: s.randDropBehavior(),
	}, true
}
