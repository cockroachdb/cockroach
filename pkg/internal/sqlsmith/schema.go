// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	// Import builtins so they are reflected in tree.FunDefs.
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// tableRef represents a table and its columns.
type tableRef struct {
	TableName *tree.TableName
	Columns   []*tree.ColumnTableDef
}

func (t *tableRef) insertDefaultsAllowed() bool {
	for _, column := range t.Columns {
		if column.Nullable.Nullability == tree.NotNull &&
			!column.HasDefaultExpr() {
			return false
		}
	}
	return true
}

type aliasedTableRef struct {
	*tableRef
	indexFlags *tree.IndexFlags
}

type tableRefs []*tableRef

func WithTableDescriptor(tn tree.TableName, desc descpb.TableDescriptor) SmitherOption {
	return option{
		name: fmt.Sprintf("inject table %s", tn.FQString()),
		apply: func(s *Smither) {
			s.lock.Lock()
			defer s.lock.Unlock()
			if tn.SchemaName != "" {
				if !slices.ContainsFunc(s.schemas, func(ref *schemaRef) bool {
					return ref.SchemaName == tn.SchemaName
				}) {
					s.schemas = append(s.schemas, &schemaRef{SchemaName: tn.SchemaName})
				}
			}

			var cols []*tree.ColumnTableDef
			for _, col := range desc.Columns {
				column := tree.ColumnTableDef{
					Name: tree.Name(col.Name),
					Type: col.Type,
				}
				if col.Nullable {
					column.Nullable.Nullability = tree.Null
				}
				if col.IsComputed() {
					column.Computed.Computed = true
				}
				cols = append(cols, &column)
			}

			s.tables = append(s.tables, &tableRef{
				TableName: &tn,
				Columns:   cols,
			})
			if s.columns == nil {
				s.columns = make(map[tree.TableName]map[tree.Name]*tree.ColumnTableDef)
			}
			s.columns[tn] = make(map[tree.Name]*tree.ColumnTableDef)
			for _, col := range cols {
				s.columns[tn][col.Name] = col
			}
		},
	}
}

// ReloadSchemas loads tables from the database.
func (s *Smither) ReloadSchemas() error {
	if s.db == nil {
		s.schemas = []*schemaRef{{SchemaName: "public"}}
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	var err error
	s.types, err = s.extractTypes()
	if err != nil {
		return err
	}
	s.tables, err = s.extractTables()
	if err != nil {
		return err
	}
	s.types.tableImplicitRecordTypes, s.types.tableImplicitRecordTypeNames, err = s.extractTableImplicitRecordTypes()
	if err != nil {
		return err
	}
	s.schemas, err = s.extractSchemas()
	if err != nil {
		return err
	}
	s.sequences, err = s.extractSequences()
	if err != nil {
		return err
	}
	s.indexes, err = s.extractIndexes(s.tables)
	s.columns = make(map[tree.TableName]map[tree.Name]*tree.ColumnTableDef)
	for _, ref := range s.tables {
		s.columns[*ref.TableName] = make(map[tree.Name]*tree.ColumnTableDef)
		for _, col := range ref.Columns {
			s.columns[*ref.TableName][col.Name] = col
		}
	}
	return err
}

// indexesWithNames is a helper struct to sort CreateIndex nodes based on the
// names.
type indexesWithNames struct {
	names []string
	nodes []*tree.CreateIndex
}

func (t *indexesWithNames) Len() int {
	return len(t.names)
}

func (t *indexesWithNames) Less(i, j int) bool {
	return t.names[i] < t.names[j]
}

func (t *indexesWithNames) Swap(i, j int) {
	t.names[i], t.names[j] = t.names[j], t.names[i]
	t.nodes[i], t.nodes[j] = t.nodes[j], t.nodes[i]
}

var _ sort.Interface = &indexesWithNames{}

// getAllIndexesForTableRLocked returns information about all indexes of the
// given table in the deterministic order. s.lock is assumed to be read-locked.
func (s *Smither) getAllIndexesForTableRLocked(tableName tree.TableName) []*tree.CreateIndex {
	s.lock.AssertRHeld()
	indexes, ok := s.indexes[tableName]
	if !ok {
		return nil
	}
	names := make([]string, 0, len(indexes))
	nodes := make([]*tree.CreateIndex, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, string(index.Name))
		nodes = append(nodes, index)
	}
	sort.Sort(&indexesWithNames{names: names, nodes: nodes})
	return nodes
}

func (s *Smither) getRandTable() (*aliasedTableRef, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if len(s.tables) == 0 {
		return nil, false
	}
	table := s.tables[s.rnd.Intn(len(s.tables))]
	aliased := &aliasedTableRef{
		tableRef: table,
	}

	if !s.disableIndexHints && s.coin() {
		indexes := s.getAllIndexesForTableRLocked(*table.TableName)
		var indexFlags tree.IndexFlags
		indexNames := make([]tree.Name, 0, len(indexes))
		for _, index := range indexes {
			if index.Type == idxtype.FORWARD {
				indexNames = append(indexNames, index.Name)
			}
		}
		if len(indexNames) > 0 {
			indexFlags.Index = tree.UnrestrictedName(indexNames[s.rnd.Intn(len(indexNames))])
		}
		aliased.indexFlags = &indexFlags
	}
	return aliased, true
}

func (s *Smither) getRandTableIndex(
	table, alias tree.TableName,
) (*tree.TableIndexName, *tree.CreateIndex, colRefs, bool) {
	var indexes []*tree.CreateIndex
	func() {
		s.lock.RLock()
		defer s.lock.RUnlock()
		indexes = s.getAllIndexesForTableRLocked(table)
	}()
	if len(indexes) == 0 {
		return nil, nil, nil, false
	}
	idx := indexes[s.rnd.Intn(len(indexes))]
	var refs colRefs
	s.lock.RLock()
	defer s.lock.RUnlock()
	for _, col := range idx.Columns {
		ref := s.columns[table][col.Column]
		if ref == nil {
			// TODO(yuzefovich): there are some cases here where colRef is nil,
			// but we aren't yet sure why. Rather than panicking, just return.
			return nil, nil, nil, false
		}
		refs = append(refs, &colRef{
			typ:  tree.MustBeStaticallyKnownType(ref.Type),
			item: tree.NewColumnItem(&alias, col.Column),
		})
	}
	return &tree.TableIndexName{
		Table: alias,
		Index: tree.UnrestrictedName(idx.Name),
	}, idx, refs, true
}

func (s *Smither) getRandIndex() (*tree.TableIndexName, *tree.CreateIndex, colRefs, bool) {
	tableRef, ok := s.getRandTable()
	if !ok {
		return nil, nil, nil, false
	}
	name := *tableRef.TableName
	return s.getRandTableIndex(name, name)
}

func (s *Smither) getRandUserDefinedTypeLabel() (*tree.EnumValue, *tree.TypeName, bool) {
	udt, typName, ok := s.getRandUserDefinedType()
	if !ok {
		return nil, nil, false
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	logicalRepresentations := udt.TypeMeta.EnumData.LogicalRepresentations
	// There are no values in this enum.
	if len(logicalRepresentations) == 0 {
		return nil, nil, false
	}
	enumVal := tree.EnumValue(logicalRepresentations[s.rnd.Intn(len(logicalRepresentations))])
	return &enumVal, typName, true
}

func (s *Smither) getRandUserDefinedType() (*types.T, *tree.TypeName, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.types == nil || len(s.types.udts) == 0 {
		return nil, nil, false
	}
	idx := s.rnd.Intn(len(s.types.udts))
	return s.types.udts[idx], &s.types.udtNames[idx], true
}

// extractTypes should be called before extractTables.
func (s *Smither) extractTypes() (*typeInfo, error) {
	rows, err := s.db.Query(`
SELECT
	schema_name, descriptor_name, descriptor_id, enum_members
FROM
	crdb_internal.create_type_statements
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	evalCtx := eval.Context{}
	var udts []*types.T
	var udtNames []tree.TypeName

	for rows.Next() {
		// For each row, collect columns.
		var scName, name string
		var id int
		var membersRaw []byte
		if err := rows.Scan(&scName, &name, &id, &membersRaw); err != nil {
			return nil, err
		}
		// If enum members were provided, parse the result into a string array.
		var members []string
		if len(membersRaw) != 0 {
			arr, _, err := tree.ParseDArrayFromString(&evalCtx, string(membersRaw), types.String)
			if err != nil {
				return nil, err
			}
			for _, d := range arr.Array {
				members = append(members, string(tree.MustBeDString(d)))
			}
		}
		// Construct type information from the resulting row. Note that the UDT
		// may have no members (e.g., `CREATE TYPE t AS ENUM ()`).
		typ := types.MakeEnum(catid.TypeIDToOID(descpb.ID(id)), 0 /* arrayTypeID */)
		typ.TypeMeta = types.UserDefinedTypeMetadata{
			Name: &types.UserDefinedTypeName{
				Schema: scName,
				Name:   name,
			},
			EnumData: &types.EnumMetadata{
				LogicalRepresentations: members,
				// The physical representations don't matter in this case, but the
				// enum related code in tree expects that the length of
				// PhysicalRepresentations is equal to the length of LogicalRepresentations.
				PhysicalRepresentations: make([][]byte, len(members)),
				IsMemberReadOnly:        make([]bool, len(members)),
			},
		}
		udts = append(udts, typ)
		udtNames = append(udtNames, tree.MakeSchemaQualifiedTypeName(scName, name))
	}
	// Make sure that future appends to udts force a copy.
	udts = udts[:len(udts):len(udts)]

	return &typeInfo{
		udts:        udts,
		udtNames:    udtNames,
		scalarTypes: append(udts, types.Scalar...),
		seedTypes:   append(udts, randgen.SeedTypes...),
	}, nil
}

// extractTableImplicitRecordTypes should only be called after extractTables.
func (s *Smither) extractTableImplicitRecordTypes() (
	[]*types.T,
	[]tree.ResolvableTypeReference,
	error,
) {
	var tableImplicitRecordTypes []*types.T
	var tableImplicitRecordTypeNames []tree.ResolvableTypeReference
	for _, t := range s.tables {
		contents := make([]*types.T, 0, len(t.Columns))
		labels := make([]string, 0, len(t.Columns))
		for _, col := range t.Columns {
			if colinfo.IsSystemColumnName(string(col.Name)) {
				// Ignore system columns since they are inaccessible.
				continue
			}
			typ, ok := col.Type.(*types.T)
			if !ok {
				return nil, nil, errors.AssertionFailedf("unexpectedly column type is not *types.T: %T", col.Type)
			}
			contents = append(contents, typ)
			labels = append(labels, string(col.Name))
		}
		typ := types.MakeLabeledTuple(contents, labels)
		tableImplicitRecordTypes = append(tableImplicitRecordTypes, typ)
		typeName := tree.MakeSchemaQualifiedTypeName(t.TableName.Schema(), t.TableName.Table())
		tableImplicitRecordTypeNames = append(tableImplicitRecordTypeNames, &typeName)
	}
	return tableImplicitRecordTypes, tableImplicitRecordTypeNames, nil
}

type schemaRef struct {
	SchemaName tree.Name
}

func (s *Smither) extractSchemas() ([]*schemaRef, error) {
	rows, err := s.db.Query(`
SELECT nspname FROM pg_catalog.pg_namespace
WHERE nspname NOT IN ('crdb_internal', 'pg_catalog', 'pg_extension',
		'information_schema')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ret []*schemaRef
	for rows.Next() {
		var schema tree.Name
		if err := rows.Scan(&schema); err != nil {
			return nil, err
		}
		ret = append(ret, &schemaRef{SchemaName: schema})
	}
	return ret, nil
}

func (s *Smither) extractTables() ([]*tableRef, error) {
	rows, err := s.db.Query(`
SELECT
	table_catalog,
	table_schema,
	table_name,
	column_name,
	crdb_sql_type,
	generation_expression != '' AS computed,
	is_nullable = 'YES' AS nullable,
	is_hidden = 'YES' AS hidden
FROM
	information_schema.columns
WHERE
	table_schema NOT IN ('crdb_internal', 'pg_catalog', 'pg_extension',
	                     'information_schema')
ORDER BY
	table_catalog, table_schema, table_name
	`)
	// TODO(justin): have a flag that includes system tables?
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// This is a little gross: we want to operate on each segment of the results
	// that corresponds to a single table. We could maybe json_agg the results
	// or something for a cleaner processing step?

	firstTime := true
	var lastCatalog, lastSchema, lastName tree.Name
	var tables []*tableRef
	var currentCols []*tree.ColumnTableDef
	emit := func() error {
		if len(currentCols) == 0 {
			return fmt.Errorf("zero columns for %s.%s", lastCatalog, lastName)
		}
		// All non virtual tables contain implicit system columns.
		for i := range colinfo.AllSystemColumnDescs {
			col := &colinfo.AllSystemColumnDescs[i]
			if s.postgres {
				switch col.ID {
				case colinfo.MVCCTimestampColumnID, colinfo.OriginTimestampColumnID, colinfo.OriginIDColumnID:
					continue
				}
			}
			currentCols = append(currentCols, &tree.ColumnTableDef{
				Name:   tree.Name(col.Name),
				Type:   col.Type,
				Hidden: true,
			})
		}
		tableName := tree.MakeTableNameWithSchema(lastCatalog, lastSchema, lastName)
		tables = append(tables, &tableRef{
			TableName: &tableName,
			Columns:   currentCols,
		})
		return nil
	}
	for rows.Next() {
		var catalog, schema, name, col tree.Name
		var typ string
		var computed, nullable, hidden bool
		if err := rows.Scan(&catalog, &schema, &name, &col, &typ, &computed, &nullable, &hidden); err != nil {
			return nil, err
		}
		if hidden {
			continue
		}

		if firstTime {
			lastCatalog = catalog
			lastSchema = schema
			lastName = name
		}
		firstTime = false

		if lastCatalog != catalog || lastSchema != schema || lastName != name {
			if err := emit(); err != nil {
				return nil, err
			}
			currentCols = nil
		}

		coltyp, err := s.typeFromSQLTypeSyntax(typ)
		if err != nil {
			return nil, err
		}
		column := tree.ColumnTableDef{
			Name: col,
			Type: coltyp,
		}
		if nullable {
			column.Nullable.Nullability = tree.Null
		}
		if computed {
			column.Computed.Computed = true
		}
		currentCols = append(currentCols, &column)
		lastCatalog = catalog
		lastSchema = schema
		lastName = name
	}
	if !firstTime {
		if err := emit(); err != nil {
			return nil, err
		}
	}
	return tables, rows.Err()
}

func (s *Smither) extractIndexes(
	tables tableRefs,
) (map[tree.TableName]map[tree.Name]*tree.CreateIndex, error) {
	ret := map[tree.TableName]map[tree.Name]*tree.CreateIndex{}

	for _, t := range tables {
		indexes := map[tree.Name]*tree.CreateIndex{}
		// Ignore rowid indexes since those columns aren't known to
		// sqlsmith.
		rows, err := s.db.Query(fmt.Sprintf(`
			SELECT
			    si.index_name, column_name, storing, direction = 'ASC',
          is_inverted
			FROM
			    [SHOW INDEXES FROM %s] si
      JOIN crdb_internal.table_indexes ti
           ON si.table_name = ti.descriptor_name
           AND si.index_name = ti.index_name
			WHERE
			    column_name != 'rowid'
			`, t.TableName))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var idx, col tree.Name
			var storing, ascending, inverted bool
			if err := rows.Scan(&idx, &col, &storing, &ascending, &inverted); err != nil {
				rows.Close()
				return nil, err
			}
			if _, ok := indexes[idx]; !ok {
				indexType := idxtype.FORWARD
				if inverted {
					indexType = idxtype.INVERTED
				}
				indexes[idx] = &tree.CreateIndex{
					Name:  idx,
					Table: *t.TableName,
					Type:  indexType,
				}
			}
			create := indexes[idx]
			if storing {
				create.Storing = append(create.Storing, col)
			} else {
				dir := tree.Ascending
				if !ascending {
					dir = tree.Descending
				}
				create.Columns = append(create.Columns, tree.IndexElem{
					Column:    col,
					Direction: dir,
				})
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		// Remove indexes with empty Columns. This is the case for rowid indexes
		// where the only index column, rowid, is ignored in the SQL statement
		// above, but the stored columns are not.
		//
		// Note that here non-deterministic iteration order over 'indexes' map
		// doesn't matter.
		for name, idx := range indexes {
			if len(idx.Columns) == 0 {
				delete(indexes, name)
			}
		}
		ret[*t.TableName] = indexes
	}
	return ret, nil
}

type sequenceRef struct {
	SequenceName tree.TableName
}

func (s *Smither) extractSequences() ([]*sequenceRef, error) {
	rows, err := s.db.Query(`SELECT sequence_catalog, sequence_schema, sequence_name FROM information_schema.sequences`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ret []*sequenceRef
	for rows.Next() {
		var db, schema, name tree.Name
		if err = rows.Scan(&db, &schema, &name); err != nil {
			return nil, err
		}
		ret = append(ret, &sequenceRef{SequenceName: tree.MakeTableNameWithSchema(db, schema, name)})
	}
	return ret, nil
}

type operator struct {
	*tree.BinOp
	Operator treebin.BinaryOperator
}

var operators = func() map[oid.Oid][]operator {
	// Ensure deterministic order of populating operators map.
	binOps := make([]treebin.BinaryOperatorSymbol, 0, len(tree.BinOps))
	for op := range tree.BinOps {
		binOps = append(binOps, op)
	}
	sort.Slice(binOps, func(i, j int) bool {
		return binOps[i] < binOps[j]
	})
	m := map[oid.Oid][]operator{}
	for _, binOp := range binOps {
		overload := tree.BinOps[binOp]
		_ = overload.ForEachBinOp(func(bo *tree.BinOp) error {
			m[bo.ReturnType.Oid()] = append(m[bo.ReturnType.Oid()], operator{
				BinOp:    bo,
				Operator: treebin.MakeBinaryOperator(binOp),
			})
			return nil
		})
	}
	return m
}()

type function struct {
	def      *tree.FunctionDefinition
	overload *tree.Overload
}

type functionsMu struct {
	syncutil.Mutex
	// User-defined functions are added into the map after the initialization,
	// so we need to protect the map with the mutex.
	fns map[tree.FunctionClass]map[oid.Oid][]function
}

var functions = func() *functionsMu {
	// Ensure deterministic order of populating functions map.
	funcNames := make([]string, 0, len(tree.FunDefs))
	for name := range tree.FunDefs {
		funcNames = append(funcNames, name)
	}
	sort.Strings(funcNames)
	m := map[tree.FunctionClass]map[oid.Oid][]function{}
	for _, name := range funcNames {
		def := tree.FunDefs[name]
		if n := tree.Name(def.Name); n.String() != def.Name {
			// sqlsmith doesn't know how to quote function names, e.g. for
			// the numeric cast, we need to use `"numeric"(val)`, but sqlsmith
			// makes it `numeric(val)` which is incorrect.
			continue
		}

		skip := false
		for _, substr := range []string{
			"pg_sleep",
			// Some spatial functions can be very computationally expensive and
			// run for a long time or never finish, so we avoid generating them.
			// See #69213.
			"st_frechetdistance",
			"st_buffer",
			"stream_ingestion",
			"crdb_internal.force_",
			"crdb_internal.unsafe_",
			"crdb_internal.create_join_token",
			"crdb_internal.reset_multi_region_zone_configs_for_database",
			"crdb_internal.reset_index_usage_stats",
			"crdb_internal.start_replication_stream",
			"crdb_internal.replication_stream_progress",
			"crdb_internal.complete_replication_stream",
			"crdb_internal.revalidate_unique_constraint",
			"crdb_internal.request_statement_bundle",
			"crdb_internal.set_compaction_concurrency",
			"crdb_internal.reset_sql_stats",
		} {
			skip = skip || strings.Contains(def.Name, substr)
		}
		if skip {
			continue
		}
		// Ignore pg compat functions since many are unimplemented.
		if def.Category == "Compatibility" {
			continue
		}
		if def.Private {
			continue
		}
		for _, ov := range def.Definition {
			if m[ov.Class] == nil {
				m[ov.Class] = map[oid.Oid][]function{}
			}
			// Ignore documented unusable functions.
			if strings.Contains(ov.Info, "Not usable") {
				continue
			}
			typ := ov.FixedReturnType()
			found := false
			for _, scalarTyp := range types.Scalar {
				if typ.Family() == scalarTyp.Family() {
					found = true
				}
			}
			if !found {
				continue
			}
			m[ov.Class][typ.Oid()] = append(m[ov.Class][typ.Oid()], function{
				def:      def,
				overload: ov,
			})
		}
	}
	return &functionsMu{fns: m}
}()
