// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlsmith

import (
	gosql "database/sql"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	// Import builtins so they are reflected in tree.FunDefs.
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/lib/pq/oid"
)

// tableRef represents a table and its columns.
type tableRef struct {
	TableName *tree.TableName
	Columns   []*tree.ColumnTableDef
}

// ReloadSchemas loads tables from the database. Not safe to use concurrently
// with Generate.
func (s *Smither) ReloadSchemas(db *gosql.DB) error {
	var err error
	s.tables, err = extractTables(db)
	return err
}

func extractTables(db *gosql.DB) ([]*tableRef, error) {
	rows, err := db.Query(`
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
	table_schema = 'public'
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
	emit := func() {
		if lastSchema != "public" {
			return
		}
		tables = append(tables, &tableRef{
			TableName: tree.NewTableName(lastCatalog, lastName),
			Columns:   currentCols,
		})
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
			emit()
			currentCols = nil
		}

		coltyp, err := coltypes.DatumTypeToColumnType(typeFromName(typ))
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
		emit()
	}
	return tables, rows.Err()
}

type operator struct {
	*tree.BinOp
	Operator tree.BinaryOperator
}

var operators = func() map[oid.Oid][]operator {
	m := map[oid.Oid][]operator{}
	for BinaryOperator, overload := range tree.BinOps {
		for _, ov := range overload {
			bo := ov.(*tree.BinOp)
			m[bo.ReturnType.Oid()] = append(m[bo.ReturnType.Oid()], operator{
				BinOp:    bo,
				Operator: BinaryOperator,
			})
		}
	}
	return m
}()

type function struct {
	def      *tree.FunctionDefinition
	overload *tree.Overload
}

var functions = func() map[oid.Oid][]function {
	m := map[oid.Oid][]function{}
	for _, def := range tree.FunDefs {
		switch def.Name {
		case "pg_sleep":
			continue
		}
		if strings.Contains(def.Name, "crdb_internal.force_") {
			continue
		}
		for _, ov := range def.Definition {
			ov := ov.(*tree.Overload)
			// Ignore window and aggregate funcs.
			if ov.Fn == nil {
				continue
			}
			typ := ov.FixedReturnType()
			found := false
			for _, nonArrayTyp := range types.AnyNonArray {
				if typ == nonArrayTyp {
					found = true
				}
			}
			if !found {
				continue
			}
			m[typ.Oid()] = append(m[typ.Oid()], function{
				def:      def,
				overload: ov,
			})
		}
	}
	return m
}()
