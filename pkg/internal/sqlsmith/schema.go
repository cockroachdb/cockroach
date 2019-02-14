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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
)

type operator struct {
	name  string
	left  types.T
	right types.T
	out   types.T
}

type function struct {
	name   string
	inputs []types.T
	out    types.T
}

// schema represents the state of the database as sqlsmith-go understands it, including
// not only the tables present but also things like what operator overloads exist.
type schema struct {
	db        *gosql.DB
	rnd       *rand.Rand
	lock      syncutil.Mutex
	tables    []namedRelation
	operators map[oid.Oid][]operator
	functions map[oid.Oid][]function
}

func (s *schema) makeScope() *scope {
	return &scope{
		namer:  &namer{make(map[string]int)},
		schema: s,
	}
}

func (s *schema) GetOperatorsByOutputType(outTyp types.T) []operator {
	return s.operators[outTyp.Oid()]
}

func (s *schema) GetFunctionsByOutputType(outTyp types.T) []function {
	return s.functions[outTyp.Oid()]
}

func makeSchema(db *gosql.DB, rnd *rand.Rand) (*schema, error) {
	s := &schema{
		db:  db,
		rnd: rnd,
	}
	return s, s.ReloadSchemas()
}

func (s *schema) ReloadSchemas() error {
	var err error
	s.tables, err = s.extractTables()
	if err != nil {
		return err
	}
	s.operators, err = s.extractOperators()
	if err != nil {
		return err
	}
	s.functions, err = s.extractFunctions()
	return err
}

func (s *schema) extractTables() ([]namedRelation, error) {
	rows, err := s.db.Query(`
	SELECT
		table_catalog,
		table_schema,
		table_name,
		column_name,
		crdb_sql_type,
		generation_expression != '' AS computed,
		is_nullable = 'YES' AS nullable
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
	var lastCatalog, lastSchema, lastName string
	var tables []namedRelation
	var currentCols []column
	emit := func() {
		tables = append(tables, namedRelation{
			cols: currentCols,
			name: lastName,
		})
	}
	for rows.Next() {
		var catalog, schema, name, col, typ string
		var computed, nullable bool
		if err := rows.Scan(&catalog, &schema, &name, &col, &typ, &computed, &nullable); err != nil {
			return nil, err
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

		writability := writable
		if computed {
			writability = notWritable
		}

		currentCols = append(
			currentCols,
			column{
				name:        col,
				typ:         typeFromName(typ),
				nullable:    nullable,
				writability: writability,
			},
		)
		lastCatalog = catalog
		lastSchema = schema
		lastName = name
	}
	if !firstTime {
		emit()
	}
	return tables, rows.Err()
}

func (s *schema) extractOperators() (map[oid.Oid][]operator, error) {
	rows, err := s.db.Query(`
SELECT
	oprname, oprleft, oprright, oprresult
FROM
	pg_catalog.pg_operator
WHERE
	0 NOT IN (oprresult, oprright, oprleft)
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[oid.Oid][]operator{}
	for rows.Next() {
		var name string
		var left, right, out oid.Oid
		if err := rows.Scan(&name, &left, &right, &out); err != nil {
			return nil, err
		}
		leftTyp, ok := types.OidToType[left]
		if !ok {
			continue
		}
		rightTyp, ok := types.OidToType[right]
		if !ok {
			continue
		}
		outTyp, ok := types.OidToType[out]
		if !ok {
			continue
		}
		result[out] = append(
			result[out],
			operator{
				name:  name,
				left:  leftTyp,
				right: rightTyp,
				out:   outTyp,
			},
		)
	}
	return result, rows.Err()
}

func (s *schema) extractFunctions() (map[oid.Oid][]function, error) {
	rows, err := s.db.Query(`
SELECT
	proname, proargtypes::INT[], prorettype
FROM
	pg_catalog.pg_proc
WHERE
	NOT proisagg
	AND NOT proiswindow
	AND NOT proretset
	AND proname NOT LIKE 'crdb_internal.force_%'
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[oid.Oid][]function{}
	for rows.Next() {
		var name string
		var inputs []int64
		var returnType oid.Oid
		if err := rows.Scan(&name, pq.Array(&inputs), &returnType); err != nil {
			return nil, err
		}

		typs := make([]types.T, len(inputs))
		unsupported := false
		for i, id := range inputs {
			t, ok := types.OidToType[oid.Oid(id)]
			if !ok {
				unsupported = true
				break
			}
			typs[i] = t
		}

		if unsupported {
			continue
		}

		out, ok := types.OidToType[returnType]
		if !ok {
			continue
		}

		result[returnType] = append(result[returnType], function{
			name,
			typs,
			out,
		})
	}
	return result, rows.Err()
}
