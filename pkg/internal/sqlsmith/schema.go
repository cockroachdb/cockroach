package sqlsmith

import (
	"database/sql"
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
	db        *sql.DB
	rnd       *rand.Rand
	lock      sync.Mutex
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

func makeSchema(db *sql.DB, rnd *rand.Rand) *schema {
	s := &schema{
		db:  db,
		rnd: rnd,
	}
	s.ReloadSchemas()
	return s
}

func (s *schema) ReloadSchemas() {
	s.tables = s.extractTables()
	s.operators = s.extractOperators()
	s.functions = s.extractFunctions()
}

func (s *schema) extractTables() []namedRelation {
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
		panic(err)
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
		rows.Scan(&catalog, &schema, &name, &col, &typ, &computed, &nullable)

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
	return tables
}

func (s *schema) extractOperators() map[oid.Oid][]operator {
	rows, err := s.db.Query(`
SELECT
	oprname, oprleft, oprright, oprresult
FROM
	pg_catalog.pg_operator
WHERE
	0 NOT IN (oprresult, oprright, oprleft)
`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	result := make(map[oid.Oid][]operator, 0)
	for rows.Next() {
		var name string
		var left, right, out oid.Oid
		rows.Scan(&name, &left, &right, &out)
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
	return result
}

func (s *schema) extractFunctions() map[oid.Oid][]function {
	rows, err := s.db.Query(`
SELECT
	proname, proargtypes::INT[], prorettype
FROM
	pg_catalog.pg_proc
WHERE
	NOT proisagg
	AND NOT proiswindow
	AND NOT proretset
	AND proname NOT IN ('crdb_internal.force_panic', 'crdb_internal.force_log_fatal')
`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	result := make(map[oid.Oid][]function, 0)
	for rows.Next() {
		var name string
		var inputs []oid.Oid
		var returnType oid.Oid
		rows.Scan(&name, pq.Array(&inputs), &returnType)

		typs := make([]types.T, len(inputs))
		unsupported := false
		for i, oid := range inputs {
			t, ok := types.OidToType[oid]
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
	return result
}
