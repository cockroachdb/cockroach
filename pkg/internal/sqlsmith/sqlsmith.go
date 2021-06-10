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
	"fmt"
	"math/rand"
	"net/http/httptest"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// sqlsmith-go
//
// sqlsmith-go is a random SQL query generator, based off of sqlsmith:
//
//   https://github.com/anse1/sqlsmith
//
// You can think of it as walking a randomly generated AST and materializing
// that AST as it goes, which it then feeds into Cockroach with the hopes of
// finding panics.
//
// However, naively generating such an AST will only find certain kinds of
// panics: they're almost guaranteed not to pass semantic analysis, and so
// any components of the system beyond that will probably not be tested.
// To get around this, sqlsmith tracks scopes and types, very similar to
// how the optbuilder works, to create ASTs which will likely pass
// semantic analysis.
//
// It does this by building the tree top-down. Every level of the tree
// requests input of a certain form. For instance, a SELECT will request
// a list of projections which respect the scope that the SELECT introduces,
// and a function call will request an input value of a particular type,
// subject to the same scope it has. This raises a question: what if we
// are unable to construct an expression meeting the restrictions requested
// by the parent expression? Rather than do some fancy constraint solving
// (which could be an interesting direction for this tool to go in the
// future, but I've found to be difficult when I've tried in the past)
// sqlsmith will simply try randomly to generate an expression, and once
// it fails a certain number of times, it will retreat up the tree and
// retry at a higher level.

const retryCount = 20

// Smither is a sqlsmith generator.
type Smither struct {
	rnd              *rand.Rand
	db               *gosql.DB
	lock             syncutil.RWMutex
	dbName           string
	schemas          []*schemaRef
	tables           []*tableRef
	columns          map[tree.TableName]map[tree.Name]*tree.ColumnTableDef
	indexes          map[tree.TableName]map[tree.Name]*tree.CreateIndex
	nameCounts       map[string]int
	activeSavepoints []string
	types            *typeInfo

	stmtWeights, alterWeights          []statementWeight
	stmtSampler, alterSampler          *statementSampler
	tableExprWeights                   []tableExprWeight
	tableExprSampler                   *tableExprSampler
	selectStmtWeights                  []selectStatementWeight
	selectStmtSampler                  *selectStatementSampler
	scalarExprWeights, boolExprWeights []scalarExprWeight
	scalarExprSampler, boolExprSampler *scalarExprSampler

	disableWith        bool
	disableImpureFns   bool
	disableLimits      bool
	disableWindowFuncs bool
	simpleDatums       bool
	avoidConsts        bool
	vectorizable       bool
	outputSort         bool
	postgres           bool
	ignoreFNs          []*regexp.Regexp
	complexity         float64

	bulkSrv     *httptest.Server
	bulkFiles   map[string][]byte
	bulkBackups map[string]tree.TargetList
	bulkExports []string
}

type (
	statement       func(*Smither) (tree.Statement, bool)
	tableExpr       func(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool)
	selectStatement func(s *Smither, desiredTypes []*types.T, refs colRefs, withTables tableRefs) (tree.SelectStatement, colRefs, bool)
	scalarExpr      func(*Smither, Context, *types.T, colRefs) (expr tree.TypedExpr, ok bool)
)

// NewSmither creates a new Smither. db is used to populate existing tables
// for use as column references. It can be nil to skip table population.
func NewSmither(db *gosql.DB, rnd *rand.Rand, opts ...SmitherOption) (*Smither, error) {
	s := &Smither{
		rnd:        rnd,
		db:         db,
		nameCounts: map[string]int{},

		stmtWeights:       allStatements,
		alterWeights:      alters,
		tableExprWeights:  allTableExprs,
		selectStmtWeights: selectStmts,
		scalarExprWeights: scalars,
		boolExprWeights:   bools,

		complexity: 0.2,
	}
	for _, opt := range opts {
		opt.Apply(s)
	}
	s.stmtSampler = newWeightedStatementSampler(s.stmtWeights, rnd.Int63())
	s.alterSampler = newWeightedStatementSampler(s.alterWeights, rnd.Int63())
	s.tableExprSampler = newWeightedTableExprSampler(s.tableExprWeights, rnd.Int63())
	s.selectStmtSampler = newWeightedSelectStatementSampler(s.selectStmtWeights, rnd.Int63())
	s.scalarExprSampler = newWeightedScalarExprSampler(s.scalarExprWeights, rnd.Int63())
	s.boolExprSampler = newWeightedScalarExprSampler(s.boolExprWeights, rnd.Int63())
	s.enableBulkIO()
	row := s.db.QueryRow("SELECT current_database()")
	if err := row.Scan(&s.dbName); err != nil {
		return nil, err
	}
	return s, s.ReloadSchemas()
}

// Close closes resources used by the Smither.
func (s *Smither) Close() {
	if s.bulkSrv != nil {
		s.bulkSrv.Close()
	}
}

var prettyCfg = func() tree.PrettyCfg {
	cfg := tree.DefaultPrettyCfg()
	cfg.LineWidth = 120
	cfg.Simplify = false
	return cfg
}()

// Generate returns a random SQL string.
func (s *Smither) Generate() string {
	i := 0
	for {
		stmt, ok := s.makeStmt()
		if !ok {
			i++
			if i > 1000 {
				panic("exhausted generation attempts")
			}
			continue
		}
		i = 0
		return prettyCfg.Pretty(stmt)
	}
}

// GenerateExpr returns a random SQL expression that does not depend on any
// tables or columns.
func (s *Smither) GenerateExpr() tree.TypedExpr {
	return makeScalar(s, s.randScalarType(), nil)
}

func (s *Smither) name(prefix string) tree.Name {
	s.lock.Lock()
	s.nameCounts[prefix]++
	count := s.nameCounts[prefix]
	s.lock.Unlock()
	return tree.Name(fmt.Sprintf("%s_%d", prefix, count))
}

// SmitherOption is an option for the Smither client.
type SmitherOption interface {
	Apply(*Smither)
	String() string
}

func simpleOption(name string, apply func(s *Smither)) func() SmitherOption {
	return func() SmitherOption {
		return option{
			name:  name,
			apply: apply,
		}
	}
}

func multiOption(name string, opts ...SmitherOption) func() SmitherOption {
	var sb strings.Builder
	sb.WriteString(name)
	sb.WriteString("(")
	delim := ""
	for _, opt := range opts {
		sb.WriteString(delim)
		delim = ", "
		sb.WriteString(opt.String())
	}
	sb.WriteString(")")
	return func() SmitherOption {
		return option{
			name: sb.String(),
			apply: func(s *Smither) {
				for _, opt := range opts {
					opt.Apply(s)
				}
			},
		}
	}
}

type option struct {
	name  string
	apply func(s *Smither)
}

func (o option) String() string {
	return o.name
}

func (o option) Apply(s *Smither) {
	o.apply(s)
}

// DisableMutations causes the Smither to not emit statements that could
// mutate any on-disk data.
var DisableMutations = simpleOption("disable mutations", func(s *Smither) {
	s.stmtWeights = nonMutatingStatements
	s.tableExprWeights = nonMutatingTableExprs
})

// DisableDDLs causes the Smither to not emit statements that change table
// schema (CREATE, DROP, ALTER, etc.)
var DisableDDLs = simpleOption("disable DDLs", func(s *Smither) {
	s.stmtWeights = []statementWeight{
		{20, makeSelect},
		{5, makeInsert},
		{5, makeUpdate},
		{1, makeDelete},
		// If we don't have any DDL's, allow for use of savepoints and transactions.
		{2, makeBegin},
		{2, makeSavepoint},
		{2, makeReleaseSavepoint},
		{2, makeRollbackToSavepoint},
		{2, makeCommit},
		{2, makeRollback},
	}
})

// OnlyNoDropDDLs causes the Smither to only emit DDLs, but won't ever drop
// a table.
var OnlyNoDropDDLs = simpleOption("only DDLs", func(s *Smither) {
	s.stmtWeights = append(append([]statementWeight{
		{1, makeBegin},
		{2, makeRollback},
		{6, makeCommit},
	},
		altersExistingTable...,
	),
		altersExistingTypes...,
	)
})

// DisableWith causes the Smither to not emit WITH clauses.
var DisableWith = simpleOption("disable WITH", func(s *Smither) {
	s.disableWith = true
})

// DisableImpureFns causes the Smither to disable impure functions.
var DisableImpureFns = simpleOption("disable impure funcs", func(s *Smither) {
	s.disableImpureFns = true
})

// DisableCRDBFns causes the Smither to disable crdb_internal functions.
func DisableCRDBFns() SmitherOption {
	return IgnoreFNs("^crdb_internal")
}

// SimpleDatums causes the Smither to emit simpler constant datums.
var SimpleDatums = simpleOption("simple datums", func(s *Smither) {
	s.simpleDatums = true
})

// MutationsOnly causes the Smither to emit 80% INSERT, 10% UPDATE, and 10%
// DELETE statements.
var MutationsOnly = simpleOption("mutations only", func(s *Smither) {
	s.stmtWeights = []statementWeight{
		{8, makeInsert},
		{1, makeUpdate},
		{1, makeDelete},
	}
})

// IgnoreFNs causes the Smither to ignore functions that match the regex.
func IgnoreFNs(regex string) SmitherOption {
	r := regexp.MustCompile(regex)
	return option{
		name: fmt.Sprintf("ignore fns: %q", r.String()),
		apply: func(s *Smither) {
			s.ignoreFNs = append(s.ignoreFNs, r)
		},
	}
}

// DisableLimits causes the Smither to disable LIMIT clauses.
var DisableLimits = simpleOption("disable LIMIT", func(s *Smither) {
	s.disableLimits = true
})

// AvoidConsts causes the Smither to prefer column references over generating
// constants.
var AvoidConsts = simpleOption("avoid consts", func(s *Smither) {
	s.avoidConsts = true
})

// DisableWindowFuncs disables window functions.
var DisableWindowFuncs = simpleOption("disable window funcs", func(s *Smither) {
	s.disableWindowFuncs = true
})

// Vectorizable causes the Smither to limit query generation to queries
// supported by vectorized execution.
var Vectorizable = multiOption(
	"Vectorizable",
	DisableMutations(),
	DisableWith(),
	DisableWindowFuncs(),
	AvoidConsts(),
	// This must be last so it can make the final changes to table
	// exprs and statements.
	simpleOption("vectorizable", func(s *Smither) {
		s.vectorizable = true
		s.stmtWeights = nonMutatingStatements
		s.tableExprWeights = vectorizableTableExprs
	})(),
)

// OutputSort adds a top-level ORDER BY on all columns.
var OutputSort = simpleOption("output sort", func(s *Smither) {
	s.outputSort = true
})

// CompareMode causes the Smither to generate statements that have
// deterministic output.
var CompareMode = multiOption(
	"compare mode",
	DisableMutations(),
	DisableImpureFns(),
	DisableCRDBFns(),
	IgnoreFNs("^version"),
	DisableLimits(),
	OutputSort(),
)

// PostgresMode causes the Smither to generate statements that work identically
// in Postgres and Cockroach.
var PostgresMode = multiOption(
	"postgres mode",
	CompareMode(),
	DisableWith(),
	SimpleDatums(),
	IgnoreFNs("^current_"),
	simpleOption("postgres", func(s *Smither) {
		s.postgres = true
	})(),

	// Some func impls differ from postgres, so skip them here.
	// #41709
	IgnoreFNs("^sha"),
	IgnoreFNs("^isnan"),
	IgnoreFNs("^crc32c"),
	IgnoreFNs("^fnv32a"),
	IgnoreFNs("^experimental_"),
	IgnoreFNs("^json_set"),
	IgnoreFNs("^concat_agg"),
	IgnoreFNs("^to_english"),
	IgnoreFNs("^substr$"),
	// We use e'XX' instead of E'XX' for hex strings, so ignore these.
	IgnoreFNs("^quote"),
	// We have some differences here with empty string and "default"; skip until fixed.
	IgnoreFNs("^pg_collation_for"),
	// Postgres does not have the `.*_escape` functions.
	IgnoreFNs("_escape$"),
	// Some spatial functions are CockroachDB-specific.
	IgnoreFNs("st_.*withinexclusive$"),
	IgnoreFNs("^postgis_.*build_date"),
	IgnoreFNs("^postgis_.*version"),
)
