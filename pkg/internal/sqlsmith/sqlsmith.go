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
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	rnd            *rand.Rand
	db             *gosql.DB
	lock           syncutil.RWMutex
	tables         []*tableRef
	columns        map[tree.TableName]map[tree.Name]*tree.ColumnTableDef
	indexes        map[tree.TableName]map[tree.Name]*tree.CreateIndex
	nameCounts     map[string]int
	alters         *WeightedSampler
	scalars, bools *WeightedSampler
	selectStmts    *WeightedSampler

	stmtSampler, tableExprSampler *WeightedSampler
	statements                    statementWeights
	tableExprs                    tableExprWeights

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
}

// NewSmither creates a new Smither. db is used to populate existing tables
// for use as column references. It can be nil to skip table population.
func NewSmither(db *gosql.DB, rnd *rand.Rand, opts ...SmitherOption) (*Smither, error) {
	s := &Smither{
		rnd:         rnd,
		db:          db,
		nameCounts:  map[string]int{},
		scalars:     NewWeightedSampler(scalarWeights, rnd.Int63()),
		bools:       NewWeightedSampler(boolWeights, rnd.Int63()),
		selectStmts: NewWeightedSampler(selectStmtWeights, rnd.Int63()),
		alters:      NewWeightedSampler(alterWeights, rnd.Int63()),

		statements: allStatements,
		tableExprs: allTableExprs,
		complexity: 0.2,
	}
	for _, opt := range opts {
		opt.Apply(s)
	}
	s.stmtSampler = NewWeightedSampler(s.statements.Weights(), rnd.Int63())
	s.tableExprSampler = NewWeightedSampler(s.tableExprs.Weights(), rnd.Int63())
	return s, s.ReloadSchemas()
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
	s.statements = nonMutatingStatements
	s.tableExprs = nonMutatingTableExprs
})

// DisableDDLs causes the Smither to not emit statements that change table
// schema (CREATE, DROP, ALTER, etc.)
var DisableDDLs = simpleOption("disable DDLs", func(s *Smither) {
	s.statements = statementWeights{
		{20, makeSelect},
		{5, makeInsert},
		{5, makeUpdate},
		{1, makeDelete},
	}
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
		s.statements = nonMutatingStatements
		s.tableExprs = vectorizableTableExprs
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
	DisableLimits(),
	OutputSort(),
)

// PostgresMode causes the Smither to generate statements that work identically
// in Postgres and Cockroach.
var PostgresMode = multiOption(
	"postgres mode",
	CompareMode(),
	DisableWith(),
	DisableCRDBFns(),
	SimpleDatums(),
	IgnoreFNs("^current_"),
	IgnoreFNs("^version"),
	simpleOption("postgres", func(s *Smither) {
		s.postgres = true
	})(),

	// Some func impls differ from postgres, so skip them here.
	// #41709
	IgnoreFNs("^sha"),
	// #41707
	IgnoreFNs("^to_hex"),
	// #41708
	IgnoreFNs("^quote_literal"),
)
