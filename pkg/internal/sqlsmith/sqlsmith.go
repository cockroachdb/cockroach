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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	indexes        map[tree.TableName]map[tree.Name]*tree.CreateIndex
	nameCounts     map[string]int
	alters         *WeightedSampler
	scalars, bools *WeightedSampler
	selectStmts    *WeightedSampler

	stmtSampler, tableExprSampler *WeightedSampler
	statements                    statementWeights
	tableExprs                    tableExprWeights

	disableWith      bool
	disableImpureFns bool
	disableLimits    bool
	simpleDatums     bool
	avoidConsts      bool
	ignoreFNs        []*regexp.Regexp
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
		scope := s.makeScope()
		stmt, ok := scope.makeStmt()
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
	scope := s.makeScope()
	return makeScalar(scope, sqlbase.RandScalarType(s.rnd), nil)
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
}

// DisableMutations causes the Smither to not emit statements that could
// mutate any on-disk data.
func DisableMutations() SmitherOption {
	return disableMutations{}
}

type disableMutations struct{}

func (d disableMutations) Apply(s *Smither) {
	s.statements = nonMutatingStatements
	s.tableExprs = nonMutatingTableExprs
}

// DisableWith causes the Smither to not emit WITH clauses.
func DisableWith() SmitherOption {
	return disableWith{}
}

type disableWith struct{}

func (d disableWith) Apply(s *Smither) {
	s.disableWith = true
}

// DisableImpureFns causes the Smither to disable impure functions.
func DisableImpureFns() SmitherOption {
	return disableImpureFns{}
}

type disableImpureFns struct{}

func (d disableImpureFns) Apply(s *Smither) {
	s.disableImpureFns = true
}

// DisableCRDBFns causes the Smither to disable crdb_internal functions.
func DisableCRDBFns() SmitherOption {
	return IgnoreFNs("^crdb_internal")
}

// SimpleDatums causes the Smither to emit simpler constant datums.
func SimpleDatums() SmitherOption {
	return simpleDatums{}
}

type simpleDatums struct{}

func (d simpleDatums) Apply(s *Smither) {
	s.simpleDatums = true
}

// IgnoreFNs causes the Smither to ignore functions that match the regex.
func IgnoreFNs(regex string) SmitherOption {
	return ignoreFNs{r: regexp.MustCompile(regex)}
}

type ignoreFNs struct {
	r *regexp.Regexp
}

func (d ignoreFNs) Apply(s *Smither) {
	s.ignoreFNs = append(s.ignoreFNs, d.r)
}

// DisableLimits causes the Smither to disable LIMIT clauses.
func DisableLimits() SmitherOption {
	return disableLimits{}
}

type disableLimits struct{}

func (d disableLimits) Apply(s *Smither) {
	s.disableLimits = true
}

// AvoidConsts causes the Smither to prefer column references over generating
// constants.
func AvoidConsts() SmitherOption {
	return avoidConsts{}
}

type avoidConsts struct{}

func (d avoidConsts) Apply(s *Smither) {
	s.avoidConsts = true
}

type multiOption []SmitherOption

func (d multiOption) Apply(s *Smither) {
	for _, opt := range d {
		opt.Apply(s)
	}
}

// CompareMode causes the Smither to generate statements that have
// deterministic output.
func CompareMode() SmitherOption {
	return multiOption{
		DisableMutations(),
		DisableImpureFns(),
		DisableLimits(),
	}
}

// PostgresMode causes the Smither to generate statements that work identically
// in Postgres and Cockroach.
func PostgresMode() SmitherOption {
	return multiOption{
		CompareMode(),
		DisableWith(),
		DisableCRDBFns(),
		SimpleDatums(),
		IgnoreFNs("^current_"),
		IgnoreFNs("^version"),
	}
}

// SeedTable is a SQL statement that creates a table with most data types and
// some sample rows.
const SeedTable = `
CREATE TABLE IF NOT EXISTS tab_orig AS
	SELECT
		g::INT2 AS _int2,
		g::INT4 AS _int4,
		g::INT8 AS _int8,
		g::FLOAT4 AS _float4,
		g::FLOAT8 AS _float8,
		'2001-01-01'::DATE + g AS _date,
		'2001-01-01'::TIMESTAMP + g * '1 day'::INTERVAL AS _timestamp,
		'2001-01-01'::TIMESTAMPTZ + g * '1 day'::INTERVAL AS _timestamptz,
		g * '1 day'::INTERVAL AS _interval,
		g % 2 = 1 AS _bool,
		g::DECIMAL AS _decimal,
		g::STRING AS _string,
		g::STRING::BYTES AS _bytes,
		substring('00000000-0000-0000-0000-' || g::STRING || '00000000000', 1, 36)::UUID AS _uuid,
		'0.0.0.0'::INET + g AS _inet,
		g::STRING::JSONB AS _jsonb
	FROM
		generate_series(1, 5) AS g;
`
