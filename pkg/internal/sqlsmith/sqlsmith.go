// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randident/randidentcfg"
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
	rnd *rand.Rand
	db  *gosql.DB
	// TODO(yuzefovich): clarify which objects this lock is protecting.
	lock      syncutil.RWMutex
	dbName    string
	schemas   []*schemaRef
	tables    []*tableRef
	sequences []*sequenceRef
	columns   map[tree.TableName]map[tree.Name]*tree.ColumnTableDef
	// Note: consider using getAllIndexesForTable helper if you need to iterate
	// over all indexes for a particular table.
	indexes map[tree.TableName]map[tree.Name]*tree.CreateIndex
	// Only one of nameCounts and nameGens will be used. nameCounts is used when
	// simpleNames is true.
	nameCounts       map[string]int
	nameGens         map[string]*nameGenInfo
	nameGenCfg       randidentcfg.Config
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
	plpgsqlStmtSampler                 *plpgsqlStmtSampler
	plpgsqlStmtWeights                 []plpgsqlStatementWeight

	disableWith                   bool
	disableNondeterministicFns    bool
	disableLimits                 bool
	disableNondeterministicLimits bool
	disableWindowFuncs            bool
	disableAggregateFuncs         bool
	disableMutations              bool
	simpleDatums                  bool
	simpleNames                   bool
	avoidConsts                   bool
	outputSort                    bool
	postgres                      bool
	ignoreFNs                     []*regexp.Regexp
	complexity                    float64
	scalarComplexity              float64
	simpleScalarTypes             bool
	unlikelyConstantPredicate     bool
	favorCommonData               bool
	unlikelyRandomNulls           bool
	stringConstPrefix             string
	disableJoins                  bool
	disableCrossJoins             bool
	disableIndexHints             bool
	lowProbWhereWithJoinTables    bool
	disableInsertSelect           bool
	disableDivision               bool
	disableDecimals               bool
	disableOIDs                   bool
	// disableUDFCreation indicates whether we're not allowed to create UDFs.
	// It follows that if we haven't created any UDFs, we have no UDFs to invoke
	// too.
	disableUDFCreation     bool
	disableIsolationChange bool

	bulkSrv     *httptest.Server
	bulkFiles   map[string][]byte
	bulkBackups map[string]tree.BackupTargetList
	bulkExports []string
}

type (
	statement        func(*Smither) (tree.Statement, bool)
	tableExpr        func(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool)
	selectStatement  func(s *Smither, desiredTypes []*types.T, refs colRefs, withTables tableRefs) (tree.SelectStatement, colRefs, bool)
	scalarExpr       func(*Smither, Context, *types.T, colRefs) (expr tree.TypedExpr, ok bool)
	plpgsqlStatement func(*Smither, plpgsqlBlockScope) (stmt plpgsqltree.Statement, ok bool)
)

// NewSmither creates a new Smither. db is used to populate existing tables
// for use as column references. It can be nil to skip table population.
func NewSmither(db *gosql.DB, rnd *rand.Rand, opts ...SmitherOption) (*Smither, error) {
	s := &Smither{
		rnd:        rnd,
		db:         db,
		nameCounts: map[string]int{},
		nameGens:   map[string]*nameGenInfo{},
		nameGenCfg: randident.DefaultNameGeneratorConfig(),

		stmtWeights:        allStatements,
		alterWeights:       alters,
		tableExprWeights:   allTableExprs,
		selectStmtWeights:  selectStmts,
		scalarExprWeights:  scalars,
		boolExprWeights:    bools,
		plpgsqlStmtWeights: plpgsqlStmts,

		complexity:       0.2,
		scalarComplexity: 0.2,
	}
	s.nameGenCfg.Finalize()
	for _, opt := range opts {
		opt.Apply(s)
	}
	s.stmtSampler = newWeightedStatementSampler(s.stmtWeights, rnd.Int63())
	s.alterSampler = newWeightedStatementSampler(s.alterWeights, rnd.Int63())
	s.tableExprSampler = newWeightedTableExprSampler(s.tableExprWeights, rnd.Int63())
	s.selectStmtSampler = newWeightedSelectStatementSampler(s.selectStmtWeights, rnd.Int63())
	s.scalarExprSampler = newWeightedScalarExprSampler(s.scalarExprWeights, rnd.Int63())
	s.boolExprSampler = newWeightedScalarExprSampler(s.boolExprWeights, rnd.Int63())
	s.plpgsqlStmtSampler = newWeightedPLpgSQLStmtSampler(s.plpgsqlStmtWeights, rnd.Int63())
	s.enableBulkIO()
	if s.db != nil {
		row := s.db.QueryRow("SELECT current_database()")
		if err := row.Scan(&s.dbName); err != nil {
			return nil, err
		}
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

// TestingPrettyCfg is only exposed to be used in tests.
var TestingPrettyCfg = prettyCfg

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

		printCfg := prettyCfg
		fl := tree.FmtParsable
		if s.postgres {
			printCfg.FmtFlags = tree.FmtPGCatalog
			fl = tree.FmtPGCatalog
		}
		p, err := printCfg.Pretty(stmt)
		if err != nil {
			// Use simple printing if pretty-printing fails.
			p = tree.AsStringWithFlags(stmt, fl)
		}
		return p
	}
}

// GenerateExpr returns a random SQL expression that does not depend on any
// tables or columns.
func (s *Smither) GenerateExpr() tree.TypedExpr {
	return makeScalar(s, s.randScalarType(), nil)
}

// GenerateUDF returns a random CREATE FUNCTION statement.
func (s *Smither) GenerateUDF() tree.Statement {
	for {
		routine, ok := s.makeCreateFunc()
		if ok {
			return routine
		}
	}
}

type nameGenInfo struct {
	g     randident.NameGenerator
	count int
}

func (s *Smither) name(prefix string) tree.Name {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.simpleNames {
		s.nameCounts[prefix]++
		count := s.nameCounts[prefix]
		return tree.Name(fmt.Sprintf("%s_%d", prefix, count))
	}
	g := s.nameGens[prefix]
	if g == nil {
		g = &nameGenInfo{
			g: randident.NewNameGenerator(&s.nameGenCfg, s.rnd, prefix),
		}
		s.nameGens[prefix] = g
	}
	g.count++
	return tree.Name(g.g.GenerateOne(strconv.Itoa(g.count)))
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

// DisableEverything disables every kind of statement.
var DisableEverything = simpleOption("disable every kind of statement", func(s *Smither) {
	s.stmtWeights = nil
})

// DisableMutations causes the Smither to not emit statements that could
// mutate any on-disk data.
var DisableMutations = simpleOption("disable mutations", func(s *Smither) {
	s.stmtWeights = nonMutatingStatements
	s.tableExprWeights = nonMutatingTableExprs
	s.disableMutations = true
})

// SetComplexity configures the Smither's complexity, in other words the
// likelihood that at any given node the Smither will recurse and create a
// deeper query tree. The default is .2. Note that this does not affect the
// complexity of generated scalar expressions, unless non-scalar expressions
// occur within a scalar expression.
func SetComplexity(complexity float64) SmitherOption {
	return option{
		name: "set complexity (likelihood of making a deeper random tree)",
		apply: func(s *Smither) {
			s.complexity = complexity
		},
	}
}

// SetScalarComplexity configures the Smither's scalar complexity, in other
// words the likelihood that within any given scalar expression the Smither will
// recurse and create a deeper nested expression. The default is .2.
func SetScalarComplexity(scalarComplexity float64) SmitherOption {
	return option{
		name: "set complexity (likelihood of making a deeper random tree)",
		apply: func(s *Smither) {
			s.scalarComplexity = scalarComplexity
		},
	}
}

// DisableDDLs causes the Smither to not emit statements that change table
// schema (CREATE, DROP, ALTER, etc.)
var DisableDDLs = simpleOption("disable DDLs", func(s *Smither) {
	s.stmtWeights = []statementWeight{
		{20, makeSelect},
		{5, makeInsert},
		{5, makeUpdate},
		{1, makeDelete},
		{1, makeCreateStats},
		{1, makeSetSessionCharacteristics},
		// If we don't have any DDL's, allow for use of savepoints and transactions.
		{2, makeBegin},
		{2, makeSavepoint},
		{2, makeReleaseSavepoint},
		{2, makeRollbackToSavepoint},
		{2, makeCommit},
		{2, makeRollback},
		// TODO(nvanbenschoten): add two-phase commit statements.
	}
})

// OnlySingleDMLs causes the Smither to only emit single-statement DML (SELECT,
// INSERT, UPDATE, DELETE), CREATE STATISTICS, and SET SESSION CHARACTERISTICS
// statements.
var OnlySingleDMLs = simpleOption("only single DMLs", func(s *Smither) {
	s.stmtWeights = []statementWeight{
		{20, makeSelect},
		{5, makeInsert},
		{5, makeUpdate},
		{1, makeDelete},
		{1, makeCreateStats},
		{1, makeSetSessionCharacteristics},
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

// MultiRegionDDLs causes the Smither to enable multiregion features.
var MultiRegionDDLs = simpleOption("include multiregion DDLs", func(s *Smither) {
	s.alterWeights = append(s.alterWeights, alterMultiregion...)
})

// EnableAlters enables ALTER statements.
var EnableAlters = simpleOption("include ALTER statements", func(s *Smither) {
	s.stmtWeights = append(s.stmtWeights, statementWeight{1, makeAlter})
})

// DisableWith causes the Smither to not emit WITH clauses.
var DisableWith = simpleOption("disable WITH", func(s *Smither) {
	s.disableWith = true
})

// EnableWith causes the Smither to probabilistically emit WITH clauses.
var EnableWith = simpleOption("enable WITH", func(s *Smither) {
	s.disableWith = false
})

// DisableNondeterministicFns causes the Smither to disable nondeterministic functions.
var DisableNondeterministicFns = simpleOption("disable nondeterministic funcs", func(s *Smither) {
	s.disableNondeterministicFns = true
})

// DisableCRDBFns causes the Smither to disable crdb_internal functions.
func DisableCRDBFns() SmitherOption {
	return IgnoreFNs("^crdb_internal")
}

// SimpleDatums causes the Smither to emit simpler constant datums.
var SimpleDatums = simpleOption("simple datums", func(s *Smither) {
	s.simpleDatums = true
})

// SimpleScalarTypes causes the Smither to use simpler scalar types (e.g. avoid Geometry).
var SimpleScalarTypes = simpleOption("simple scalar types", func(s *Smither) {
	s.simpleScalarTypes = true
})

// SimpleNames specifies that complex name generation should be disabled.
var SimpleNames = simpleOption("simple names", func(s *Smither) {
	s.simpleNames = true
})

// MutationsOnly causes the Smither to emit 60% INSERT, 10% UPDATE, 10% DELETE,
// 10% CREATE STATISTICS, and 10% SET SESSION CHARACTERISTICS statements.
var MutationsOnly = simpleOption("mutations only", func(s *Smither) {
	s.stmtWeights = []statementWeight{
		{6, makeInsert},
		{1, makeUpdate},
		{1, makeDelete},
		{1, makeCreateStats},
		{1, makeSetSessionCharacteristics},
	}
})

// InsUpdOnly causes the Smither to emit 70% INSERT, 10% UPDATE, 10% CREATE
// STATISTICS, and 10% SET SESSION CHARACTERISTICS statements.
var InsUpdOnly = simpleOption("inserts and updates only", func(s *Smither) {
	s.stmtWeights = []statementWeight{
		{7, makeInsert},
		{1, makeUpdate},
		{1, makeCreateStats},
		{1, makeSetSessionCharacteristics},
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

// DisableNondeterministicLimits causes the Smither to disable non-deterministic
// LIMIT clauses.
var DisableNondeterministicLimits = simpleOption("disable non-deterministic LIMIT", func(s *Smither) {
	s.disableNondeterministicLimits = true
})

// EnableLimits causes the Smither to probabilistically emit LIMIT clauses.
var EnableLimits = simpleOption("enable LIMIT", func(s *Smither) {
	s.disableLimits = false
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

// DisableAggregateFuncs disables window functions.
var DisableAggregateFuncs = simpleOption("disable aggregate funcs", func(s *Smither) {
	s.disableAggregateFuncs = true
})

// OutputSort adds a top-level ORDER BY on all columns.
var OutputSort = simpleOption("output sort", func(s *Smither) {
	s.outputSort = true
})

// MaybeSortOutput probabilistically adds ORDER by clause.
var MaybeSortOutput = simpleOption("maybe output sort", func(s *Smither) {
	s.outputSort = s.coin()
})

// UnlikelyConstantPredicate causes the Smither to make generation of constant
// WHERE clause, ON clause or HAVING clause predicates which only contain
// constant boolean expressions such as `TRUE` or `FALSE OR TRUE` much less
// likely.
var UnlikelyConstantPredicate = simpleOption("unlikely constant predicate", func(s *Smither) {
	s.unlikelyConstantPredicate = true
})

// FavorCommonData increases the chances the Smither generates scalar data
// from a predetermined set of common values, as opposed to purely random
// values. This helps increase the chances that two columns from the same
// type family will hold some of the same data values.
var FavorCommonData = simpleOption("favor common data", func(s *Smither) {
	s.favorCommonData = true
})

// UnlikelyRandomNulls causes the Smither to make random generation of null
// values much less likely than generation of random non-null data.
var UnlikelyRandomNulls = simpleOption("unlikely random nulls", func(s *Smither) {
	s.unlikelyRandomNulls = true
})

// PrefixStringConsts causes the Smither to add a prefix to all generated
// string constants.
func PrefixStringConsts(prefix string) SmitherOption {
	return option{
		name: fmt.Sprintf("prefix string constants with: %q", prefix),
		apply: func(s *Smither) {
			s.stringConstPrefix = prefix
		},
	}
}

// DisableJoins causes the Smither to disable joins.
var DisableJoins = simpleOption("disable joins", func(s *Smither) {
	s.disableJoins = true
})

// DisableCrossJoins causes the Smither to disable cross joins.
var DisableCrossJoins = simpleOption("disable cross joins", func(s *Smither) {
	s.disableCrossJoins = true
})

// DisableIndexHints causes the Smither to disable generation of index hints.
var DisableIndexHints = simpleOption("disable index hints", func(s *Smither) {
	s.disableIndexHints = true
})

// LowProbabilityWhereClauseWithJoinTables causes the Smither to generate WHERE
// clauses much less frequently in the presence of join tables. The default is
// to generate WHERE clauses 50% of the time.
var LowProbabilityWhereClauseWithJoinTables = simpleOption("low probability where clause with join tables", func(s *Smither) {
	s.lowProbWhereWithJoinTables = true
})

// DisableInsertSelect causes the Smither to avoid generating INSERT SELECT
// statements. Any INSERTs generated use a VALUES clause. The current main
// motivation for disabling INSERT SELECT is that we cannot detect when the
// source expression is nullable and the target column is not.
var DisableInsertSelect = simpleOption("disable insert select", func(s *Smither) {
	s.disableInsertSelect = true
})

// DisableDecimals disables use of decimal type columns in the query.
var DisableDecimals = simpleOption("disable decimals", func(s *Smither) {
	s.disableDecimals = true
})

// DisableOIDs disables use of OID types in the query.
var DisableOIDs = simpleOption("disable OIDs", func(s *Smither) {
	s.disableOIDs = true
})

// DisableUDFs causes the Smither to disable user-defined functions.
var DisableUDFs = simpleOption("disable udfs", func(s *Smither) {
	s.disableUDFCreation = true
})

// DisableIsolationChange causes the Smither to disable stmts that modify the
// txn isolation level.
var DisableIsolationChange = simpleOption("disable isolation change", func(s *Smither) {
	s.disableIsolationChange = true
})

// CompareMode causes the Smither to generate statements that have
// deterministic output.
var CompareMode = multiOption(
	"compare mode",
	DisableMutations(),
	DisableNondeterministicFns(),
	DisableCRDBFns(),
	IgnoreFNs("^version"),
	DisableNondeterministicLimits(),
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
	// Postgres does not support index hinting.
	DisableIndexHints(),
	// CockroachDB supports OID type but the same OID value might be assigned to
	// different objects from Postgres, and we thus disable using OID types in
	// randomly generated queries.
	DisableOIDs(),

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
	IgnoreFNs("^postgis_.*scripts"),
	IgnoreFNs("hlc_to_timestamp"),
	IgnoreFNs("st_s2covering"),
	IgnoreFNs("sum_int"),
)

// MutatingMode causes the Smither to generate mutation statements in the same
// way as the query-comparison roachtests (costfuzz and
// unoptimized-query-oracle).
var MutatingMode = multiOption(
	"mutating mode",
	MutationsOnly(),
	FavorCommonData(),
	UnlikelyRandomNulls(),
	DisableInsertSelect(),
	DisableCrossJoins(),
	SetComplexity(.05),
	SetScalarComplexity(.01),
)
