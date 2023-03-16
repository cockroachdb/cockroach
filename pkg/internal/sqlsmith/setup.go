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
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Setup generates a SQL query that can be executed to initialize a database
// for smithing.
type Setup func(*rand.Rand) []string

// RandTableSetupName is the name of the table setup that creates random tables.
const RandTableSetupName = "rand-tables"

// SeedMultiRegionSetupName is the name of the table setup that creates
// multi-region tables.
const SeedMultiRegionSetupName = "seed-multi-region"

// Setups is a collection of useful initial table states.
var Setups = map[string]Setup{
	"empty": wrapCommonSetup(stringSetup("")),
	// seed is a SQL statement that creates a table with most data types
	// and some sample rows.
	"seed":                   wrapCommonSetup(stringSetup(seedTable)),
	SeedMultiRegionSetupName: wrapCommonSetup(stringSetup(multiregionSeed)),
	RandTableSetupName:       wrapCommonSetup(randTables),
}

// wrapCommonSetup wraps setup steps common to all SQLSmith setups around the
// specific setup passed in.
func wrapCommonSetup(setupFn Setup) Setup {
	return func(r *rand.Rand) []string {
		return append([]string{
			"SET sql_safe_updates = false;",
		}, setupFn(r)...)
	}
}

var setupNames = func() []string {
	var ret []string
	for k := range Setups {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}()

// RandSetup returns a random key from Setups.
func RandSetup(r *rand.Rand) string {
	n := r.Intn(len(setupNames))
	return setupNames[n]
}

func stringSetup(s string) Setup {
	return func(*rand.Rand) []string {
		return []string{s}
	}
}

// RandTablesPrefixStringConsts is similar to the rand-tables setup but injects
// a prefix into string constants used in the CREATE TABLE statements.
func RandTablesPrefixStringConsts(r *rand.Rand, prefix string) []string {
	return randTablesN(r, r.Intn(5)+1, prefix)
}

// randTables is a Setup function that creates 1-5 random tables.
func randTables(r *rand.Rand) []string {
	return randTablesN(r, r.Intn(5)+1, "")
}

// stringConstRegex is a pattern that matches SQL string literals with type
// assertions. It deliberately does not match string literals containing an
// escaped single quote (either doubled or backslash-escaped) to try to avoid
// matches that do not cover the entire string literal. It's not a parser, so it
// will mismatch on some pathological cases (e.g. something evil like
// e'\':::STRING ':::STRING) in which case the mutated CREATE TABLE statement
// might be malformed.
var stringConstRegex = regexp.MustCompile(`[^'\\]'[^']*':::STRING[^:[]`)

// randTablesN is a Setup function that creates n random tables.
func randTablesN(r *rand.Rand, n int, prefix string) []string {
	var stmts []string
	// Since we use the stats mutator, disable auto stats generation.
	stmts = append(stmts, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`)
	stmts = append(stmts, `SET CLUSTER SETTING sql.stats.histogram_collection.enabled = false;`)

	// Create the random tables.
	createTableStatements := randgen.RandCreateTables(
		r, "table", n, false /* isMultiRegion */, randgen.StatisticsMutator,
		randgen.PartialIndexMutator, randgen.ForeignKeyMutator, //prefixStringConstsMutator,
	)

	for _, ast := range createTableStatements {
		stmt := tree.SerializeForDisplay(ast)
		// Inject prefix into string constants.
		if prefix != "" {
			stmt = stringConstRegex.ReplaceAllStringFunc(stmt, func(match string) string {
				return strings.Replace(match, "'", "'"+prefix, 1)
			})
		}
		stmts = append(stmts, stmt)
	}

	// Create some random types as well.
	numTypes := r.Intn(5) + 1
	for i := 0; i < numTypes; i++ {
		name := fmt.Sprintf("rand_typ_%d", i)
		stmt := randgen.RandCreateType(r, name, letters)
		stmts = append(stmts, stmt.String())
	}
	return stmts
}

const (
	seedTable = `
BEGIN; CREATE TYPE greeting AS ENUM ('hello', 'howdy', 'hi', 'good day', 'morning'); COMMIT;
BEGIN;
CREATE TABLE IF NOT EXISTS seed AS
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
		g::STRING::JSONB AS _jsonb,
		enum_range('hello'::greeting)[g] as _enum
	FROM
		generate_series(1, 5) AS g;
COMMIT;

INSERT INTO seed DEFAULT VALUES;
CREATE INDEX on seed (_int8, _float8, _date);
CREATE INVERTED INDEX on seed (_jsonb);
`

	multiregionSeed = `
CREATE TABLE IF NOT EXISTS seed_mr_table AS
	SELECT
		g::INT2 AS _int2,
		g::INT4 AS _int4,
		g::INT8 AS _int8,
		g::FLOAT8 AS _float8,
		'2001-01-01'::DATE + g AS _date,
		'2001-01-01'::TIMESTAMP + g * '1 day'::INTERVAL AS _timestamp,
		'2001-01-01'::TIMESTAMPTZ + g * '1 day'::INTERVAL AS _timestamptz,
		g * '1 day'::INTERVAL AS _interval,
		g % 2 = 1 AS _bool,
		g::DECIMAL AS _decimal,
		g::STRING AS _string,
		g::STRING::BYTES AS _bytes,
		substring('00000000-0000-0000-0000-' || g::STRING || '00000000000', 1, 36)::UUID AS _uuid
	FROM
		generate_series(1, 5) AS g;
`
)

// SettingFunc generates a Setting.
type SettingFunc func(*rand.Rand) Setting

// Setting defines options and execution modes for a Smither.
type Setting struct {
	Options []SmitherOption
	Mode    ExecMode
}

// ExecMode definitions define how a Setting can be executed.
type ExecMode int

const (
	// NoParallel indicates that, if determinism is desired, this Setting
	// should not be executed in parallel.
	NoParallel ExecMode = iota
	// Parallel indicates that this Setting can be executed in parallel and
	// still preserve determinism.
	Parallel
)

// Settings is a collection of useful Setting options.
var Settings = map[string]SettingFunc{
	"default":           staticSetting(Parallel),
	"no-mutations":      staticSetting(Parallel, DisableMutations()),
	"no-ddl":            staticSetting(NoParallel, DisableDDLs()),
	"default+rand":      randSetting(Parallel),
	"no-mutations+rand": randSetting(Parallel, DisableMutations()),
	"no-ddl+rand":       randSetting(NoParallel, DisableDDLs()),
	"ddl-nodrop":        randSetting(NoParallel, OnlyNoDropDDLs()),
	"multi-region":      randSetting(Parallel, MultiRegionDDLs()),
}

var settingNames = func() []string {
	var ret []string
	for k := range Settings {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}()

// RandSetting returns a random key from Settings.
func RandSetting(r *rand.Rand) string {
	return settingNames[r.Intn(len(settingNames))]
}

func staticSetting(mode ExecMode, opts ...SmitherOption) SettingFunc {
	return func(*rand.Rand) Setting {
		return Setting{
			Options: opts,
			Mode:    mode,
		}
	}
}

func randSetting(mode ExecMode, staticOpts ...SmitherOption) SettingFunc {
	return func(r *rand.Rand) Setting {
		// Generate a random subset of randOptions.
		opts := append([]SmitherOption(nil), randOptions...)
		r.Shuffle(len(opts), func(i, j int) {
			opts[i], opts[j] = opts[j], opts[i]
		})
		// Use between (inclusive) none and all of the shuffled options.
		opts = opts[:r.Intn(len(opts)+1)]
		opts = append(opts, staticOpts...)
		return Setting{
			Options: opts,
			Mode:    mode,
		}
	}
}

// randOptions is the list of SmitherOptions that can be chosen from randomly
// that are guaranteed to not add mutations or remove determinism from
// generated queries.
var randOptions = []SmitherOption{
	AvoidConsts(),
	CompareMode(),
	DisableLimits(),
	DisableWindowFuncs(),
	DisableWith(),
	PostgresMode(),
	SimpleDatums(),
}
