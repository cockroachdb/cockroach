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
	"math/rand"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type Setup func(*rand.Rand) string

var Setups = map[string]Setup{
	"empty": stringSetup(""),
	// seed is a SQL statement that creates a table with most data types
	// and some sample rows.
	"seed": stringSetup(seedTable),
	// seed-vec is like seed except only types supported by vectorized
	// execution are used.
	"seed-vec":    stringSetup(vecSeedTable),
	"rand-tables": randTables,
}

var setupNames = func() []string {
	var ret []string
	for k := range Setups {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}()

func RandSetup(r *rand.Rand) string {
	n := r.Intn(len(setupNames))
	return setupNames[n]
}

func stringSetup(s string) Setup {
	return func(*rand.Rand) string {
		return s
	}
}

func randTables(r *rand.Rand) string {
	var sb strings.Builder

	n := r.Intn(5)
	for i := 0; i <= n; i++ {
		create := sqlbase.RandCreateTable(r, i+1)
		sb.WriteString(create.String())
		sb.WriteString(";\n")
	}

	// TODO(mjibson): add random INSERTs.

	return sb.String()
}

const (
	seedTable = `
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
		g::STRING::JSONB AS _jsonb
	FROM
		generate_series(1, 5) AS g;

INSERT INTO seed DEFAULT VALUES;
CREATE INDEX on seed (_int8, _float8, _date);
`

	vecSeedTable = `
CREATE TABLE IF NOT EXISTS seed_vec AS
	SELECT
		g::INT8 AS _int8,
		g::FLOAT8 AS _float8,
		'2001-01-01'::DATE + g AS _date,
		g % 2 = 1 AS _bool,
		g::DECIMAL AS _decimal,
		g::STRING AS _string,
		g::STRING::BYTES AS _bytes
	FROM
		generate_series(1, 5) AS g;

INSERT INTO seed_vec DEFAULT VALUES;
CREATE INDEX on seed_vec (_int8, _float8, _date);
`
)

type SettingFunc func(*rand.Rand) Setting

type Setting struct {
	Options []SmitherOption
	Mode    ExecMode
}

type ExecMode int

const (
	NoParallel ExecMode = iota
	Parallel
)

var Settings = map[string]SettingFunc{
	"default":           staticSetting(Parallel),
	"no-mutations":      staticSetting(Parallel, DisableMutations()),
	"no-ddl":            staticSetting(NoParallel, DisableDDLs()),
	"default+rand":      randSetting(Parallel),
	"no-mutations+rand": randSetting(Parallel, DisableMutations()),
	"no-ddl+rand":       randSetting(NoParallel, DisableDDLs()),
}

var settingNames = func() []string {
	var ret []string
	for k := range Settings {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}()

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

// randOptions is the list of SmitherOptions that can be chosen from randomly that are guaranteed to not add mutations or remove determinism from generated queries.
var randOptions = []SmitherOption{
	AvoidConsts(),
	CompareMode(),
	DisableLimits(),
	DisableWindowFuncs(),
	DisableWith(),
	PostgresMode(),
	SimpleDatums(),

	// Vectorizable() is not included here because it assumes certain
	// types don't exist in table schemas. Since we don't yet have a way to
	// verify that assumption, don't enable this.
}
