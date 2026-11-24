// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstats

import (
	"context"
	gosql "database/sql"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	dummyTable = `(
		id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
		col1 INT,
		col2 INT,
		col3 INT,
		col4 INT,
		col5 INT,
		col6 INT,
		col7 INT,
		col8 INT,
		col9 INT,
		col10 INT,
    INDEX idx_1 (col1 ASC)
	)`
	tableName     = "sql_stats_workload"
	defaultDbName = "sql_stats"
)

// fingerprintSizeConfigValues configures the size of the query generated for
// READ operations. The values of this map are used to determine how many times
// a character is repeated in the column alias, increasing the size of the
// query.
var fingerprintSizeConfigValues = map[string]int{
	"xsmall": 1,
	"small":  10,
	"medium": 75,
	"large":  100,
	"xlarge": 500,
}

var fingerprintSizeConfigOptions = strings.Join(slices.Collect(maps.Keys(fingerprintSizeConfigValues)), ", ")

var RandomSeed = workload.NewUint64RandomSeed()

type sqlStats struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	readPercent     float64
	cardinality     int
	fingerprintSize string
}

var _ workload.Generator = &sqlStats{}
var _ workload.Opser = &sqlStats{}
var _ workload.Hookser = &sqlStats{}

func init() {
	workload.Register(sqlStatsMeta)
}

var sqlStatsMeta = workload.Meta{
	Name:          "sqlstats",
	Description:   `Sqlstats generates a workload with a high cardinality of statement fingerprints`,
	RandomSeed:    RandomSeed,
	Version:       "1.0.0",
	TestInfraOnly: true,
	New: func() workload.Generator {
		g := &sqlStats{}
		g.flags.FlagSet = pflag.NewFlagSet(`sqlstats`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`read-percent`:     {RuntimeOnly: true},
			`cardinality`:      {RuntimeOnly: true},
			`fingerprint-size`: {RuntimeOnly: true},
		}
		g.flags.Float64Var(&g.readPercent, `read-percent`, 0.0,
			`Percent (0-1.0) of operations that are reads vs writes`)
		g.flags.IntVar(&g.cardinality, `cardinality`, 6,
			"Configures the cardinality of the workload. The value provided "+
				"determines the amount of unique query fingerprints generated for read "+
				"and write queries. For example, a cardinality of 4 will generate 4! "+
				"(24) unique read and write query fingerprints")

		g.flags.StringVar(&g.fingerprintSize, `fingerprint-size`, "small",
			fmt.Sprintf(`The cardinality of the workload. Options are %s`,
				fingerprintSizeConfigOptions))

		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (s *sqlStats) Meta() workload.Meta { return sqlStatsMeta }

// Flags implements the Flagser interface.
func (s *sqlStats) Flags() workload.Flags { return s.flags }

// ConnFlags implements the ConnFlagser interface.
func (s *sqlStats) ConnFlags() *workload.ConnFlags { return s.connFlags }

func (s *sqlStats) Tables() []workload.Table {
	return []workload.Table{{
		Name:   tableName,
		Schema: dummyTable,
		Splits: workload.BatchedTuples{},
	}}
}

func (s *sqlStats) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			return s.validateConfig()
		},
	}
}

func (s *sqlStats) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(s.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(s.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}

	for i := 0; i < s.connFlags.Concurrency; i++ {
		cols := make([]int, s.cardinality)
		for col := range s.cardinality {
			cols[col] = col + 1
		}

		worker := sqlStatsWorker{
			queryCols:   cols,
			colNameLen:  fingerprintSizeConfigValues[s.fingerprintSize],
			db:          db,
			hists:       reg.GetHandle(),
			rng:         rand.New(rand.NewPCG(RandomSeed.Seed(), uint64(i))),
			readPercent: s.readPercent,
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.work)
	}
	return ql, nil
}

func (s *sqlStats) validateConfig() error {
	if s.readPercent < 0 || s.readPercent > 1 {
		return errors.Newf("read-percent must be between 0 and 1.0, got %f", s.readPercent)
	}

	if s.cardinality < 1 || s.cardinality > 10 {
		return errors.Newf("cardinality must be between greater than 0 and less than 11, got %d", s.cardinality)
	}

	if _, ok := fingerprintSizeConfigValues[s.fingerprintSize]; !ok {
		return errors.Newf("fingerprint-size must be one of %s, got %s", fingerprintSizeConfigOptions, s.fingerprintSize)
	}
	return nil
}

type sqlStatsWorker struct {
	db    *gosql.DB
	hists *histogram.Histograms
	rng   *rand.Rand

	// readPercent is the percentage of operations that are reads
	readPercent float64
	// queryCols are the columns to be used in the queries for this workload
	queryCols []int
	// colNameLen is used to determine the length of the column alias in the
	// SELECT queries.
	colNameLen int
}

func (sw *sqlStatsWorker) ShuffleCols() {
	sw.rng.Shuffle(len(sw.queryCols), func(i, j int) {
		sw.queryCols[i], sw.queryCols[j] = sw.queryCols[j], sw.queryCols[i]
	})
}

func (sw *sqlStatsWorker) insert() error {
	sw.ShuffleCols()
	maxCols := len(sw.queryCols)

	cols := make([]string, maxCols)
	placeholders := make([]string, maxCols)
	args := make([]interface{}, maxCols)
	for i := range maxCols {
		cols[i] = fmt.Sprintf("col%d", sw.queryCols[i])
		placeholders[i] = "$" + fmt.Sprintf("%d", i+1)
		args[i] = sw.rng.Int()
	}
	query := fmt.Sprintf(`INSERT INTO sql_stats_workload (%s) VALUES (%s)`, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
	_, err := sw.db.Exec(query, args...)
	return err
}

func (sw *sqlStatsWorker) query() error {
	sw.ShuffleCols()
	cols := make([]string, len(sw.queryCols))
	for i := 0; i < len(cols); i++ {
		s := fmt.Sprintf(`%d`, sw.queryCols[i])
		cols[i] = fmt.Sprintf(`col%s as "col-%s"`, s, strings.Repeat(s, sw.colNameLen))
	}
	query := fmt.Sprintf(`SELECT %s FROM sql_stats_workload WHERE col1 = (SELECT max(col1) FROM sql_stats_workload)`, strings.Join(cols, ", "))
	_, err := sw.db.Exec(query)
	return err
}

func (sw *sqlStatsWorker) work(ctx context.Context) error {
	startTime := timeutil.Now()
	var err error
	var opName string
	if sw.rng.Float64() < sw.readPercent {
		opName = "read"
		err = sw.query()
	} else {
		opName = "write"
		err = sw.insert()
	}

	if err == nil {
		sw.hists.Get(opName).Record(timeutil.Since(startTime))
	}

	return err
}
