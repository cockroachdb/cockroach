// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstats

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
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
		col10 INT
	)`
	tableName     = "sql_stats_workload"
	defaultDbName = "sql_stats"
)

var RandomSeed = workload.NewUint64RandomSeed()

type sqlStats struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags
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
			return nil
		},
	}
}

type gen struct {
	syncutil.Mutex

	in  []string
	rng *rand.Rand
}

func (g *gen) Next() string {
	g.Lock()
	defer g.Unlock()

	g.shuffleLocked()

	s := strings.Join(g.in, ", ")
	return fmt.Sprintf(`
		INSERT INTO sql_stats_workload
		(%s) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, s)
}

func (g *gen) shuffleLocked() {
	g.rng.Shuffle(len(g.in), func(i, j int) {
		g.in[i], g.in[j] = g.in[j], g.in[i]
	})
}

func genPermutations() *gen {
	rng := rand.New(rand.NewSource(RandomSeed.Seed()))
	return &gen{
		rng: rng,
		in:  []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"},
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

	gen := genPermutations()

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}
	for i := 0; i < s.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(RandomSeed.Seed()))
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			start := timeutil.Now()

			query := gen.Next()
			updateStmt, err := db.Prepare(query)
			if err != nil {
				return errors.CombineErrors(err, db.Close())
			}

			_, err = updateStmt.Exec(rng.Int(), rng.Int(), rng.Int(), rng.Int(), rng.Int(), rng.Int(), rng.Int(), rng.Int(), rng.Int(), rng.Int())
			elapsed := timeutil.Since(start)
			hists.Get(`transfer`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}
