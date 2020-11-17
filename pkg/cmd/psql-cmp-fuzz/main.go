// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// psql-cmp-fuzz fuzzes commands against postgres and CRDB locally.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/jackc/pgx/v4"
)

type sqlConn struct {
	internalID string
	*pgx.Conn
}

func connect(ctx context.Context, id string, pgURL string) sqlConn {
	conn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		log.Fatalf("error connecting to %s: %v", id, err)
	}
	return sqlConn{
		internalID: id,
		Conn:       conn,
	}
}

var flagTimeLimit = flag.Duration(
	"time_limit",
	1*time.Minute,
	"limit to run tests for",
)
var flagSeed = flag.Int(
	"seed",
	0,
	"seed to run randomizer - 0 means choose random seed",
)

type randomizer interface {
	genSQL(rng *rand.Rand) string
}

type timestamptzSubtract struct{}

var _ randomizer = &timestamptzSubtract{}

func (timestamptzSubtract) genSQL(rng *rand.Rand) string {
	tz1 := rowenc.RandDatum(rng, types.TimestampTZ, false /* nullOk */)
	tz2 := rowenc.RandDatum(rng, types.TimestampTZ, false /* nullOk */)
	// We can only tolerate 292 years of difference.
	// Postgres errors a lot in years < -1000.
	for math.Abs(float64(tz1.(*tree.DTimestampTZ).Time.Year()-tz2.(*tree.DTimestampTZ).Time.Year())) > 290 ||
		tz1.(*tree.DTimestampTZ).Time.Year() < -1000 {
		tz1 = rowenc.RandDatum(rng, types.TimestampTZ, false /* nullOk */)
		tz2 = rowenc.RandDatum(rng, types.TimestampTZ, false /* nullOk */)
	}
	return fmt.Sprintf("SELECT (%s::timestamptz - %s::timestamptz)::text", tz1.String(), tz2.String())
}

var randomizers = []randomizer{
	&timestamptzSubtract{},
}

func main() {
	ctx := context.Background()
	flag.Parse()

	conns := make(map[string]sqlConn)
	for _, toConnect := range []struct {
		id    string
		pgURL string
	}{
		{"cockroach", "postgresql://root@localhost:26257?sslmode=disable"},
		{"postgres", "postgresql://localhost:5432"},
	} {
		conns[toConnect.id] = connect(ctx, toConnect.id, toConnect.pgURL)
	}

	seed := *flagSeed
	if seed == 0 {
		seed = rand.Int()
	}
	fmt.Printf("* Running with seed %d\n", seed)
	rng := rand.New(rand.NewSource(int64(seed)))
	start := time.Now()
	for time.Now().Sub(start).Nanoseconds() < (*flagTimeLimit).Nanoseconds() {
		r := randomizers[rng.Intn(len(randomizers))]
		sql := r.genSQL(rng)
		fmt.Printf("* running %s\n", sql)
		type result struct {
			id     string
			result string
		}
		var results []result
		for id, conn := range conns {
			res := conn.QueryRow(ctx, sql)
			var ret string
			if err := res.Scan(&ret); err != nil {
				log.Fatalf("error from %s: %v", id, err)
			}
			results = append(results, result{id: id, result: ret})
		}

		for _, toCmp := range results[1:] {
			if toCmp.result != results[0].result {
				log.Fatalf("mismatch: %s: %v vs %s: %v\n", toCmp.id, toCmp.result, results[0].id, results[0].result)
			}
		}
	}
}
