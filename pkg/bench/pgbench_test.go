// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os/exec"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func BenchmarkPgbenchQuery(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		if err := SetupBenchDB(db.DB, 20000, true /*quiet*/); err != nil {
			b.Fatal(err)
		}
		src := rand.New(rand.NewSource(5432))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := RunOne(db.DB, src, 20000); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func BenchmarkPgbenchQueryParallel(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		if err := SetupBenchDB(db.DB, 20000, true /*quiet*/); err != nil {
			b.Fatal(err)
		}

		retryOpts := retry.Options{
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     200 * time.Millisecond,
			Multiplier:     2,
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			src := rand.New(rand.NewSource(5432))
			r := retry.Start(retryOpts)
			var err error
			for pb.Next() {
				r.Reset()
				for r.Next() {
					err = RunOne(db.DB, src, 20000)
					if err == nil {
						break
					}
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.StopTimer()
	})
}

func execPgbench(b *testing.B, pgURL url.URL) {
	if _, err := exec.LookPath("pgbench"); err != nil {
		skip.IgnoreLint(b, "pgbench is not available on PATH")
	}
	c, err := SetupExec(pgURL, "bench", 20000, b.N)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	out, err := c.CombinedOutput()
	if testing.Verbose() || err != nil {
		fmt.Println(string(out))
	}
	if err != nil {
		b.Log(c)
		b.Fatal(err)
	}
	b.StopTimer()
}

func BenchmarkPgbenchExec(b *testing.B) {
	defer log.Scope(b).Close(b)
	b.Run("Cockroach", func(b *testing.B) {
		s, _, _ := serverutils.StartServer(b, base.TestServerArgs{Insecure: true})
		defer s.Stopper().Stop(context.Background())

		pgURL, cleanupFn := sqlutils.PGUrl(
			b, s.ServingSQLAddr(), "benchmarkCockroach", url.User(security.RootUser))
		pgURL.RawQuery = "sslmode=disable"
		defer cleanupFn()

		execPgbench(b, pgURL)
	})

	b.Run("Postgres", func(b *testing.B) {
		pgURL := url.URL{
			Scheme:   "postgres",
			Host:     "localhost:5432",
			RawQuery: "sslmode=disable&dbname=postgres",
		}
		if conn, err := net.Dial("tcp", pgURL.Host); err != nil {
			skip.IgnoreLintf(b, "unable to connect to postgres server on %s: %s", pgURL.Host, err)
		} else {
			conn.Close()
		}
		execPgbench(b, pgURL)
	})
}
