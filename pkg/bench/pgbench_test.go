// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package bench

import (
	"context"
	gosql "database/sql"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func BenchmarkPgbenchQuery(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		if err := SetupBenchDB(db, 20000, true /*quiet*/); err != nil {
			b.Fatal(err)
		}
		src := rand.New(rand.NewSource(5432))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := RunOne(db, src, 20000); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func BenchmarkPgbenchQueryParallel(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		if err := SetupBenchDB(db, 20000, true /*quiet*/); err != nil {
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
					err = RunOne(db, src, 20000)
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
		b.Skip("pgbench is not available on PATH")
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
	b.Run("Cockroach", func(b *testing.B) {
		s, _, _ := serverutils.StartServer(b, base.TestServerArgs{Insecure: true})
		defer s.Stopper().Stop(context.TODO())

		pgURL, cleanupFn := sqlutils.PGUrl(
			b, s.ServingAddr(), "benchmarkCockroach", url.User(security.RootUser))
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
			b.Skipf("unable to connect to postgres server on %s: %s", pgURL.Host, err)
		} else {
			conn.Close()
		}
		execPgbench(b, pgURL)
	})
}
