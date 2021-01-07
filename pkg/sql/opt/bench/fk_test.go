// Copyright 2019 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func runFKBench(
	b *testing.B,
	setup func(b *testing.B, r *sqlutils.SQLRunner, setupFKs bool),
	run func(b *testing.B, r *sqlutils.SQLRunner),
) {
	configs := []struct {
		name           string
		setupFKs       bool
		insertFastPath bool
	}{
		{name: "None", setupFKs: false},
		{name: "NoFastPath", setupFKs: true, insertFastPath: false},
		{name: "FastPath", setupFKs: true, insertFastPath: true},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			s, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
			defer s.Stopper().Stop(context.Background())
			r := sqlutils.MakeSQLRunner(db)
			// Don't let auto stats interfere with the test. Stock stats are
			// sufficient to get the right plans (i.e. lookup join).
			r.Exec(b, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
			r.Exec(b, fmt.Sprintf("SET enable_insert_fast_path = %v", cfg.insertFastPath))
			setup(b, r, cfg.setupFKs)
			b.ResetTimer()
			run(b, r)
		})
	}
}

func BenchmarkFKInsert(b *testing.B) {
	defer log.Scope(b).Close(b)

	const parentRows = 1000
	setup := func(b *testing.B, r *sqlutils.SQLRunner, setupFKs bool) {
		r.Exec(b, "CREATE TABLE child (k int primary key, p int)")
		r.Exec(b, "CREATE TABLE parent (p int primary key, data int)")

		if setupFKs {
			r.Exec(b, "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (p) REFERENCES parent(p)")
		} else {
			// Create the index on p manually so it's a more fair comparison.
			r.Exec(b, "CREATE INDEX idx ON child(p)")
		}

		r.Exec(b, fmt.Sprintf(
			"INSERT INTO parent SELECT i, i FROM generate_series(0,%d) AS g(i)", parentRows-1,
		))
	}

	b.Run("SingleRow", func(b *testing.B) {
		runFKBench(b, setup, func(b *testing.B, r *sqlutils.SQLRunner) {
			for i := 0; i < b.N; i++ {
				r.Exec(b, fmt.Sprintf("INSERT INTO child VALUES (%d, %d)", i, i%parentRows))
			}
		})
	})

	const batch = 20
	b.Run("MultiRowSingleParent", func(b *testing.B) {
		runFKBench(b, setup, func(b *testing.B, r *sqlutils.SQLRunner) {
			k := 0
			for i := 0; i < b.N; i++ {
				// All rows in the batch reference the same parent value.
				parent := i % parentRows
				vals := make([]string, batch)
				for j := range vals {
					vals[j] = fmt.Sprintf("(%d, %d)", k, parent)
					k++
				}
				r.Exec(b, fmt.Sprintf("INSERT INTO child VALUES %s", strings.Join(vals, ",")))
			}
		})
	})

	b.Run("MultiRowMultiParent", func(b *testing.B) {
		runFKBench(b, setup, func(b *testing.B, r *sqlutils.SQLRunner) {
			k := 0
			for i := 0; i < b.N; i++ {
				vals := make([]string, batch)
				for j := range vals {
					vals[j] = fmt.Sprintf("(%d, %d)", k, k%parentRows)
					k++
				}
				r.Exec(b, fmt.Sprintf("INSERT INTO child VALUES %s", strings.Join(vals, ",")))
			}
		})
	})
}
