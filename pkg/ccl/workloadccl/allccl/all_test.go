// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package allccl

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

const (
	directIngestion  = true
	oneFilePerNode   = 1
	noInjectStats    = false
	skipCSVRoundtrip = ``
)

func bigInitialData(meta workload.Meta) bool {
	switch meta.Name {
	case `tpcc`, `tpch`:
		return true
	default:
		return false
	}
}

func TestAllRegisteredImportFixture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, meta := range workload.Registered() {
		meta := meta
		gen := meta.New()
		hasInitialData := true
		for _, table := range gen.Tables() {
			if table.InitialRows.FillBatch == nil {
				hasInitialData = false
				break
			}
		}
		if !hasInitialData {
			continue
		}

		switch meta.Name {
		case `ycsb`, `startrek`, `roachmart`, `interleavedpartitioned`:
			// These don't work with IMPORT.
			continue
		case `tpch`:
			// tpch has an incomplete initial data implemention.
			continue
		}

		t.Run(meta.Name, func(t *testing.T) {
			if bigInitialData(meta) && (testing.Short() || util.RaceEnabled) {
				t.Skipf(`%s loads a lot of data`, meta.Name)
			}

			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase: "d",
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)

			if _, err := workloadccl.ImportFixture(
				ctx, db, gen, `d`, directIngestion, oneFilePerNode, noInjectStats, skipCSVRoundtrip,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAllRegisteredWorkloadsValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, meta := range workload.Registered() {
		gen := meta.New()

		var checkConsistencyFn func(context.Context, *gosql.DB) error
		if h, ok := gen.(workload.Hookser); ok {
			checkConsistencyFn = h.Hooks().CheckConsistency
		}
		if checkConsistencyFn == nil {
			// Not all workloads have CheckConsistency defined.
			continue
		}

		t.Run(meta.Name, func(t *testing.T) {
			if bigInitialData(meta) && (testing.Short() || util.RaceEnabled) {
				t.Skipf(`%s loads a lot of data`, meta.Name)
			}

			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase: "d",
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)

			if bigInitialData(meta) {
				// Special case generators with large initial data because Setup using
				// the batched inserts takes so long. Unfortunately, we can't do this
				// for all generators because some of them use things that IMPORT
				// doesn't yet handle, like foreign keys.
				if _, err := workloadccl.ImportFixture(
					ctx, db, gen, `d`, directIngestion, oneFilePerNode, noInjectStats, skipCSVRoundtrip,
				); err != nil {
					t.Fatal(err)
				}
			} else {
				const batchSize, concurrency = 0, 0
				if _, err := workload.Setup(ctx, db, gen, batchSize, concurrency); err != nil {
					t.Fatalf(`%+v`, err)
				}
			}

			if err := checkConsistencyFn(ctx, db); err != nil {
				t.Errorf(`%+v`, err)
			}
		})
	}
}

func TestConsistentSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test that the table schemas are consistent when the workload is created
	// multiple times with the same seed.

	for _, meta := range workload.Registered() {
		t.Run(meta.Name, func(t *testing.T) {
			tables1 := meta.New().Tables()
			tables2 := meta.New().Tables()
			for i := range tables1 {
				name := tables1[i].Name
				schema1 := tables1[i].Schema
				schema2 := tables2[i].Schema
				if schema1 != schema2 {
					t.Errorf("schema mismatch for table %s: %s, %s", name, schema1, schema2)
				}
			}
		})
	}
}
