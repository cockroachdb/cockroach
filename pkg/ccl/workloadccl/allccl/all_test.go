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
	case `tpcc`, `tpch`, `tpcds`:
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

		// This test is big enough that it causes timeout issues under race, so only
		// run one workload. Doing any more than this doesn't get us enough to be
		// worth the hassle.
		if util.RaceEnabled && meta.Name != `bank` {
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
			if bigInitialData(meta) && testing.Short() {
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

			// Run the consistency check if this workload has one.
			if h, ok := gen.(workload.Hookser); ok {
				if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
					if err := checkConsistencyFn(ctx, db); err != nil {
						t.Errorf(`%+v`, err)
					}
				}
			}
		})
	}
}

func TestAllRegisteredSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, meta := range workload.Registered() {
		if bigInitialData(meta) {
			continue
		}

		// This test is big enough that it causes timeout issues under race, so only
		// run one workload. Doing any more than this doesn't get us enough to be
		// worth the hassle.
		if util.RaceEnabled && meta.Name != `bank` {
			continue
		}

		switch meta.Name {
		case `roachmart`, `interleavedpartitioned`:
			// These require a specific node locality setup
			continue
		}

		gen := meta.New()
		t.Run(meta.Name, func(t *testing.T) {
			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase: "d",
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)
			sqlutils.MakeSQLRunner(db).Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = false`)

			const batchSize, concurrency = 0, 0
			if _, err := workload.Setup(ctx, db, gen, batchSize, concurrency); err != nil {
				t.Fatalf(`%+v`, err)
			}

			// Run the consistency check if this workload has one.
			if h, ok := gen.(workload.Hookser); ok {
				if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
					if err := checkConsistencyFn(ctx, db); err != nil {
						t.Errorf(`%+v`, err)
					}
				}
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
