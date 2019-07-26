// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)

func TestBankSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("bank")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestBulkingestSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("bulkingest")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestIndexesSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("indexes")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestInterleavedpartitionedSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("interleavedpartitioned")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestIntroSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("intro")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestJsonSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("json")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestKvSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("kv")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestLedgerSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("ledger")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestMovrSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("movr")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestQuerybenchSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("querybench")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestQuerylogSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("querylog")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestQueueSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("queue")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestRandSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("rand")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestRoachmartSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("roachmart")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestSqlsmithSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("sqlsmith")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestStartrekSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("startrek")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestTpccSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("tpcc")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestTpcdsSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("tpcds")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestTpchSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("tpch")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}

func TestYcsbSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("ycsb")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}
