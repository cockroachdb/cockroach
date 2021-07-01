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
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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

	sqlMemoryPoolSize := int64(1000 << 20) // 1GiB

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
		case `startrek`, `roachmart`, `interleavedpartitioned`:
			// These don't work with IMPORT.
			continue
		case `tpch`:
			// TODO(dan): Implement a timmed down version of TPCH to keep the test
			// runtime down.
			continue
		}

		t.Run(meta.Name, func(t *testing.T) {
			if bigInitialData(meta) {
				skip.UnderShort(t, fmt.Sprintf(`%s loads a lot of data`, meta.Name))
			}
			defer log.Scope(t).Close(t)

			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase:       "d",
				SQLMemoryPoolSize: sqlMemoryPoolSize,
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)

			l := workloadccl.ImportDataLoader{}
			if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
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

		gen := meta.New()
		switch meta.Name {
		case `roachmart`:
			// TODO(dan): It'd be nice to test this with the default flags. For now,
			// this is better than nothing.
			flags := gen.(workload.Flagser).Flags()
			if err := flags.Parse([]string{
				`--users=10`, `--orders=100`, `--partition=false`,
			}); err != nil {
				t.Fatal(err)
			}
		case `interleavedpartitioned`:
			// This require a specific node locality setup
			continue
		}

		t.Run(meta.Name, func(t *testing.T) {
			defer log.Scope(t).Close(t)
			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase: "d",
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)
			sqlutils.MakeSQLRunner(db).Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = false`)

			var l workloadsql.InsertsDataLoader
			if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
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
	defer log.Scope(t).Close(t)
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

func hashTableInitialData(
	h hash.Hash, data workload.BatchedTuples, a *bufalloc.ByteAllocator,
) error {
	var scratch [8]byte
	b := coldata.NewMemBatchWithCapacity(nil /* typs */, 0 /* capacity */, coldata.StandardColumnFactory)
	for batchIdx := 0; batchIdx < data.NumBatches; batchIdx++ {
		*a = a.Truncate()
		data.FillBatch(batchIdx, b, a)
		for _, col := range b.ColVecs() {
			switch t := col.Type(); col.CanonicalTypeFamily() {
			case types.BoolFamily:
				for _, x := range col.Bool()[:b.Length()] {
					if x {
						scratch[0] = 1
					} else {
						scratch[0] = 0
					}
					_, _ = h.Write(scratch[:1])
				}
			case types.IntFamily:
				switch t.Width() {
				case 0, 64:
					for _, x := range col.Int64()[:b.Length()] {
						binary.LittleEndian.PutUint64(scratch[:8], uint64(x))
						_, _ = h.Write(scratch[:8])
					}
				case 16:
					for _, x := range col.Int16()[:b.Length()] {
						binary.LittleEndian.PutUint16(scratch[:2], uint16(x))
						_, _ = h.Write(scratch[:2])
					}
				}
			case types.FloatFamily:
				for _, x := range col.Float64()[:b.Length()] {
					bits := math.Float64bits(x)
					binary.LittleEndian.PutUint64(scratch[:8], bits)
					_, _ = h.Write(scratch[:8])
				}
			case types.BytesFamily:
				colBytes := col.Bytes()
				for i := 0; i < b.Length(); i++ {
					_, _ = h.Write(colBytes.Get(i))
				}
			default:
				return errors.Errorf(`unhandled type %s`, col.Type())
			}
		}
	}
	return nil
}

func TestDeterministicInitialData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// There are other tests that run initial data generation under race, so we
	// don't get anything from running this one under race as well.
	skip.UnderRace(t, "uninteresting under race")

	// Hardcode goldens for the fingerprint of the initial data of generators with
	// default flags. This lets us opt in generators known to be deterministic and
	// also protects against initialized-once global state (which tpcc did have at
	// one point).
	//
	// TODO(dan): We're starting to accumulate these various lists, bigInitialData
	// is another. Consider moving them to be properties on the workload.Meta.
	fingerprintGoldens := map[string]uint64{
		`bank`:       0x7b4d519ed8bd07ce,
		`bulkingest`: 0xcf3e4028ac084aea,
		`indexes`:    0xcbf29ce484222325,
		`intro`:      0x81c6a8cfd9c3452a,
		`json`:       0xcbf29ce484222325,
		`ledger`:     0xebe27d872d980271,
		`movr`:       0x4c0da49085e0bc5c,
		`queue`:      0xcbf29ce484222325,
		`rand`:       0xcbf29ce484222325,
		`roachmart`:  0xda5e73423dbdb2d9,
		`sqlsmith`:   0xcbf29ce484222325,
		`startrek`:   0xa0249fbdf612734c,
		`tpcc`:       0xab32e4f5e899eb2f,
		`tpch`:       0xdd952207e22aa577,
		`ycsb`:       0x1244ea1c29ef67f6,
	}

	var a bufalloc.ByteAllocator
	for _, meta := range workload.Registered() {
		fingerprintGolden, ok := fingerprintGoldens[meta.Name]
		if !ok {
			// TODO(dan): It'd be nice to eventually require that all registered
			// workloads are deterministic, but given that tpcc was a legitimate
			// exception for a while (for performance reasons), it's not clear right
			// now that we should be strict about this.
			continue
		}
		t.Run(meta.Name, func(t *testing.T) {
			if bigInitialData(meta) {
				skip.UnderShort(t, fmt.Sprintf(`%s involves a lot of data`, meta.Name))
			}

			h := fnv.New64()
			tables := meta.New().Tables()
			for _, table := range tables {
				require.NoError(t, hashTableInitialData(h, table.InitialRows, &a))
			}
			require.Equal(t, fingerprintGolden, h.Sum64())
		})
	}
}
