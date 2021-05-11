// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowcontainer

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

func TestHashDiskBackedRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These monitors are started and stopped by subtests.
	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	const numRows = 10
	const numCols = 1
	rows := randgen.MakeIntRows(numRows, numCols)
	storedEqColumns := columns{0}
	typs := types.OneIntCol
	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	getRowContainer := func() *HashDiskBackedRowContainer {
		rc := NewHashDiskBackedRowContainer(&evalCtx, memoryMonitor, diskMonitor, tempEngine)
		err = rc.Init(
			ctx,
			false, /* shouldMark */
			typs,
			storedEqColumns,
			true, /*encodeNull */
		)
		if err != nil {
			t.Fatalf("unexpected error while initializing hashDiskBackedRowContainer: %s", err.Error())
		}
		return rc
	}

	// NormalRun adds rows to a hashDiskBackedRowContainer, makes it spill to
	// disk halfway through, keeps on adding rows, and then verifies that all
	// rows were properly added to the hashDiskBackedRowContainer.
	t.Run("NormalRun", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)
		rc := getRowContainer()
		defer rc.Close(ctx)
		mid := len(rows) / 2
		for i := 0; i < mid; i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.UsingDisk() {
			t.Fatal("unexpectedly using disk")
		}
		func() {
			// We haven't marked any rows, so the unmarked iterator should iterate
			// over all rows added so far.
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows[:mid], &evalCtx, ordering); err != nil {
				t.Fatalf("verifying memory rows failed with: %s", err)
			}
		}()
		if err := rc.SpillToDisk(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingDisk() {
			t.Fatal("unexpectedly using memory")
		}
		for i := mid; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		func() {
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows, &evalCtx, ordering); err != nil {
				t.Fatalf("verifying disk rows failed with: %s", err)
			}
		}()
	})

	t.Run("AddRowOutOfMem", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(1))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)
		rc := getRowContainer()
		defer rc.Close(ctx)

		if err := rc.AddRow(ctx, rows[0]); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingDisk() {
			t.Fatal("expected to have spilled to disk")
		}
		if diskMonitor.AllocBytes() == 0 {
			t.Fatal("disk monitor reports no disk usage")
		}
		if memoryMonitor.AllocBytes() > 0 {
			t.Fatal("memory monitor reports unexpected usage")
		}
	})

	t.Run("AddRowOutOfDisk", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(1))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(1))
		rc := getRowContainer()
		defer rc.Close(ctx)

		err := rc.AddRow(ctx, rows[0])
		if code := pgerror.GetPGCode(err); code != pgcode.DiskFull {
			t.Fatalf(
				"unexpected error %v, expected disk full error %s", err, pgcode.DiskFull,
			)
		}
		if !rc.UsingDisk() {
			t.Fatal("expected to have tried to spill to disk")
		}
		if diskMonitor.AllocBytes() != 0 {
			t.Fatal("disk monitor reports unexpected usage")
		}
		if memoryMonitor.AllocBytes() != 0 {
			t.Fatal("memory monitor reports unexpected usage")
		}
	})

	// VerifyIteratorRecreation adds all rows to the container, creates a
	// recreatable unmarked iterator, iterates over half of the rows, spills the
	// container to disk, and verifies that the iterator was recreated and points
	// to the appropriate row.
	t.Run("VerifyIteratorRecreation", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)
		rc := getRowContainer()
		defer rc.Close(ctx)

		for i := 0; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.UsingDisk() {
			t.Fatal("unexpectedly using disk")
		}
		i, err := rc.NewAllRowsIterator(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer i.Close()
		counter := 0
		for i.Rewind(); counter < len(rows)/2; i.Next() {
			if ok, err := i.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				break
			}
			row, err := i.Row()
			if err != nil {
				t.Fatal(err)
			}
			if cmp, err := compareRows(
				types.OneIntCol, row, rows[counter], &evalCtx, &rowenc.DatumAlloc{}, ordering,
			); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatal(fmt.Errorf("unexpected row %v, expected %v", row, rows[counter]))
			}
			counter++
		}
		if err := rc.SpillToDisk(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingDisk() {
			t.Fatal("unexpectedly using memory")
		}
		for ; ; i.Next() {
			if ok, err := i.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				break
			}
			row, err := i.Row()
			if err != nil {
				t.Fatal(err)
			}
			if cmp, err := compareRows(
				types.OneIntCol, row, rows[counter], &evalCtx, &rowenc.DatumAlloc{}, ordering,
			); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatal(fmt.Errorf("unexpected row %v, expected %v", row, rows[counter]))
			}
			counter++
		}
		if counter != len(rows) {
			t.Fatal(fmt.Errorf("iterator returned %d rows but %d were expected", counter, len(rows)))
		}
	})

	// VerifyIteratorRecreationAfterExhaustion adds all rows to the container,
	// creates a recreatable unmarked iterator, iterates over all of the rows,
	// spills the container to disk, and verifies that the iterator was recreated
	// and is not valid.
	t.Run("VerifyIteratorRecreationAfterExhaustion", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)
		rc := getRowContainer()
		defer rc.Close(ctx)

		for i := 0; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.UsingDisk() {
			t.Fatal("unexpectedly using disk")
		}
		i, err := rc.NewAllRowsIterator(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer i.Close()
		counter := 0
		for i.Rewind(); ; i.Next() {
			if ok, err := i.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				break
			}
			row, err := i.Row()
			if err != nil {
				t.Fatal(err)
			}
			if cmp, err := compareRows(
				types.OneIntCol, row, rows[counter], &evalCtx, &rowenc.DatumAlloc{}, ordering,
			); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatal(fmt.Errorf("unexpected row %v, expected %v", row, rows[counter]))
			}
			counter++
		}
		if counter != len(rows) {
			t.Fatal(fmt.Errorf("iterator returned %d rows but %d were expected", counter, len(rows)))
		}
		if err := rc.SpillToDisk(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingDisk() {
			t.Fatal("unexpectedly using memory")
		}
		if valid, err := i.Valid(); err != nil {
			t.Fatal(err)
		} else if valid {
			t.Fatal("iterator is unexpectedly valid after recreating an exhausted iterator")
		}
	})
}

func TestHashDiskBackedRowContainerPreservesMatchesAndMarks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These monitors are started and stopped by subtests.
	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	const numRowsInBucket = 4
	const numRows = 12
	const numCols = 2
	rows := randgen.MakeRepeatedIntRows(numRowsInBucket, numRows, numCols)
	storedEqColumns := columns{0}
	types := []*types.T{types.Int, types.Int}
	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	getRowContainer := func() *HashDiskBackedRowContainer {
		rc := NewHashDiskBackedRowContainer(&evalCtx, memoryMonitor, diskMonitor, tempEngine)
		err = rc.Init(
			ctx,
			true, /* shouldMark */
			types,
			storedEqColumns,
			true, /*encodeNull */
		)
		if err != nil {
			t.Fatalf("unexpected error while initializing hashDiskBackedRowContainer: %s", err.Error())
		}
		return rc
	}

	// PreservingMatches adds rows from three different buckets to a
	// hashDiskBackedRowContainer, makes it spill to disk, keeps on adding rows
	// from the same buckets, and then verifies that all rows were properly added
	// to the hashDiskBackedRowContainer.
	t.Run("PreservingMatches", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)
		rc := getRowContainer()
		defer rc.Close(ctx)

		mid := len(rows) / 2
		for i := 0; i < mid; i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.UsingDisk() {
			t.Fatal("unexpectedly using disk")
		}
		func() {
			// We haven't marked any rows, so the unmarked iterator should iterate
			// over all rows added so far.
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows[:mid], &evalCtx, ordering); err != nil {
				t.Fatalf("verifying memory rows failed with: %s", err)
			}
		}()
		if err := rc.SpillToDisk(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingDisk() {
			t.Fatal("unexpectedly using memory")
		}
		for i := mid; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		func() {
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows, &evalCtx, ordering); err != nil {
				t.Fatalf("verifying disk rows failed with: %s", err)
			}
		}()
	})

	// PreservingMarks adds rows from three buckets to a
	// hashDiskBackedRowContainer, marks all rows belonging to the first bucket,
	// spills to disk, and checks that marks are preserved correctly.
	t.Run("PreservingMarks", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)
		rc := getRowContainer()
		defer rc.Close(ctx)

		for i := 0; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.UsingDisk() {
			t.Fatal("unexpectedly using disk")
		}
		func() {
			// We haven't marked any rows, so the unmarked iterator should iterate
			// over all rows added so far.
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows, &evalCtx, ordering); err != nil {
				t.Fatalf("verifying memory rows failed with: %s", err)
			}
		}()
		if err := rc.ReserveMarkMemoryMaybe(ctx); err != nil {
			t.Fatal(err)
		}
		func() {
			i, err := rc.NewBucketIterator(ctx, rows[0], storedEqColumns)
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			for i.Rewind(); ; i.Next() {
				if ok, err := i.Valid(); err != nil {
					t.Fatal(err)
				} else if !ok {
					break
				}
				if err := i.Mark(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}()
		if err := rc.SpillToDisk(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingDisk() {
			t.Fatal("unexpectedly using memory")
		}
		func() {
			i, err := rc.NewBucketIterator(ctx, rows[0], storedEqColumns)
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			for i.Rewind(); ; i.Next() {
				if ok, err := i.Valid(); err != nil {
					t.Fatal(err)
				} else if !ok {
					break
				}
				if !i.IsMarked(ctx) {
					t.Fatal("Mark is not preserved during spilling to disk")
				}
			}
		}()
	})
}
