// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

var (
	// testAllocator is an Allocator with an unlimited budget for use in tests.
	testAllocator *Allocator

	// testMemMonitor and testMemAcc are a test monitor with an unlimited budget
	// and a memory account bound to it for use in tests.
	testMemMonitor *mon.BytesMonitor
	testMemAcc     *mon.BoundAccount

	// testDiskMonitor and testDiskmAcc are a test monitor with an unlimited budget
	// and a disk account bound to it for use in tests.
	testDiskMonitor *mon.BytesMonitor
	testDiskAcc     *mon.BoundAccount
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	os.Exit(func() int {
		ctx := context.Background()
		testMemMonitor = execinfra.NewTestMemMonitor(ctx, cluster.MakeTestingClusterSettings())
		defer testMemMonitor.Stop(ctx)
		memAcc := testMemMonitor.MakeBoundAccount()
		testMemAcc = &memAcc
		testAllocator = NewAllocator(ctx, testMemAcc)
		defer testMemAcc.Close(ctx)

		testDiskMonitor = execinfra.NewTestDiskMonitor(ctx, cluster.MakeTestingClusterSettings())
		defer testDiskMonitor.Stop(ctx)
		diskAcc := testDiskMonitor.MakeBoundAccount()
		testDiskAcc = &diskAcc
		defer testDiskAcc.Close(ctx)

		// Pick a random batch size in [minBatchSize, coldata.MaxBatchSize]
		// range. The randomization can be disabled using COCKROACH_RANDOMIZE_BATCH_SIZE=false.
		randomBatchSize := generateBatchSize()
		fmt.Printf("coldata.BatchSize() is set to %d\n", randomBatchSize)
		if err := coldata.SetBatchSizeForTests(randomBatchSize); err != nil {
			panic(err)
		}
		return m.Run()
	}())
}

// minBatchSize is the minimum acceptable size of batches for tests in this
// package.
const minBatchSize = 3

func generateBatchSize() int {
	randomizeBatchSize := envutil.EnvOrDefaultBool("COCKROACH_RANDOMIZE_BATCH_SIZE", true)
	if randomizeBatchSize {
		rng, _ := randutil.NewPseudoRand()
		batchSize := minBatchSize +
			rng.Intn(coldata.MaxBatchSize-minBatchSize)
		return batchSize
	}
	return coldata.BatchSize()
}
