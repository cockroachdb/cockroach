// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecproj

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	// testAllocator is an Allocator with an unlimited budget for use in tests.
	testAllocator *colmem.Allocator

	testMemAcc *mon.BoundAccount
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	os.Exit(func() int {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
		defer testMemMonitor.Stop(ctx)
		memAcc := testMemMonitor.MakeBoundAccount()
		testMemAcc = &memAcc
		evalCtx := tree.MakeTestingEvalContext(st)
		testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
		testAllocator = colmem.NewAllocator(ctx, testMemAcc, testColumnFactory)
		defer testMemAcc.Close(ctx)

		flag.Parse()
		if !skip.UnderBench() {
			// (If we're running benchmarks, don't set a random batch size.)
			randomBatchSize := colexectestutils.GenerateBatchSize()
			fmt.Printf("coldata.BatchSize() is set to %d\n", randomBatchSize)
			if err := coldata.SetBatchSizeForTests(randomBatchSize); err != nil {
				colexecerror.InternalError(err)
			}
		}

		return m.Run()
	}())
}
