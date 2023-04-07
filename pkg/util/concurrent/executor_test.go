// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package concurrent_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/concurrent"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestDefaultExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numClosures = 1024
	testCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(numClosures)
	for i := 0; i < numClosures; i++ {
		concurrent.DefaultExecutor.GoCtx(context.Background(), func(ctx context.Context) {
			<-testCh
			wg.Done()
		})
	}
	close(testCh)
	wg.Wait()
}

func TestWorkQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numClosures = 1024
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for n := 0; n <= 16; n++ {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			testCh := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(numClosures)

			ex, err := concurrent.NewWorkQueue(ctx, fmt.Sprintf("n%d", n), stopper, concurrent.WithNumWorkers(n))
			require.NoError(t, err)

			for i := 0; i < numClosures; i++ {
				ex.GoCtx(context.Background(), func(ctx context.Context) {
					<-testCh
					wg.Done()
				})
			}
			close(testCh)
			wg.Wait()
		})
	}
}

func BenchmarkExecutors(b *testing.B) {
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	for numWorkers := 0; numWorkers <= 64; {
		var ex concurrent.Executor
		var benchName string
		if numWorkers == 0 {
			ex = concurrent.DefaultExecutor
			benchName = "DefaultExecutor"
			numWorkers++
		} else {
			var err error
			benchName = fmt.Sprintf("WQ=%d", numWorkers)
			ex, err = concurrent.NewWorkQueue(ctx, benchName, stopper, concurrent.WithNumWorkers(numWorkers))
			if err != nil {
				b.Fatal(err)
			}
			numWorkers *= 2
		}

		b.ResetTimer()
		b.Run(benchName, func(b *testing.B) {
			testCh := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(b.N)

			for n := 0; n < b.N; n++ {
				ex.GoCtx(context.Background(), func(ctx context.Context) {
					<-testCh
					wg.Done()
				})
			}
			close(testCh)
			wg.Wait()
		})
	}
}
