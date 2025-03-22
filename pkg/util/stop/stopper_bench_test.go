// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stop

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func BenchmarkStopper(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	s := NewStopper()
	defer s.Stop(ctx)

	opts := TaskOpts{
		TaskName: "testTask",
	}

	b.Run("RunTask", func(b *testing.B) {
		b.ReportAllocs()
		var wg sync.WaitGroup // for fairness
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			if err := s.RunTask(ctx, opts.TaskName, func(context.Context) { wg.Done() }); err != nil {
				b.Fatal(err)
			}
		}
		wg.Wait() // noop
	})

	b.Run("AsyncTaskEx", func(b *testing.B) {
		b.ReportAllocs()
		var wg sync.WaitGroup
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			if err := s.RunAsyncTaskEx(ctx, opts, func(ctx context.Context) {
				defer wg.Done()
			}); err != nil {
				b.Fatal(err)
			}
			wg.Wait()
		}
	})

	hdlf := func(ctx context.Context, hdl *Handle, wg *sync.WaitGroup) {
		defer hdl.Activate(ctx).Release(ctx)
		defer wg.Done()
	}

	b.Run("Handle", func(b *testing.B) {
		b.ReportAllocs()
		var wg sync.WaitGroup
		for i := 0; i < b.N; i++ {
			ctx, hdl, err := s.GetHandle(ctx, opts)
			if err != nil {
				b.Fatal(err)
			}
			wg.Add(1)
			go hdlf(ctx, hdl, &wg)
			wg.Wait()
		}
	})
}
