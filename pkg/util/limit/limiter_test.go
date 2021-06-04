// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package limit

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentRequestLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l := MakeConcurrentRequestLimiter("test", 1)
	var wg errgroup.Group

	const threads = 20
	const runs = 1000000
	var count int64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for thread := 0; thread < threads; thread++ {
		wg.Go(func() error {
			runtime.Gosched()
			req := 0
			for {
				//t.Logf("waiting to make request %d... (%d / %d)", req+1, l.sem.GetCount(), l.sem.GetLimit())
				alloc, err := l.Begin(ctx)
				if err != nil {
					if errors.Is(err, ctx.Err()) {
						break
					} else {
						return err
					}
				}
				if x := atomic.AddInt64(&count, 1); x >= runs {
					t.Logf("canceling ctx after %d runs", x)
					cancel()
				}
				req++
				alloc.Release()
			}
			t.Logf("thread done after handling %d requests", req)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}
