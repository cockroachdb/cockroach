// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package limit

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentRequestLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l := MakeConcurrentRequestLimiter("test", 1)
	var wg errgroup.Group

	const threads = 20
	const runs = 1000000
	var count int64

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	for thread := 0; thread < threads; thread++ {
		wg.Go(func() error {
			runtime.Gosched()
			req := 0
			for {
				//t.Logf("waiting to make request %d... (%d / %d)", req+1, l.sem.GetCount(), l.sem.GetLimit())
				if err := l.Begin(ctx); err != nil {
					if err == ctx.Err() {
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
				l.Finish()
			}
			t.Logf("thread done after handling %d requests", req)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}
