// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testVar1 = RegisterIntSetting(TenantWritable, "n1", "n1", 1)
var testVar2 = RegisterIntSetting(TenantWritable, "n2", "n2", 2)
var testVar3 = RegisterIntSetting(TenantWritable, "n3", "n3", 3)

func TestNotifier(t *testing.T) {
	ctx := context.Background()
	sv := &Values{}
	sv.Init(ctx, TestOpaque)

	n1 := sv.NewNotifier(testVar1)
	n23 := sv.NewNotifier(testVar2, testVar3)
	n123 := sv.NewNotifier(testVar1, testVar2, testVar3)

	var count1, count23, count123 atomic.Uint32

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-n1.Ch():
				count1.Add(1)
			case <-n23.Ch():
				count23.Add(1)
			case <-n123.Ch():
				count123.Add(1)
			}
		}
	}(cancelCtx)

	expect := func(exp1, exp23, exp123 uint32) {
		require.Equal(t, count1.Load(), exp1)
		require.Equal(t, count23.Load(), exp23)
		require.Equal(t, count123.Load(), exp123)
	}
	wait := func(exp1, exp23, exp123 uint32) {
		require.Eventuallyf(
			t,
			func() bool {
				return count1.Load() == exp1 && count23.Load() == exp23 && count123.Load() == exp123
			},
			10*time.Second,
			10*time.Millisecond,
			"expected %d/%d/%d, got %d/%d/%d",
			exp1, exp23, exp123, count1.Load(), count23.Load(), count123.Load())
	}

	expect(0 /* exp1 */, 0 /* exp23 */, 0 /* exp123 */)
	testVar1.set(ctx, sv, 10)
	wait(1 /* exp1 */, 0 /* exp23 */, 1 /* exp123 */)
	testVar2.set(ctx, sv, 20)
	wait(1 /* exp1 */, 1 /* exp23 */, 2 /* exp123 */)
	testVar1.set(ctx, sv, 100)
	wait(2 /* exp1 */, 1 /* exp23 */, 3 /* exp123 */)
	testVar3.set(ctx, sv, 300)
	wait(2 /* exp1 */, 2 /* exp23 */, 4 /* exp123 */)
	n1.Close()
	n23.Close()
	n123.Close()
	// We should get no more notifications.
	testVar1.set(ctx, sv, 1000)
	testVar2.set(ctx, sv, 2000)
	testVar3.set(ctx, sv, 3000)
	expect(2 /* exp1 */, 2 /* exp23 */, 4 /* exp123 */)
}
