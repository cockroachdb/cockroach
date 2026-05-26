// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's internal/singleflight package.

package singleflight

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDo(t *testing.T) {
	ctx := context.Background()
	g := NewGroup("test", "test")
	v, _, err := g.Do(ctx, "key", func(context.Context) (interface{}, error) {
		return "bar", nil
	})
	res := Result{Val: v, Err: err}
	assertRes(t, res, false)
}

func TestDoChan(t *testing.T) {
	ctx := context.Background()
	g := NewGroup("test", "test")
	res, leader := g.DoChan(ctx, "key", DoOpts{}, func(context.Context) (interface{}, error) {
		return "bar", nil
	})
	if !leader {
		t.Errorf("DoChan returned not leader, expected leader")
	}
	result := res.WaitForResult(ctx)
	assertRes(t, result, false)
}

func TestDoErr(t *testing.T) {
	ctx := context.Background()
	g := NewGroup("test", "test")
	someErr := errors.New("Some error")
	v, _, err := g.Do(ctx, "key", func(context.Context) (interface{}, error) {
		return nil, someErr
	})
	if !errors.Is(err, someErr) {
		t.Errorf("Do error = %v; want someErr %v", err, someErr)
	}
	if v != nil {
		t.Errorf("unexpected non-nil value %#v", v)
	}
}

func TestDoDupSuppress(t *testing.T) {
	ctx := context.Background()
	g := NewGroup("test", "key")
	var wg1, wg2 sync.WaitGroup
	c := make(chan string, 1)
	var calls int32
	fn := func(context.Context) (interface{}, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do

		return v, nil
	}

	const n = 10
	wg1.Add(1)
	for i := 0; i < n; i++ {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			v, _, err := g.Do(ctx, "key", fn)
			if err != nil {
				t.Errorf("Do error: %v", err)
				return
			}
			if s, _ := v.(string); s != "bar" {
				t.Errorf("Do = %T %v; want %q", v, v, "bar")
			}
		}()
	}
	wg1.Wait()
	// At least one goroutine is in fn now and all of them have at
	// least reached the line before the Do.
	c <- "bar"
	wg2.Wait()
	if got := atomic.LoadInt32(&calls); got <= 0 || got >= n {
		t.Errorf("number of calls = %d; want over 0 and less than %d", got, n)
	}
}

// Test that a Do caller that's recording the trace gets the call's recording
// even if it's not the leader.
func TestDoTracing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "leader recording", func(t *testing.T, leaderRecording bool) {
		ctx := context.Background()
		tr := tracing.NewTracerWithOpt(ctx, tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry))
		opName := "test"
		g := NewGroup(opName, NoTags)
		leaderSem := make(chan struct{})
		leaderTraceC := make(chan tracingpb.Recording)
		const key = ""
		go func() {
			// The leader needs to have a span, but not necessarily a recording one.
			var opt tracing.SpanOption
			if leaderRecording {
				opt = tracing.WithRecording(tracingpb.RecordingVerbose)
			}
			sp := tr.StartSpan("leader", opt)
			defer sp.Finish()
			ctx := tracing.ContextWithSpan(ctx, sp)
			_, _, err := g.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
				log.Eventf(ctx, "inside the flight 1")
				leaderSem <- struct{}{}
				<-leaderSem
				log.Eventf(ctx, "inside the flight 2")
				return nil, nil
			})
			if err != nil {
				panic(err)
			}
			leaderTraceC <- sp.GetConfiguredRecording()
		}()
		// Wait for the call above to become the flight leader.
		<-leaderSem
		followerTraceC := make(chan tracingpb.Recording)
		go func() {
			ctx, getRec := tracing.ContextWithRecordingSpan(context.Background(), tr, "follower")
			_, _, err := g.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
				panic("this should never be called; the leader uses another closure")
			})
			if err != nil {
				panic(err)
			}
			followerTraceC <- getRec()
		}()
		// Wait until the second caller blocks.
		testutils.SucceedsSoon(t, func() error {
			if g.NumCalls("") != 2 {
				return errors.New("second call not blocked yet")
			}
			return nil
		})
		// Unblock the leader.
		leaderSem <- struct{}{}
		// Wait for the both the leader and the follower.
		leaderRec := <-leaderTraceC
		followerRec := <-followerTraceC

		// Check that the trace's contains the call. In the leader's case, it should
		// only have if the leader was recording. The follower is always recording
		// in these tests.
		_, ok := leaderRec.FindSpan(opName)
		require.Equal(t, leaderRecording, ok)
		_, ok = followerRec.FindSpan(opName)
		require.True(t, ok)

		// If the leader is not recording, the first message is not in the trace
		// because the follower started recording on the leader's span only
		// afterwards.
		expectedToSeeFirstMsg := leaderRecording
		_, ok = followerRec.FindLogMessage("inside the flight 1")
		require.Equal(t, expectedToSeeFirstMsg, ok)
		// The second message is there.
		_, ok = followerRec.FindLogMessage("inside the flight 2")
		require.True(t, ok)
	})
}

func TestDoChanDupSuppress(t *testing.T) {
	c := make(chan struct{})
	fn := func(context.Context) (interface{}, error) {
		<-c
		return "bar", nil
	}

	ctx := context.Background()
	g := NewGroup("test", "key")
	res1, leader1 := g.DoChan(ctx, "key", DoOpts{}, fn)
	if !leader1 {
		t.Errorf("DoChan returned not leader, expected leader")
	}

	res2, leader2 := g.DoChan(ctx, "key", DoOpts{}, fn)
	if leader2 {
		t.Errorf("DoChan returned leader, expected not leader")
	}

	close(c)

	for _, res := range []Result{res1.WaitForResult(ctx), res2.WaitForResult(ctx)} {
		assertRes(t, res, true)
	}
}

// Test that a DoChan caller that's recording the trace gets the call's
// recording even if it's not the leader.
func TestDoChanTracing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We test both the case when a follower waits for the flight completion, and
	// the case where it doesn't. In both cases, the follower should have the
	// leader's recording (at least part of it in the canceled case).
	testutils.RunTrueAndFalse(t, "follower-canceled", func(t *testing.T, cancelFollower bool) {
		ctx := context.Background()
		tr := tracing.NewTracerWithOpt(ctx, tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry))
		const opName = "test"
		g := NewGroup(opName, NoTags)
		leaderSem := make(chan struct{})
		leaderResultC := make(chan Result)
		followerResultC := make(chan Result)
		go func() {
			// The leader needs to have a span, but not necessarily a recording one.
			ctx := tracing.ContextWithSpan(ctx, tr.StartSpan("leader"))
			future, leader := g.DoChan(ctx, "",
				DoOpts{
					Stop:               nil,
					InheritCancelation: false,
				},
				func(ctx context.Context) (interface{}, error) {
					log.Eventf(ctx, "inside the flight 1")
					// Signal that the leader started.
					leaderSem <- struct{}{}
					// Wait until the follower joins the flight.
					<-leaderSem
					log.Eventf(ctx, "inside the flight 2")
					// Signal that the leader has logged a message.
					leaderSem <- struct{}{}
					// Wait for the test to signal one more time.
					<-leaderSem
					return nil, nil
				})
			if !leader {
				panic("expected to be leader")
			}

			res := future.WaitForResult(ctx)
			leaderResultC <- res
		}()
		// Wait for the call above to become the flight leader.
		<-leaderSem
		// Call another DoChan, which will get de-duped onto the blocked leader above.
		followerTraceC := make(chan tracingpb.Recording)
		followerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			ctx := followerCtx
			ctx, getRec := tracing.ContextWithRecordingSpan(ctx, tr, "follower")
			future, leader := g.DoChan(ctx, "", DoOpts{}, func(ctx context.Context) (interface{}, error) {
				panic("this should never be called; the leader uses another closure")
			})
			if leader {
				panic("expected to be follower")
			}
			res := future.WaitForResult(ctx)
			followerResultC <- res
			followerTraceC <- getRec()
		}()
		// Wait until the second caller blocks.
		testutils.SucceedsSoon(t, func() error {
			if g.NumCalls("") != 2 {
				return errors.New("second call not blocked yet")
			}
			return nil
		})
		// Now that the follower is blocked, unblock the leader.
		leaderSem <- struct{}{}
		// Wait for the leader to make a little progress, logging a message.
		<-leaderSem

		if cancelFollower {
			cancel()
		} else {
			// Signal the leader that it can terminate.
			leaderSem <- struct{}{}
		}

		// Wait for the follower. In the `cancelFollower` case, this happens while
		// the leader is still running.
		followerRes := <-followerResultC
		require.False(t, followerRes.Leader)
		if !cancelFollower {
			require.NoError(t, followerRes.Err)
		} else {
			require.ErrorContains(t, followerRes.Err, "interrupted during singleflight")
		}

		if cancelFollower {
			// Signal the leader that it can terminate. In the `!cancelFollower` case,
			// we've done it above.
			leaderSem <- struct{}{}
		}

		// Wait for the leader.
		res := <-leaderResultC
		require.True(t, res.Leader)
		require.NoError(t, res.Err)

		// Check that the follower's trace contains the call.
		followerRec := <-followerTraceC
		_, ok := followerRec.FindSpan(opName)
		require.True(t, ok)
		// The first message is not in the trace because the follower started
		// recording on the leader's span only afterwards.
		_, ok = followerRec.FindLogMessage("inside the flight 1")
		require.False(t, ok)
		// The second message is there.
		_, ok = followerRec.FindLogMessage("inside the flight 2")
		require.True(t, ok)
	})
}

// Test that a DoChan flight is not interrupted by the canceling of the caller's
// ctx.
func TestDoChanCtxCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	g := NewGroup("test", NoTags)
	future, _ := g.DoChan(ctx, "",
		DoOpts{
			Stop:               nil,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Millisecond):
			}
			return nil, nil
		})
	cancel()
	<-future.C()
	// Note that when it doesn't have to block, Result() returns the flight's
	// result even when called with a canceled ctx.
	res := future.WaitForResult(ctx)
	// Expect that the call ended after the timer expired, not because of ctx
	// cancelation.
	require.NoError(t, res.Err)
}

func TestNumCalls(t *testing.T) {
	c := make(chan struct{})
	fn := func(ctx context.Context) (interface{}, error) {
		<-c
		return "bar", nil
	}
	ctx := context.Background()
	g := NewGroup("test", "key")
	assertNumCalls(t, g.NumCalls("key"), 0)
	resC1, _ := g.DoChan(ctx, "key", DoOpts{}, fn)
	assertNumCalls(t, g.NumCalls("key"), 1)
	resC2, _ := g.DoChan(ctx, "key", DoOpts{}, fn)
	assertNumCalls(t, g.NumCalls("key"), 2)
	close(c)
	<-resC1.C()
	<-resC2.C()
	assertNumCalls(t, g.NumCalls("key"), 0)
}

func assertRes(t *testing.T, res Result, expectShared bool) {
	if got, want := fmt.Sprintf("%v (%T)", res.Val, res.Val), "bar (string)"; got != want {
		t.Errorf("Res.Val = %v; want %v", got, want)
	}
	if res.Err != nil {
		t.Errorf("Res.Err = %v", res.Err)
	}
	if res.Shared != expectShared {
		t.Errorf("Res.Shared = %t; want %t", res.Shared, expectShared)
	}
}

func assertNumCalls(t *testing.T, actualCalls int, expectedCalls int) {
	if actualCalls != expectedCalls {
		t.Errorf("NumCalls = %d; want %d", actualCalls, expectedCalls)
	}
}
