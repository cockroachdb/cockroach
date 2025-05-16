// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCoalescedFlightRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var nameIdx int
	files := map[string]*bytes.Buffer{}
	opts := CoalescedFlightRecorderOptions{
		UniqueFileName: func() string {
			nameIdx++
			return fmt.Sprintf("file_%d.bin", nameIdx)
		},
		CreateFile: func(s string) (io.Writer, error) {
			files[s] = &bytes.Buffer{}
			return files[s], nil
		},
		IdleDuration: time.Nanosecond,
		Warningf:     t.Logf,
	}
	cfr := NewCoalescedFlightRecorder(opts)

	hdl := cfr.New()
	defer hdl.Release()

	name, futErr := hdl.Dump()
	t.Log(name)
	select {
	case <-futErr.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out")
	}
	require.NoError(t, futErr.Get())
	require.Len(t, files, 1)
	require.NotZero(t, files["file_1.bin"].Bytes())
}

func TestCoalescedFlightRecorderParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const idleDuration = time.Microsecond

	var idx atomic.Int64
	opts := CoalescedFlightRecorderOptions{
		UniqueFileName: func() string {
			return fmt.Sprintf("file_%d.bin", idx.Add(1))
		},
		CreateFile: func(s string) (io.Writer, error) {
			return io.Discard, nil
		},
		IdleDuration: idleDuration,
		Warningf:     t.Errorf,
	}
	cfr := NewCoalescedFlightRecorder(opts)

	randDur := func() time.Duration {
		return 1 + time.Duration(rand.Int63n(int64(idleDuration*3/2)))
	}
	randSleep := func() {
		time.Sleep(randDur())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer time.AfterFunc(10*time.Second, func() {
		stacks := allstacks.Get()
		t.Errorf("timed out, stacks:\n%s", stacks)
	}).Stop()

	done := make(chan struct{})
	time.AfterFunc(5*time.Millisecond, func() {
		close(done)
	})
	g := ctxgroup.WithContext(ctx)
	n := 2 * runtime.GOMAXPROCS(0)

	g.GoCtx(func(ctx context.Context) error {
		randSleep()
		h := cfr.New()
		defer h.Release()
		_, err := h.RecordSyncTrace(ctx, randDur(), io.Discard)
		if errors.Is(err, errTraceInProgress) {
			return nil
		}
		return err
	})

	for i := 0; i < n; i++ {
		g.GoCtx(func(ctx context.Context) error {
			hdl := cfr.New()
			defer hdl.Release()
			for {
				select {
				case <-done:
					return nil
				default:
					randSleep()
					_, futErr := hdl.Dump()
					randSleep()
					err1, err2 := futErr.Wait(ctx)
					if err := errors.CombineErrors(err1, err2); err != nil {
						return err
					}
				}
			}
		})
	}
	require.NoError(t, g.Wait())
	// Each goroutine should have completed at least one iteration. They may all
	// have coalesced into one in the worst case. Usually we get 10-15 (on my
	// machine).
	require.LessOrEqual(t, 1, int(idx.Load()))
	t.Logf("ran Dump %d times", idx.Load())
	time.Sleep(time.Microsecond)
	require.False(t, cfr.fr.Enabled())
}
