// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxgroup

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestErrorAfterCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "canceled", func(t *testing.T, canceled bool) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g := WithContext(ctx)
		g.Go(func() error {
			return nil
		})
		expErr := context.Canceled
		if !canceled {
			expErr = nil
		} else {
			cancel()
		}

		if err := g.Wait(); !errors.Is(err, expErr) {
			t.Errorf("expected %v, got %v", expErr, err)
		}
	})
}

func funcThatPanics(x interface{}) {
	panic(x)
}

func deferThatPanics() {
	defer func() {
		panic("in deferred panic")
	}()
	funcThatPanics("in func panic")
}

func TestPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "multi", func(t *testing.T, b bool) {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("recovered: %+v", r)
					require.Contains(t, fmt.Sprintf("%+v", r), "funcThatPanics", "panic stack missing")
				}
			}()
			g := WithContext(context.Background())
			g.GoCtx(func(_ context.Context) error { funcThatPanics(1); return errors.New("unseen") })
			if b {
				g.GoCtx(func(_ context.Context) error { funcThatPanics(2); return nil })
				g.GoCtx(func(_ context.Context) error { funcThatPanics(3); return nil })
			}
			t.Fatal(g.Wait(), "unreachable if we hit expected panic in Wait")
		}()
	})
}

func TestPanicInDefer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		r := recover()
		require.NotNil(t, r, "expected panic from Wait")
		err := r.(error)
		require.Contains(t, err.Error(), "in deferred panic", "expected deferred panic message")
		require.Contains(t, err.Error(), "deferThatPanics", "expected deferThatPanics in stack")
		require.Contains(t, err.Error(), "funcThatPanics", "expected deferThatPanics in stack")
		t.Log("recovered: ", err.Error())
	}()

	g := WithContext(context.Background())
	g.GoCtx(func(_ context.Context) error { deferThatPanics(); return errors.New("unseen") })
	t.Fatal(g.Wait(), "unreachable if we hit expected panic in Wait")
}

func innerGroup() error {
	g := WithContext(context.Background())
	g.GoCtx(func(_ context.Context) error { funcThatPanics("inner"); return nil })
	return g.Wait()
}

func outerGroup() error {
	g := WithContext(context.Background())
	g.GoCtx(func(_ context.Context) error { return innerGroup() })
	return g.Wait()
}

func TestNestedContextGroup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		r := recover()
		require.NotNil(t, r, "expected panic from Wait")
		err := r.(error)
		require.Contains(t, err.Error(), "inner", "expected inner panic message")
		require.Contains(t, err.Error(), "innerGroup", "expected innerGroup in stack")
		require.Contains(t, err.Error(), "outerGroup", "expected outerGroup in stack")
		t.Log("recovered: ", err.Error())
	}()

	g := WithContext(context.Background())
	g.GoCtx(func(_ context.Context) error { return outerGroup() })
	t.Fatal(g.Wait(), "unreachable if we hit expected panic in Wait")
}
