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
