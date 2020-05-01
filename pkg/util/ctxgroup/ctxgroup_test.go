// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctxgroup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
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
