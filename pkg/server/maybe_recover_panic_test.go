// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMaybeRecoverPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	event := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: kvpb.RangeFeedEvent{
			Checkpoint: &kvpb.RangeFeedCheckpoint{
				Span: roachpb.Span{
					Key:    roachpb.Key("a"),
					EndKey: roachpb.Key("b"),
				},
			},
		},
	}

	// Case 1: error already set, function returns early without recovering.
	// This tests the `if *errp != nil { return }` branch.
	t.Run("error already set", func(t *testing.T) {
		existingErr := errors.New("existing error")
		err := existingErr
		require.NotPanics(t, func() {
			defer maybeRecoverPanic(ctx, &err, event)
			// No panic - just verify the function handles err being set.
		})
		require.Equal(t, existingErr, err)
	})

	// Case 2: no panic occurs, common case.
	t.Run("no panic", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() {
			defer maybeRecoverPanic(ctx, &err, event)
			// No panic, just return normally.
		})
		require.NoError(t, err)
	})

	// Case 3: panic with "index out of range" - should recover and set error.
	t.Run("index out of range panic is recovered", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() {
			defer maybeRecoverPanic(ctx, &err, event)
			panic("runtime error: index out of range [0] with length 0")
		})
		require.Error(t, err)
		t.Logf("err: %v", err)
		require.Contains(t, err.Error(), "recovered from panic in lockedMuxStream.Send")
	})

	// Case 4: panic without "index out of range" - should re-panic.
	t.Run("other panic is re-raised", func(t *testing.T) {
		var err error
		require.Panics(t, func() {
			defer maybeRecoverPanic(ctx, &err, event)
			panic("some other panic")
		})
		require.NoError(t, err)
	})
}
