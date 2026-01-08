// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWithTraceCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tr := tracing.NewTracer()

	t.Run("recordVerbose=true returns recording", func(t *testing.T) {
		result, rec, err := withTraceCollection(ctx, tr, "test-span", true,
			func(ctx context.Context) (bool, error) {
				sp := tracing.SpanFromContext(ctx)
				require.NotNil(t, sp)
				require.Equal(t, tracingpb.RecordingVerbose, sp.RecordingType())
				sp.Record("test message")
				return true, nil
			},
		)

		require.NoError(t, err)
		require.True(t, result)
		require.NotNil(t, rec)
		require.GreaterOrEqual(t, rec.Len(), 1)
		// Verify the recording contains our span and message.
		found := false
		for _, span := range rec {
			if span.Operation == "test-span" {
				found = true
				break
			}
		}
		require.True(t, found, "expected to find test-span in recording")
	})

	t.Run("recordVerbose=false returns nil recording", func(t *testing.T) {
		result, rec, err := withTraceCollection(ctx, tr, "test-span", false,
			func(ctx context.Context) (bool, error) {
				return false, nil
			},
		)

		require.NoError(t, err)
		require.False(t, result)
		require.Nil(t, rec)
	})

	t.Run("error passthrough", func(t *testing.T) {
		testErr := errors.New("test error")
		result, rec, err := withTraceCollection(ctx, tr, "test-span", true,
			func(ctx context.Context) (bool, error) {
				return false, testErr
			},
		)

		require.ErrorIs(t, err, testErr)
		require.False(t, result)
		require.NotNil(t, rec, "recording should be returned even on error when recordVerbose=true")
	})

	t.Run("bool result passthrough", func(t *testing.T) {
		for _, expected := range []bool{true, false} {
			result, _, err := withTraceCollection(ctx, tr, "test-span", false,
				func(ctx context.Context) (bool, error) {
					return expected, nil
				},
			)
			require.NoError(t, err)
			require.Equal(t, expected, result)
		}
	})
}
