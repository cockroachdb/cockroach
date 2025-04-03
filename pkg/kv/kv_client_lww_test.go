// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// unwrapConditionFailedError unwraps an error into a kvpb.ConditionFailedError.
// It fails the test if there is no error or if the error cannot be unwrapped
// into the expected type.
func unwrapConditionFailedError(t *testing.T, err error) *kvpb.ConditionFailedError {
	t.Helper()
	require.Error(t, err, "expected an error")

	var conditionFailedErr *kvpb.ConditionFailedError
	require.True(t, errors.As(err, &conditionFailedErr),
		"expected error to unwrap to *kvpb.ConditionFailedError, got: %v", err)

	return conditionFailedErr
}

func TestDeleteWithOriginTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	key := roachpb.Key("lww-test-key")
	value := []byte("value")

	// Put a value with the current timestamp.
	require.NoError(t, db.Put(ctx, key, value))

	// Get the current timestamp to use as a reference.
	gr, err := db.Get(ctx, key)
	require.NoError(t, err)
	currentTS := gr.Value.Timestamp

	// Helper function to attempt a delete with a specific origin timestamp
	attemptDelete := func(ts hlc.Timestamp) error {
		b := &kv.Batch{}
		b.Del(key)
		b.Header.WriteOptions = &kvpb.WriteOptions{
			OriginTimestamp: ts,
		}
		return db.Run(ctx, b)
	}

	t.Run("OlderTimestamp", func(t *testing.T) {
		// Delete with an origin timestamp older than the mvcc timestamp should fail
		err := attemptDelete(currentTS.Prev())
		condErr := unwrapConditionFailedError(t, err)
		require.False(t, condErr.OriginTimestampOlderThan.IsEmpty(), "expected OriginTimestampOlderThan to be set")
		require.Equal(t, currentTS, condErr.OriginTimestampOlderThan, "expected OriginTimestampOlderThan to match current timestamp")

		// Verify the original value still exists
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, value, gr.ValueBytes(), "value should match what was put")
	})

	t.Run("NewerTimestamp", func(t *testing.T) {
		// Delete with an origin timestamp newer than the mvcc timestamp should succeed
		err := attemptDelete(currentTS.Next())
		require.NoError(t, err)

		// Verify the value was deleted
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.Nil(t, gr.Value, "expected value to be deleted")
	})
}

func TestDelRangeWithOriginTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()

	// Setup test data once for all subtests
	keys := []roachpb.Key{
		roachpb.Key("lww-delrange-a"),
		roachpb.Key("lww-delrange-b"),
		roachpb.Key("lww-delrange-c"),
	}
	startKey := keys[0]
	endKey := keys[len(keys)-1].Next()
	value := []byte("value")

	// Put values for all keys
	for _, key := range keys {
		require.NoError(t, db.Put(ctx, key, value))
	}

	// Get the current timestamp to use as a reference
	gr, err := db.Get(ctx, keys[2])
	require.NoError(t, err)
	currentTS := gr.Value.Timestamp

	t.Run("OlderTimestamp", func(t *testing.T) {
		// DelRange with an origin timestamp older than the mvcc timestamp should fail
		b := &kv.Batch{}
		b.DelRange(startKey, endKey, false)
		b.Header.WriteOptions = &kvpb.WriteOptions{
			OriginTimestamp: currentTS.Prev(),
		}
		err := db.Run(ctx, b)
		condErr := unwrapConditionFailedError(t, err)
		require.False(t, condErr.OriginTimestampOlderThan.IsEmpty(), "expected OriginTimestampOlderThan to be set")
		require.Equal(t, currentTS, condErr.OriginTimestampOlderThan, "expected OriginTimestampOlderThan to match current timestamp")

		// Verify the original values still exist
		for _, key := range keys {
			gr, err := db.Get(ctx, key)
			require.NoError(t, err)
			require.NotNil(t, gr.Value, "expected value to exist at %s", key)
			require.Equal(t, value, gr.ValueBytes(), "value should match what was put")
		}
	})

	t.Run("NewerTimestamp", func(t *testing.T) {
		// Delete the range with the newer timestamp
		b := &kv.Batch{}
		b.DelRange(startKey, endKey, false)
		b.Header.WriteOptions = &kvpb.WriteOptions{
			OriginTimestamp: currentTS.Next(),
		}
		err := db.Run(ctx, b)
		require.NoError(t, err, "unexpected error when deleting range with newer origin timestamp")

		// Verify all values were deleted
		for _, key := range keys {
			gr, err := db.Get(ctx, key)
			require.NoError(t, err)
			require.Nil(t, gr.Value, "expected value to be deleted at %s", key)
		}
	})
}

func TestPutWithOriginTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	key := roachpb.Key("lww-put-test-key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put a value with the current timestamp.
	require.NoError(t, db.Put(ctx, key, value1))

	// Get the current timestamp to use as a reference.
	gr, err := db.Get(ctx, key)
	require.NoError(t, err)
	currentTS := gr.Value.Timestamp

	// Helper function to attempt a put with a specific origin timestamp
	attemptPut := func(ts hlc.Timestamp, value []byte) error {
		b := &kv.Batch{}
		b.Put(key, value)
		b.Header.WriteOptions = &kvpb.WriteOptions{
			OriginTimestamp: ts,
		}
		return db.Run(ctx, b)
	}

	t.Run("OlderTimestamp", func(t *testing.T) {
		// Put with an origin timestamp older than the mvcc timestamp should fail
		olderTS := currentTS.Prev()
		err := attemptPut(olderTS, value2)
		condErr := unwrapConditionFailedError(t, err)
		require.False(t, condErr.OriginTimestampOlderThan.IsEmpty(), "expected OriginTimestampOlderThan to be set")
		require.Equal(t, currentTS, condErr.OriginTimestampOlderThan, "expected OriginTimestampOlderThan to match current timestamp")

		// Verify the original value still exists
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, value1, gr.ValueBytes(), "value should match expected")
	})

	t.Run("NewerTimestamp", func(t *testing.T) {
		// Put with an origin timestamp newer than the mvcc timestamp should succeed
		newerTS := currentTS.Next()
		err := attemptPut(newerTS, value2)
		require.NoError(t, err, "unexpected error when putting with newer origin timestamp")

		// Verify the value was updated
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, value2, gr.ValueBytes(), "value should match expected")

		// Get the new timestamp
		gr, err = db.Get(ctx, key)
		require.NoError(t, err)
		updatedTS := gr.Value.Timestamp

		// Try to put with an even newer timestamp
		newestValue := []byte("newest-value")
		newestTS := updatedTS.Next()
		err = attemptPut(newestTS, newestValue)
		require.NoError(t, err, "unexpected error when putting with newest origin timestamp")

		// Verify the newest value was written
		gr, err = db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, newestValue, gr.ValueBytes(), "value should match new value")
	})
}

func TestCPutWithOriginTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	key := roachpb.Key("lww-cput-test-key")
	initialValue := []byte("initial")
	newValue := []byte("new-value")
	wrongExpectedValue := []byte("wrong-expected")

	// Put a value with the current timestamp.
	require.NoError(t, db.Put(ctx, key, initialValue))

	// Get the current timestamp to use as a reference.
	gr, err := db.Get(ctx, key)
	require.NoError(t, err)
	currentTS := gr.Value.Timestamp

	// Helper function to attempt a conditional put with a specific origin timestamp
	attemptCPut := func(ts hlc.Timestamp, expectedValue, newValue []byte) error {
		b := &kv.Batch{}
		b.CPut(key, newValue, expectedValue)
		b.Header.WriteOptions = &kvpb.WriteOptions{
			OriginTimestamp: ts,
		}
		return db.Run(ctx, b)
	}

	t.Run("ConditionFailsAndLWWFails", func(t *testing.T) {
		// Both the condition fails (wrong expected value) and the origin timestamp is older
		olderTS := currentTS.Prev()
		err := attemptCPut(olderTS, wrongExpectedValue, newValue)
		condErr := unwrapConditionFailedError(t, err)

		// The error should indicate the origin timestamp is older
		require.False(t, condErr.OriginTimestampOlderThan.IsEmpty(), "expected OriginTimestampOlderThan to be set")
		require.Equal(t, currentTS, condErr.OriginTimestampOlderThan, "expected OriginTimestampOlderThan to match current timestamp")

		// Verify the original value still exists
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, initialValue, gr.ValueBytes(), "value should match initial value")
	})

	t.Run("ConditionPassesButLWWFails", func(t *testing.T) {
		// The condition passes (correct expected value) but the origin timestamp is older
		olderTS := currentTS.Prev()
		err := attemptCPut(olderTS, initialValue, newValue)
		condErr := unwrapConditionFailedError(t, err)

		// The error should indicate the origin timestamp is older
		require.False(t, condErr.OriginTimestampOlderThan.IsEmpty(), "expected OriginTimestampOlderThan to be set")
		require.Equal(t, currentTS, condErr.OriginTimestampOlderThan, "expected OriginTimestampOlderThan to match current timestamp")

		// Verify the original value still exists
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, initialValue, gr.ValueBytes(), "value should match initial value")
	})

	t.Run("ConditionFailsButLWWPasses", func(t *testing.T) {
		// The condition fails (wrong expected value) but the origin timestamp is newer
		newerTS := currentTS.Next()
		err := attemptCPut(newerTS, wrongExpectedValue, newValue)
		condErr := unwrapConditionFailedError(t, err)

		// The error should indicate the condition failed but not that the timestamp is older
		require.True(t, condErr.OriginTimestampOlderThan.IsEmpty(), "expected OriginTimestampOlderThan to be empty")
		require.NotNil(t, condErr.ActualValue, "expected ActualValue to be set")

		// Verify the original value still exists
		gr, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, initialValue, gr.ValueBytes(), "value should match initial value")
	})

	t.Run("BothConditionAndLWWPass", func(t *testing.T) {
		// Create a new key for this test to avoid interference from previous test cases
		successKey := roachpb.Key("lww-cput-success-key")

		// Put an initial value
		require.NoError(t, db.Put(ctx, successKey, initialValue))

		// Get the current value and timestamp
		gr, err := db.Get(ctx, successKey)
		require.NoError(t, err)
		require.NotNil(t, gr.Value)
		currentValue := gr.Value.TagAndDataBytes()
		currentTS := gr.Value.Timestamp

		// Both the condition passes and the origin timestamp is newer
		newerTS := currentTS.Next()

		// Use a helper function to perform the CPut with the current value
		cputErr := func() error {
			b := &kv.Batch{}
			b.CPut(successKey, newValue, currentValue)
			b.Header.WriteOptions = &kvpb.WriteOptions{
				OriginTimestamp: newerTS,
			}
			return db.Run(ctx, b)
		}()

		require.NoError(t, cputErr, "unexpected error when both condition and timestamp are valid")

		// Verify the value was updated
		gr, err = db.Get(ctx, successKey)
		require.NoError(t, err)
		require.NotNil(t, gr.Value, "expected value to exist")
		require.Equal(t, newValue, gr.ValueBytes(), "value should match new value")
	})
}
