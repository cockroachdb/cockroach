// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryLogStore_WriteAndRead(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()
	sink := store.NewSink(taskID)

	entries := []logger.LogEntry{
		{Message: "line 1", Level: "INFO"},
		{Message: "line 2", Level: "INFO"},
		{Message: "line 3", Level: "WARN"},
	}
	for _, e := range entries {
		require.NoError(t, sink.WriteEntry(e))
	}

	// Read all from offset 0.
	result, nextOffset, done, err := store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Equal(t, 3, nextOffset)
	assert.False(t, done)

	assert.Equal(t, "line 1", result[0].Message)
	assert.Equal(t, "line 2", result[1].Message)
	assert.Equal(t, "line 3", result[2].Message)
}

func TestMemoryLogStore_ReadWithOffset(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()
	sink := store.NewSink(taskID)

	for range 5 {
		require.NoError(t, sink.WriteEntry(logger.LogEntry{Message: "line"}))
	}

	// Read from offset 3: should return 2 entries.
	result, nextOffset, done, err := store.ReadLogs(context.Background(), taskID, 3)
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, 5, nextOffset)
	assert.False(t, done)
}

func TestMemoryLogStore_ReadBeyondOffset(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()
	sink := store.NewSink(taskID)

	require.NoError(t, sink.WriteEntry(logger.LogEntry{Message: "line"}))

	// Offset at end of entries.
	result, nextOffset, _, err := store.ReadLogs(context.Background(), taskID, 1)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 1, nextOffset)

	// Offset beyond entries.
	result, nextOffset, _, err = store.ReadLogs(context.Background(), taskID, 99)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 99, nextOffset)
}

func TestMemoryLogStore_Completion(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()
	sink := store.NewSink(taskID)

	require.NoError(t, sink.WriteEntry(logger.LogEntry{Message: "line 1"}))

	// Before close: done should be false.
	_, _, done, err := store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	assert.False(t, done)

	// After close: done should be true.
	require.NoError(t, sink.Close())
	_, _, done, err = store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	assert.True(t, done)
}

func TestMemoryLogStore_DeleteLogs(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()
	sink := store.NewSink(taskID)

	require.NoError(t, sink.WriteEntry(logger.LogEntry{Message: "line 1"}))
	require.NoError(t, sink.Close())

	require.NoError(t, store.DeleteLogs(context.Background(), taskID))

	result, _, done, err := store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.False(t, done) // completion flag removed too
}

func TestMemoryLogStore_ReadNonExistentTask(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()

	result, nextOffset, done, err := store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 0, nextOffset)
	assert.False(t, done)
}

func TestMemoryLogStore_DeleteNonExistentTask(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()

	// Should succeed silently.
	require.NoError(t, store.DeleteLogs(context.Background(), taskID))
}

func TestMemoryLogStore_IsolationBetweenTasks(t *testing.T) {
	store := NewMemoryLogStore()
	taskA := uuid.MakeV4()
	taskB := uuid.MakeV4()
	sinkA := store.NewSink(taskA)
	sinkB := store.NewSink(taskB)

	require.NoError(t, sinkA.WriteEntry(logger.LogEntry{Message: "A1"}))
	require.NoError(t, sinkA.WriteEntry(logger.LogEntry{Message: "A2"}))
	require.NoError(t, sinkB.WriteEntry(logger.LogEntry{Message: "B1"}))

	resultA, _, _, err := store.ReadLogs(context.Background(), taskA, 0)
	require.NoError(t, err)
	assert.Len(t, resultA, 2)

	resultB, _, _, err := store.ReadLogs(context.Background(), taskB, 0)
	require.NoError(t, err)
	assert.Len(t, resultB, 1)
	assert.Equal(t, "B1", resultB[0].Message)
}

func TestMemoryLogStore_ReadReturnsCopy(t *testing.T) {
	store := NewMemoryLogStore()
	taskID := uuid.MakeV4()
	sink := store.NewSink(taskID)

	require.NoError(t, sink.WriteEntry(logger.LogEntry{Message: "original"}))

	// Read and mutate the result.
	result, _, _, err := store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	result[0].Message = "mutated"

	// Re-read: should still return the original.
	result2, _, _, err := store.ReadLogs(context.Background(), taskID, 0)
	require.NoError(t, err)
	assert.Equal(t, "original", result2[0].Message)
}
