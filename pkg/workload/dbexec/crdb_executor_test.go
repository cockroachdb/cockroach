// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbexec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCRDBExecutor_Capabilities(t *testing.T) {
	exec := NewCRDBExecutor()
	caps := exec.Capabilities()

	// CRDBExecutor should support all CockroachDB features.
	require.True(t, caps.FollowerReads, "CRDBExecutor should support follower reads")
	require.True(t, caps.Scatter, "CRDBExecutor should support scatter")
	require.True(t, caps.Splits, "CRDBExecutor should support splits")
	require.True(t, caps.HashShardedPK, "CRDBExecutor should support hash-sharded primary keys")
	require.True(t, caps.TransactionQoS, "CRDBExecutor should support transaction QoS")
	require.True(t, caps.SelectForUpdate, "CRDBExecutor should support SELECT FOR UPDATE")
	require.True(t, caps.TransactionPriority, "CRDBExecutor should support transaction priority")
	require.True(t, caps.SerializationRetry, "CRDBExecutor should support serialization retry")
	require.True(t, caps.Select1, "CRDBExecutor should support SELECT 1 warmup")
	require.True(t, caps.EnumColumn, "CRDBExecutor should support enum columns")
}

func TestNewCRDBExecutor(t *testing.T) {
	exec := NewCRDBExecutor()
	require.NotNil(t, exec, "NewCRDBExecutor should return non-nil executor")
}
