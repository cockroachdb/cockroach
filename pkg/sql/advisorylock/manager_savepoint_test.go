// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package advisorylock_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/advisorylock"
	"github.com/stretchr/testify/require"
)

func TestGetHeldLocksEmptyStack(t *testing.T) {
	m := advisorylock.NewManager(nil, keys.SystemSQLCodec)
	locks := m.GetHeldLocks()
	require.Empty(t, locks)
}

func TestSavepointCallbacksMarkerStack(t *testing.T) {
	m := advisorylock.NewManager(nil, keys.SystemSQLCodec)
	m.OnSQLSavepointCreated()
	m.OnSQLSavepointCreated()
	s := m.ExportRewindSnapshot()
	require.Len(t, s.markers, 2)
	require.Zero(t, s.acqLen)

	m.OnSQLRollbackToSavepoint(0)
	s2 := m.ExportRewindSnapshot()
	require.Len(t, s2.markers, 1)

	m.OnSQLSavepointStackTruncated(0)
	s3 := m.ExportRewindSnapshot()
	require.Empty(t, s3.markers)
}
