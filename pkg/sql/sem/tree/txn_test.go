// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestIsolationLevelFromKVTxnIsolationLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		In  isolation.Level
		Out tree.IsolationLevel
	}{
		{
			In:  isolation.Serializable,
			Out: tree.SerializableIsolation,
		},
		{
			In:  isolation.ReadCommitted,
			Out: tree.ReadCommittedIsolation,
		},
		{
			In:  isolation.Snapshot,
			Out: tree.SnapshotIsolation,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.Out, tree.IsolationLevelFromKVTxnIsolationLevel(tc.In))
	}
}
