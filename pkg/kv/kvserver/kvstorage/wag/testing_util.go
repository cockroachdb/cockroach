// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/stretchr/testify/require"
)

// AssertMutationBatch asserts that at most one WAG node in the raft batch
// carries an embedded state engine batch, and that it matches stateRepr at the
// byte level. Returns true if such a node was found.
func AssertMutationBatch(t *testing.T, raftRepr, stateRepr []byte) bool {
	t.Helper()
	r, err := storage.NewBatchReader(raftRepr)
	require.NoError(t, err)
	var found bool
	for r.Next() {
		key, err := r.EngineKey()
		require.NoError(t, err)
		if !bytes.HasPrefix(key.Key, keys.StoreWAGPrefix()) {
			continue
		}
		var node wagpb.Node
		require.NoError(t, node.Unmarshal(r.Value())) // nolint:protounmarshal
		if len(node.Mutation.Batch) == 0 {
			continue
		}
		require.False(t, found,
			"expected exactly one WAG node with a mutation batch, found multiple")
		found = true
		require.Equal(t, stateRepr, node.Mutation.Batch,
			"WAG mutation batch should match state engine batch")
	}
	return found
}
