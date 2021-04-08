// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// TestStringerGolangProto is an anti-test: it shows that
// undesirable behavior is present.
func TestStringerGolangProto(t *testing.T) {
	ent := raftpb.Entry{
		Term:  1,
		Index: 2,
		Type:  3,
		Data:  []byte("foo"),
	}
	require.PanicsWithValue(
		t,
		"field raftpb.Entry.Term has invalid type: got uint64, want pointer",
		func() { _ = ent.String() },
	)
}
