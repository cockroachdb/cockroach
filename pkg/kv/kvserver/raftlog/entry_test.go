// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package raftlog

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestNewEntry(t *testing.T) {
	// TODO(replication): Add more cases.
	testcases := map[string]struct {
		data        []byte
		expectEmpty bool
		expectErr   bool
	}{
		// Proposed by Raft on leader change.
		"empty entry": {data: nil, expectEmpty: true},
		// Proposed by CRDB on unquiescence.
		"empty payload": {
			data: EncodeCommandBytes(
				EntryEncodingStandardWithoutAC, "00000000", nil, 0 /* pri */),
			expectEmpty: true,
		},
		"invalid": {
			data: EncodeCommandBytes(
				EntryEncodingStandardWithAC, "00000000", []byte("not a protobuf"), 0 /* pri */),
			expectErr: true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ent, err := NewEntry(raftpb.Entry{
				Term:  1,
				Index: 1,
				Data:  tc.data,
			})
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Clear out the passed Raft entry, and only assert on the decoded entry.
			require.NotNil(t, ent)
			ent.Entry = raftpb.Entry{}
			if tc.expectEmpty {
				require.Zero(t, *ent)
			} else {
				require.NotZero(t, *ent)
			}
		})
	}
}
