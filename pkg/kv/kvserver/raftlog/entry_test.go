// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package raftlog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
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
			data:        EncodeRaftCommand(EntryEncodingStandardWithoutAC, "00000000", nil),
			expectEmpty: true,
		},
		"invalid": {
			data:      EncodeRaftCommand(EntryEncodingStandardWithAC, "00000000", []byte("not a protobuf")),
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
