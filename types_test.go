// Copyright 2024 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestEntryID(t *testing.T) {
	// Some obvious checks first.
	require.Equal(t, entryID{term: 5, index: 10}, entryID{term: 5, index: 10})
	require.NotEqual(t, entryID{term: 4, index: 10}, entryID{term: 5, index: 10})
	require.NotEqual(t, entryID{term: 5, index: 9}, entryID{term: 5, index: 10})

	for _, tt := range []struct {
		entry pb.Entry
		want  entryID
	}{
		{entry: pb.Entry{}, want: entryID{term: 0, index: 0}},
		{entry: pb.Entry{Term: 1, Index: 2, Data: []byte("data")}, want: entryID{term: 1, index: 2}},
		{entry: pb.Entry{Term: 10, Index: 123}, want: entryID{term: 10, index: 123}},
	} {
		require.Equal(t, tt.want, pbEntryID(&tt.entry))
	}
}
