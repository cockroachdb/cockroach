// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

func TestRaftStorageWrites(t *testing.T) {
	ctx := context.Background()
	const rangeID = roachpb.RangeID(123)
	sl := NewStateLoader(rangeID)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	trunc := kvserverpb.RaftTruncatedState{Index: 100, Term: 20}
	state := RaftState{LastIndex: trunc.Index, LastTerm: trunc.Term}
	var output string

	printCommand := func(name, batch string) {
		output += fmt.Sprintf(">> %s\n%s\nState:%+v RaftTruncatedState:%+v\n",
			name, batch, state, trunc)
	}
	printCommand("init", "")

	writeBatch := func(prepare func(rw storage.ReadWriter)) string {
		t.Helper()
		batch := eng.NewBatch()
		defer batch.Close()
		prepare(batch)
		wb := kvserverpb.WriteBatch{Data: batch.Repr()}
		str, err := print.DecodeWriteBatch(wb.GetData())
		require.NoError(t, err)
		require.NoError(t, batch.Commit(true))
		return str
	}
	stats := func() int64 {
		t.Helper()
		prefix := keys.RaftLogPrefix(rangeID)
		prefixEnd := prefix.PrefixEnd()
		ms, err := storage.ComputeStats(ctx, eng, prefix, prefixEnd, 0 /* nowNanos */)
		require.NoError(t, err)
		return ms.SysBytes
	}

	write := func(name string, hs raftpb.HardState, entries []raftpb.Entry) {
		t.Helper()
		var newState RaftState
		batch := writeBatch(func(rw storage.ReadWriter) {
			require.NoError(t, storeHardState(ctx, rw, sl, hs))
			var err error
			newState, err = logAppend(ctx, sl.RaftLogPrefix(), rw, state, entries)
			require.NoError(t, err)
		})
		state = newState
		require.Equal(t, stats(), state.ByteSize)
		printCommand(name, batch)
	}
	truncate := func(name string, ts kvserverpb.RaftTruncatedState) {
		t.Helper()
		batch := writeBatch(func(rw storage.ReadWriter) {
			require.NoError(t, Compact(ctx, trunc, ts, sl, rw))
		})
		trunc = ts
		state.ByteSize = stats()
		printCommand(name, batch)
	}

	write("append (100,103]", raftpb.HardState{
		Term: 21, Vote: 3, Commit: 100, Lead: 3, LeadEpoch: 5,
	}, []raftpb.Entry{
		{Index: 101, Term: 20},
		{Index: 102, Term: 21},
		{Index: 103, Term: 21},
	})
	write("append (101,102] with overlap", raftpb.HardState{
		Term: 22, Commit: 100,
	}, []raftpb.Entry{
		{Index: 102, Term: 22},
	})
	write("append (102,105]", raftpb.HardState{}, []raftpb.Entry{
		{Index: 103, Term: 22},
		{Index: 104, Term: 22},
		{Index: 105, Term: 22},
	})
	truncate("truncate at 103", kvserverpb.RaftTruncatedState{Index: 103, Term: 22})
	truncate("truncate all", kvserverpb.RaftTruncatedState{Index: 105, Term: 22})

	// TODO(pav-kv): print the engine content as well.

	output = strings.ReplaceAll(output, "\n\n", "\n")
	output = strings.ReplaceAll(output, "\n\n", "\n")
	echotest.Require(t, output, filepath.Join("testdata", t.Name()+".txt"))
}
