// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	s := store{eng: eng}

	var out string
	write := func(name string, f func(w storage.Writer) error) {
		b := eng.NewWriteBatch()
		defer b.Close()
		require.NoError(t, f(b))

		str, err := print.DecodeWriteBatch(b.Repr())
		require.NoError(t, err)
		out += fmt.Sprintf(">> %s\n%s", name, str)

		require.NoError(t, b.Commit(false /* sync */))
	}

	id := roachpb.FullReplicaID{RangeID: 123, ReplicaID: 4}
	rhsID := roachpb.FullReplicaID{RangeID: 567, ReplicaID: 1}
	write("create", func(w storage.Writer) error { return createReplica(&s, w, id) })
	write("init", func(w storage.Writer) error { return initReplica(&s, w, id, 10) })
	write("split", func(w storage.Writer) error { return splitReplica(&s, w, id, rhsID, 200) })

	// TODO(pav-kv): the trailing \n in DecodeWriteBatch is duplicated with
	// recursion. Remove it, and let the caller handle new lines.
	out = strings.ReplaceAll(out, "\n\n", "\n")
	echotest.Require(t, out, filepath.Join("testdata", t.Name()+".txt"))

	// Smoke check that the iterator works.
	var iter Iterator
	count := 0
	for range iter.Iter(context.Background(), s.eng) {
		count++
	}
	require.NoError(t, iter.Error())
	// 3 WAG nodes: create, init, split. The split is a single node with two
	// events (Split + Init) rather than two separate nodes (dep + event).
	require.Equal(t, 3, count)
}

type store struct {
	eng storage.Engine
	seq Seq
}

func createReplica(s *store, w storage.Writer, id roachpb.FullReplicaID) error {
	b := s.eng.NewWriteBatch()
	defer b.Close()
	if err := writeStateMachine(b, "state-machine-key", "state"); err != nil {
		return err
	}
	return Write(w, s.seq.Next(1), wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.MakeAddr(id, 0), Type: wagpb.EventCreate},
		},
		Mutation: wagpb.Mutation{Batch: b.Repr()},
	})
}

func initReplica(s *store, w storage.Writer, id roachpb.FullReplicaID, index uint64) error {
	return Write(w, s.seq.Next(1), wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.MakeAddr(id, kvpb.RaftIndex(index)), Type: wagpb.EventInit},
		},
		Mutation: wagpb.Mutation{Ingestion: &wagpb.Ingestion{
			SSTs: []string{"tmp/1.sst", "tmp/2.sst"},
		}},
	})
}

func splitReplica(
	s *store, w storage.Writer, lhsID, rhsID roachpb.FullReplicaID, index uint64,
) error {
	b := s.eng.NewWriteBatch()
	defer b.Close()
	if err := writeStateMachine(b, "lhs-key", "lhs-state"); err != nil {
		return err
	} else if err := writeStateMachine(b, "rhs-key", "rhs-state"); err != nil {
		return err
	}
	return Write(w, s.seq.Next(1), wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.MakeAddr(rhsID, 10), Type: wagpb.EventInit},
			{Addr: wagpb.MakeAddr(lhsID, kvpb.RaftIndex(index)), Type: wagpb.EventSplit},
		},
		Mutation: wagpb.Mutation{Batch: b.Repr()},
	})
}

func writeStateMachine(w storage.Writer, k, v string) error {
	return w.PutUnversioned(roachpb.Key(k), []byte(v))
}

func TestSeqInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("empty log engine", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		var seq Seq
		require.NoError(t, seq.Init(ctx, eng))
		require.Equal(t, uint64(1), seq.Next(1))
	})

	t.Run("resumes after last node", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		// Write 3 WAG nodes with a gap in the sequence.
		id := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
		rhsID := roachpb.FullReplicaID{RangeID: 2, ReplicaID: 1}
		s := store{eng: eng}
		b := eng.NewWriteBatch()
		require.NoError(t, createReplica(&s, b, id))
		require.NoError(t, initReplica(&s, b, id, 10))
		s.seq.Next(3) // skip indices 3-5
		require.NoError(t, splitReplica(&s, b, id, rhsID, 20))
		require.NoError(t, b.Commit(false /* sync */))

		// Init a fresh sequencer from the log engine.
		var seq2 Seq
		require.NoError(t, seq2.Init(ctx, eng))
		// Next allocation should be after the last persisted index (6).
		require.Equal(t, uint64(7), seq2.Next(1))
	})
}
