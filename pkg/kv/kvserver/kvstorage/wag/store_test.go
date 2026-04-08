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

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	// Intentionally introduce a gap in the sequence. We will later make sure that
	// the iterator correctly skips over this gap.
	s.seq.Next()
	write("init", func(w storage.Writer) error { return initReplica(&s, w, id, 10) })
	write("split", func(w storage.Writer) error { return splitReplica(&s, w, id, rhsID, 200) })

	// TODO(pav-kv): the trailing \n in DecodeWriteBatch is duplicated with
	// recursion. Remove it, and let the caller handle new lines.
	out = strings.ReplaceAll(out, "\n\n", "\n")
	echotest.Require(t, out, filepath.Join("testdata", t.Name()+".txt"))

	// readIndices returns the WAG node indices by scanning the engine.
	readIndices := func() []uint64 {
		var it Iterator
		var res []uint64
		for index := range it.Iter(context.Background(), s.eng) {
			res = append(res, index)
		}
		require.NoError(t, it.Error())
		return res
	}

	// Verify that the iterator returns nodes with the correct indices.
	// 3 WAG nodes: create, init, split. The split is a single node with two
	// events (Split + Init) rather than two separate nodes (dep + event).
	require.Equal(t, []uint64{1, 3, 4}, readIndices())
}

func TestDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	id := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	node := wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.MakeAddr(id, 10), Type: wagpb.EventApply},
		},
	}

	// Write 5 WAG nodes with indices 1 through 5.
	for i := uint64(1); i <= 5; i++ {
		require.NoError(t, Write(eng, i, node))
	}

	// Read back all indices.
	readIndices := func() []uint64 {
		var it Iterator
		var res []uint64
		for index := range it.Iter(context.Background(), eng) {
			res = append(res, index)
		}
		require.NoError(t, it.Error())
		return res
	}
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, readIndices())

	// Delete nodes 2 and 4.
	require.NoError(t, Delete(eng, 2))
	require.NoError(t, Delete(eng, 4))

	// Verify that only nodes 1, 3, 5 remain.
	require.Equal(t, []uint64{1, 3, 5}, readIndices())
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
	return Write(w, s.seq.Next(), wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.MakeAddr(id, 0), Type: wagpb.EventCreate},
		},
		Mutation: wagpb.Mutation{Batch: b.Repr()},
	})
}

func initReplica(s *store, w storage.Writer, id roachpb.FullReplicaID, index uint64) error {
	return Write(w, s.seq.Next(), wagpb.Node{
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
	return Write(w, s.seq.Next(), wagpb.Node{
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

func TestIterFrom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write 3 WAG nodes at indices 1, 5, 10.
	id := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	node := wagpb.Node{
		Events: []wagpb.Event{
			{Addr: wagpb.MakeAddr(id, 10), Type: wagpb.EventApply},
		},
	}
	require.NoError(t, Write(eng, 1, node))
	require.NoError(t, Write(eng, 5, node))
	require.NoError(t, Write(eng, 10, node))

	readIndicesFrom := func(fromIndex uint64) []uint64 {
		var it Iterator
		var res []uint64
		for index := range it.IterFrom(ctx, eng, keys.StoreWAGNodeKey(fromIndex)) {
			res = append(res, index)
		}
		require.NoError(t, it.Error())
		return res
	}

	require.Equal(t, []uint64{1, 5, 10}, readIndicesFrom(0))
	require.Equal(t, []uint64{1, 5, 10}, readIndicesFrom(1))
	require.Equal(t, []uint64{5, 10}, readIndicesFrom(4))
	require.Equal(t, []uint64{10}, readIndicesFrom(6))
	require.Equal(t, []uint64{10}, readIndicesFrom(10))
	require.Empty(t, readIndicesFrom(11))
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
		require.Equal(t, uint64(1), seq.Next())
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
		s.seq.Next() // skip index 3
		require.NoError(t, splitReplica(&s, b, id, rhsID, 20))
		require.NoError(t, b.Commit(false /* sync */))

		// Init a fresh sequencer from the log engine.
		var seq2 Seq
		require.NoError(t, seq2.Init(ctx, eng))
		// Next allocation should be after the last persisted index (5).
		require.Equal(t, uint64(5), seq2.Next())
	})
}
