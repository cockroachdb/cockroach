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
	write("create", func(w storage.Writer) error { return createReplica(&s, w, id) })
	write("init", func(w storage.Writer) error { return initReplica(&s, w, id, 10) })
	write("split", func(w storage.Writer) error { return splitReplica(&s, w, id, 200) })

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
	require.Equal(t, 4, count)
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
		Addr:     wagpb.Addr{RangeID: id.RangeID, ReplicaID: id.ReplicaID, Index: 0},
		Event:    wagpb.ReplicaEvent{RangeID: id.RangeID, Type: wagpb.EventType_EventCreate},
		Mutation: wagpb.Mutation{Batch: b.Repr()},
	})
}

func initReplica(s *store, w storage.Writer, id roachpb.FullReplicaID, index uint64) error {
	return Write(w, s.seq.Next(1), wagpb.Node{
		Addr:  wagpb.Addr{RangeID: id.RangeID, ReplicaID: id.ReplicaID, Index: kvpb.RaftIndex(index)},
		Event: wagpb.ReplicaEvent{RangeID: id.RangeID, Type: wagpb.EventType_EventSnap},
		Mutation: wagpb.Mutation{Ingestion: &wagpb.Ingestion{
			SSTs: []string{"tmp/1.sst", "tmp/2.sst"},
		}},
	})
}

func splitReplica(s *store, w storage.Writer, id roachpb.FullReplicaID, index uint64) error {
	b := s.eng.NewWriteBatch()
	defer b.Close()
	if err := writeStateMachine(b, "lhs-key", "lhs-state"); err != nil {
		return err
	} else if err := writeStateMachine(b, "rhs-key", "rhs-state"); err != nil {
		return err
	}

	seq := s.seq.Next(2)
	if err := Write(w, seq, wagpb.Node{
		Addr:  wagpb.Addr{RangeID: id.RangeID, ReplicaID: id.ReplicaID, Index: kvpb.RaftIndex(index - 1)},
		Event: wagpb.ReplicaEvent{RangeID: id.RangeID, Type: wagpb.EventType_EventApply},
	}); err != nil {
		return err
	}
	return Write(w, seq+1, wagpb.Node{
		Addr:  wagpb.Addr{RangeID: id.RangeID, ReplicaID: id.ReplicaID, Index: kvpb.RaftIndex(index)},
		Event: wagpb.ReplicaEvent{RangeID: id.RangeID, Type: wagpb.EventType_EventApply},
		Events: []wagpb.ReplicaEvent{
			{RangeID: 567, Type: wagpb.EventType_EventSplit}, // the RHS
		},
		Mutation: wagpb.Mutation{Batch: b.Repr()},
	})
}

func writeStateMachine(w storage.Writer, k, v string) error {
	return w.PutUnversioned(roachpb.Key(k), []byte(v))
}
