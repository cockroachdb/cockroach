// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"path/filepath"
	"testing"

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

	b := eng.NewWriteBatch()
	defer b.Close()
	require.NoError(t, createReplica(&s, b, roachpb.FullReplicaID{RangeID: 123, ReplicaID: 4}))
	str, err := print.DecodeWriteBatch(b.Repr())
	require.NoError(t, err)

	echotest.Require(t, str, filepath.Join("testdata", t.Name()+".txt"))
}

type store struct {
	eng storage.Engine
	seq Seq
}

func createReplica(s *store, w storage.Writer, id roachpb.FullReplicaID) error {
	b := s.eng.NewWriteBatch()
	defer b.Close()
	if err := writeStateMachine(b); err != nil {
		return err
	}
	return Write(w, s.seq.Next(1), wagpb.Node{
		Addr:     wagpb.Addr{RangeID: id.RangeID, ReplicaID: id.ReplicaID, Index: 0},
		Type:     wagpb.NodeType_NodeCreate,
		Mutation: wagpb.Mutation{Batch: b.Repr()},
	})
}

func writeStateMachine(w storage.Writer) error {
	return w.PutUnversioned(roachpb.Key("state-machine-key"), []byte("state"))
}
