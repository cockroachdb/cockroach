// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestBelowRaftProtosDontChange is a manually curated list of protos that we
// use below Raft. Care must be taken to change these protos, as replica
// divergence could ensue (if the old code and the new code handle the updated
// or old proto differently). Changes to the encoding of these protos will be
// detected by this test. The expectations should only be updated after a
// reflection on the safety of the proposed change.
func TestBelowRaftProtosDontChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []func(r *rand.Rand) protoutil.Message{
		func(r *rand.Rand) protoutil.Message {
			m := enginepb.NewPopulatedMVCCMetadata(r, false)
			m.Txn = nil                 // never populated below Raft
			m.TxnDidNotUpdateMeta = nil // never populated below Raft
			return m
		},
		func(r *rand.Rand) protoutil.Message {
			return kvserverpb.NewPopulatedRangeAppliedState(r, false)
		},
		func(r *rand.Rand) protoutil.Message {
			type expectedHardState struct {
				Term      uint64
				Vote      raftpb.PeerID
				Commit    uint64
				Lead      raftpb.PeerID
				LeadEpoch raftpb.Epoch
			}
			// Conversion fails if new fields are added to `HardState`, in which case this method
			// and the expected sums should be updated.
			var _ = expectedHardState(raftpb.HardState{})

			n := r.Uint64()
			return &raftpb.HardState{
				Term:      n % 3,
				Vote:      raftpb.PeerID(n % 7),
				Commit:    n % 11,
				Lead:      raftpb.PeerID(n % 13),
				LeadEpoch: raftpb.Epoch(n % 17),
			}
		},
		func(r *rand.Rand) protoutil.Message {
			// This is used downstream of Raft only to write it into unreplicated keyspace
			// as part of VersionUnreplicatedRaftTruncatedState.
			// However, it has been sent through Raft for a long time, as part of
			// ReplicatedEvalResult.
			return kvserverpb.NewPopulatedRaftTruncatedState(r, false)
		},
		func(r *rand.Rand) protoutil.Message {

			// These are marshaled below Raft by the Pebble merge operator. The Pebble
			// merge operator can be called below Raft whenever a Pebble MVCCIterator is
			// used.
			return roachpb.NewPopulatedInternalTimeSeriesData(r, false)
		},
		func(r *rand.Rand) protoutil.Message {
			return enginepb.NewPopulatedMVCCMetadataSubsetForMergeSerialization(r, false)
		},
		func(r *rand.Rand) protoutil.Message {
			return kvserverpb.NewPopulatedRaftReplicaID(r, false)
		},
	}

	// An arbitrary number chosen to seed the PRNGs used to populate the tested
	// protos.
	const goldenSeed = 1337
	// We'll randomly populate, marshal, and hash each proto. Doing this more than
	// once is necessary to make it very likely that all fields will be nonzero at
	// some point.
	const itersPerProto = 50

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for _, fn := range testCases {
		name := fmt.Sprintf("%T", fn(rand.New(rand.NewSource(0))))
		name = regexp.MustCompile(`.*\.`).ReplaceAllString(name, "")
		t.Run(name, w.Run(t, name, func(t *testing.T) string {
			rng := rand.New(rand.NewSource(goldenSeed))
			hash := fnv.New64a()
			for i := 0; i < itersPerProto; i++ {
				msg := fn(rng)
				dst := make([]byte, msg.Size())
				_, err := msg.MarshalTo(dst)
				require.NoError(t, err)
				hash.Write(dst)
			}
			return fmt.Sprint(hash.Sum64())
		}))
	}
}
