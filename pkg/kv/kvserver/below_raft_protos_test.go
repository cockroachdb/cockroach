// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"go.etcd.io/etcd/raft/raftpb"
)

func verifyHash(b []byte, expectedSum uint64) error {
	hash := fnv.New64a()
	if _, err := hash.Write(b); err != nil {
		return err
	}
	if sum := hash.Sum64(); sum != expectedSum {
		return fmt.Errorf("expected sum %d; got %d", expectedSum, sum)
	}
	return nil
}

// An arbitrary number chosen to seed the PRNGs used to populate the tested
// protos.
const goldenSeed = 1337

// The count of randomly populated protos that will be concatenated and hashed
// per proto type. Given that the population functions have a chance of leaving
// some fields zero-valued, this number must be greater than `1` to give this
// test a reasonable chance of encountering a non-zero value of every field.
const itersPerProto = 20

type fixture struct {
	populatedConstructor   func(*rand.Rand) protoutil.Message
	emptySum, populatedSum uint64
}

var belowRaftGoldenProtos = map[reflect.Type]fixture{
	reflect.TypeOf(&enginepb.MVCCMetadata{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			m := enginepb.NewPopulatedMVCCMetadata(r, false)
			m.Txn = nil // never populated below Raft
			return m
		},
		emptySum:     7551962144604783939,
		populatedSum: 12720006657210437557,
	},
	reflect.TypeOf(&enginepb.RangeAppliedState{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return enginepb.NewPopulatedRangeAppliedState(r, false)
		},
		emptySum:     615555020845646359,
		populatedSum: 6873743885651366543,
	},
	reflect.TypeOf(&raftpb.HardState{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			type expectedHardState struct {
				Term             uint64
				Vote             uint64
				Commit           uint64
				XXX_unrecognized []byte
			}
			// Conversion fails if new fields are added to `HardState`, in which case this method
			// and the expected sums should be updated.
			var _ = expectedHardState(raftpb.HardState{})

			n := r.Uint64()
			return &raftpb.HardState{
				Term:             n % 3,
				Vote:             n % 7,
				Commit:           n % 11,
				XXX_unrecognized: nil,
			}
		},
		emptySum:     13621293256077144893,
		populatedSum: 13375098491754757572,
	},
	// This is used downstream of Raft only to write it into unreplicated keyspace
	// as part of VersionUnreplicatedRaftTruncatedState.
	// However, it has been sent through Raft for a long time, as part of
	// ReplicatedEvalResult.
	reflect.TypeOf(&roachpb.RaftTruncatedState{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return roachpb.NewPopulatedRaftTruncatedState(r, false)
		},
		emptySum:     5531676819244041709,
		populatedSum: 14781226418259198098,
	},
}

func init() {
	if storage.DefaultStorageEngine != enginepb.EngineTypeRocksDB {
		// These are marshaled below Raft by the Pebble merge operator. The Pebble
		// merge operator can be called below Raft whenever a Pebble Iterator is
		// used. Note that we only see these protos marshaled below Raft when the
		// engine type is not RocksDB. If the engine type is RocksDB the marshaling
		// occurs in C++ which is invisible to the tracking mechanism.
		belowRaftGoldenProtos[reflect.TypeOf(&roachpb.InternalTimeSeriesData{})] = fixture{
			populatedConstructor: func(r *rand.Rand) protoutil.Message {
				return roachpb.NewPopulatedInternalTimeSeriesData(r, false)
			},
			emptySum:     5531676819244041709,
			populatedSum: 8911200268508796945,
		}
		belowRaftGoldenProtos[reflect.TypeOf(&enginepb.MVCCMetadataSubsetForMergeSerialization{})] =
			fixture{
				populatedConstructor: func(r *rand.Rand) protoutil.Message {
					return enginepb.NewPopulatedMVCCMetadataSubsetForMergeSerialization(r, false)
				},
				emptySum:     14695981039346656037,
				populatedSum: 7432412240713840291,
			}
	}
}

func TestBelowRaftProtos(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Enable the additional checks in TestMain. NB: running this test by itself
	// will fail those extra checks - such failures are safe to ignore, so long
	// as this test passes when run with the entire package's tests.
	verifyBelowRaftProtos = true

	slice := make([]byte, 1<<20)
	for typ, fix := range belowRaftGoldenProtos {
		if b, err := protoutil.Marshal(reflect.New(typ.Elem()).Interface().(protoutil.Message)); err != nil {
			t.Fatal(err)
		} else if err := verifyHash(b, fix.emptySum); err != nil {
			t.Errorf("%s (empty): %s\n", typ, err)
		}

		randGen := rand.New(rand.NewSource(goldenSeed))

		bytes := slice
		numBytes := 0
		for i := 0; i < itersPerProto; i++ {
			if n, err := fix.populatedConstructor(randGen).MarshalTo(bytes); err != nil {
				t.Fatal(err)
			} else {
				bytes = bytes[n:]
				numBytes += n
			}
		}
		if err := verifyHash(slice[:numBytes], fix.populatedSum); err != nil {
			t.Errorf("%s (populated): %s\n", typ, err)
		}
	}
}
