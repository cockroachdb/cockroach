// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"reflect"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
		populatedSum: 3716674106872807900,
	},
	reflect.TypeOf(&enginepb.MVCCStats{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return enginepb.NewPopulatedMVCCStats(r, false)
		},
		emptySum:     18064891702890239528,
		populatedSum: 4287370248246326846,
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
	reflect.TypeOf(&roachpb.RangeDescriptor{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return roachpb.NewPopulatedRangeDescriptor(r, false)
		},
		emptySum:     5524024218313206949,
		populatedSum: 7661699749677660364,
	},
	reflect.TypeOf(&storage.Liveness{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return storage.NewPopulatedLiveness(r, false)
		},
		emptySum:     892800390935990883,
		populatedSum: 16231745342114354146,
	},
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
