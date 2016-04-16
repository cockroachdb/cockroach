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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package storage_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
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

func marshalTo(pb proto.Message, b []byte) (int, error) {
	return pb.(interface {
		MarshalTo([]byte) (int, error)
	}).MarshalTo(b)
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
	populatedConstructor   func(*rand.Rand) proto.Message
	emptySum, populatedSum uint64
}

var belowRaftGoldenProtos = map[reflect.Type]fixture{
	reflect.TypeOf(&engine.MVCCMetadata{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return engine.NewPopulatedMVCCMetadata(r, false) },
		emptySum:             7551962144604783939,
		populatedSum:         16635523155996652761,
	},
	reflect.TypeOf(&engine.MVCCStats{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return engine.NewPopulatedMVCCStats(r, false) },
		emptySum:             12760152523270066584,
		populatedSum:         17551055561102061772,
	},
	reflect.TypeOf(&roachpb.AbortCacheEntry{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return roachpb.NewPopulatedAbortCacheEntry(r, false) },
		emptySum:             11932598136014321867,
		populatedSum:         5118321872981034391,
	},
	reflect.TypeOf(&roachpb.Lease{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return roachpb.NewPopulatedLease(r, false) },
		emptySum:             10006158318270644799,
		populatedSum:         717371977055084394,
	},
	reflect.TypeOf(&roachpb.RaftTruncatedState{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return roachpb.NewPopulatedRaftTruncatedState(r, false) },
		emptySum:             5531676819244041709,
		populatedSum:         14781226418259198098,
	},
	reflect.TypeOf(&roachpb.Timestamp{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return roachpb.NewPopulatedTimestamp(r, false) },
		emptySum:             5531676819244041709,
		populatedSum:         10735653246768912584,
	},
	reflect.TypeOf(&roachpb.Transaction{}): {
		populatedConstructor: func(r *rand.Rand) proto.Message { return roachpb.NewPopulatedTransaction(r, false) },
		emptySum:             1224176153607079475,
		populatedSum:         154941000985571253,
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
		if b, err := protoutil.Marshal(reflect.New(typ.Elem()).Interface().(proto.Message)); err != nil {
			t.Fatal(err)
		} else if err := verifyHash(b, fix.emptySum); err != nil {
			t.Errorf("%s (empty): %s\n", typ, err)
		}

		randGen := rand.New(rand.NewSource(goldenSeed))

		bytes := slice
		numBytes := 0
		for i := 0; i < itersPerProto; i++ {
			if n, err := marshalTo(fix.populatedConstructor(randGen), bytes); err != nil {
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
