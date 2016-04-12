// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package storage_test

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/gogo/protobuf/proto"
)

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

func init() {
	security.SetReadFileFn(securitytest.Asset)
}

func TestMain(m *testing.M) {
	randGen := rand.New(rand.NewSource(1337))

	type fixture struct {
		pb  proto.Message
		sum uint64
	}

	goldenProtos := map[reflect.Type][]fixture{
		reflect.TypeOf(&engine.MVCCMetadata{}): {
			{pb: engine.NewPopulatedMVCCMetadata(randGen, false), sum: 2728728440076281602},
			{pb: &engine.MVCCMetadata{}, sum: 6473109783500985267},
		},
		reflect.TypeOf(&engine.MVCCStats{}): {
			{pb: engine.NewPopulatedMVCCStats(randGen, false), sum: 11858799000012002160},
			{pb: &engine.MVCCStats{}, sum: 1326193836893560210},
		},
		reflect.TypeOf(&roachpb.AbortCacheEntry{}): {
			{pb: roachpb.NewPopulatedAbortCacheEntry(randGen, false), sum: 7918534545804338596},
			{pb: &roachpb.AbortCacheEntry{}, sum: 8917802744911433179},
		},
		reflect.TypeOf(&roachpb.Lease{}): {
			{pb: roachpb.NewPopulatedLease(randGen, false), sum: 1441559666090216627},
			{pb: &roachpb.Lease{}, sum: 16583902425383634009},
		},
		reflect.TypeOf(&roachpb.RaftTruncatedState{}): {
			{pb: roachpb.NewPopulatedRaftTruncatedState(randGen, false), sum: 11267962160822594540},
			{pb: &roachpb.RaftTruncatedState{}, sum: 565675090684819917},
		},
		reflect.TypeOf(&roachpb.Timestamp{}): {
			{pb: roachpb.NewPopulatedTimestamp(randGen, false), sum: 7610952670154447675},
			{pb: &roachpb.Timestamp{}, sum: 565675090684819917},
		},
		reflect.TypeOf(&roachpb.Transaction{}): {
			{pb: roachpb.NewPopulatedTransaction(randGen, false), sum: 16240475230282655127},
			{pb: &roachpb.Transaction{}, sum: 14457414605350033667},
		},
	}

	stopTrackingAndGetTypes := storage.TrackRaftProtos()

	randutil.SeedForTests()

	code := m.Run()

	notBelowRaftProtos := make(map[reflect.Type]struct{}, len(goldenProtos))
	for t := range goldenProtos {
		notBelowRaftProtos[t] = struct{}{}
	}

	var buf bytes.Buffer
	for _, t := range stopTrackingAndGetTypes() {
		if fixtures, ok := goldenProtos[t]; ok {
			delete(notBelowRaftProtos, t)

			for i, fixture := range fixtures {
				if b, err := protoutil.Marshal(fixture.pb); err != nil {
					fmt.Fprintf(&buf, "%s/%d: %s\n", t, i, err)
				} else {
					hash := fnv.New64()
					if _, err := hash.Write(b); err != nil {
						fmt.Fprintf(&buf, "%s/%d: %s\n", t, i, err)
					}
					if a, e := hash.Sum64(), fixture.sum; a != e {
						fmt.Fprintf(&buf, "%s/%d: expected sum %d, got %d\n", t, i, e, a)
					}
				}
			}
		} else {
			fmt.Fprintf(&buf, "%s: missing fixtures!\n", t)
		}
	}

	for t := range notBelowRaftProtos {
		fmt.Fprintf(&buf, "%s: not observed below raft!\n", t)
	}

	if out := buf.String(); len(out) != 0 {
		fmt.Print(out)

		// In case the rest of the test suite passed, fail it.
		if code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}
