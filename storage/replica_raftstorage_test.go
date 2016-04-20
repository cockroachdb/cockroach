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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/tracing"
)

func BenchmarkReplicaSnapshot(b *testing.B) {
	defer tracing.Disable()()
	defer config.TestingDisableTableSplits()()
	store, stopper, _ := createTestStore(b)
	// We want to manually control the size of the raft log.
	store.DisableRaftLogQueue(true)
	defer stopper.Stop()

	const rangeID = 1
	const keySize = 1 << 7   // 128 B
	const valSize = 1 << 10  // 1 KiB
	const snapSize = 1 << 25 // 32 MiB

	rep, err := store.GetReplica(rangeID)
	if err != nil {
		b.Fatal(err)
	}

	src := rand.New(rand.NewSource(0))
	for i := 0; i < snapSize/(keySize+valSize); i++ {
		key := keys.MakeNonColumnKey(randutil.RandBytes(src, keySize))
		val := randutil.RandBytes(src, valSize)
		pArgs := putArgs(key, val)
		if _, pErr := client.SendWrappedWith(rep, nil, roachpb.Header{
			RangeID: rangeID,
		}, &pArgs); pErr != nil {
			b.Fatal(pErr)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rep.GetSnapshot(); err != nil {
			b.Fatal(err)
		}
	}
}
