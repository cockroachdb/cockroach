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

package storage

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/tracing"
)

func BenchmarkReplicaSnapshot(b *testing.B) {
	defer tracing.Disable()()
	sCtx := TestStoreContext()
	sCtx.TestingKnobs.DisableSplitQueue = true
	store, _, stopper := createTestStoreWithContext(b, &sCtx)
	defer stopper.Stop()
	// We want to manually control the size of the raft log.
	store.SetRaftLogQueueActive(false)

	rep, err := store.GetReplica(rangeID)
	if err != nil {
		b.Fatal(err)
	}

	snapSize := rep.GetMaxBytes()
	fillTestRange(b, rep, snapSize)
	b.SetBytes(snapSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rep.GetSnapshot(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
