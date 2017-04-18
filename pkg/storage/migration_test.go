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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestMigrate7310And6991(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<10)
	stopper.AddCloser(eng)

	desc := *testRangeDescriptor()

	if err := migrate7310And6991(context.Background(), eng, desc); err != nil {
		t.Fatal(err)
	}

	rsl := makeReplicaStateLoader(desc.RangeID)
	ts, err := rsl.loadTruncatedState(context.Background(), eng)
	if err != nil {
		t.Fatal(err)
	}

	hs, err := rsl.loadHardState(context.Background(), eng)
	if err != nil {
		t.Fatal(err)
	}

	rApplied, lApplied, err := rsl.loadAppliedIndex(context.Background(), eng)
	if err != nil {
		t.Fatal(err)
	}

	expTS := roachpb.RaftTruncatedState{Term: raftInitialLogTerm, Index: raftInitialLogIndex}
	if expTS != ts {
		t.Errorf("expected %+v, got %+v", &expTS, &ts)
	}

	expHS := raftpb.HardState{Term: raftInitialLogTerm, Commit: raftInitialLogIndex}
	if !reflect.DeepEqual(expHS, hs) {
		t.Errorf("expected %+v, got %+v", &expHS, &hs)
	}

	expRApplied, expLApplied := uint64(raftInitialLogIndex), uint64(0)
	if expRApplied != rApplied || expLApplied != lApplied {
		t.Errorf("expected (raftApplied,leaseApplied)=(%d,%d), got (%d,%d)",
			expRApplied, expLApplied, rApplied, lApplied)
	}
}
