// Copyright 2018 The Cockroach Authors.
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

package distsqlplan

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that the binPackingOracle is consistent in its choices: once a range has
// been assigned to one node, that choice is reused.
func TestBinPackingOracleIsConsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := roachpb.RangeDescriptor{RangeID: 99}

	queryState := makeOracleQueryState()
	expRepl := kv.ReplicaInfo{
		ReplicaDescriptor: roachpb.ReplicaDescriptor{
			NodeID: 99, StoreID: 99, ReplicaID: 99}}
	queryState.assignedRanges[rng.RangeID] = expRepl
	// For our purposes, an uninitialized binPackingOracle will do.
	bp := binPackingOracle{}
	repl, err := bp.ChoosePreferredLeaseHolder(rng, queryState)
	if err != nil {
		t.Fatal(err)
	}
	if repl != expRepl {
		t.Fatalf("expected replica %+v, got: %+v", expRepl, repl)
	}
}
