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
