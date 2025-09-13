// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gen

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// generateClusterVisualization generates a visualization of the cluster state.
// Example: mma_one_voter_skewed_cpu_skewed_write_setup.txt.
//
// Cluster Set Up
// n1(AU_EAST,AU_EAST_1,0vcpu): {s1}
// n2(AU_EAST,AU_EAST_1,0vcpu): {s2}
// Key Space
// [1,10000): 100(rf=1), 25MiB, [{s1*}:100]
// [10001,20000): 100(rf=1), 25MiB, [{s2*}:100]
// Event
// set LBRebalancingMode to 4
// Workload Set Up
// [1,10000): read-only high-cpu [500.00cpu-us/op, 1B/op, 1000ops/s]
// [10001,20000): write-only large-block [0.00cpu-us/write(raft), 1000B/op, 5000ops/s]
func generateClusterVisualization(
	buf *strings.Builder, s state.State, loadGen LoadGen, eventGen EventGen, rangeStateStr string,
) {
	if buf == nil {
		return
	}
	_, _ = fmt.Fprintf(buf, "Cluster Set Up\n")
	for _, n := range s.Nodes() {
		_, _ = fmt.Fprintf(buf, "%v", n)
		_, _ = fmt.Fprintf(buf, "\n")
	}
	_, _ = fmt.Fprintf(buf, "Key Space\n%s", rangeStateStr)
	_, _ = fmt.Fprintf(buf, "Event\n%s", eventGen.String())
	_, _ = fmt.Fprintf(buf, "Workload Set Up\n%s", loadGen.String())
}
