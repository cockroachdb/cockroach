// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestLowPriOverrideState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var lpos lowPriOverrideState
	lposString := func() string {
		var b strings.Builder
		fmt.Fprintf(&b, "leader-term: %d", lpos.leaderTerm)
		if n := lpos.intervals.Length(); n > 0 {
			fmt.Fprintf(&b, "\nintervals:")
			for j := 0; j < n; j++ {
				i := lpos.intervals.At(j)
				fmt.Fprintf(&b, "\n [%3d, %3d] => %t", i.first, i.last, i.lowPriOverride)
			}
		}
		return b.String()
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "low_pri_override_state"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "reset":
				lpos = lowPriOverrideState{}
				return ""

			case "side-channel":
				// Example:
				//  side-channel leader-term=3 first=5 last=10 low-pri
				//
				// Provides side-channel info for the the interval [5, 10] for the given
				// leader-term and with the specific override.
				leaderTerm := dd.ScanArg[uint64](t, d, "leader-term")
				first := dd.ScanArg[uint64](t, d, "first")
				last := dd.ScanArg[uint64](t, d, "last")
				lowPriOverride := d.HasArg("low-pri")

				notStaleTerm := lpos.sideChannelForLowPriOverride(leaderTerm, first, last, lowPriOverride)
				return fmt.Sprintf("not-stale-term: %t\n%s", notStaleTerm, lposString())

			case "get-effective-priority":
				// Example:
				//  get-effective-priority index=4 pri=HighPri
				// Gets the effective priority for index 4, where the original
				// priority is HighPri
				index := dd.ScanArg[uint64](t, d, "index")
				pri := parsePriority(t, dd.ScanArg[string](t, d, "pri"))
				effectivePri := lpos.getEffectivePriority(index, pri)
				return fmt.Sprintf("pri: %s\n%s", effectivePri, lposString())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func parsePriority(t *testing.T, str string) raftpb.Priority {
	switch str {
	case "LowPri":
		return raftpb.LowPri
	case "NormalPri":
		return raftpb.NormalPri
	case "AboveNormalPri":
		return raftpb.AboveNormalPri
	case "HighPri":
		return raftpb.HighPri
	default:
		t.Fatalf("unknown pri %s", str)
	}
	return 0
}
