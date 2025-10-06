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
				var leaderTerm uint64
				d.ScanArgs(t, "leader-term", &leaderTerm)
				var first, last uint64
				d.ScanArgs(t, "first", &first)
				d.ScanArgs(t, "last", &last)
				var lowPriOverride bool
				if d.HasArg("low-pri") {
					lowPriOverride = true
				}
				notStaleTerm := lpos.sideChannelForLowPriOverride(leaderTerm, first, last, lowPriOverride)
				return fmt.Sprintf("not-stale-term: %t\n%s", notStaleTerm, lposString())

			case "side-channel-v1":
				var leaderTerm uint64
				d.ScanArgs(t, "leader-term", &leaderTerm)
				termAdvanced := lpos.sideChannelForV1Leader(leaderTerm)
				return fmt.Sprintf("term-advanced: %t\n%s", termAdvanced, lposString())

			case "get-effective-priority":
				// Example:
				//  get-effective-priority index=4 pri=HighPri
				// Gets the effective priority for index 4, where the original
				// priority is HighPri
				var index uint64
				d.ScanArgs(t, "index", &index)
				pri := readPriority(t, d)
				effectivePri := lpos.getEffectivePriority(index, pri)
				return fmt.Sprintf("pri: %s\n%s", effectivePri, lposString())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func readPriority(t *testing.T, d *datadriven.TestData) raftpb.Priority {
	var priStr string
	d.ScanArgs(t, "pri", &priStr)
	switch priStr {
	case "LowPri":
		return raftpb.LowPri
	case "NormalPri":
		return raftpb.NormalPri
	case "AboveNormalPri":
		return raftpb.AboveNormalPri
	case "HighPri":
		return raftpb.HighPri
	default:
		t.Fatalf("unknown pri %s", priStr)
	}
	return 0
}
