// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		if len(lpos.intervals) > 0 {
			fmt.Fprintf(&b, "\nintervals:")
			for _, i := range lpos.intervals {
				fmt.Fprintf(&b, "\n [%4d, %4d] => %t", i.first, i.last, i.lowPriOverride)
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
				var leaderTerm uint64
				d.ScanArgs(t, "leader-term", &leaderTerm)
				var first, last uint64
				d.ScanArgs(t, "first", &first)
				d.ScanArgs(t, "last", &last)
				var lowPriOverride bool
				if d.HasArg("low-pri") {
					lowPriOverride = true
				}
				lpos.sideChannelForLowPriOverride(leaderTerm, first, last, lowPriOverride)
				return lposString()

			case "get-effective-priority":
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

func TestWaitingForAdmissionState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var w waitingForAdmissionState
	waitingStateString := func() string {
		var b strings.Builder
		for i := range w.waiting {
			fmt.Fprintf(&b, "%s:", raftpb.Priority(i))
			for _, entry := range w.waiting[i] {
				fmt.Fprintf(&b, " (i: %d, term: %d)", entry.index, entry.leaderTerm)
			}
			fmt.Fprintf(&b, "\n")
		}
		return b.String()
	}
	argsLeaderIndexPri := func(
		t *testing.T, d *datadriven.TestData) (leaderTerm uint64, index uint64, pri raftpb.Priority) {
		d.ScanArgs(t, "leader-term", &leaderTerm)
		d.ScanArgs(t, "index", &index)
		pri = readPriority(t, d)
		return
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "waiting_for_admission_state"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "add":
				leaderTerm, index, pri := argsLeaderIndexPri(t, d)
				w.add(leaderTerm, index, pri)
				return waitingStateString()

			case "remove":
				leaderTerm, index, pri := argsLeaderIndexPri(t, d)
				advanced := w.remove(leaderTerm, index, pri)
				return fmt.Sprintf("admittedAdvanced: %t\n%s", advanced, waitingStateString())

			case "compute-admitted":
				var stableIndex uint64
				d.ScanArgs(t, "stable-index", &stableIndex)
				admitted := w.computeAdmitted(stableIndex)
				return fmt.Sprintf("admitted: [%d, %d, %d, %d]\n",
					admitted[0], admitted[1], admitted[2], admitted[3])

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
