// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowsequencer

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestSequencer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var sequencer *Sequencer
	var lastSeqNum int64
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "sequencer"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				sequencer = New()
				return ""

			case "sequence":
				var arg, movement string

				// Parse create-time=<duration>.
				d.ScanArgs(t, "create-time", &arg)
				dur, err := time.ParseDuration(arg)
				require.NoError(t, err)

				// Parse log-position=<int>/<int>.
				logPosition := parseLogPosition(t, d)
				_ = logPosition
				sequenceNum := sequencer.Sequence(tzero.Add(dur)).UnixNano()
				if lastSeqNum < sequenceNum {
					movement = " (advanced)"
				}
				lastSeqNum = sequenceNum
				return fmt.Sprintf("seq=%d ≈%s%s",
					sequenceNum,
					timeutil.FromUnixNanos(sequenceNum).Sub(tzero),
					movement,
				)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		},
	)
}

// tzero represents the t=0, the earliest possible time. All other
// create-time=<duration> is relative to this time.
var tzero = timeutil.Unix(0, 0)

func parseLogPosition(t *testing.T, d *datadriven.TestData) kvflowcontrolpb.RaftLogPosition {
	// Parse log-position=<int>/<int>.
	var arg string
	d.ScanArgs(t, "log-position", &arg)
	inner := strings.Split(arg, "/")
	require.Len(t, inner, 2)
	term, err := strconv.Atoi(inner[0])
	require.NoError(t, err)
	index, err := strconv.Atoi(inner[1])
	require.NoError(t, err)
	return kvflowcontrolpb.RaftLogPosition{
		Term:  uint64(term),
		Index: uint64(index),
	}
}
