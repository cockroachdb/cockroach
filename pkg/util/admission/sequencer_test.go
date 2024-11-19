// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"testing"
	"time"

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

	var seq *sequencer
	var lastSeqNum int64
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "sequencer"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				seq = &sequencer{}
				return ""

			case "sequence":
				var arg, movement string

				// Parse create-time=<duration>.
				d.ScanArgs(t, "create-time", &arg)
				dur, err := time.ParseDuration(arg)
				require.NoError(t, err)

				// Parse log-position=<int>/<int>.
				logPosition := parseLogPosition(t, d)
				_ = logPosition // unused
				sequenceNum := seq.sequence(tzero.Add(dur).UnixNano())
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
