// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package disk

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func compareDeviceIDs(a, b DeviceID) int {
	if v := cmp.Compare(a.major, b.major); v != 0 {
		return v
	}
	return cmp.Compare(a.minor, b.minor)
}

func TestLinux_CollectDiskStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/linux_diskstats", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "parse":
			var disks []*monitoredDisk
			for _, cmdArg := range td.CmdArgs {
				var deviceID DeviceID
				v, err := strconv.ParseUint(cmdArg.Vals[0], 10, 32)
				require.NoError(t, err)
				deviceID.major = uint32(v)
				v, err = strconv.ParseUint(cmdArg.Vals[1], 10, 32)
				require.NoError(t, err)
				deviceID.minor = uint32(v)
				tracer := newMonitorTracer(1000)
				disks = append(disks, &monitoredDisk{deviceID: deviceID, tracer: tracer})
			}
			slices.SortFunc(disks, func(a, b *monitoredDisk) int { return compareDeviceIDs(a.deviceID, b.deviceID) })

			buf.Reset()
			s := &linuxStatsCollector{
				File: vfs.NewMemFile([]byte(td.Input)),
				// Use a small initial buffer size to exercise the buffer
				// resizing logic.
				buf: make([]byte, 16),
			}
			err := s.collect(disks)
			if err != nil {
				return err.Error()
			}
			for i := range disks {
				monitor := Monitor{monitoredDisk: disks[i]}
				stats, err := monitor.CumulativeStats()
				require.NoError(t, err)

				if i > 0 {
					fmt.Fprintln(&buf)
				}
				fmt.Fprintf(&buf, "%s: ", disks[i].deviceID)
				fmt.Fprint(&buf, stats.String())
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
	time.Sleep(time.Second)
}
