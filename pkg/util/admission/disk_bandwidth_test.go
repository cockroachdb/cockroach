// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
)

func TestDiskLoadWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dlw diskLoadWatcher
	watcherToString := func() string {
		level := dlw.getLoadLevel()
		return fmt.Sprintf("%s\nload-level: %s", redact.Sprint(dlw),
			diskLoadLevelString(level))
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "disk_load_watcher"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				dlw = diskLoadWatcher{}
				return watcherToString()

			case "interval-info":
				var readBandwidth, writeBandwidth, provisionedBandwidth int
				d.ScanArgs(t, "read-bw", &readBandwidth)
				d.ScanArgs(t, "write-bw", &writeBandwidth)
				d.ScanArgs(t, "provisioned-bw", &provisionedBandwidth)
				dlw.setIntervalInfo(intervalDiskLoadInfo{
					readBandwidth:        int64(readBandwidth),
					writeBandwidth:       int64(writeBandwidth),
					provisionedBandwidth: int64(provisionedBandwidth),
				})
				return watcherToString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestDiskBandwidthLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dbl diskBandwidthLimiter
	dblToString := func() string {
		return string(redact.Sprint(&dbl))
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "disk_bandwidth_limiter"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				dbl = makeDiskBandwidthLimiter()
				return dblToString()

			case "compute":
				var readBandwidth, writeBandwidth, provisionedBandwidth int
				d.ScanArgs(t, "read-bw", &readBandwidth)
				d.ScanArgs(t, "write-bw", &writeBandwidth)
				d.ScanArgs(t, "provisioned-bw", &provisionedBandwidth)
				diskLoad := intervalDiskLoadInfo{
					readBandwidth:        int64(readBandwidth),
					writeBandwidth:       int64(writeBandwidth),
					provisionedBandwidth: int64(provisionedBandwidth),
				}
				var incomingBytes, regularTokensUsed, elasticTokensUsed int
				d.ScanArgs(t, "incoming-bytes", &incomingBytes)
				d.ScanArgs(t, "regular-tokens-used", &regularTokensUsed)
				d.ScanArgs(t, "elastic-tokens-used", &elasticTokensUsed)
				lsmInfo := intervalLSMInfo{
					incomingBytes:     int64(incomingBytes),
					regularTokensUsed: int64(regularTokensUsed),
					elasticTokensUsed: int64(elasticTokensUsed),
				}
				dbl.computeElasticTokens(context.Background(), diskLoad, lsmInfo)
				return dblToString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
