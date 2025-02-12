// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
)

func TestDiskBandwidthLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dbl *diskBandwidthLimiter
	dblToString := func() string {
		return string(redact.Sprint(dbl))
	}

	var intProvisionedBytes, intProvisionedIOPS int

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "disk_bandwidth_limiter"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				dbl = newDiskBandwidthLimiter()
				d.ScanArgs(t, "int-provisioned-bytes", &intProvisionedBytes)
				d.ScanArgs(t, "int-provisioned-iops", &intProvisionedIOPS)
				return dblToString()

			case "compute":
				var readBytes, readCount, writeBytes, writeCount int
				d.ScanArgs(t, "int-read-bytes", &readBytes)
				d.ScanArgs(t, "int-read-count", &readCount)
				d.ScanArgs(t, "int-write-bytes", &writeBytes)
				d.ScanArgs(t, "int-write-count", &writeCount)
				diskLoad := intervalDiskLoadInfo{
					intReadBytes:            int64(readBytes),
					intWriteBytes:           int64(writeBytes),
					intReadCount:            int64(readCount),
					intWriteCount:           int64(writeCount),
					intProvisionedDiskBytes: int64(intProvisionedBytes),
					intProvisionedIOCount:   int64(intProvisionedIOPS),
					elasticBandwidthMaxUtil: 0.9,
				}
				var regularTokensUsed, snapshotTokensUsed, elasticTokensUsed int64
				var regularIOPSTokensUsed, snapshotIOPSTokensUsed, elasticIOPSTokensUsed int64
				d.ScanArgs(t, "regular-tokens-used", &regularTokensUsed)
				d.ScanArgs(t, "snapshot-tokens-used", &snapshotTokensUsed)
				d.ScanArgs(t, "elastic-tokens-used", &elasticTokensUsed)
				d.ScanArgs(t, "regular-iops-tokens-used", &regularIOPSTokensUsed)
				d.ScanArgs(t, "snapshot-iops-tokens-used", &snapshotIOPSTokensUsed)
				d.ScanArgs(t, "elastic-iops-tokens-used", &elasticIOPSTokensUsed)
				usedTokens := [admissionpb.NumStoreWorkTypes]diskTokens{
					{writeByteTokens: regularTokensUsed, writeIOPSTokens: regularIOPSTokensUsed},   // regular
					{writeByteTokens: snapshotTokensUsed, writeIOPSTokens: snapshotIOPSTokensUsed}, // snapshot
					{writeByteTokens: elasticTokensUsed, writeIOPSTokens: elasticIOPSTokensUsed},   // elastic
				}

				dbl.computeElasticTokens(diskLoad, usedTokens)
				return dblToString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
