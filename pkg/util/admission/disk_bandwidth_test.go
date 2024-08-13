// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "disk_bandwidth_limiter"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				dbl = newDiskBandwidthLimiter()
				return dblToString()

			case "compute":
				var readBandwidth, writeBandwidth, provisionedBandwidth int
				d.ScanArgs(t, "read-bw", &readBandwidth)
				d.ScanArgs(t, "write-bw", &writeBandwidth)
				d.ScanArgs(t, "provisioned-bw", &provisionedBandwidth)
				diskLoad := intervalDiskLoadInfo{
					readBandwidth:           int64(readBandwidth),
					writeBandwidth:          int64(writeBandwidth),
					provisionedBandwidth:    int64(provisionedBandwidth),
					elasticBandwidthMaxUtil: 0.9,
				}
				var regularTokensUsed, elasticTokensUsed int64
				var regularTokensRequested, elasticTokensRequested int64
				d.ScanArgs(t, "regular-tokens-used", &regularTokensUsed)
				d.ScanArgs(t, "elastic-tokens-used", &elasticTokensUsed)
				d.ScanArgs(t, "regular-tokens-requested", &regularTokensRequested)
				d.ScanArgs(t, "elastic-tokens-requested", &elasticTokensRequested)
				utilInfo := intervalUtilInfo{
					actualTokensUsed: [admissionpb.NumWorkClasses]diskTokens{
						{writeBWTokens: regularTokensUsed}, // regular
						{writeBWTokens: elasticTokensUsed}, // elastic
					},
					requestedTokens: [admissionpb.NumWorkClasses]diskTokens{
						{writeBWTokens: regularTokensRequested}, // regular
						{writeBWTokens: elasticTokensRequested}, // elastic
					},
				}
				dbl.computeElasticTokens(diskLoad, utilInfo)
				return dblToString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
