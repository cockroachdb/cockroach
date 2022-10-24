// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/dustin/go-humanize"
)

// hardwareLimiter is used to limit the performance of hardware in different tests.
type hardwareLimiter struct {
	context context.Context
	test    test.Test
	cluster cluster.Cluster
}

// getWriteIO returns the median rate of IO from the provided list. It first
// runs a command to determine the PID of the CRDB process and then monitors how
// much activity it does over the next 30 seconds. Finally, it will take the max
// throughput across all machines in the list of nodes passed in. Typically this
// is used in conjunction with throttleWriteIO to limit the max IO relative to
// the current usage.
func (h *hardwareLimiter) getWriteIO(node option.NodeListOption) uint64 {
	if h.cluster.IsLocal() {
		h.test.L().Printf("Can not get writeIO from a local cluster")
		return 0
	}

	// Measure the amount of write throughput over a 30 second window.
	cmd := `PID=$(systemctl show --property MainPID --value cockroach | xargs pgrep -P)
START=$(grep ^write_bytes /proc/$PID/io  | awk '{print $2}')
sleep 30
END=$(grep ^write_bytes /proc/$PID/io  | awk '{print $2}')
echo \( $END - $START \) / 30 | bc`

	writeThroughput, err := h.cluster.RunWithDetails(h.context, h.test.L(), node, cmd)
	if err != nil {
		h.test.Fatal(err)
	}
	maxThroughput := uint64(0)
	for i, wt := range writeThroughput {
		storeThroughput, err := strconv.Atoi(strings.TrimSpace(wt.Stdout))
		if err != nil {
			h.test.L().Printf("stdout:\n%v\n", wt.Stdout)
			h.test.Fatal(err)
		}
		h.test.Status(fmt.Sprintf("store %d throughput = %d", i, storeThroughput))
		if uint64(storeThroughput) > maxThroughput {
			maxThroughput = uint64(storeThroughput)
		}
	}
	return maxThroughput
}

// throttleWriteIO will limit the write io performance of a node to the provided
// rate using Linux capabilities. Rate is measured in bytes.
func (h *hardwareLimiter) throttleWriteIO(node option.NodeListOption, byteRate uint64) {
	if h.cluster.IsLocal() {
		h.test.L().Printf("Can not get throttle writeIO for a local cluster")
		return
	}

	h.test.Status(fmt.Sprintf("throttling IO to %s", humanize.Bytes(byteRate)))

	cmd := fmt.Sprintf("'IOWriteBandwidthMax={store-dir} %d'", byteRate)
	h.cluster.Run(h.context, node, "sudo", "systemctl", "set-property", "--runtime", "cockroach", cmd)
}

// throttleCPU will limit the rate of CPU to the given fraction. 1.0 means no
// throttling. The throttle will be based on the total number of CPUS on the
// system.
func (h *hardwareLimiter) throttleCPU(node option.NodeListOption, fraction float64) {
	if h.cluster.IsLocal() {
		h.test.L().Printf("Can not get throttle CPU for a local cluster")
		return
	}
	cpus := h.cluster.Spec().CPUs
	h.test.Status(fmt.Sprintf("throttling %d CPU to %f", cpus, fraction))

	quota := int64(fraction * float64(cpus) * 100)

	cmd := fmt.Sprintf("'CPUQuota=%d%%'", quota)
	h.cluster.Run(h.context, node, "sudo", "systemctl", "set-property", "--runtime", "cockroach", cmd)
}

// TODO(abaptist): Throttle network using trickle.
