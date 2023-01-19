// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ClusterMetricsTracker gathers metrics and prints those to stdout.
type ClusterMetricsTracker struct {
	writers []*csv.Writer
}

// NewClusterMetricsTracker returns a MetricsTracker object that prints tick metrics to
// Stdout, in a CSV format.
func NewClusterMetricsTracker(writers ...io.Writer) *ClusterMetricsTracker {
	m := &ClusterMetricsTracker{}

	for _, w := range writers {
		m.writers = append(m.writers, csv.NewWriter(w))
	}

	headline := []string{
		// The rest of the data is cumulative, up to this tick.
		"tick",
		// The number of ranges in the cluster and the total load.
		"c_ranges", "c_write", "c_write_b", "c_read", "c_read_b",
		// The max value seen on a single store.
		"s_ranges", "s_write", "s_write_b", "s_read", "s_read_b",
		// The churn in the cluster.
		"c_lease_moves", "c_replica_moves", "c_replica_b_moves",
	}
	_ = m.write(headline)
	return m
}

func (m *ClusterMetricsTracker) write(record []string) error {
	for _, w := range m.writers {
		if err := w.Write(record); err != nil {
			return err
		}
		w.Flush()
	}
	return nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Listen implements the StoreMetricsListener interface.
func (m *ClusterMetricsTracker) Listen(ctx context.Context, sms []StoreMetrics) {
	var (
		tick                 time.Time
		totalRangeCount      int64
		totalLeaseTransfers  int64
		totalRebalances      int64
		totalBytesRebalanced int64
		totalWriteKeys       int64
		totalWriteBytes      int64
		totalReadKeys        int64
		totalReadBytes       int64
		maxWriteKeys         int64
		maxWriteBytes        int64
		maxReadKeys          int64
		maxReadBytes         int64
	)

	for _, u := range sms {
		tick = u.Tick
		totalRangeCount += u.Leases
		totalLeaseTransfers += u.LeaseTransfers
		totalRebalances += u.Rebalances
		totalBytesRebalanced += u.RebalanceRcvdBytes
		totalWriteKeys += u.WriteKeys
		totalWriteBytes += u.WriteBytes
		totalReadKeys += u.ReadKeys
		totalReadBytes += u.ReadBytes
		maxWriteKeys = max(maxWriteKeys, u.WriteKeys)
		maxWriteBytes = max(maxWriteBytes, u.WriteBytes)
		maxReadKeys = max(maxReadKeys, u.ReadKeys)
		maxReadBytes = max(maxReadBytes, u.ReadBytes)
	}

	record := make([]string, 0, 10)
	record = append(record, tick.String())
	record = append(record, fmt.Sprintf("%d", totalRangeCount))
	record = append(record, fmt.Sprintf("%d", totalWriteKeys))
	record = append(record, fmt.Sprintf("%d", totalWriteBytes))
	record = append(record, fmt.Sprintf("%d", totalReadKeys))
	record = append(record, fmt.Sprintf("%d", totalReadBytes))
	record = append(record, fmt.Sprintf("%d", maxWriteKeys))
	record = append(record, fmt.Sprintf("%d", maxWriteBytes))
	record = append(record, fmt.Sprintf("%d", maxReadKeys))
	record = append(record, fmt.Sprintf("%d", maxReadBytes))
	record = append(record, fmt.Sprintf("%d", totalLeaseTransfers))
	record = append(record, fmt.Sprintf("%d", totalRebalances))
	record = append(record, fmt.Sprintf("%d", totalBytesRebalanced))

	if err := m.write(record); err != nil {
		log.Errorf(ctx, "Error writing cluster metrics %s", err.Error())
	}
}
