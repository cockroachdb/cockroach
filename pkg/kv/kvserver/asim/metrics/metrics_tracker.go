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
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
)

// MetricsTracker gathers metrics and prints those to stdout.
type MetricsTracker struct {
	writers []*csv.Writer
}

// NewMetricsTracker returns a MetricsTracker object that prints tick metrics to
// Stdout, in a CSV format.
func NewMetricsTracker(writers ...io.Writer) *MetricsTracker {
	m := &MetricsTracker{}

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

func (m *MetricsTracker) write(record []string) error {
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

// Tick collects cluster information and prints it.
func (m *MetricsTracker) Tick(tick time.Time, state state.State) error {
	usage := state.ClusterUsageInfo()
	var (
		totalWriteKeys  int64
		totalWriteBytes int64
		totalReadKeys   int64
		totalReadBytes  int64
		maxWriteKeys    int64
		maxWriteBytes   int64
		maxReadKeys     int64
		maxReadBytes    int64
	)
	for _, u := range usage.StoreUsage {
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
	record = append(record, fmt.Sprintf("%d", state.RangeCount()))
	record = append(record, fmt.Sprintf("%d", totalWriteKeys))
	record = append(record, fmt.Sprintf("%d", totalWriteBytes))
	record = append(record, fmt.Sprintf("%d", totalReadKeys))
	record = append(record, fmt.Sprintf("%d", totalReadBytes))
	record = append(record, fmt.Sprintf("%d", maxWriteKeys))
	record = append(record, fmt.Sprintf("%d", maxWriteBytes))
	record = append(record, fmt.Sprintf("%d", maxReadKeys))
	record = append(record, fmt.Sprintf("%d", maxReadBytes))
	record = append(record, fmt.Sprintf("%d", usage.LeaseTransfers))
	record = append(record, fmt.Sprintf("%d", usage.Rebalances))
	record = append(record, fmt.Sprintf("%d", usage.BytesRebalanced))

	if err := m.write(record); err != nil {
		return err
	}
	return nil
}
