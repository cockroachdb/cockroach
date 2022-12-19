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

	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// StoreMetricsTracker tracks metrics for each store individually.
type StoreMetricsTracker struct {
	writers []*csv.Writer
}

// NewStoreMetricsTracker returns a StoreMetricsTracker that prints metrics
// about each store.
func NewStoreMetricsTracker(writers ...io.Writer) *StoreMetricsTracker {
	m := &StoreMetricsTracker{}

	for _, w := range writers {
		m.writers = append(m.writers, csv.NewWriter(w))
	}

	headline := []string{
		// The rest of the data is cumulative, up to this tick.
		"tick",
		// The store ID corresponding to the metrics in this record.
		"store",
		// the queries per second.
		"qps",
		// The read/writes for keys and bytes.
		"write", "write_b", "read", "read_b",
		// The replica and lease count.
		"replicas", "leases",
		// The churn from the store.
		"lease_moves", "replica_moves", "replica_b_rcvd", "replica_b_sent", "range_splits",
	}
	_ = m.write(headline)
	return m
}

func (m *StoreMetricsTracker) write(records ...[]string) error {
	for _, w := range m.writers {
		for _, record := range records {
			if err := w.Write(record); err != nil {
				return err
			}
			w.Flush()
		}
	}
	return nil
}

// Listen implements the StoreMetricsListener interface.
func (m *StoreMetricsTracker) Listen(ctx context.Context, sms []StoreMetrics) {
	records := [][]string{}
	for _, sm := range sms {
		record := make([]string, 0, 14)
		record = append(record, sm.Tick.String())
		record = append(record, fmt.Sprintf("%d", sm.StoreID))
		record = append(record, fmt.Sprintf("%d", sm.QPS))
		record = append(record, fmt.Sprintf("%d", sm.WriteKeys))
		record = append(record, fmt.Sprintf("%d", sm.WriteBytes))
		record = append(record, fmt.Sprintf("%d", sm.ReadKeys))
		record = append(record, fmt.Sprintf("%d", sm.ReadBytes))
		record = append(record, fmt.Sprintf("%d", sm.Replicas))
		record = append(record, fmt.Sprintf("%d", sm.Leases))
		record = append(record, fmt.Sprintf("%d", sm.LeaseTransfers))
		record = append(record, fmt.Sprintf("%d", sm.Rebalances))
		record = append(record, fmt.Sprintf("%d", sm.RebalanceRcvdBytes))
		record = append(record, fmt.Sprintf("%d", sm.RebalanceSentBytes))
		record = append(record, fmt.Sprintf("%d", sm.RangeSplits))
		records = append(records, record)
	}

	if err := m.write(records...); err != nil {
		log.Errorf(ctx, "Error writing store metrics %s", err.Error())
	}
}
