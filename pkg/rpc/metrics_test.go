// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// TestMetricsRelease verifies that peerMetrics.release() removes tracking for
// *all* the metrics from their parent aggregate metric.
func TestMetricsRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// All metrics in aggmetric package satisfy this interface. The `Each` method
	// can be used to scan all child metrics of the aggregated metric. We use it
	// for counting children.
	type eacher interface {
		Each([]*io_prometheus_client.LabelPair, func(metric *io_prometheus_client.Metric))
	}
	countChildren := func(metric eacher) (count int) {
		metric.Each(nil /*labels*/, func(*io_prometheus_client.Metric) {
			count++
		})
		return count
	}

	verifyAllFields := func(m *Metrics, wantChildren int) (metricFields int) {
		r := reflect.ValueOf(m).Elem()
		for i, n := 0, r.NumField(); i < n; i++ {
			if !r.Field(i).CanInterface() {
				continue
			}
			field := r.Field(i).Interface()
			metric, ok := field.(eacher)
			if !ok { // skip all non-metric fields
				continue
			}
			metricFields++
			require.Equal(t, wantChildren, countChildren(metric), r.Type().Field(i).Name)
		}
		return metricFields
	}

	const expectedCount = 11
	k1 := peerKey{NodeID: 5, TargetAddr: "192.168.0.1:1234", Class: DefaultClass}
	k2 := peerKey{NodeID: 6, TargetAddr: "192.168.0.1:1234", Class: DefaultClass}
	l1 := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}}
	l2 := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west"}}}
	m := newMetrics(l1)
	// Verify that each metric doesn't have any children at first. Verify the
	// number of metric fields, as a sanity check (to be modified if fields are
	// added/deleted).
	require.Equal(t, expectedCount, verifyAllFields(m, 0))
	// Verify that a new peer's metrics all get registered.
	pm, lm := m.acquire(k1, l1)
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	// Acquire the same peer. The count remains at 1.
	pm2, lm2 := m.acquire(k1, l1)
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	require.Equal(t, pm, pm2)
	require.Equal(t, lm, lm2)

	// Acquire a different peer but the same locality.
	pm3, lm3 := m.acquire(k2, l1)
	require.NotEqual(t, pm, pm3)
	require.Equal(t, lm, lm3)

	// Acquire a different locality but the same peer.
	pm4, lm4 := m.acquire(k1, l2)
	require.Equal(t, pm, pm4)
	require.NotEqual(t, lm, lm4)

	// We added one extra peer and one extra locality, verify counts.
	require.Equal(t, expectedCount, verifyAllFields(m, 2))
}
