// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	m := newMetrics(roachpb.Locality{})
	// Verify that each metric doesn't have any children at first. Verify the
	// number of metric fields, as a sanity check (to be modified if fields are
	// added/deleted).
	require.Equal(t, expectedCount, verifyAllFields(m, 0))
	// Verify that a new peer's metrics all get registered.
	k := peerKey{NodeID: 5, TargetAddr: "192.168.0.1:1234", Class: DefaultClass}
	pm, lm := m.acquire(k, roachpb.Locality{})
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	// Verify that the metrics are not released even if the peer is removed.
	pm.release()
	lm.release()
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	require.Equal(t, 0, *pm.registeredCount)
	// Acquire the same peer twice and release twice.
	pm = m.acquire(k)
	pm = m.acquire(k)
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	require.Equal(t, 2, *pm.registeredCount)
	// Verify that the metrics are not released even if the peer is removed.
	pm.release()
	pm.release()
	require.Equal(t, expectedCount, verifyAllFields(m, 1))
	require.Equal(t, 0, *pm.registeredCount)
}
