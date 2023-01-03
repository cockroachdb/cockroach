// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMetricsUpdateForError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := makeProxyMetrics()
	type testCase struct {
		code     errorCode
		counters []*metric.Counter
	}
	tests := []testCase{
		{codeClientReadFailed, []*metric.Counter{m.ClientDisconnectCount}},
		{codeClientWriteFailed, []*metric.Counter{m.ClientDisconnectCount}},
		{codeClientDisconnected, []*metric.Counter{m.ClientDisconnectCount}},

		{codeBackendDisconnected, []*metric.Counter{m.BackendDisconnectCount}},
		{codeBackendReadFailed, []*metric.Counter{m.BackendDisconnectCount}},
		{codeBackendWriteFailed, []*metric.Counter{m.BackendDisconnectCount}},

		{codeExpiredClientConnection, []*metric.Counter{m.ExpiredClientConnCount}},

		{codeProxyRefusedConnection, []*metric.Counter{m.RefusedConnCount, m.BackendDownCount}},

		{codeParamsRoutingFailed, []*metric.Counter{m.RoutingErrCount, m.BackendDownCount}},
		{codeUnavailable, []*metric.Counter{m.RoutingErrCount, m.BackendDownCount}},

		{codeBackendDown, []*metric.Counter{m.BackendDownCount}},

		{codeAuthFailed, []*metric.Counter{m.AuthFailedCount}},
	}

	for _, tc := range tests {
		t.Run(tc.code.String(), func(t *testing.T) {
			var before []int64
			for _, counter := range tc.counters {
				before = append(before, counter.Count())
			}
			m.updateForError(withCode(errors.New("test error"), tc.code))
			for i, counter := range tc.counters {
				require.Equal(t, counter.Count(), before[i]+1)
			}
		})
	}
}
