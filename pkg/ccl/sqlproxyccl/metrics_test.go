// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMetricsUpdateForError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
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

		{codeProxyRefusedConnection, []*metric.Counter{m.RefusedConnCount}},

		{codeParamsRoutingFailed, []*metric.Counter{m.RoutingErrCount}},
		{codeUnavailable, []*metric.Counter{m.RoutingErrCount}},

		{codeBackendDialFailed, []*metric.Counter{m.BackendDownCount}},

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
