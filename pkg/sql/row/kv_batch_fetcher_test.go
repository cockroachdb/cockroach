// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestKVBatchMetricsCPUTime verifies the guard branches and accumulation
// behavior of GetKVCPUTime and GetLocalKVCPUTime on kvBatchMetrics.
func TestKVBatchMetricsCPUTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("accumulation", func(t *testing.T) {
		var kvPairsRead, batchRequestsIssued, kvCPUTime, localKVCPUTime int64
		var m kvBatchMetrics
		m.init(&kvPairsRead, &batchRequestsIssued, &kvCPUTime, &localKVCPUTime)

		atomic.StoreInt64(&kvCPUTime, 100)
		atomic.StoreInt64(&localKVCPUTime, 42)
		require.Equal(t, int64(100), m.GetKVCPUTime())
		require.Equal(t, int64(42), m.GetLocalKVCPUTime())
	})

	t.Run("nil pointer", func(t *testing.T) {
		var kvPairsRead, batchRequestsIssued int64
		var m kvBatchMetrics
		m.init(
			&kvPairsRead, &batchRequestsIssued,
			nil /* kvCPUTime */, nil, /* localKVCPUTime */
		)

		require.Equal(t, int64(0), m.GetKVCPUTime())
		require.Equal(t, int64(0), m.GetLocalKVCPUTime())
	})

	t.Run("nil receiver", func(t *testing.T) {
		var m *kvBatchMetrics
		require.Equal(t, int64(0), m.GetKVCPUTime())
		require.Equal(t, int64(0), m.GetLocalKVCPUTime())
	})
}
