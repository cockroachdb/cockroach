// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterstats

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/stretchr/testify/require"
)

func TestHandlePromWarnings(t *testing.T) {
	const (
		// Sample annotations as emitted by Prometheus 2.53+.
		infoNotCounter   = `PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "sys_host_disk_write_bytes" (1:6)`
		warnGaugeHist    = `PromQL warning: rate() applied to gauge histogram has undefined semantics`
		legacyRemoteRead = `remote read failed: partial response`
	)

	tests := []struct {
		name        string
		warnings    promv1.Warnings
		expectedErr string
	}{
		{
			name:     "no warnings",
			warnings: nil,
		},
		{
			name:     "only info is silenced",
			warnings: promv1.Warnings{infoNotCounter},
		},
		{
			name:        "warning is still fatal",
			warnings:    promv1.Warnings{warnGaugeHist},
			expectedErr: "found warnings querying prometheus: [" + warnGaugeHist + "]",
		},
		{
			name:        "unprefixed annotation is treated as warning",
			warnings:    promv1.Warnings{legacyRemoteRead},
			expectedErr: "found warnings querying prometheus: [" + legacyRemoteRead + "]",
		},
		{
			name:        "info filtered out, warning preserved",
			warnings:    promv1.Warnings{infoNotCounter, warnGaugeHist},
			expectedErr: "found warnings querying prometheus: [" + warnGaugeHist + "]",
		},
	}

	l, err := (&logger.Config{}).NewLogger("")
	require.NoError(t, err)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := handlePromWarnings(context.Background(), l, "q", tc.warnings)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}
