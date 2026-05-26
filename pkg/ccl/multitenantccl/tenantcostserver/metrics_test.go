// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostserver

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMetricRates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name              string
		initial           metricRates
		now               time.Time
		last              time.Time
		consumption       kvpb.TenantConsumption
		consumptionPeriod time.Duration
		expected          metricRates
	}{
		{
			name: "now before update",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 15, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 16, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100, EstimatedCPUSeconds: 100},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
		},
		{
			name: "update same period",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 16, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 15, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100, EstimatedCPUSeconds: 100},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 30, EstimatedCPURate: 50},
			},
		},
		{
			name: "skip to next period",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 30, EstimatedCPURate: 50},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 23, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 16, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 200, EstimatedCPUSeconds: 400},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
		},
		{
			name: "skip two periods",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 48, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 23, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100, EstimatedCPUSeconds: 100},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				next: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 10},
			},
		},
		{
			name: "start on 10s boundary and skip ahead 10s",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 20, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 10, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 150, EstimatedCPUSeconds: 250},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 15, EstimatedCPURate: 25},
			},
		},
		{
			name: "longer delta interval",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 15, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 10, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 200, EstimatedCPUSeconds: 400},
			consumptionPeriod: 40 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 15, EstimatedCPURate: 30},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 25, EstimatedCPURate: 50},
			},
		},
		{
			name: "zero usage",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 21, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 15, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 0, EstimatedCPUSeconds: 0},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 0, EstimatedCPURate: 0},
			},
		},
		{
			name: "zero interval",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 25, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 21, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100, EstimatedCPUSeconds: 200},
			consumptionPeriod: 0,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10, EstimatedCPURate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20, EstimatedCPURate: 40},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			rates := test.initial
			rates.Update(test.now, test.last, &test.consumption, test.consumptionPeriod)
			require.Equal(t, test.expected, rates)
		})
	}
}
