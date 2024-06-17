package tenantcostserver

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/stretchr/testify/require"
)

func TestMetricRates(t *testing.T) {
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
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 15, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 16, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
		},
		{
			name: "update same period",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 16, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 15, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 30},
			},
		},
		{
			name: "skip to next period",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 30},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 10},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 23, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 16, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 200},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
		},
		{
			name: "skip two periods",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 12, 0, 48, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 12, 0, 23, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100},
			consumptionPeriod: 10 * time.Second,
			expected:          metricRates{next: kvpb.TenantConsumptionRates{WriteBatchRate: 10}},
		},
		{
			name: "start on 10s boundary and skip ahead 10s",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 20, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 10, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 150},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 15},
			},
		},
		{
			name: "longer delta interval",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 15, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 10, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 200},
			consumptionPeriod: 40 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 15},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 25},
			},
		},
		{
			name: "zero usage",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 21, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 15, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 0},
			consumptionPeriod: 10 * time.Second,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 20},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 0},
			},
		},
		{
			name: "zero interval",
			initial: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
			},
			now:               time.Date(2023, 5, 27, 13, 0, 25, 0, time.UTC),
			last:              time.Date(2023, 5, 27, 13, 0, 21, 0, time.UTC),
			consumption:       kvpb.TenantConsumption{WriteBatches: 100},
			consumptionPeriod: 0,
			expected: metricRates{
				current: kvpb.TenantConsumptionRates{WriteBatchRate: 10},
				next:    kvpb.TenantConsumptionRates{WriteBatchRate: 20},
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
