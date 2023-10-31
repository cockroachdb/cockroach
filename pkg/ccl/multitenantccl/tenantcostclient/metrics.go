package tenantcostclient

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaCurrentBlocked = metric.Metadata{
		Name:        "tenant.client.current_blocked",
		Help:        "Number of requests currently blocked by the rate limiter",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

// metrics manage the metrics used by the tenant cost client.
type metrics struct {
	CurrentBlocked *metric.Gauge
}

// Init initializes the tenant cost client metrics.
func (m *metrics) Init() {
	m.CurrentBlocked = metric.NewGauge(metaCurrentBlocked)
}
