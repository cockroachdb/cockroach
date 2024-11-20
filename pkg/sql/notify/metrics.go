package notify

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaNumChannels = metric.Metadata{
		Name:        "notifications.channels",
		Help:        "Number of notification channels",
		Measurement: "Channels",
		Unit:        metric.Unit_COUNT,
	}
	metaDroppedNotifications = metric.Metadata{
		Name:        "notifications.dropped",
		Help:        "Number of notifications dropped",
		Measurement: "Notifications",
		Unit:        metric.Unit_COUNT,
	}
	metaRangefeedRestarts = metric.Metadata{
		Name:        "notifications.rangefeed_restarts",
		Help:        "Number of rangefeed restarts",
		Measurement: "Restarts",
		Unit:        metric.Unit_COUNT,
	}
)

type Metrics struct {
	NumChannels       *metric.Gauge
	Dropped           *metric.Counter
	RangefeedRestarts *metric.Counter
}

func NewMetrics(histogramWindow time.Duration) *Metrics {
	return &Metrics{
		NumChannels:       metric.NewGauge(metaNumChannels),
		Dropped:           metric.NewCounter(metaDroppedNotifications),
		RangefeedRestarts: metric.NewCounter(metaRangefeedRestarts),
	}
}

func (m *Metrics) MetricStruct() {}

var _ metric.Struct = &Metrics{}
