package metric

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/graphite"
	prometheusgo "github.com/prometheus/client_model/go"
)

// implements Gatherer
type GraphiteExporter struct {
	pm  *PrometheusExporter
	ctx context.Context
	b   *graphite.Bridge
}

func MakeGraphiteExporter(pm *PrometheusExporter) GraphiteExporter {
	return GraphiteExporter{pm: pm}
}

func (ge *GraphiteExporter) Init(ctx context.Context) error {
	ge.ctx = ctx
	h, err := os.Hostname()
	if err != nil {
		return err
	}
	if b, err := graphite.NewBridge(&graphite.Config{
		// Questions: what is the best way to set this parameters?
		// should I supply these as cmd-line arguments?
		// - URL
		// - Timeout
		// Similarly, based on this, I can enable/disable the
		// graphite exporter.
		URL:           "localhost:2003",
		Gatherer:      ge,
		Prefix:        h,
		Timeout:       10 * time.Second,
		ErrorHandling: graphite.AbortOnError,
		Logger:        ge,
	}); err != nil {
		return err
	} else {
		ge.b = b
	}
	log.Infof(ctx, "Init")
	return nil
}

func (ge *GraphiteExporter) Gather() ([]*prometheusgo.MetricFamily, error) {
	num_metrics := 0
	for _, family := range ge.pm.families {
		num_metrics += len(family.Metric)
	}
	v := make([]*prometheusgo.MetricFamily, len(ge.pm.families))
	i := 0
	for _, family := range ge.pm.families {
		v[i] = family
		i++
	}
	return v, nil
}

// Verify GraphiteExporter implements Gatherer interface
var _ prometheus.Gatherer = (*GraphiteExporter)(nil)

func (ge *GraphiteExporter) Println(v ...interface{}) {
	log.Info(ge.ctx, v...)
}

func (ge *GraphiteExporter) Push(ctx context.Context) error {
	ge.ctx = ctx
	log.Infof(ctx, "Push - start\n")
	err := ge.b.Push()
	log.Info(ctx, "Push - now clear")
	// Clear metrics for reuse.
	for _, family := range ge.pm.families {
		family.Metric = []*prometheusgo.Metric{}
	}
	return err
}
