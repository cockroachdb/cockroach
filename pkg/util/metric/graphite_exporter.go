package metric

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/graphite"
	prometheusgo "github.com/prometheus/client_model/go"
)

var (
	GraphiteURL = settings.RegisterStringSetting(
		"external.graphite.url",
		"if nonempty, push server metrics to the Graphite or Carbon server at the specified host:port",
		"",
	)

	GraphitePeriod = settings.RegisterDurationSetting(
		"external.graphite.frequency",
		"the frequency with which metrics are pushed to Graphite (if enabled)",
		10*time.Second,
	)
)

// implements Gatherer
type GraphiteExporter struct {
	pm  *PrometheusExporter
	ctx context.Context
	b   *graphite.Bridge
	st  *cluster.Settings
}

func MakeGraphiteExporter(pm *PrometheusExporter, st *cluster.Settings) GraphiteExporter {
	return GraphiteExporter{pm: pm, st: st}
}

func (ge *GraphiteExporter) Init(ctx context.Context) error {
	url := GraphiteURL.Get(&ge.st.SV)
	if url == "" {
		return nil
	}
	ge.ctx = ctx
	h, err := os.Hostname()
	if err != nil {
		return err
	}
	if b, err := graphite.NewBridge(&graphite.Config{
		URL:           url, // TODO (neeral) will remove this comment  "localhost:2003"
		Gatherer:      ge,
		Prefix:        fmt.Sprintf("%s.cockroach", h),
		Timeout:       10 * time.Second,
		ErrorHandling: graphite.AbortOnError,
		Logger:        ge,
	}); err != nil {
		return err
	} else {
		ge.b = b
	}
	return nil
}

func (ge *GraphiteExporter) Gather() ([]*prometheusgo.MetricFamily, error) {
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
	err := ge.b.Push()
	// Clear metrics for reuse.
	for _, family := range ge.pm.families {
		family.Metric = []*prometheusgo.Metric{}
	}
	return err
}
