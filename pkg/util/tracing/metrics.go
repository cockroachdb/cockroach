package tracing

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var metaSpansCreatedCount = metric.Metadata{
	Name:        "trace.spans_created.count",
	Help:        "Count of Span objects created.",
	Measurement: "Spans",
	Unit:        metric.Unit_COUNT,
}

var metaLocalRootSpansCount = metric.Metadata{
	// NB: this isn't a very useful metric - you really want to account for all
	// spans, but it's not cheap to do this with the current structure.
	Name: "trace.registry.size",
	Help: `Number of local root spans present in this Tracer's registry.
A local root span is a span that has no local parent. This means that given two
spans sp1 and sp2, where sp2 was derived from sp1 via
WithParentAndAutoCollection, sp2 will not be accounted for in this metric, even
though it is reachable through s1. Similarly, recordings imported into a Span
created by this Tracer are not represented here.
`,
	Measurement: "Local root spans",
	Unit:        metric.Unit_COUNT,
}

// Metrics contains metrics related to a Tracer.
type Metrics struct {
	SpanCreated       *metric.Counter
	NumLocalRootSpans *metric.Gauge
}

func newMetrics() *Metrics {
	return &Metrics{
		SpanCreated:       metric.NewCounter(metaSpansCreatedCount),
		NumLocalRootSpans: metric.NewGauge(metaLocalRootSpansCount),
	}
}
