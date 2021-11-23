// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aggmetric_test

import (
	"bufio"
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestAggMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	writePrometheusMetrics := func(t *testing.T) string {
		var in bytes.Buffer
		ex := metric.MakePrometheusExporter()
		ex.ScrapeRegistry(r, true /* includeChildMetrics */)
		require.NoError(t, ex.PrintAsText(&in))
		var lines []string
		for sc := bufio.NewScanner(&in); sc.Scan(); {
			if !bytes.HasPrefix(sc.Bytes(), []byte{'#'}) {
				lines = append(lines, sc.Text())
			}
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}

	c := aggmetric.NewCounter(metric.Metadata{
		Name: "foo_counter",
	}, "tenant_id")
	r.AddMetric(c)

	g := aggmetric.NewGauge(metric.Metadata{
		Name: "bar_gauge",
	}, "tenant_id")
	r.AddMetric(g)

	f := aggmetric.NewGaugeFloat64(metric.Metadata{
		Name: "baz_gauge",
	}, "tenant_id")
	r.AddMetric(f)

	h := aggmetric.NewHistogram(metric.Metadata{
		Name: "histo_gram",
	}, base.DefaultHistogramWindowInterval(), 100, 1, "tenant_id")
	r.AddMetric(h)

	tenant2 := roachpb.MakeTenantID(2)
	tenant3 := roachpb.MakeTenantID(3)
	c2 := c.AddChild(tenant2.String())
	c3 := c.AddChild(tenant3.String())
	g2 := g.AddChild(tenant2.String())
	g3 := g.AddChild(tenant3.String())
	f2 := f.AddChild(tenant2.String())
	f3 := f.AddChild(tenant3.String())
	h2 := h.AddChild(tenant2.String())
	h3 := h.AddChild(tenant3.String())

	t.Run("basic", func(t *testing.T) {
		c2.Inc(2)
		c3.Inc(4)
		g2.Inc(2)
		g3.Inc(3)
		g3.Dec(1)
		f2.Update(1.5)
		f3.Update(2.5)
		h2.RecordValue(10)
		h3.RecordValue(90)
		require.Equal(t,
			`bar_gauge 4
bar_gauge{tenant_id="2"} 2
bar_gauge{tenant_id="3"} 2
baz_gauge 4
baz_gauge{tenant_id="2"} 1.5
baz_gauge{tenant_id="3"} 2.5
foo_counter 6
foo_counter{tenant_id="2"} 2
foo_counter{tenant_id="3"} 4
histo_gram_bucket{le="+Inf"} 2
histo_gram_bucket{le="10"} 1
histo_gram_bucket{le="91"} 2
histo_gram_bucket{tenant_id="2",le="+Inf"} 1
histo_gram_bucket{tenant_id="2",le="10"} 1
histo_gram_bucket{tenant_id="3",le="+Inf"} 1
histo_gram_bucket{tenant_id="3",le="91"} 1
histo_gram_count 2
histo_gram_count{tenant_id="2"} 1
histo_gram_count{tenant_id="3"} 1
histo_gram_sum 101
histo_gram_sum{tenant_id="2"} 10
histo_gram_sum{tenant_id="3"} 91`,
			writePrometheusMetrics(t))
	})

	t.Run("destroy", func(t *testing.T) {
		g3.Destroy()
		c2.Destroy()
		f3.Destroy()
		h3.Destroy()
		require.Equal(t,
			`bar_gauge 2
bar_gauge{tenant_id="2"} 2
baz_gauge 1.5
baz_gauge{tenant_id="2"} 1.5
foo_counter 6
foo_counter{tenant_id="3"} 4
histo_gram_bucket{le="+Inf"} 2
histo_gram_bucket{le="10"} 1
histo_gram_bucket{le="91"} 2
histo_gram_bucket{tenant_id="2",le="+Inf"} 1
histo_gram_bucket{tenant_id="2",le="10"} 1
histo_gram_count 2
histo_gram_count{tenant_id="2"} 1
histo_gram_sum 101
histo_gram_sum{tenant_id="2"} 10`,
			writePrometheusMetrics(t))
	})

	t.Run("panic on already exists", func(t *testing.T) {
		// These are the tenants which still exist.
		require.Panics(t, func() {
			c.AddChild(tenant3.String())
		})
		require.Panics(t, func() {
			g.AddChild(tenant2.String())
		})
		require.Panics(t, func() {
			h.AddChild(tenant2.String())
		})
	})

	t.Run("add after destroy", func(t *testing.T) {
		g3 = g.AddChild(tenant3.String())
		c2 = c.AddChild(tenant2.String())
		f3 = f.AddChild(tenant3.String())
		h3 = h.AddChild(tenant3.String())
		require.Equal(t,
			`bar_gauge 2
bar_gauge{tenant_id="2"} 2
bar_gauge{tenant_id="3"} 0
baz_gauge 1.5
baz_gauge{tenant_id="2"} 1.5
baz_gauge{tenant_id="3"} 0
foo_counter 6
foo_counter{tenant_id="2"} 0
foo_counter{tenant_id="3"} 4
histo_gram_bucket{le="+Inf"} 2
histo_gram_bucket{le="10"} 1
histo_gram_bucket{le="91"} 2
histo_gram_bucket{tenant_id="2",le="+Inf"} 1
histo_gram_bucket{tenant_id="2",le="10"} 1
histo_gram_bucket{tenant_id="3",le="+Inf"} 0
histo_gram_count 2
histo_gram_count{tenant_id="2"} 1
histo_gram_count{tenant_id="3"} 0
histo_gram_sum 101
histo_gram_sum{tenant_id="2"} 10
histo_gram_sum{tenant_id="3"} 0`,
			writePrometheusMetrics(t))
	})

	t.Run("panic on label length mismatch", func(t *testing.T) {
		require.Panics(t, func() { c.AddChild() })
		require.Panics(t, func() { g.AddChild("", "") })
	})
}

func TestAggMetricBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := aggmetric.MakeBuilder("tenant_id")
	c := b.Counter(metric.Metadata{Name: "foo_counter"})
	g := b.Gauge(metric.Metadata{Name: "bar_gauge"})
	f := b.GaugeFloat64(metric.Metadata{Name: "baz_gauge"})
	h := b.Histogram(metric.Metadata{Name: "histo_gram"},
		base.DefaultHistogramWindowInterval(), 100, 1)

	for i := 5; i < 10; i++ {
		tenantLabel := roachpb.MakeTenantID(uint64(i)).String()
		c.AddChild(tenantLabel)
		g.AddChild(tenantLabel)
		f.AddChild(tenantLabel)
		h.AddChild(tenantLabel)
	}

	c.Each(nil, func(pm *prometheusgo.Metric) {
		require.Equal(t, 1, len(pm.GetLabel()))
	})
}
