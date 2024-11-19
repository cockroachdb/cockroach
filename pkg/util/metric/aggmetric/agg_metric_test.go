// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"bufio"
	"bytes"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestAggMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := metric.NewRegistry()
	writePrometheusMetrics := func(t *testing.T) string {
		var in bytes.Buffer
		ex := metric.MakePrometheusExporter()
		scrape := func(ex *metric.PrometheusExporter) {
			ex.ScrapeRegistry(r, true /* includeChildMetrics */)
		}
		require.NoError(t, ex.ScrapeAndPrintAsText(&in, expfmt.FmtText, scrape))
		var lines []string
		for sc := bufio.NewScanner(&in); sc.Scan(); {
			if !bytes.HasPrefix(sc.Bytes(), []byte{'#'}) {
				lines = append(lines, sc.Text())
			}
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}

	c := NewCounter(metric.Metadata{
		Name: "foo_counter",
	}, "tenant_id")
	r.AddMetric(c)

	d := NewCounterFloat64(metric.Metadata{
		Name: "fob_counter",
	}, "tenant_id")
	r.AddMetric(d)

	g := NewGauge(metric.Metadata{
		Name: "bar_gauge",
	}, "tenant_id")
	r.AddMetric(g)

	f := NewGaugeFloat64(metric.Metadata{
		Name: "baz_gauge",
	}, "tenant_id")
	r.AddMetric(f)
	h := NewHistogram(metric.HistogramOptions{
		Metadata: metric.Metadata{
			Name: "histo_gram",
		},
		Duration:     base.DefaultHistogramWindowInterval(),
		MaxVal:       100,
		SigFigs:      1,
		BucketConfig: metric.Count1KBuckets,
	}, "tenant_id")
	r.AddMetric(h)

	tenant2 := roachpb.MustMakeTenantID(2)
	tenant3 := roachpb.MustMakeTenantID(3)
	c2 := c.AddChild(tenant2.String())
	c3 := c.AddChild(tenant3.String())
	d2 := d.AddChild(tenant2.String())
	d3 := d.AddChild(tenant3.String())
	g2 := g.AddChild(tenant2.String())
	g3 := g.AddChild(tenant3.String())
	f2 := f.AddChild(tenant2.String())
	f3 := f.AddChild(tenant3.String())
	h2 := h.AddChild(tenant2.String())
	h3 := h.AddChild(tenant3.String())

	t.Run("basic", func(t *testing.T) {
		c2.Inc(2)
		c3.Inc(4)
		d2.Inc(123456.5)
		d3.Inc(789089.5)
		g2.Inc(2)
		g3.Inc(3)
		g3.Dec(1)
		f2.Update(1.5)
		f3.Update(2.5)
		h2.RecordValue(10)
		h3.RecordValue(90)
		testFile := "basic.txt"
		if metric.HdrEnabled() {
			testFile = "basic_hdr.txt"
		}
		echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
	})

	t.Run("destroy", func(t *testing.T) {
		g3.Unlink()
		c2.Unlink()
		d3.Unlink()
		f3.Unlink()
		h3.Unlink()
		testFile := "destroy.txt"
		if metric.HdrEnabled() {
			testFile = "destroy_hdr.txt"
		}
		echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
	})

	t.Run("panic on already exists", func(t *testing.T) {
		// These are the tenants which still exist.
		require.Panics(t, func() {
			c.AddChild(tenant3.String())
		})
		require.Panics(t, func() {
			d.AddChild(tenant2.String())
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
		d3 = d.AddChild(tenant3.String())
		f3 = f.AddChild(tenant3.String())
		h3 = h.AddChild(tenant3.String())
		testFile := "add_after_destroy.txt"
		if metric.HdrEnabled() {
			testFile = "add_after_destroy_hdr.txt"
		}
		echotest.Require(t, writePrometheusMetrics(t), datapathutils.TestDataPath(t, testFile))
	})

	t.Run("panic on label length mismatch", func(t *testing.T) {
		require.Panics(t, func() { c.AddChild() })
		require.Panics(t, func() { d.AddChild() })
		require.Panics(t, func() { g.AddChild("", "") })
	})
}

type Eacher interface {
	Each(
		labels []*prometheusgo.LabelPair, f func(metric *prometheusgo.Metric),
	)
}

func TestAggMetricBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := MakeBuilder("tenant_id")
	c := b.Counter(metric.Metadata{Name: "foo_counter"})
	d := b.CounterFloat64(metric.Metadata{Name: "fob_counter"})
	g := b.Gauge(metric.Metadata{Name: "bar_gauge"})
	f := b.GaugeFloat64(metric.Metadata{Name: "baz_gauge"})
	h := b.Histogram(metric.HistogramOptions{
		Metadata:     metric.Metadata{Name: "histo_gram"},
		Duration:     base.DefaultHistogramWindowInterval(),
		MaxVal:       100,
		SigFigs:      1,
		BucketConfig: metric.Count1KBuckets,
	})

	for i := 5; i < 10; i++ {
		tenantLabel := roachpb.MustMakeTenantID(uint64(i)).String()
		c.AddChild(tenantLabel)
		d.AddChild(tenantLabel)
		g.AddChild(tenantLabel)
		f.AddChild(tenantLabel)
		h.AddChild(tenantLabel)
	}

	for _, m := range [5]Eacher{
		c, d, g, f, h,
	} {
		numChildren := 0
		m.Each(nil, func(pm *prometheusgo.Metric) {
			require.Equal(t, 1, len(pm.GetLabel()))
			numChildren += 1
		})
		require.Equal(t, 5, numChildren)
	}
}

func TestAggHistogramRotate(t *testing.T) {
	now := time.UnixMicro(1699565116)
	defer TestingSetNow(func() time.Time {
		return now
	})()

	b := MakeBuilder("test")
	h := b.Histogram(metric.HistogramOptions{
		Mode:     metric.HistogramModePrometheus,
		Metadata: metric.Metadata{Name: "hist"},
		Duration: 10 * time.Second,
	})
	child1 := h.AddChild("child-1")
	child2 := h.AddChild("child-2")
	for i := 0; i < 4; i++ {
		// Windowed histogram is initially empty.
		h.Inspect(func(interface{}) {}) // triggers ticking
		// Verify new histogram windows have a 0 sum.
		_, parentSum := h.WindowedSnapshot().Total()
		_, child1Sum := child1.h.WindowedSnapshot().Total()
		_, child2Sum := child2.h.WindowedSnapshot().Total()
		require.Zero(t, parentSum)
		require.Zero(t, child1Sum)
		require.Zero(t, child2Sum)
		// But cumulative histogram has history (if i > 0).
		parentCount, _ := h.CumulativeSnapshot().Total()
		child1Count, _ := child1.h.CumulativeSnapshot().Total()
		child2Count, _ := child2.h.CumulativeSnapshot().Total()
		require.EqualValues(t, i, child1Count)
		require.EqualValues(t, i, child2Count)
		// The children aggregate into the parent.
		require.EqualValues(t, child1Count+child2Count, parentCount)
		// Add a measurement and verify it's there.
		{
			child1RecVal := int64(12345)
			child2RecVal := int64(56789)
			child1.RecordValue(child1RecVal)
			child2.RecordValue(child2RecVal)
			child1SumExp := float64(child1RecVal) + child1Sum
			child2SumExp := float64(child2RecVal) + child2Sum
			// The children should aggregate to the parent.
			parentSumExp := float64(child1RecVal) + float64(child2RecVal) + parentSum
			_, parentWindowSum := h.WindowedSnapshot().Total()
			_, child1WindowSum := child1.h.WindowedSnapshot().Total()
			_, child2WindowSum := child2.h.WindowedSnapshot().Total()
			require.Equal(t, parentSumExp, parentWindowSum)
			require.Equal(t, child1SumExp, child1WindowSum)
			require.Equal(t, child2SumExp, child2WindowSum)
		}
		// Tick. This rotates the histogram.
		now = now.Add(time.Duration(i+1) * 10 * time.Second)
		// Go to beginning.
	}
}
