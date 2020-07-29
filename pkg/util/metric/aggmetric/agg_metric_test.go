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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
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
	tenant2 := roachpb.MakeTenantID(2)
	tenant3 := roachpb.MakeTenantID(3)
	c2 := c.AddChild(tenant2.String())
	c3 := c.AddChild(tenant3.String())
	g2 := g.AddChild(tenant2.String())
	g3 := g.AddChild(tenant3.String())

	t.Run("basic", func(t *testing.T) {
		c2.Inc(2)
		c3.Inc(4)
		g2.Inc(2)
		g3.Inc(3)
		g3.Dec(1)
		require.Equal(t,
			`bar_gauge 4
bar_gauge{tenant_id="2"} 2
bar_gauge{tenant_id="3"} 2
foo_counter 6
foo_counter{tenant_id="2"} 2
foo_counter{tenant_id="3"} 4`,
			writePrometheusMetrics(t))
	})

	t.Run("destroy", func(t *testing.T) {
		g3.Destroy()
		c2.Destroy()
		require.Equal(t,
			`bar_gauge 2
bar_gauge{tenant_id="2"} 2
foo_counter 6
foo_counter{tenant_id="3"} 4`,
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
	})

	t.Run("add after destroy", func(t *testing.T) {
		g3 = g.AddChild(tenant3.String())
		c2 = c.AddChild(tenant2.String())
		require.Equal(t,
			`bar_gauge 2
bar_gauge{tenant_id="2"} 2
bar_gauge{tenant_id="3"} 0
foo_counter 6
foo_counter{tenant_id="2"} 0
foo_counter{tenant_id="3"} 4`,
			writePrometheusMetrics(t))
	})

	t.Run("panic on label length mismatch", func(t *testing.T) {
		require.Panics(t, func() { c.AddChild() })
		require.Panics(t, func() { g.AddChild("", "") })
	})
}
