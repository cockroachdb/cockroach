package aggmetric

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/tick"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// SummaryMetric is similar to SQLMetric but uses LabelSliceCacheKey as the key.
// children is now an UnorderedCacheWrapper.
type SummaryMetric struct {
	labels []string
	mu     struct {
		sync.RWMutex
		children        *UnorderedCacheWrapper
		labelValueCache *metric.LabelSliceCache
	}
}

// NewSummaryMetric creates a new SummaryMetric.
func NewSummaryMetric(labels []string) *SummaryMetric {
	sm := &SummaryMetric{
		labels: labels,
	}

	sm.mu.children = &UnorderedCacheWrapper{
		cache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			//TODO (aa-joshi) : make cacheSize configurable in the future
			ShouldEvict: func(size int, key, value interface{}) bool {
				return size > 10000
			},
		}),
	}
	return sm
}

// GetLocked Get returns the child metric for the given LabelSliceCacheKey.
func (sm *SummaryMetric) getLocked(key metric.LabelSliceCacheKey) (ChildMetric, bool) {
	val, ok := sm.mu.children.cache.Get(key)
	if !ok {
		return nil, false
	}
	return val.(ChildMetric), true
}

// AddLocked Add adds a child metric for the given LabelSliceCacheKey.
func (sm *SummaryMetric) addLocked(key metric.LabelSliceCacheKey, metric ChildMetric) {
	if _, ok := sm.mu.children.cache.Get(key); ok {
		panic(errors.AssertionFailedf("child for key %v already exists", key))
	}
	sm.mu.children.cache.Add(key, metric)
}

func (sm *SummaryMetric) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sm.mu.children.ForEach(func(cm ChildMetric) {
		m := cm.ToPrometheusMetric()
		childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels)+len(sm.labels))
		childLabels = append(childLabels, labels...)
		lvs := cm.labelValues()
		key := metricKey(lvs...)
		labelValueCacheValues, _ := sm.mu.labelValueCache.Get(metric.LabelSliceCacheKey(key))
		for i := range sm.labels {
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &sm.labels[i],
				Value: &labelValueCacheValues.LabelValues[i],
			})
		}

		m.Label = childLabels
		f(m)
	})

}

// Clear removes all child metrics.
func (sm *SummaryMetric) Clear() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.children.cache.Clear()
}

type createChildSummaryMetricFunc func(key metric.LabelSliceCacheKey, cache *metric.LabelSliceCache) ChildMetric

// getOrAddChild returns the child metric for the given label values. If the child
// doesn't exist, it creates a new one and adds it to the collection.
func (sm *SummaryMetric) getOrAddChild(
	f createChildSummaryMetricFunc, labels []string,
) ChildMetric {
	if len(labels) != len(sm.labels) {
		panic(errors.AssertionFailedf(
			"cannot get/add child with %d label values %v to a metric with %d labels %v",
			len(labels), labels, len(sm.labels), sm.labels))
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Create a LabelSliceCacheKey from the tenantID.
	key := metric.LabelSliceCacheKey(metricKey(labels...))

	// If the child already exists, return it.
	if child, ok := sm.getLocked(key); ok {
		return child
	}

	child := f(key, sm.mu.labelValueCache)

	sm.mu.labelValueCache.Upsert(key, &metric.LabelSliceCacheValue{
		LabelValues: labels,
	})

	sm.addLocked(key, child)
	return child
}

func (sm *SummaryMetric) InitializeMetrics(
	f func(entry *cache.Entry), valueCache *metric.LabelSliceCache,
) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.mu.children.cache.OnEvictedEntry = f
	sm.mu.labelValueCache = valueCache
}

// SummaryCounter maintains a value as the sum of its children, keyed by LabelSliceCacheKey.
type SummaryCounter struct {
	g metric.Counter
	*SummaryMetric
}

var _ metric.Iterable = (*SummaryCounter)(nil)
var _ metric.PrometheusEvictable = (*SummaryCounter)(nil)
var _ metric.PrometheusExportable = (*SummaryCounter)(nil)

// NewSummaryCounter constructs a new SummaryCounter.
func NewSummaryCounter(metadata metric.Metadata, childLabels ...string) *SummaryCounter {
	c := &SummaryCounter{
		g: *metric.NewCounter(metadata),
	}
	c.SummaryMetric = NewSummaryMetric(childLabels)
	return c
}

// GetType is part of the metric.PrometheusExportable interface.
func (c *SummaryCounter) GetType() *io_prometheus_client.MetricType {
	return c.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (c *SummaryCounter) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return c.g.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (c *SummaryCounter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return c.g.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (c *SummaryCounter) GetName(useStaticLabels bool) string {
	return c.g.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (c *SummaryCounter) GetHelp() string {
	return c.g.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (c *SummaryCounter) GetMeasurement() string {
	return c.g.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (c *SummaryCounter) GetUnit() metric.Unit {
	return c.g.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (c *SummaryCounter) GetMetadata() metric.Metadata {
	return c.g.GetMetadata()
}

// Inspect is part of the metric.Iterable interface.
func (c *SummaryCounter) Inspect(f func(interface{})) {
	f(c)
}

// Clear resets the counter to zero.
func (c *SummaryCounter) Clear() {
	c.g.Clear()
}

// Count returns the aggregate count of all of its current and past children.
func (c *SummaryCounter) Count() int64 {
	return c.g.Count()
}

// Inc increments the counter value by i for the given LabelSliceCacheKey.
// If a counter with the given key doesn't exist yet, it creates a new counter and increments it.
func (c *SummaryCounter) Inc(i int64, labels ...string) {

	c.g.Inc(i)

	childMetric := c.SummaryMetric.getOrAddChild(c.createComponentCounter, labels)

	childMetric.(*ComponentCounter).Inc(i)
}

func (c *SummaryCounter) createComponentCounter(
	key metric.LabelSliceCacheKey, cache *metric.LabelSliceCache,
) ChildMetric {
	return &ComponentCounter{
		LabelSliceCacheKey: key,
		LabelSliceCache:    cache,
	}
}

// ComponentCounter is a child of a SummaryCounter. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type ComponentCounter struct {
	metric.LabelSliceCacheKey
	value metric.Counter
	*metric.LabelSliceCache
}

func (cc *ComponentCounter) labelValues() []string {
	lv, ok := cc.LabelSliceCache.Get(cc.LabelSliceCacheKey)
	if !ok {
		return nil
	}
	return lv.LabelValues
}

// ToPrometheusMetric constructs a prometheus metric for this Counter.
func (cc *ComponentCounter) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Counter: &io_prometheus_client.Counter{
			Value: proto.Float64(float64(cc.Value())),
		},
	}
}

// Value returns the ComponentCounter's current value.
func (cc *ComponentCounter) Value() int64 {
	return cc.value.Count()
}

// Inc increments the ComponentCounter's value.
func (cc *ComponentCounter) Inc(i int64) {
	cc.value.Inc(i)
}

// SummaryGauge maintains a value as the sum of its children, keyed by LabelSliceCacheKey.
type SummaryGauge struct {
	g metric.Gauge
	*SummaryMetric
}

var _ metric.Iterable = (*SummaryGauge)(nil)
var _ metric.PrometheusEvictable = (*SummaryGauge)(nil)
var _ metric.PrometheusExportable = (*SummaryGauge)(nil)

// NewSummaryGauge constructs a new SummaryGauge.
func NewSummaryGauge(metadata metric.Metadata, childLabels ...string) *SummaryGauge {
	g := &SummaryGauge{
		g: *metric.NewGauge(metadata),
	}
	g.SummaryMetric = NewSummaryMetric(childLabels)
	return g
}

// GetType is part of the metric.PrometheusExportable interface.
func (g *SummaryGauge) GetType() *io_prometheus_client.MetricType {
	return g.g.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (g *SummaryGauge) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return g.g.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (g *SummaryGauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return g.g.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (g *SummaryGauge) GetName(useStaticLabels bool) string {
	return g.g.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (g *SummaryGauge) GetHelp() string {
	return g.g.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (g *SummaryGauge) GetMeasurement() string {
	return g.g.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (g *SummaryGauge) GetUnit() metric.Unit {
	return g.g.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (g *SummaryGauge) GetMetadata() metric.Metadata {
	return g.g.GetMetadata()
}

// Inspect is part of the metric.Iterable interface.
func (g *SummaryGauge) Inspect(f func(interface{})) {
	f(g)
}

// Value returns the aggregate value of all of its current and past children.
func (g *SummaryGauge) Value() int64 {
	return g.g.Value()
}

// Update updates the Gauge value by val for the given label values. If a
// Gauge with the given label values doesn't exist yet, it creates a new
// Gauge and updates it. Update increments parent metrics.
func (g *SummaryGauge) Update(val int64, labels ...string) {
	childMetric := g.SummaryMetric.getOrAddChild(g.createComponentGauge, labels)

	delta := val - childMetric.(*ComponentGauge).Value()
	g.g.Inc(delta)

	childMetric.(*ComponentGauge).Update(val)
}

func (g *SummaryGauge) createComponentGauge(
	key metric.LabelSliceCacheKey, cache *metric.LabelSliceCache,
) ChildMetric {
	return &ComponentGauge{
		LabelSliceCacheKey: key,
		LabelSliceCache:    cache,
	}
}

// ComponentGauge is a child of a SummaryGauge. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type ComponentGauge struct {
	metric.LabelSliceCacheKey
	value metric.Gauge
	*metric.LabelSliceCache
}

func (cg *ComponentGauge) labelValues() []string {
	lv, ok := cg.LabelSliceCache.Get(cg.LabelSliceCacheKey)
	if !ok {
		return nil
	}
	return lv.LabelValues
}

// ToPrometheusMetric constructs a prometheus metric for this Gauge.
func (cg *ComponentGauge) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Gauge: &io_prometheus_client.Gauge{
			Value: proto.Float64(float64(cg.Value())),
		},
	}
}

// Value returns the ComponentGauge's current value.
func (cg *ComponentGauge) Value() int64 {
	return cg.value.Value()
}

// Update sets the ComponentGauge's value.
func (cg *ComponentGauge) Update(value int64) {
	cg.value.Update(value)
}

// SummaryHistogram maintains a value as the sum of its children, keyed by LabelSliceCacheKey.
type SummaryHistogram struct {
	h      metric.IHistogram
	create func() metric.IHistogram
	*SummaryMetric
	ticker struct {
		// We use a RWMutex, because we don't want child histograms to contend when
		// recording values, unless we're rotating histograms for the parent & children.
		// In this instance, the "writer" for the RWMutex is the ticker, and the "readers"
		// are all the child histograms recording their values.
		syncutil.RWMutex
		*tick.Ticker
	}
}

var _ metric.Iterable = (*SummaryHistogram)(nil)
var _ metric.PrometheusEvictable = (*SummaryHistogram)(nil)
var _ metric.PrometheusExportable = (*SummaryHistogram)(nil)
var _ metric.WindowedHistogram = (*SQLHistogram)(nil)
var _ metric.CumulativeHistogram = (*SQLHistogram)(nil)

// NewSummaryHistogram constructs a new SummaryHistogram.
func NewSummaryHistogram(opts metric.HistogramOptions) *SummaryHistogram {
	create := func() metric.IHistogram {
		return metric.NewHistogram(opts)
	}
	s := &SummaryHistogram{
		h:      create(),
		create: create,
	}
	s.SummaryMetric = NewSummaryHistogram(metric.LabelConfigDisabled)
	s.ticker.Ticker = tick.NewTicker(
		now(),
		opts.Duration/metric.WindowedHistogramWrapNum,
		func() {
			// Atomically rotate the histogram window for the
			// parent histogram, and all the child histograms.
			s.h.Tick()
			s.apply(func(childMetric ChildMetric) {
				childHist, ok := childMetric.(*SQLChildHistogram)
				if !ok {
					panic(errors.AssertionFailedf(
						"unable to assert type of child for histogram %q when rotating histogram windows",
						opts.Metadata.Name))
				}
				childHist.h.Tick()
			})
		})
	return s
}

// GetType is part of the metric.PrometheusExportable interface.
func (h *SummaryHistogram) GetType() *io_prometheus_client.MetricType {
	return h.h.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (h *SummaryHistogram) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return h.h.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (h *SummaryHistogram) ToPrometheusMetric() *io_prometheus_client.Metric {
	return h.h.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (h *SummaryHistogram) GetName(useStaticLabels bool) string {
	return h.h.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (h *SummaryHistogram) GetHelp() string {
	return h.h.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (h *SummaryHistogram) GetMeasurement() string {
	return h.h.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (h *SummaryHistogram) GetUnit() metric.Unit {
	return h.h.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (h *SummaryHistogram) GetMetadata() metric.Metadata {
	return h.h.GetMetadata()
}

// Inspect is part of the metric.Iterable interface.
func (h *SummaryHistogram) Inspect(f func(interface{})) {
	f(h)
}

// RecordValue records a value for the given LabelSliceCacheKey.
// If a histogram with the given key doesn't exist yet, it creates a new histogram and records the value.
func (h *SummaryHistogram) RecordValue(value int64, labels ...string) {
	h.h.RecordValue(value)

	childMetric := h.SummaryMetric.getOrAddChild(h.createComponentHistogram, labels)

	childMetric.(*ComponentHistogram).RecordValue(value)
}

func (h *SummaryHistogram) createComponentHistogram(
	key metric.LabelSliceCacheKey, cache *metric.LabelSliceCache,
) ChildMetric {
	return &ComponentHistogram{
		LabelSliceCacheKey: key,
		LabelSliceCache:    cache,
	}
}

// ComponentHistogram is a child of a SummaryHistogram. When metrics are collected by prometheus,
// each of the children will appear with a distinct label, however, when cockroach
// internally collects metrics, only the parent is collected.
type ComponentHistogram struct {
	metric.LabelSliceCacheKey
	histogram metric.Histogram
	*metric.LabelSliceCache
}

func (ch *ComponentHistogram) labelValues() []string {
	lv, ok := ch.LabelSliceCache.Get(ch.LabelSliceCacheKey)
	if !ok {
		return nil
	}
	return lv.LabelValues
}

// ToPrometheusMetric constructs a prometheus metric for this Histogram.
func (ch *ComponentHistogram) ToPrometheusMetric() *io_prometheus_client.Metric {
	return &io_prometheus_client.Metric{
		Histogram: &io_prometheus_client.Histogram{
			SampleCount: proto.Uint64(uint64(ch.histogram.TotalCount())),
			SampleSum:   proto.Float64(float64(ch.histogram.TotalSum())),
		},
	}
}

// RecordValue records a value in the ComponentHistogram.
func (ch *ComponentHistogram) RecordValue(value int64) {
	ch.histogram.RecordValue(value)
}
