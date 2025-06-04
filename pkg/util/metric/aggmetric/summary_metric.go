package aggmetric

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
