// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package aggmetric provides functionality to create metrics which expose
// aggregate metrics for internal collection and additionally per-child
// reporting to prometheus.
package aggmetric

import (
	"context"
	"hash/fnv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/google/btree"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var delimiter = []byte{'_'}

const (
	dbLabel  = "database"
	appLabel = "application_name"
	// defaultCacheSize is the default maximum number of distinct label value combinations
	// before eviction starts in high cardinality metrics.
	defaultCacheSize = 5000
	// defaultRetentionTimeTillEviction is the default duration after which unused
	// label value combinations can be evicted from high cardinality metrics.
	defaultRetentionTimeTillEviction = 20 * time.Second
)

// This is a no-op context used during logging.
var noOpCtx = context.TODO()

// Builder is used to ease constructing metrics with the same labels.
type Builder struct {
	labels []string
}

// parentMetric is an interface that agg metric parent types must satisfy. It
// is used to allow child metrics to update the parent metric when they are updated.
type parentMetric interface {
	metric.Iterable
	metric.PrometheusExportable
}

// MakeBuilder makes a new Builder.
func MakeBuilder(labels ...string) Builder {
	return Builder{labels: labels}
}

// Gauge constructs a new AggGauge with the Builder's labels.
func (b Builder) Gauge(metadata metric.Metadata) *AggGauge {
	return NewGauge(metadata, b.labels...)
}

// FunctionalGauge constructs a new AggGauge with the Builder's labels who's
// value is determined when asked for.
func (b Builder) FunctionalGauge(metadata metric.Metadata, f func(cvs []int64) int64) *AggGauge {
	return NewFunctionalGauge(metadata, f, b.labels...)
}

// GaugeFloat64 constructs a new AggGaugeFloat64 with the Builder's labels.
func (b Builder) GaugeFloat64(metadata metric.Metadata) *AggGaugeFloat64 {
	return NewGaugeFloat64(metadata, b.labels...)
}

// Counter constructs a new AggCounter with the Builder's labels.
func (b Builder) Counter(metadata metric.Metadata) *AggCounter {
	return NewCounter(metadata, b.labels...)
}

// CounterFloat64 constructs a new AggCounter with the Builder's labels.
func (b Builder) CounterFloat64(metadata metric.Metadata) *AggCounterFloat64 {
	return NewCounterFloat64(metadata, b.labels...)
}

// Histogram constructs a new AggHistogram with the Builder's labels.
func (b Builder) Histogram(opts metric.HistogramOptions) *AggHistogram {
	return NewHistogram(opts, b.labels...)
}

type childSet struct {
	labels []string
	mu     struct {
		syncutil.Mutex
		children ChildrenStorage
	}
}

func (cs *childSet) initWithBTreeStorageType(labels []string) {
	cs.labels = labels

	lessFn := func(a, b MetricItem) bool {
		av, bv := a.labelValues(), b.labelValues()
		if len(av) != len(bv) {
			panic(errors.AssertionFailedf("mismatch in label values lengths %v vs %v", av, bv))
		}
		for i := range av {
			if cmp := strings.Compare(av[i], bv[i]); cmp != 0 {
				return cmp < 0
			}
		}
		return false
	}
	cs.mu.children = &BtreeWrapper{
		tree: btree.NewG[MetricItem](8, lessFn),
	}
}

func (cs *childSet) initWithCacheStorageType(
	labels []string, metricName string, opts metric.HighCardinalityMetricOptions,
) {
	cs.labels = labels

	// Determine maxLabelValues: use the maximum of env variable and metric defined opts.
	maxLabelValues := opts.MaxLabelValues
	if maxLabelValues == 0 {
		maxLabelValues = defaultCacheSize
	}
	maxLabelValues = max(maxLabelValues, metric.MaxLabelValues)

	// Determine retentionDuration: use the maximum of env variable and metric defined opts.
	retentionDuration := opts.RetentionTimeTillEviction
	if retentionDuration == 0 {
		retentionDuration = defaultRetentionTimeTillEviction
	}
	retentionDuration = max(retentionDuration, metric.RetentionTimeTillEviction)

	cs.mu.children = &UnorderedCacheWrapper{
		cache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, key, value any) bool {
				if childMetric, ok := value.(ChildMetric); ok {
					// Check if the child metric has exceeded the retention time and cache size is greater than max
					if labelSliceCachedChildMetric, ok := childMetric.(LabelSliceCachedChildMetric); ok {
						currentTime := timeutil.Now()
						age := currentTime.Sub(labelSliceCachedChildMetric.CreatedAt())
						return size > maxLabelValues && age > retentionDuration
					}
				}
				return size > maxLabelValues
			},
			OnEvictedEntry: func(entry *cache.Entry) {
				if childMetric, ok := entry.Value.(ChildMetric); ok {
					labelValues := childMetric.labelValues()

					// log metric name and label values of evicted entry
					log.Dev.Infof(noOpCtx, "evicted child of metric %s with label values: %s\n",
						redact.SafeString(metricName), redact.SafeString(strings.Join(labelValues, ",")))

					// Invoke DecrementAndDeleteIfZero from ChildMetric which relies on LabelSliceCache
					if boundedChild, ok := childMetric.(LabelSliceCachedChildMetric); ok {
						boundedChild.DecrementLabelSliceCacheReference()
					}
				}
			},
		}),
	}
}

func getCacheStorage() *cache.UnorderedCache {
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > defaultCacheSize
		},
	})
	return cacheStorage
}

func (cs *childSet) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.ForEach(func(cm ChildMetric) {
		pm := cm.ToPrometheusMetric()

		childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels)+len(cs.labels))
		childLabels = append(childLabels, labels...)
		lvs := cm.labelValues()
		for i := range cs.labels {
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &cs.labels[i],
				Value: &lvs[i],
			})
		}
		pm.Label = childLabels
		f(pm)
	})
}

// apply applies the given applyFn to every item in the childSet
func (cs *childSet) apply(applyFn func(item MetricItem)) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.ForEach(func(cm ChildMetric) {
		applyFn(cm)
	})
}

func (cs *childSet) add(metric ChildMetric) {
	lvs := metric.labelValues()
	if len(lvs) != len(cs.labels) {
		panic(errors.AssertionFailedf(
			"cannot add child with %d label values %v to a metric with %d labels %v",
			len(lvs), lvs, len(cs.labels), cs.labels))
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.Add(metric)
}

func (cs *childSet) remove(metric ChildMetric) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.Del(metric)
}

func (cs *childSet) get(labelVals ...string) (ChildMetric, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.mu.children.Get(labelVals...)
}

func (cs *childSet) getOrAddWithLabelSliceCache(
	metricName string,
	createFn func(key uint64, cache *metric.LabelSliceCache) LabelSliceCachedChildMetric,
	labelSliceCache *metric.LabelSliceCache,
	labelVals ...string,
) ChildMetric {
	// Validate label values count
	if len(labelVals) != len(cs.labels) {
		if log.V(2) {
			log.Dev.Errorf(noOpCtx,
				"cannot add child with %d label values %v to  metric %s with %d labels %s",
				len(labelVals), redact.SafeString(metricName), redact.SafeString(strings.Join(labelVals, ",")),
				len(cs.labels), redact.SafeString(strings.Join(cs.labels, ",")))
		}
		return nil
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Create a LabelSliceCacheKey from the label.
	key := metricKey(labelVals...)

	// Check if the child already exists
	if child, ok := cs.mu.children.GetValue(key); ok {
		return child
	}

	// Create and add the new child
	child := createFn(key, labelSliceCache)
	err := cs.mu.children.AddKey(key, child)
	if err != nil {
		if log.V(2) {
			log.Dev.Errorf(context.TODO(), "child metric creation failed for metric %s with error %v", redact.SafeString(metricName), err)
		}
		return nil
	}
	return child
}

// EachWithLabels is a generic implementation for iterating over child metrics and building prometheus metrics.
// This can be used by any aggregate metric type that embeds childSet.
func (cs *childSet) EachWithLabels(
	labels []*io_prometheus_client.LabelPair,
	f func(metric *io_prometheus_client.Metric),
	labelCache *metric.LabelSliceCache,
) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.mu.children.ForEach(func(cm ChildMetric) {
		m := cm.ToPrometheusMetric()
		childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels)+len(cs.labels))
		childLabels = append(childLabels, labels...)
		lvs := cm.labelValues()
		key := metricKey(lvs...)
		labelValueCacheValues, _ := labelCache.Get(metric.LabelSliceCacheKey(key))
		for i := range cs.labels {
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &cs.labels[i],
				Value: &labelValueCacheValues.LabelValues[i],
			})
		}

		m.Label = childLabels
		f(m)
	})
}

// clear method removes all children from the childSet. It does not reset parent metric values.
// Method should cautiously be used when childSet is reinitialised/updated. Today, it is
// only used when cluster settings are updated to support app and db label values. For normal
// operations, please use add, remove and get method to update the childSet.
func (cs *childSet) clear() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.Clear()
}

type SQLMetric struct {
	parent parentMetric
	mu     struct {
		labelConfig metric.LabelConfig
		syncutil.Mutex
		children ChildrenStorage
	}
}

var _ metric.PrometheusIterable = (*SQLMetric)(nil)
var _ metric.PrometheusExportable = (*SQLMetric)(nil)

func NewSQLMetric(labelConfig metric.LabelConfig, parent parentMetric) *SQLMetric {
	sm := &SQLMetric{
		parent: parent,
	}
	sm.mu.labelConfig = labelConfig
	sm.mu.children = &UnorderedCacheWrapper{
		cache: getCacheStorage(),
	}
	return sm
}

// GetType is part of the metric.PrometheusExportable interface.
func (sm *SQLMetric) GetType() *io_prometheus_client.MetricType {
	return sm.parent.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (sm *SQLMetric) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return sm.parent.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (sm *SQLMetric) ToPrometheusMetric() *io_prometheus_client.Metric {
	return sm.parent.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (sm *SQLMetric) GetName(useStaticLabels bool) string {
	return sm.parent.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (sm *SQLMetric) GetHelp() string {
	return sm.parent.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (sm *SQLMetric) GetMeasurement() string {
	return sm.parent.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (sm *SQLMetric) GetUnit() metric.Unit {
	return sm.parent.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (sm *SQLMetric) GetMetadata() metric.Metadata {
	return sm.parent.GetMetadata()
}

func (sm *SQLMetric) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.mu.children.ForEach(func(cm ChildMetric) {
		pm := cm.ToPrometheusMetric()

		childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels)+2)
		childLabels = append(childLabels, labels...)
		lvs := cm.labelValues()
		dbLabel := dbLabel
		appLabel := appLabel
		switch sm.mu.labelConfig {
		case metric.LabelConfigDB:
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &dbLabel,
				Value: &lvs[0],
			})
		case metric.LabelConfigApp:
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &appLabel,
				Value: &lvs[0],
			})
		case metric.LabelConfigAppAndDB:
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &dbLabel,
				Value: &lvs[0],
			})
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &appLabel,
				Value: &lvs[1],
			})
		default:
		}
		pm.Label = childLabels
		f(pm)
	})
}

func (sm *SQLMetric) get(labelVals ...string) (ChildMetric, bool) {
	return sm.mu.children.Get(labelVals...)
}

func (sm *SQLMetric) add(metric ChildMetric) {
	sm.mu.children.Add(metric)
}

type createChildMetricFunc func(labelValues labelValuesSlice) ChildMetric

// getOrAddChildLocked returns the child metric for the given label values. If the child
// doesn't exist, it creates a new one and adds it to the collection.
// REQUIRES: sm.mu is locked.
func (sm *SQLMetric) getOrAddChildLocked(
	f createChildMetricFunc, labelValues ...string,
) ChildMetric {
	// If the child already exists, return it.
	if child, ok := sm.get(labelValues...); ok {
		return child
	}

	child := f(labelValues)

	sm.add(child)
	return child
}

// getChildByLabelConfig returns the child metric based on the label configuration.
// It returns the child metric and a boolean indicating if the child was found.
// If the label configuration is either LabelConfigDisabled or unrecognised, it returns
// ChildMetric as nil and false.
func (sm *SQLMetric) getChildByLabelConfig(
	f createChildMetricFunc, db string, app string,
) (ChildMetric, bool) {
	// We should acquire the lock before evaluating the label configuration
	// and accessing the children storage in a thread-safe manner. We have moved
	// the lock acquisition from getOrAddChildLocked to here to fix bug #147475.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var childMetric ChildMetric
	switch sm.mu.labelConfig {
	case metric.LabelConfigDisabled:
		return nil, false
	case metric.LabelConfigDB:
		childMetric = sm.getOrAddChildLocked(f, db)
		return childMetric, true
	case metric.LabelConfigApp:
		childMetric = sm.getOrAddChildLocked(f, app)
		return childMetric, true
	case metric.LabelConfigAppAndDB:
		childMetric = sm.getOrAddChildLocked(f, db, app)
		return childMetric, true
	default:
		return nil, false
	}
}

// ReinitialiseChildMetrics clears the child metrics and
// sets the label configuration.
func (sm *SQLMetric) ReinitialiseChildMetrics(labelConfig metric.LabelConfig) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.children.Clear()
	sm.mu.labelConfig = labelConfig
}

type MetricItem interface {
	labelValuer
}

type CacheMetricItem interface {
	MetricItem
}

type ChildMetric interface {
	MetricItem
	ToPrometheusMetric() *io_prometheus_client.Metric
}

// LabelSliceCachedChildMetric extends ChildMetric with label slice caching capabilities.
// This interface is designed for child metrics that relies on label slice reference
// counting system. Metrics implementing this interface can have their label values
// cached and shared among multiple metrics with identical label combinations,
// reducing memory usage and improving performance in scenarios with many similar metrics.
type LabelSliceCachedChildMetric interface {
	ChildMetric
	CreatedAt() time.Time
	DecrementLabelSliceCacheReference()
}

type labelValuer interface {
	labelValues() []string
}

type labelValuesSlice []string

func (lv *labelValuesSlice) labelValues() []string { return []string(*lv) }

func metricKey(labels ...string) uint64 {
	hash := fnv.New64a()
	for _, label := range labels {
		_, _ = hash.Write([]byte(label))
		_, _ = hash.Write(delimiter)
	}
	return hash.Sum64()
}

type ChildrenStorage interface {
	Get(labelVals ...string) (ChildMetric, bool)
	GetValue(key uint64) (ChildMetric, bool)
	Add(metric ChildMetric)
	AddKey(key uint64, metric ChildMetric) error
	Del(key ChildMetric)
	// ForEach calls f for each child metric, in arbitrary order.
	ForEach(f func(metric ChildMetric))
	Clear()
}

var _ ChildrenStorage = &UnorderedCacheWrapper{}
var _ ChildrenStorage = &BtreeWrapper{}

type UnorderedCacheWrapper struct {
	cache *cache.UnorderedCache
}

func (ucw *UnorderedCacheWrapper) GetValue(key uint64) (ChildMetric, bool) {
	value, ok := ucw.cache.Get(key)
	if !ok {
		return nil, false
	}
	return value.(ChildMetric), ok
}

func (ucw *UnorderedCacheWrapper) AddKey(key uint64, metric ChildMetric) error {
	if _, ok := ucw.cache.Get(key); ok {
		return errors.Newf("child %s already exists\n", redact.SafeString(strings.Join(metric.labelValues(), ",")))
	}
	ucw.cache.Add(key, metric)
	return nil
}

func (ucw *UnorderedCacheWrapper) Get(labelVals ...string) (ChildMetric, bool) {
	hashKey := metricKey(labelVals...)
	value, ok := ucw.cache.Get(hashKey)
	if !ok {
		return nil, false
	}
	return value.(ChildMetric), ok
}

func (ucw *UnorderedCacheWrapper) Add(metric ChildMetric) {
	labelValues := metric.labelValues()
	hashKey := metricKey(labelValues...)
	if _, ok := ucw.cache.Get(hashKey); ok {
		panic(errors.AssertionFailedf("child %v already exists", metric.labelValues()))
	}
	ucw.cache.Add(hashKey, metric)
}

func (ucw *UnorderedCacheWrapper) Del(metric ChildMetric) {
	hashKey := metricKey(metric.labelValues()...)
	if _, ok := ucw.cache.Get(hashKey); ok {
		ucw.cache.Del(hashKey)
	}
}

func (ucw *UnorderedCacheWrapper) ForEach(f func(metric ChildMetric)) {
	ucw.cache.Do(func(e *cache.Entry) {
		f(e.Value.(ChildMetric))
	})
}

func (ucw *UnorderedCacheWrapper) Clear() {
	ucw.cache.Clear()
}

type BtreeWrapper struct {
	tree *btree.BTreeG[MetricItem]
}

func (b BtreeWrapper) GetValue(key uint64) (ChildMetric, bool) {
	// GetValue method is not relevant for BtreeWrapper as it uses ChildMetric
	// as an item in Btree. We are going to remove BtreeWrapper as ChildrenStorage.
	panic("unimplemented")
}

func (b BtreeWrapper) AddKey(_ uint64, _ ChildMetric) error {
	// AddKey method is not relevant for BtreeWrapper as it uses ChildMetric
	// as an item in Btree. We are going to remove BtreeWrapper as ChildrenStorage.
	panic("unimplemented")
}

func (b BtreeWrapper) Get(labelVals ...string) (ChildMetric, bool) {
	key := labelValuesSlice(labelVals)
	cm, ok := b.tree.Get(&key)
	if !ok {
		return nil, false
	}
	return cm.(ChildMetric), true
}

func (b BtreeWrapper) Add(metric ChildMetric) {
	if b.tree.Has(metric) {
		panic(errors.AssertionFailedf("child %v already exists", metric.labelValues()))
	}
	b.tree.ReplaceOrInsert(metric)
}

func (b BtreeWrapper) Del(metric ChildMetric) {
	if _, ok := b.tree.Delete(metric); !ok {
		panic(errors.AssertionFailedf("child %v does not exist", metric.labelValues()))
	}
}

func (b BtreeWrapper) ForEach(f func(metric ChildMetric)) {
	b.tree.Ascend(func(i MetricItem) bool {
		f(i.(ChildMetric))
		return true
	})
}

func (b BtreeWrapper) Clear() {
	b.tree.Clear(false)
}

// baseAggMetric provides the shared implementation for aggregate metric types.
// It delegates Iterable and PrometheusExportable methods to the parent metric,
// which tracks the aggregate value, and manages per-label child metrics via
// the embedded childSet.
// Note: This struct does not fully implement metric.Iterable because it doesn't
// implement the metric.Iterable Inspect method, which is expected to be
// implemented by the concrete aggregate metric types that embed baseAggMetric.
type baseAggMetric struct {
	parent parentMetric
	childSet
}

func newBaseAggMetric(parent parentMetric) *baseAggMetric {
	return &baseAggMetric{
		parent: parent,
	}
}

// GetType is part of the metric.PrometheusExportable interface.
func (b *baseAggMetric) GetType() *io_prometheus_client.MetricType {
	return b.parent.GetType()
}

// GetLabels is part of the metric.PrometheusExportable interface.
func (b *baseAggMetric) GetLabels(useStaticLabels bool) []*io_prometheus_client.LabelPair {
	return b.parent.GetLabels(useStaticLabels)
}

// ToPrometheusMetric is part of the metric.PrometheusExportable interface.
func (b *baseAggMetric) ToPrometheusMetric() *io_prometheus_client.Metric {
	return b.parent.ToPrometheusMetric()
}

// GetName is part of the metric.Iterable interface.
func (b *baseAggMetric) GetName(useStaticLabels bool) string {
	return b.parent.GetName(useStaticLabels)
}

// GetHelp is part of the metric.Iterable interface.
func (b *baseAggMetric) GetHelp() string {
	return b.parent.GetHelp()
}

// GetMeasurement is part of the metric.Iterable interface.
func (b *baseAggMetric) GetMeasurement() string {
	return b.parent.GetMeasurement()
}

// GetUnit is part of the metric.Iterable interface.
func (b *baseAggMetric) GetUnit() metric.Unit {
	return b.parent.GetUnit()
}

// GetMetadata is part of the metric.Iterable interface.
func (b *baseAggMetric) GetMetadata() metric.Metadata {
	return b.parent.GetMetadata()
}

var _ metric.PrometheusIterable = (*baseAggMetric)(nil)
var _ metric.PrometheusExportable = (*baseAggMetric)(nil)
