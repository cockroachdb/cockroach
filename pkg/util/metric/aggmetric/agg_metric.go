// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package aggmetric provides functionality to create metrics which expose
// aggregate metrics for internal collection and additionally per-child
// reporting to prometheus.
package aggmetric

import (
	"hash/fnv"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var delimiter = []byte{'_'}

const (
	dbLabel  = "database"
	appLabel = "application_name"
)

const (
	LabelConfigDisabled = iota
	LabelConfigApp
	LabelConfigDB
	LabelConfigAppAndDB
)

// Builder is used to ease constructing metrics with the same labels.
type Builder struct {
	labels []string
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
	cs.mu.children = &BtreeWrapper{
		tree: btree.New(8),
	}
}

func getCacheStorage() *cache.UnorderedCache {
	const cacheSize = 5000
	cacheStorage := cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		//TODO (aa-joshi) : make cacheSize configurable in the future
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > cacheSize
		},
	})
	return cacheStorage
}

func (cs *childSet) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.mu.children.Do(func(e interface{}) {
		cm := cs.mu.children.GetChildMetric(e)
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
	cs.mu.children.Do(func(e interface{}) {
		applyFn(cs.mu.children.GetChildMetric(e).(MetricItem))
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
	labelConfig atomic.Uint64
	mu          struct {
		syncutil.Mutex
		children ChildrenStorage
	}
}

func NewSQLMetric(labelConfig uint64) *SQLMetric {
	sm := &SQLMetric{}
	sm.labelConfig.Store(labelConfig)
	sm.mu.children = &UnorderedCacheWrapper{
		cache: getCacheStorage(),
	}
	return sm
}

func (sm *SQLMetric) Each(
	labels []*io_prometheus_client.LabelPair, f func(metric *io_prometheus_client.Metric),
) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.mu.children.Do(func(e interface{}) {
		cm := sm.mu.children.GetChildMetric(e)
		pm := cm.ToPrometheusMetric()

		childLabels := make([]*io_prometheus_client.LabelPair, 0, len(labels)+2)
		childLabels = append(childLabels, labels...)
		lvs := cm.labelValues()
		dbLabel := dbLabel
		appLabel := appLabel
		switch sm.labelConfig.Load() {
		case LabelConfigDB:
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &dbLabel,
				Value: &lvs[0],
			})
		case LabelConfigApp:
			childLabels = append(childLabels, &io_prometheus_client.LabelPair{
				Name:  &appLabel,
				Value: &lvs[0],
			})
		case LabelConfigAppAndDB:
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

// getOrAddChild returns the child metric for the given label values. If the child
// doesn't exist, it creates a new one and adds it to the collection.
func (sm *SQLMetric) getOrAddChild(f createChildMetricFunc, labelValues ...string) ChildMetric {
	sm.mu.Lock()
	defer sm.mu.Unlock()

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
	var childMetric ChildMetric
	switch sm.labelConfig.Load() {
	case LabelConfigDB:
		childMetric = sm.getOrAddChild(f, db)
		return childMetric, true
	case LabelConfigApp:
		childMetric = sm.getOrAddChild(f, app)
		return childMetric, true
	case LabelConfigAppAndDB:
		childMetric = sm.getOrAddChild(f, db, app)
		return childMetric, true
	default:
		return nil, false
	}
}

type MetricItem interface {
	labelValuer
}

type BtreeMetricItem interface {
	btree.Item
	MetricItem
}

type CacheMetricItem interface {
	MetricItem
}

type ChildMetric interface {
	MetricItem
	ToPrometheusMetric() *io_prometheus_client.Metric
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
	Add(metric ChildMetric)
	Del(key ChildMetric)
	Do(f func(e interface{}))
	GetChildMetric(e interface{}) ChildMetric
	Clear()
}

var _ ChildrenStorage = &UnorderedCacheWrapper{}
var _ ChildrenStorage = &BtreeWrapper{}

type UnorderedCacheWrapper struct {
	cache *cache.UnorderedCache
}

func (ucw *UnorderedCacheWrapper) GetChildMetric(e interface{}) ChildMetric {
	return e.(*cache.Entry).Value.(ChildMetric)
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

func (ucw *UnorderedCacheWrapper) Do(f func(e interface{})) {
	ucw.cache.Do(func(e *cache.Entry) {
		f(e)
	})
}

func (ucw *UnorderedCacheWrapper) Clear() {
	ucw.cache.Clear()
}

type BtreeWrapper struct {
	tree *btree.BTree
}

func (b BtreeWrapper) Get(labelVals ...string) (ChildMetric, bool) {
	key := labelValuesSlice(labelVals)
	cm := b.tree.Get(&key)
	if cm == nil {
		return nil, false
	}
	return cm.(ChildMetric), true
}

func (b BtreeWrapper) Add(metric ChildMetric) {
	if b.tree.Has(metric.(BtreeMetricItem)) {
		panic(errors.AssertionFailedf("child %v already exists", metric.labelValues()))
	}
	b.tree.ReplaceOrInsert(metric.(BtreeMetricItem))
}

func (b BtreeWrapper) Del(metric ChildMetric) {
	if existing := b.tree.Delete(metric.(btree.Item)); existing == nil {
		panic(errors.AssertionFailedf(
			"child %v does not exists", metric.labelValues()))
	}
}

func (b BtreeWrapper) Do(f func(e interface{})) {
	b.tree.Ascend(func(i btree.Item) bool {
		f(i)
		return true
	})
}

func (b BtreeWrapper) GetChildMetric(e interface{}) ChildMetric {
	return e.(ChildMetric)
}

func (b BtreeWrapper) Clear() {
	b.tree.Clear(false)
}

func (lv *labelValuesSlice) Less(o btree.Item) bool {
	ov := o.(labelValuer).labelValues()
	if len(ov) != len(*lv) {
		panic(errors.AssertionFailedf("mismatch in label values lengths %v vs %v",
			ov, *lv))
	}
	for i := range ov {
		if cmp := strings.Compare((*lv)[i], ov[i]); cmp != 0 {
			return cmp < 0
		}
	}
	return false // eq
}
