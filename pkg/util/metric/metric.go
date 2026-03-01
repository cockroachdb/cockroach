// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric/tick"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

const (
	// TestSampleInterval is passed to histograms during tests which don't
	// want to concern themselves with supplying a "correct" interval.
	TestSampleInterval = time.Duration(math.MaxInt64)
	// WindowedHistogramWrapNum is the number of histograms to keep in rolling
	// window.
	WindowedHistogramWrapNum = 2
	// CardinalityLimit is the max number of distinct label values combinations for any given MetricVec.
	CardinalityLimit = 2000
	// HighCardinalityMetricsLimit is the max number of distinct label values combinations for any given
	//HighCardinality metrics.
	HighCardinalityMetricsLimit = 5000
)

// Maintaining a list of static label names here to avoid duplication and
// encourage reuse of label names across the codebase.
const (
	LabelQueryType       = "query_type"
	LabelQueryInternal   = "query_internal"
	LabelStatus          = "status"
	LabelCertificateType = "certificate_type"
	LabelName            = "name"
	LabelType            = "type"
	LabelLevel           = "level"
	LabelOrigin          = "origin"
	LabelResult          = "result"
)

type LabelConfig uint64

const (
	LabelConfigDisabled LabelConfig = iota
	LabelConfigApp      LabelConfig = 1 << iota
	LabelConfigDB
	LabelConfigAppAndDB = LabelConfigApp | LabelConfigDB
)

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// GetName returns the fully-qualified name of the metric.
	GetName(useStaticLabels bool) string
	// GetHelp returns the help text for the metric.
	GetHelp() string
	// GetMeasurement returns the label for the metric, which describes the entity
	// it measures.
	GetMeasurement() string
	// GetUnit returns the unit that should be used to display the metric
	// (e.g. in bytes).
	GetUnit() Unit
	// GetMetadata returns the metric's metadata, which can be used in charts.
	GetMetadata() Metadata
	// Inspect calls the given closure with each contained item.
	Inspect(func(interface{}))
}

type PrometheusCompatible interface {
	// GetName is a method on Metadata
	GetName(useStaticLabels bool) string
	// GetHelp is a method on Metadata
	GetHelp() string
	// GetType returns the prometheus type enum for this metric.
	GetType() *prometheusgo.MetricType
	// GetLabels is a method on Metadata
	GetLabels(useStaticLabels bool) []*prometheusgo.LabelPair
}

// PrometheusExportable is the standard interface for an individual metric
// that can be exported to prometheus.
type PrometheusExportable interface {
	PrometheusCompatible
	// ToPrometheusMetric returns a filled-in prometheus metric of the right type
	// for the given metric. It does not fill in labels.
	// The implementation must return thread-safe data to the caller, i.e.
	// usually a copy of internal state.
	// NB: For histogram metrics, ToPrometheusMetric should return the cumulative histogram.
	ToPrometheusMetric() *prometheusgo.Metric
}

type PrometheusVector interface {
	PrometheusCompatible
	ToPrometheusMetrics() []*prometheusgo.Metric
	Delete(labels map[string]string) bool
}

// PrometheusIterable is an extension of PrometheusExportable to indicate that
// this metric comprises children metrics which augment the parent's label
// values.
//
// The motivating use-case for this interface is the existence of tenants. We'd
// like to capture per-tenant metrics and expose them to prometheus while not
// polluting the internal tsdb.
type PrometheusIterable interface {
	PrometheusExportable

	// Each takes a slice of label pairs associated with the parent metric and
	// calls the passed function with each of the children metrics.
	Each([]*prometheusgo.LabelPair, func(metric *prometheusgo.Metric))
}

// PrometheusReinitialisable is an extension of PrometheusIterable to indicate that
// this metric comprises children metrics which label values are configurable
// through cluster settings.
//
// The motivating use-case for this interface is to update labels
// for child metrics based on cluster settings so that it would reflect
// in prometheus export
type PrometheusReinitialisable interface {
	PrometheusIterable

	ReinitialiseChildMetrics(labelConfig LabelConfig)
}

// PrometheusEvictable is an extension of PrometheusIterable to indicate that
// this metric uses cache as a storage and children metric can be evicted
// based on eviction policy.
// The InitializeMetrics method accepts a reference of LabelSliceCache which is
// initialised at metric registry and settings values for configurable eviction policy.
type PrometheusEvictable interface {
	PrometheusIterable

	InitializeMetrics(*LabelSliceCache)
}

// WindowedHistogram represents a histogram with data over recent window of
// time. It's used primarily to record histogram data into CRDB's internal
// time-series database, which does not know how to encode cumulative
// histograms. What it does instead is scrape off sample count, sum of values,
// and values at specific quantiles from "windowed" histograms and record that
// data directly. These windows could be arbitrary and overlapping.
//
// WindowedHistogram are generally only useful when recording histograms to TSDB,
// where they are used to calculate quantiles and the mean. The exception is that
// count and sum are calculated against the CumulativeHistogram instead when
// recording to TSDB, as these values should always be monotonically increasing.
type WindowedHistogram interface {
	// WindowedSnapshot returns a filled-in snapshot of the metric containing the current
	// histogram window. Things like Mean, Quantiles, etc. can be calculated
	// against the returned HistogramSnapshot.
	//
	// Methods implementing this interface should the merge buckets, sums, and counts
	// of previous and current windows.
	WindowedSnapshot() HistogramSnapshot
}

// CumulativeHistogram represents a histogram with data over the cumulative lifespan
// of the histogram metric.
//
// CumulativeHistograms are considered the familiar standard when using histograms,
// and are used except when recording to an internal TSDB. The exception is that
// count and sum are calculated against the CumulativeHistogram when recording to TSDB,
// instead of the WindowedHistogram, as these values should always be monotonically
// increasing.
type CumulativeHistogram interface {
	// CumulativeSnapshot returns a filled-in snapshot of the metric's cumulative histogram.
	// Things like Mean, Quantiles, etc. can be calculated against the returned
	// HistogramSnapshot.
	CumulativeSnapshot() HistogramSnapshot
}

// GetName returns the metric's name. When `useStaticLabels` is true, it returns
// the metric's labeled name if it's non-empty. Otherwise, it returns the metric's
// name.
func (m *Metadata) GetName(useStaticLabels bool) string {
	if useStaticLabels && m.LabeledName != "" {
		return m.LabeledName
	}
	return m.Name
}

// GetHelp returns the metric's help string.
func (m *Metadata) GetHelp() string {
	return m.Help
}

// GetMeasurement returns the entity measured by the metric.
func (m *Metadata) GetMeasurement() string {
	return m.Measurement
}

// GetUnit returns the metric's unit of measurement.
func (m *Metadata) GetUnit() Unit {
	return m.Unit
}

// GetLabels returns the metric's labels. For rationale behind the conversion
// from metric.LabelPair to prometheusgo.LabelPair, see the LabelPair comment
// in pkg/util/metric/metric.proto.
func (m *Metadata) GetLabels(useStaticLabels bool) []*prometheusgo.LabelPair {
	// x satisfies the field XXX_unrecognized in prometheusgo.LabelPair.
	var x []byte

	var lps []*prometheusgo.LabelPair
	numStaticLabels := 0
	if useStaticLabels {
		numStaticLabels = len(m.StaticLabels)
		lps = make([]*prometheusgo.LabelPair, len(m.Labels)+numStaticLabels)
		for i, v := range m.StaticLabels {
			lps[i] = &prometheusgo.LabelPair{Name: v.Name, Value: v.Value, XXX_unrecognized: x}
		}
	} else {
		lps = make([]*prometheusgo.LabelPair, len(m.Labels))
	}
	for i, v := range m.Labels {
		lps[i+numStaticLabels] = &prometheusgo.LabelPair{Name: v.Name, Value: v.Value, XXX_unrecognized: x}
	}
	return lps
}

// Returns the value for TsdbRecordLabeled,
// defaults to True when it is not supplied.
func (m *Metadata) GetTsdbRecordLabeled() bool {
	if m.TsdbRecordLabeled == nil {
		return true
	}
	return *m.TsdbRecordLabeled
}

// AddLabel adds a label/value pair for this metric.
func (m *Metadata) AddLabel(name, value string) {
	m.Labels = append(m.Labels,
		&LabelPair{
			Name:  proto.String(ExportedLabel(name)),
			Value: proto.String(value),
		})
}

var _ Iterable = &Gauge{}
var _ Iterable = &DerivedGauge{}
var _ Iterable = &FunctionalGauge{}
var _ Iterable = &GaugeFloat64{}
var _ Iterable = &Counter{}
var _ Iterable = &UniqueCounter{}
var _ Iterable = &CounterFloat64{}
var _ Iterable = &GaugeVec{}
var _ Iterable = &DerivedGaugeVec{}
var _ Iterable = &CounterVec{}
var _ Iterable = &HistogramVec{}

var _ json.Marshaler = &Gauge{}
var _ json.Marshaler = &DerivedGauge{}
var _ json.Marshaler = &FunctionalGauge{}
var _ json.Marshaler = &GaugeFloat64{}
var _ json.Marshaler = &Counter{}
var _ json.Marshaler = &UniqueCounter{}
var _ json.Marshaler = &CounterFloat64{}
var _ json.Marshaler = &Registry{}

var _ PrometheusExportable = &Gauge{}
var _ PrometheusExportable = &DerivedGauge{}
var _ PrometheusExportable = &FunctionalGauge{}
var _ PrometheusExportable = &GaugeFloat64{}
var _ PrometheusExportable = &Counter{}
var _ PrometheusExportable = &UniqueCounter{}
var _ PrometheusExportable = &CounterFloat64{}

var _ PrometheusVector = &GaugeVec{}
var _ PrometheusVector = &DerivedGaugeVec{}
var _ PrometheusVector = &CounterVec{}
var _ PrometheusVector = &HistogramVec{}

var now = timeutil.Now

// TestingSetNow changes the clock used by the metric system. For use by
// testing to precisely control the clock. Also sets the time in the `tick`
// package, since that is used ubiquitously here.
func TestingSetNow(f func() time.Time) func() {
	tickNowResetFn := tick.TestingSetNow(f)
	origNow := now
	now = f
	return func() {
		now = origNow
		tickNowResetFn()
	}
}

// useHdrHistogramsEnvVar can be used to switch all histograms to use the
// legacy HDR histograms (except for those that explicitly force the use
// of the newer Prometheus via HistogramModePrometheus). HDR Histograms
// dynamically generate bucket boundaries, which can lead to hundreds of
// buckets. This can cause performance issues with timeseries databases
// like Prometheus.
const useHdrHistogramsEnvVar = "COCKROACH_ENABLE_HDR_HISTOGRAMS"

var hdrEnabled = metamorphic.ConstantWithTestBool(useHdrHistogramsEnvVar, envutil.EnvOrDefaultBool(useHdrHistogramsEnvVar, false))

// HdrEnabled returns whether or not the HdrHistogram model is enabled
// in the metric package. Primarily useful in tests where we want to validate
// different outputs depending on whether or not HDR is enabled.
func HdrEnabled() bool {
	return hdrEnabled
}

// useNativeHistogramsEnvVar can be used to enable the Prometheus native
// histogram feature, which represents a histogram as a single time series
// rather than a collection of per-bucket counter series. If enabled, both
// conventional and native histograms are exported.
const useNativeHistogramsEnvVar = "COCKROACH_ENABLE_PROMETHEUS_NATIVE_HISTOGRAMS"

var nativeHistogramsEnabled = envutil.EnvOrDefaultBool(useNativeHistogramsEnvVar, false)

// nativeHistogramsBucketFactorEnvVar can be used to override the default
// bucket size exponential factor for Prometheus native histograms, if enabled.
// If not set, use the default factor of 1.1.
const nativeHistogramsBucketFactorEnvVar = "COCKROACH_PROMETHEUS_NATIVE_HISTOGRAMS_BUCKET_FACTOR"

var nativeHistogramsBucketFactor = envutil.EnvOrDefaultFloat64(nativeHistogramsBucketFactorEnvVar, 1.1)

// nativeHistogramsBucketCountMultiplierEnvVar can be used to override the
// default maximum bucket count for Prometheus native histograms, if enabled.
// The maximum bucket count is set to the number of conventional buckets for
// the histogram metric multiplied by the multiplier, which defaults to 1.0.
const nativeHistogramsBucketCountMultiplierEnvVar = "COCKROACH_PROMETHEUS_NATIVE_HISTOGRAMS_BUCKET_COUNT_MULTIPLIER"

var nativeHistogramsBucketCountMultiplier = envutil.EnvOrDefaultFloat64(nativeHistogramsBucketCountMultiplierEnvVar, 1)

// maxLabelValuesEnvVar can be used to configure the maximum number of distinct
// label value combinations for high cardinality metrics before eviction starts.
const maxLabelValuesEnvVar = "COCKROACH_HIGH_CARDINALITY_METRICS_MAX_LABEL_VALUES"

// MaxLabelValues is the configured maximum number of distinct label value combinations
// for high cardinality metrics before eviction starts, read from the environment variable.
var MaxLabelValues = envutil.EnvOrDefaultInt(maxLabelValuesEnvVar, 0)

// retentionTimeTillEvictionEnvVar can be used to configure the time duration
// after which unused label value combinations can be evicted from the cache.
const retentionTimeTillEvictionEnvVar = "COCKROACH_HIGH_CARDINALITY_METRICS_RETENTION_TIME_TILL_EVICTION"

// RetentionTimeTillEviction is the configured time duration after which unused
// label value combinations can be evicted from the cache, read from the environment variable.
// We are making sure that metrics would be scraped in at least one scrape as we have a default 10 second
// scrape interval.
var RetentionTimeTillEviction = envutil.EnvOrDefaultDuration(retentionTimeTillEvictionEnvVar, 10*time.Second)

type HistogramMode byte

const (
	// HistogramModePrometheus will force the constructed histogram to use
	// the Prometheus histogram model, regardless of the value of
	// useHdrHistogramsEnvVar. This option should be used for all
	// newly defined histograms moving forward.
	//
	// NB: If neither this mode nor the HistogramModePreferHdrLatency mode
	// is set, MaxVal and SigFigs must be defined to maintain backwards
	// compatibility with the legacy HdrHistogram model.
	HistogramModePrometheus HistogramMode = iota + 1
	// HistogramModePreferHdrLatency will cause the returned histogram to
	// use the HdrHistgoram model and be configured with suitable defaults
	// for latency tracking iff useHdrHistogramsEnvVar is enabled.
	//
	// NB: If this option is set, no MaxVal or SigFigs are required in the
	// HistogramOptions to maintain backwards compatibility with the legacy
	// HdrHistogram model, since suitable defaults are used for both.
	HistogramModePreferHdrLatency
)

// HighCardinalityMetricOptions defines the configuration options for high cardinality metrics
// (Counter, Gauge, Histogram) that use cache storage. This allows fine-grained control over
// eviction policies to manage memory usage for metrics with many distinct label combinations.
type HighCardinalityMetricOptions struct {
	// Metadata is the metric Metadata associated with the high cardinality metric.
	Metadata Metadata
	// MaxLabelValues sets the maximum number of distinct label value combinations
	// that can be stored in the cache before eviction starts. When this limit is reached,
	// the cache will evict entries based on the configured eviction policy.
	// If set to 0, the default 5000 value is used.
	MaxLabelValues int
	// RetentionTimeTillEviction specifies the time duration after which unused
	// label value combinations can be evicted from the cache. Entries that haven't
	// been accessed for longer than this duration may be evicted.
	// If set to 0, the default value of 20 seconds is used to ensure the label value is
	// scraped at least once with default scrape interval of 10 seconds.
	RetentionTimeTillEviction time.Duration
}

type HistogramOptions struct {
	// Metadata is the metric Metadata associated with the histogram.
	Metadata Metadata
	// Duration is the total duration of all windows in the histogram.
	// The individual window duration is equal to the
	// Duration/WindowedHistogramWrapNum (i.e., the number of windows
	// in the histogram).
	Duration time.Duration
	// MaxVal is only relevant to the HdrHistogram, and represents the
	// highest trackable value in the resulting histogram buckets.
	MaxVal int64
	// SigFigs is only relevant to the HdrHistogram, and represents
	// the number of significant figures to be used to determine the
	// degree of accuracy used in measurements.
	SigFigs int
	// Buckets are only relevant to Prometheus histograms, and represent
	// the pre-defined histogram bucket boundaries to be used.
	Buckets []float64
	// BucketConfig is only relevant to Prometheus histograms, and represents
	// the pre-defined histogram bucket configuration used to generate buckets.
	BucketConfig staticBucketConfig
	// Mode defines the type of histogram to be used. See individual
	// comments on each HistogramMode value for details.
	Mode HistogramMode
	// HighCardinalityOpts configures cache eviction for high cardinality histograms.
	// Only applies when using NewHighCardinalityHistogram.
	HighCardinalityOpts HighCardinalityMetricOptions
}

func NewHistogram(opt HistogramOptions) IHistogram {
	opt.Metadata.MetricType = prometheusgo.MetricType_HISTOGRAM
	if hdrEnabled && opt.Mode != HistogramModePrometheus {
		if opt.Mode == HistogramModePreferHdrLatency {
			return NewHdrLatency(opt.Metadata, opt.Duration)
		} else {
			return NewHdrHistogram(opt.Metadata, opt.Duration, opt.MaxVal, opt.SigFigs)
		}
	} else {
		return newHistogram(opt.Metadata, opt.Duration, opt.Buckets,
			opt.BucketConfig)
	}
}

// NewHistogram is a prometheus-backed histogram. Depending on the value of
// opts.Buckets, this is suitable for recording any kind of quantity. Common
// sensible choices are {IO,Network}LatencyBuckets.
func newHistogram(
	meta Metadata, duration time.Duration, buckets []float64, bucketConfig staticBucketConfig,
) *Histogram {
	// TODO(obs-inf): prometheus supports labeled histograms but they require more
	// plumbing and don't fit into the PrometheusObservable interface any more.

	// If no buckets are provided, generate buckets from bucket configuration
	if buckets == nil && bucketConfig.count != 0 {
		buckets = bucketConfig.GetBucketsFromBucketConfig()
	}
	opts := prometheus.HistogramOpts{
		Buckets: buckets,
	}
	if bucketConfig.distribution == Exponential && nativeHistogramsEnabled {
		opts.NativeHistogramBucketFactor = nativeHistogramsBucketFactor
		opts.NativeHistogramMaxBucketNumber = uint32(float64(len(buckets)) * nativeHistogramsBucketCountMultiplier)
	}
	cum := prometheus.NewHistogram(opts)
	h := &Histogram{
		Metadata: meta,
		cum:      cum,
	}
	h.windowed.Ticker = tick.NewTicker(
		now(),
		// We want to divide the total window duration by the number of windows
		// because we need to rotate the windows at uniformly distributed
		// intervals within a histogram's total duration.
		duration/WindowedHistogramWrapNum,
		func() {
			h.windowed.Lock()
			defer h.windowed.Unlock()
			if h.windowed.cur.Load() != nil {
				h.windowed.prev.Store(h.windowed.cur.Load())
			}
			h.windowed.cur.Store(prometheus.NewHistogram(opts))
		})
	h.windowed.Ticker.OnTick()
	return h
}

var _ PrometheusExportable = (*Histogram)(nil)
var _ WindowedHistogram = (*Histogram)(nil)
var _ CumulativeHistogram = (*Histogram)(nil)
var _ IHistogram = (*Histogram)(nil)

// Histogram is a prometheus-backed histogram. It collects observed values by
// keeping bucketed counts. For convenience, internally two sets of buckets are
// kept: A cumulative set (i.e. data is never evicted) and a windowed set (which
// keeps only recently collected samples).
//
// New buckets are created using TestHistogramBuckets.
type Histogram struct {
	Metadata
	cum prometheus.HistogramInternal

	// TODO(obs-inf): the way we implement windowed histograms is not great.
	// We could "just" double the rotation interval (so that the histogram really
	// collects for 20s when we expect to persist the contents every 10s).
	// Really it would make more sense to explicitly rotate the histogram
	// atomically with collecting its contents, but that is now how we have set
	// it up right now. It should be doable though, since there is only one
	// consumer of windowed histograms - our internal timeseries system.
	windowed struct {
		*tick.Ticker
		syncutil.Mutex
		prev, cur atomic.Value
	}
}

type IHistogram interface {
	Iterable
	PrometheusExportable
	WindowedHistogram
	CumulativeHistogram
	// Periodic exposes tick-related functions as part of the public API.
	// TODO(obs-infra): This shouldn't be necessary, but we need to expose tick functions
	// to metric.AggHistogram so that it has the ability to rotate the underlying histogram
	// windows. The real solution is to merge the two packages and make this piece of the API
	// package-private, but such a solution is not easily backported. This solution is meant
	// to be temporary, and the merging of packages will happen on master which will provide
	// a more holistic solution. Afterwards, interfaces involving ticking can be returned to
	// package-private.
	tick.Periodic

	RecordValue(n int64)
}

// NextTick returns the next tick timestamp of the underlying tick.Ticker
// used by this Histogram.  Generally not useful - this is part of a band-aid
// fix and should be expected to be removed.
// TODO(obs-infra): remove this once pkg/util/aggmetric is merged with this package.
func (h *Histogram) NextTick() time.Time {
	return h.windowed.NextTick()
}

// Tick triggers a tick of this Histogram, regardless of whether we've passed
// the next tick interval. Generally, this should not be used by any caller other
// than aggmetric.AggHistogram. Future work will remove the need to expose this function
// as part of the public API.
// TODO(obs-infra): remove this once pkg/util/aggmetric is merged with this package.
func (h *Histogram) Tick() {
	h.windowed.Tick()
}

// RecordValue adds the given value to the histogram.
func (h *Histogram) RecordValue(n int64) {
	v := float64(n)
	b := h.cum.FindBucket(v)
	h.cum.ObserveInternal(v, b)

	h.windowed.cur.Load().(prometheus.HistogramInternal).ObserveInternal(v, b)
}

// GetType returns the prometheus type enum for this metric.
func (h *Histogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (h *Histogram) ToPrometheusMetric() *prometheusgo.Metric {
	m := &prometheusgo.Metric{}
	if err := h.cum.Write(m); err != nil {
		panic(err)
	}
	return m
}

func (h *Histogram) CumulativeSnapshot() HistogramSnapshot {
	return MakeHistogramSnapshot(h.ToPrometheusMetric().Histogram)
}

func (h *Histogram) WindowedSnapshot() HistogramSnapshot {
	h.windowed.Lock()
	defer h.windowed.Unlock()
	cur := h.windowed.cur.Load().(prometheus.Histogram)
	// Can't cast here since prev might be nil.
	prev := h.windowed.prev.Load()

	curMetric := &prometheusgo.Metric{}
	if err := cur.Write(curMetric); err != nil {
		panic(err)
	}
	if prev != nil {
		prevMetric := &prometheusgo.Metric{}
		if err := prev.(prometheus.Histogram).Write(prevMetric); err != nil {
			panic(err)
		}
		MergeWindowedHistogram(curMetric.Histogram, prevMetric.Histogram)
	}
	return MakeHistogramSnapshot(curMetric.Histogram)
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *Histogram) GetMetadata() Metadata {
	return h.Metadata
}

// Inspect calls the closure.
func (h *Histogram) Inspect(f func(interface{})) {
	func() {
		tick.MaybeTick(&h.windowed)
	}()
	f(h)
}

var _ PrometheusExportable = (*ManualWindowHistogram)(nil)
var _ Iterable = (*ManualWindowHistogram)(nil)
var _ WindowedHistogram = (*ManualWindowHistogram)(nil)
var _ CumulativeHistogram = (*ManualWindowHistogram)(nil)

// NewManualWindowHistogram is a prometheus-backed histogram. Depending on the
// value of the buckets parameter, this is suitable for recording any kind of
// quantity. The histogram is very similar to Histogram produced by
// NewHistogram with the main difference being that Histogram supports
// collecting values over time using the Histogram.RecordValue whereas this
// histogram provides limited support RecordValue, the caller is responsible
// for calling Rotate, after recording is complete or manually providing the
// cumulative and current windowed histogram via Update. This means that it is
// the responsibility of the creator of this histogram to replace the values by
// either calling ManualWindowHistogram.Update or
// ManualWindowHistogram.RecordValue and ManualWindowHistogram.Rotate. If
// NewManualWindowHistogram is called withRotate as true, only the RecordValue
// and Rotate method may be used; withRotate as false, only Update may be used.
//
// TODO(kvoli,aadityasondhi): The two ways to use this histogram is a hack and
// "temporary", rationalize the interface. Tracked in #98622.
// TODO(aaditya): A tracking issue to overhaul the histogram interfaces into a
// more coherent one: #116584.
func NewManualWindowHistogram(
	meta Metadata, buckets []float64, manualRotate bool,
) *ManualWindowHistogram {
	opts := prometheus.HistogramOpts{
		Buckets: buckets,
	}
	cum := prometheus.NewHistogram(opts)
	// We initialize the histogram with the same bucket bounds as the cumulative
	// histogram.
	prev := &prometheusgo.Metric{}
	if err := cum.Write(prev); err != nil {
		panic(err.Error())
	}
	cur := &prometheusgo.Metric{}
	if err := cum.Write(cur); err != nil {
		panic(err.Error())
	}

	meta.MetricType = prometheusgo.MetricType_HISTOGRAM
	h := &ManualWindowHistogram{
		Metadata: meta,
	}
	h.mu.disableTick = manualRotate
	h.mu.cum = cum
	h.mu.cur = cur.GetHistogram()
	h.mu.prev = prev.GetHistogram()
	// If the caller specifies that it will not manually control rotating the
	// histogram, it will use the ticker in the same way as metric.Histogram does.
	if !manualRotate {
		h.mu.Ticker = tick.NewTicker(
			now(),
			// We want to divide the total window duration by the number of windows
			// because we need to rotate the windows at uniformly distributed
			// intervals within a histogram's total duration.
			60*time.Second/WindowedHistogramWrapNum,
			func() {
				// This is called while holding a mutex prior to calling Tick().
				newH := &prometheusgo.Metric{}
				h.mu.prev = h.mu.cur
				// Initialize the histogram with the same bucket bounds as original.
				if err := prometheus.NewHistogram(opts).Write(newH); err != nil {
					panic(err.Error())
				}
				h.mu.cur = newH.GetHistogram()
			})
	}
	return h
}

// ManualWindowHistogram is a prometheus-backed histogram. Internally there are
// three sets of histograms: one is the cumulative set (i.e. data is never
// evicted) which is a prometheus.Histogram, the cumulative histogram value
// when last rotated and the current histogram, which is windowed. Both the
// previous and current histograms are prometheusgo.Histograms. Both histograms
// must be updated by the client by calling either ManualWindowHistogram.Update
// or ManualWindowHistogram.RecordValue and subsequently Rotate.
type ManualWindowHistogram struct {
	Metadata

	mu struct {
		// prometheus.Histogram is thread safe, so we only need an RLock to
		// RecordValue. When calling Update or Rotate, we require a WLock since we
		// swap out fields.
		syncutil.RWMutex
		*tick.Ticker
		disableTick bool
		cum         prometheus.Histogram
		prev, cur   *prometheusgo.Histogram
	}
}

// Update replaces the cumulative histogram and adds the new current values to
// the previous ones.
func (mwh *ManualWindowHistogram) Update(cum prometheus.Histogram, cur *prometheusgo.Histogram) {
	mwh.mu.Lock()
	defer mwh.mu.Unlock()

	if mwh.mu.disableTick {
		panic("Unexpected call to Update with manual rotate enabled")
	}

	mwh.mu.cum = cum
	// Add the new values to the current histogram.
	MergeWindowedHistogram(mwh.mu.cur, cur)
}

// RecordValue records a value to the cumulative histogram. The value is only
// added to the current window histogram once Rotate is called.
func (mwh *ManualWindowHistogram) RecordValue(val float64) {
	mwh.mu.RLock()
	defer mwh.mu.RUnlock()

	if !mwh.mu.disableTick {
		panic("Unexpected call to RecordValue with manual rotate disabled")
	}
	mwh.mu.cum.Observe(val)
}

// SubtractPrometheusHistograms subtracts the prev histogram from the cur
// histogram, in place modifying the cur histogram. The bucket boundaries must
// be identical for both prev and cur.
func SubtractPrometheusHistograms(cur *prometheusgo.Histogram, prev *prometheusgo.Histogram) {
	prevBuckets := prev.GetBucket()
	curBuckets := cur.GetBucket()

	*cur.SampleCount -= prev.GetSampleCount()
	*cur.SampleSum -= prev.GetSampleSum()

	for idx, v := range prevBuckets {
		if *curBuckets[idx].UpperBound != *v.UpperBound {
			panic("Bucket Upperbounds don't match")
		}
		*curBuckets[idx].CumulativeCount -= *v.CumulativeCount
	}
}

// Rotate sets the current windowed histogram (cur) to be the delta of the
// cumulative histogram at the last rotation (prev) and the cumulative
// histogram currently (cum).
func (mwh *ManualWindowHistogram) Rotate() error {
	mwh.mu.Lock()
	defer mwh.mu.Unlock()

	if !mwh.mu.disableTick {
		panic("Unexpected call to Rotate with manual rotate disabled")
	}

	cur := &prometheusgo.Metric{}
	if err := mwh.mu.cum.Write(cur); err != nil {
		return err
	}

	SubtractPrometheusHistograms(cur.GetHistogram(), mwh.mu.prev)
	mwh.mu.cur = cur.GetHistogram()
	prev := &prometheusgo.Metric{}

	if err := mwh.mu.cum.Write(prev); err != nil {
		return err
	}
	mwh.mu.prev = prev.GetHistogram()

	return nil
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (mwh *ManualWindowHistogram) GetMetadata() Metadata {
	return mwh.Metadata
}

// Inspect calls the closure.
func (mwh *ManualWindowHistogram) Inspect(f func(interface{})) {
	if !mwh.mu.disableTick {
		func() {
			mwh.mu.Lock()
			defer mwh.mu.Unlock()
			tick.MaybeTick(&mwh.mu)
		}()
	}
	f(mwh)
}

// GetType returns the prometheus type enum for this metric.
func (mwh *ManualWindowHistogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (mwh *ManualWindowHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	mwh.mu.RLock()
	defer mwh.mu.RUnlock()

	m := &prometheusgo.Metric{}
	if err := mwh.mu.cum.Write(m); err != nil {
		panic(err)
	}
	return m
}

func (mwh *ManualWindowHistogram) CumulativeSnapshot() HistogramSnapshot {
	return MakeHistogramSnapshot(mwh.ToPrometheusMetric().Histogram)
}

func (mwh *ManualWindowHistogram) WindowedSnapshot() HistogramSnapshot {
	mwh.mu.RLock()
	defer mwh.mu.RUnlock()
	// Take a copy of the mwh.mu.cur.
	cur := deepCopy(*mwh.mu.cur)
	if mwh.mu.prev != nil {
		MergeWindowedHistogram(cur, mwh.mu.prev)
	}
	return MakeHistogramSnapshot(cur)
}

// deepCopy performs a deep copy of the source histogram and returns the newly
// allocated copy.
//
// NB: It only copies sample count, sample sum, and buckets (cumulative count,
// upper bounds) since those are the only things we care about in this package.
func deepCopy(source prometheusgo.Histogram) *prometheusgo.Histogram {
	count := source.GetSampleCount()
	sum := source.GetSampleSum()
	bucket := make([]*prometheusgo.Bucket, len(source.Bucket))

	for i := range bucket {
		cumCount := source.Bucket[i].GetCumulativeCount()
		upperBound := source.Bucket[i].GetUpperBound()
		bucket[i] = &prometheusgo.Bucket{
			CumulativeCount: &cumCount,
			UpperBound:      &upperBound,
		}
	}
	return &prometheusgo.Histogram{
		SampleCount: &count,
		SampleSum:   &sum,
		Bucket:      bucket,
	}
}

// A Counter holds a single mutable atomic value.
type Counter struct {
	Metadata

	count atomic.Int64
}

// NewCounter creates a counter.
func NewCounter(metadata Metadata) *Counter {
	return &Counter{Metadata: metadata}
}

// Clear resets the counter to zero.
func (c *Counter) Clear() {
	c.count.Store(0)
}

// Inc atomically increments the counter by the given value.
func (c *Counter) Inc(v int64) {
	if buildutil.CrdbTestBuild && v < 0 {
		panic("Counters should not be decremented")
	}
	c.count.Add(v)
}

// Update atomically sets the current value of the counter. The value must not
// be smaller than the existing value.
//
// Update is intended to be used when the counter itself is not the source of
// truth; instead it is a (periodically updated) copy of a counter that is
// maintained elsewhere.
func (c *Counter) Update(val int64) {
	if buildutil.CrdbTestBuild {
		if prev := c.count.Load(); val < prev && val != 0 {
			panic(fmt.Sprintf("Counters should not decrease, prev: %d, new: %d.", prev, val))
		}
	}
	c.count.Store(val)
}

// UpdateIfHigher atomically sets the current value of the counter, unless the
// current value is already greater.
func (c *Counter) UpdateIfHigher(val int64) {
	for {
		old := c.count.Load()
		if old > val {
			return
		}
		if c.count.CompareAndSwap(old, val) {
			return
		}
	}
}

// Count returns the current value of the counter.
func (c *Counter) Count() int64 {
	return c.count.Load()
}

// GetType returns the prometheus type enum for this metric.
func (c *Counter) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// Inspect calls the given closure with itself.
func (c *Counter) Inspect(f func(interface{})) { f(c) }

// MarshalJSON marshals to JSON.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Count())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (c *Counter) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Counter: &prometheusgo.Counter{Value: proto.Float64(float64(c.Count()))},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (c *Counter) GetMetadata() Metadata {
	baseMetadata := c.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_COUNTER
	return baseMetadata
}

// UniqueCounter performs set cardinality estimation. You feed keys,
// and it produces an estimate of how many unique keys its has seen.
type UniqueCounter struct {
	Metadata

	mu struct {
		syncutil.Mutex
		sketch *hyperloglog.Sketch
	}
}

// NewUniqueCounter creates a counter.
func NewUniqueCounter(metadata Metadata) *UniqueCounter {
	ret := &UniqueCounter{
		Metadata: metadata,
	}
	ret.Clear()
	return ret
}

// Clear resets the counter to zero.
func (c *UniqueCounter) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.sketch, _ = hyperloglog.NewSketch(14, true)
}

// Add a value to the set
func (c *UniqueCounter) Add(v []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.sketch.Insert(v)
}

// Count returns the current value of the counter.
func (c *UniqueCounter) Count() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return int64(c.mu.sketch.Estimate())
}

// GetType returns the prometheus type enum for this metric.
func (c *UniqueCounter) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// Inspect calls the given closure with itself.
func (c *UniqueCounter) Inspect(f func(interface{})) { f(c) }

// MarshalJSON marshals to JSON.
func (c *UniqueCounter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Count())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (c *UniqueCounter) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Counter: &prometheusgo.Counter{Value: proto.Float64(float64(c.Count()))},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (c *UniqueCounter) GetMetadata() Metadata {
	baseMetadata := c.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_COUNTER
	return baseMetadata
}

type CounterFloat64 struct {
	Metadata
	count syncutil.AtomicFloat64
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (c *CounterFloat64) GetMetadata() Metadata {
	baseMetadata := c.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_COUNTER
	return baseMetadata
}

func (c *CounterFloat64) Clear() {
	c.count.Store(0)
}

func (c *CounterFloat64) Count() float64 {
	return c.count.Load()
}

func (c *CounterFloat64) Inc(i float64) {
	if buildutil.CrdbTestBuild && i < 0 {
		panic("Counters should not be decremented")
	}
	c.count.Add(i)
}

// UpdateIfHigher atomically sets the current value of the counter, unless the
// current value is already greater.
func (c *CounterFloat64) UpdateIfHigher(i float64) (old float64, updated bool) {
	return c.count.StoreIfHigher(i)
}

func (c *CounterFloat64) Snapshot() *CounterFloat64 {
	newCounter := NewCounterFloat64(c.Metadata)
	newCounter.count.Store(c.Count())
	return newCounter
}

// GetType returns the prometheus type enum for this metric.
func (c *CounterFloat64) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// Inspect calls the given closure with the empty string and itself.
func (c *CounterFloat64) Inspect(f func(interface{})) { f(c) }

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (c *CounterFloat64) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Counter: &prometheusgo.Counter{Value: proto.Float64(c.Count())},
	}
}

// MarshalJSON marshals to JSON.
func (c *CounterFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Count())
}

// NewCounterFloat64 creates a counter.
func NewCounterFloat64(metadata Metadata) *CounterFloat64 {
	return &CounterFloat64{Metadata: metadata}
}

// A Gauge atomically stores a single integer value.
type Gauge struct {
	Metadata
	value atomic.Int64
	fn    func(val int64) int64
}

// NewGauge creates a Gauge.
func NewGauge(metadata Metadata) *Gauge {
	return &Gauge{Metadata: metadata}
}

// DerivedGauge is a Gauge whose value is derived from the callback function it
// was constructed with. The callback is called every time the value of the
// gauge is requested and is passed the current value of the gauge as an argument.
type DerivedGauge = Gauge

func NewDerivedGauge(metadata Metadata, f func(val int64) int64) *DerivedGauge {
	return &DerivedGauge{Metadata: metadata, fn: f}
}

// Update updates the gauge's value.
func (g *Gauge) Update(v int64) {
	g.value.Store(v)
}

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 {
	if g.fn != nil {
		return g.fn(g.value.Load())
	}
	return g.value.Load()
}

// Inc increments the gauge's value.
func (g *Gauge) Inc(i int64) {
	g.value.Add(i)
}

// Dec decrements the gauge's value.
func (g *Gauge) Dec(i int64) {
	g.value.Add(-i)
}

// GetType returns the prometheus type enum for this metric.
func (g *Gauge) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with the empty string and itself.
func (g *Gauge) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Value())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (g *Gauge) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(float64(g.Value()))},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (g *Gauge) GetMetadata() Metadata {
	baseMetadata := g.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_GAUGE
	return baseMetadata
}

// A GaugeFloat64 atomically stores a single float64 value.
type GaugeFloat64 struct {
	Metadata
	value syncutil.AtomicFloat64
}

// NewGaugeFloat64 creates a GaugeFloat64.
func NewGaugeFloat64(metadata Metadata) *GaugeFloat64 {
	return &GaugeFloat64{Metadata: metadata}
}

// Update updates the gauge's value.
func (g *GaugeFloat64) Update(v float64) {
	g.value.Store(v)
}

// Value returns the gauge's current value.
func (g *GaugeFloat64) Value() float64 {
	return g.value.Load()
}

// Inc increments the gauge's value.
func (g *GaugeFloat64) Inc(delta float64) {
	g.value.Add(delta)
}

// Dec decrements the gauge's value.
func (g *GaugeFloat64) Dec(delta float64) {
	g.value.Add(-delta)
}

// GetType returns the prometheus type enum for this metric.
func (g *GaugeFloat64) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with itself.
func (g *GaugeFloat64) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *GaugeFloat64) MarshalJSON() ([]byte, error) {
	v := g.Value()
	if math.IsInf(v, 0) || math.IsNaN(v) {
		v = 0
	}
	return json.Marshal(v)
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (g *GaugeFloat64) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(g.Value())},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (g *GaugeFloat64) GetMetadata() Metadata {
	baseMetadata := g.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_GAUGE
	return baseMetadata
}

// FunctionalGauge is a Gauge whose value is the result of the callback
// function it was constructed with. The callback is called every time the
// value of the gauge is requested.
type FunctionalGauge struct {
	Metadata
	fn func() int64
}

func NewFunctionalGauge(metadata Metadata, f func() int64) *FunctionalGauge {
	return &FunctionalGauge{Metadata: metadata, fn: f}
}

// GetType implements PrometheusExportable.
func (fg *FunctionalGauge) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// ToPrometheusMetric implements PrometheusExportable.
func (fg *FunctionalGauge) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Gauge: &prometheusgo.Gauge{Value: proto.Float64(float64(fg.Value()))},
	}
}

// GetMetadata implements Iterable.
func (fg *FunctionalGauge) GetMetadata() Metadata {
	baseMetadata := fg.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_GAUGE
	return baseMetadata
}

// Inspect implements Iterable.
func (fg *FunctionalGauge) Inspect(f func(interface{})) {
	f(fg)
}

// MarshalJSON implements json.Marshaler.
func (fg *FunctionalGauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(fg.Value())
}

// Value returns the functional value of the gauge, which is computed by
// by calling its callback function.
func (fg *FunctionalGauge) Value() int64 {
	return fg.fn()
}

// MergeWindowedHistogram adds the bucket counts, sample count, and sample sum
// from the previous windowed histogram to those of the current windowed
// histogram.
// NB: Buckets on each histogram must be the same
func MergeWindowedHistogram(cur *prometheusgo.Histogram, prev *prometheusgo.Histogram) {
	for i, bucket := range cur.Bucket {
		count := *bucket.CumulativeCount + *prev.Bucket[i].CumulativeCount
		*bucket.CumulativeCount = count
	}
	sampleCount := *cur.SampleCount + *prev.SampleCount
	*cur.SampleCount = sampleCount
	sampleSum := *cur.SampleSum + *prev.SampleSum
	*cur.SampleSum = sampleSum
}

// HistogramMetricComputer is the computation function used to compute and
// store histogram metrics into the internal TSDB, along with the suffix
// to be attached to the metric.
type HistogramMetricComputer struct {
	Suffix          string
	IsSummaryMetric bool
	ComputedMetric  func(h HistogramSnapshot) float64
}

// HistogramMetricComputers is a slice of all the HistogramMetricComputer
// that are used to record (windowed) histogram metrics into TSDB.
var HistogramMetricComputers = []HistogramMetricComputer{
	{
		Suffix:          "-max",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(100)
		},
	},
	{
		Suffix:          "-p99.999",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(99.999)
		},
	},
	{
		Suffix:          "-p99.99",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(99.99)
		},
	},
	{
		Suffix:          "-p99.9",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(99.9)
		},
	},
	{
		Suffix:          "-p99",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(99)
		},
	},
	{
		Suffix:          "-p90",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(90)
		},
	},
	{
		Suffix:          "-p75",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(75)
		},
	},
	{
		Suffix:          "-p50",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			return h.ValueAtQuantile(50)
		},
	},
	{
		Suffix:          "-avg",
		IsSummaryMetric: true,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			avg := h.Mean()
			if math.IsNaN(avg) || math.IsInf(avg, +1) || math.IsInf(avg, -1) {
				avg = 0
			}
			return avg
		},
	},
	{
		Suffix:          "-count",
		IsSummaryMetric: false,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			count, _ := h.Total()
			return float64(count)
		},
	},
	{
		Suffix:          "-sum",
		IsSummaryMetric: false,
		ComputedMetric: func(h HistogramSnapshot) float64 {
			_, sum := h.Total()
			return sum
		},
	},
}

// vector holds the base vector implementation. This is meant to be embedded
// by metric types that require a variable set of labels. Implements
// PrometheusVector.
type vector struct {
	*syncutil.RWMutex
	encounteredLabelsLookup map[string]struct{}
	encounteredLabelValues  [][]string
	orderedLabelNames       []string
}

func newVector(labelNames []string) vector {
	sort.Strings(labelNames)

	return vector{
		RWMutex:                 &syncutil.RWMutex{},
		encounteredLabelsLookup: make(map[string]struct{}),
		encounteredLabelValues:  [][]string{},
		orderedLabelNames:       labelNames,
	}
}

func (v *vector) getOrderedValues(labels map[string]string) []string {
	labelValues := make([]string, 0, len(labels))
	for _, labelName := range v.orderedLabelNames {
		labelValues = append(labelValues, labels[labelName])
	}

	return labelValues
}

// recordLabels records the given combination of label values if they haven't
// been seen before. This is used to iterate over all the counters created
// based on unique label combinations. recordLabels will return an error if the
// labelValues are novel, and the cardinality limit has been reached.
func (v *vector) recordLabels(labelValues []string) error {
	v.RLock()
	lookupKey := strings.Join(labelValues, "_")
	if _, ok := v.encounteredLabelsLookup[lookupKey]; ok {
		v.RUnlock()
		return nil
	}
	if len(v.encounteredLabelsLookup) >= CardinalityLimit {
		v.RUnlock()
		return fmt.Errorf("metric cardinality limit reached")
	}
	v.RUnlock()

	v.Lock()
	defer v.Unlock()
	v.encounteredLabelsLookup[lookupKey] = struct{}{}
	v.encounteredLabelValues = append(v.encounteredLabelValues, labelValues)
	return nil
}

// deleteLabels removes a specific label combination from the vector's
// tracking. This must be called alongside the prometheus vec's Delete to
// keep encounteredLabelValues in sync with the underlying metric store.
func (v *vector) deleteLabels(labels map[string]string) {
	labelValues := v.getOrderedValues(labels)
	lookupKey := strings.Join(labelValues, "_")

	v.Lock()
	defer v.Unlock()
	if _, ok := v.encounteredLabelsLookup[lookupKey]; !ok {
		return
	}
	delete(v.encounteredLabelsLookup, lookupKey)
	for i, lv := range v.encounteredLabelValues {
		if strings.Join(lv, "_") == lookupKey {
			v.encounteredLabelValues = append(
				v.encounteredLabelValues[:i],
				v.encounteredLabelValues[i+1:]...)
			break
		}
	}
}

// clear flushes the labels from the vector.
func (v *vector) clear() {
	v.Lock()
	defer v.Unlock()
	v.encounteredLabelsLookup = make(map[string]struct{})
	v.encounteredLabelValues = [][]string{}
}

// GaugeVec is a collector for gauges that have a variable set of labels.
// This uses the prometheus.GaugeVec under the hood. The contained gauges are
// not persisted by the internal TSDB, nor are they aggregated; see aggmetric
// for a metric that allows keeping labeled submetrics while recording their
// aggregation in the tsdb.
type GaugeVec struct {
	Metadata
	vector
	promVec *prometheus.GaugeVec
	fn      func(int64) int64
}

// DerivedGaugeVec is a GaugeVec whose value is derived from the callback
// function it was constructed with. The callback is called every time the
// value of the gauge is requested and is passed the current value of the gauge
// as an argument.
type DerivedGaugeVec = GaugeVec

// NewExportedGaugeVec creates a new GaugeVec containing labeled gauges to be
// exported to an external collector, but is not persisted by the internal TSDB,
// nor are the metrics in the vector aggregated in any way.
func NewExportedGaugeVec(metadata Metadata, labelSchema []string) *GaugeVec {
	return newExportedGaugeVec(metadata, labelSchema, nil)
}

// NewDerivedExportedGaugeVec creates a new DerivedGaugeVec containing labeled
// gauges to be exported to an external collector, but is not persisted by the
// internal TSDB, nor are the metrics in the vector aggregated in any way.
func NewDerivedExportedGaugeVec(
	metadata Metadata, labelSchema []string, fn func(int64) int64,
) *DerivedGaugeVec {
	return newExportedGaugeVec(metadata, labelSchema, fn)
}

func newExportedGaugeVec(metadata Metadata, labelSchema []string, fn func(int64) int64) *GaugeVec {
	vec := newVector(labelSchema)

	promVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metadata.Name,
		Help: metadata.Help,
	}, vec.orderedLabelNames)

	return &GaugeVec{
		Metadata: metadata,
		vector:   vec,
		promVec:  promVec,
		fn:       fn,
	}
}

// Delete removes the metric with the given label set from the vector.
// Returns true if a metric was deleted.
func (gv *GaugeVec) Delete(labels map[string]string) bool {
	gv.deleteLabels(labels)
	return gv.promVec.Delete(labels)
}

// Update updates the gauge value for the given combination of labels.
func (gv *GaugeVec) Update(labels map[string]string, v int64) {
	labelValues := gv.getOrderedValues(labels)
	if err := gv.recordLabels(labelValues); err != nil {
		return
	}
	gv.promVec.WithLabelValues(labelValues...).Set(float64(v))
}

// Inc increments the gauge value for the given combination of labels.
func (gv *GaugeVec) Inc(labels map[string]string, v int64) {
	labelValues := gv.getOrderedValues(labels)
	if err := gv.recordLabels(labelValues); err != nil {
		return
	}
	gv.promVec.WithLabelValues(labelValues...).Add(float64(v))
}

// Dec decrements the gauge value for the given combination of labels.
func (gv *GaugeVec) Dec(labels map[string]string, v int64) {
	labelValues := gv.getOrderedValues(labels)
	if err := gv.recordLabels(labelValues); err != nil {
		return
	}
	gv.promVec.WithLabelValues(labelValues...).Sub(float64(v))
}

// GetMetadata implements Iterable.
func (gv *GaugeVec) GetMetadata() Metadata {
	md := gv.Metadata
	md.MetricType = prometheusgo.MetricType_GAUGE
	return md
}

// Inspect implements Iterable.
func (gv *GaugeVec) Inspect(f func(interface{})) { f(gv) }

// GetType implements PrometheusExportable.
func (gv *GaugeVec) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// ToPrometheusMetrics implements PrometheusExportable.
func (gv *GaugeVec) ToPrometheusMetrics() []*prometheusgo.Metric {
	gv.RLock()
	defer gv.RUnlock()
	metrics := make([]*prometheusgo.Metric, 0, len(gv.encounteredLabelValues))

	for _, labels := range gv.encounteredLabelValues {
		m := &prometheusgo.Metric{}
		g := gv.promVec.WithLabelValues(labels...)

		if err := g.Write(m); err != nil {
			panic(err)
		}

		if gv.fn != nil {
			*m.Gauge.Value = float64(gv.fn(int64(*m.Gauge.Value)))
		}

		metrics = append(metrics, m)
	}

	return metrics
}

// Clear removes all child gauges and resets the label tracking,
// preserving the metadata and configuration.
func (gv *GaugeVec) Clear() {
	gv.vector.clear()
	gv.promVec.Reset()
}

// CounterVec wraps a prometheus.CounterVec; it is not aggregated or persisted.
type CounterVec struct {
	Metadata
	vector
	promVec *prometheus.CounterVec
}

// NewExportedCounterVec creates a new CounterVec containing labeled counters to
// be exported to an external collector; the contained counters are not
// aggregated or persisted to the tsdb (see aggmetric.Counter for a counter that
// persists the aggregation of n labeled child metrics).
func NewExportedCounterVec(metadata Metadata, labelNames []string) *CounterVec {
	vec := newVector(labelNames)

	promVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metadata.Name,
		Help: metadata.Help,
	}, vec.orderedLabelNames)

	return &CounterVec{
		Metadata: metadata,
		vector:   vec,
		promVec:  promVec,
	}
}

// Delete removes the metric with the given label set from the vector.
// Returns true if a metric was deleted.
func (cv *CounterVec) Delete(labels map[string]string) bool {
	cv.deleteLabels(labels)
	return cv.promVec.Delete(labels)
}

// Update updates the counter value for the given combination of labels.
// prometheus.CounterVec does not support an Update method, so we have to
// implement it ourselves by getting the current counter value and adding the
// difference. This panics if the current value is greater than the new value.
func (cv *CounterVec) Update(labels map[string]string, v int64) {
	labelValues := cv.getOrderedValues(labels)
	if err := cv.recordLabels(labelValues); err != nil {
		return
	}

	currentValue := cv.Count(labels)
	if currentValue > v && v != 0 {
		panic(fmt.Sprintf("Counters should not decrease, prev: %d, new: %d.", currentValue, v))
	}

	cv.promVec.WithLabelValues(labelValues...).Add(float64(v - currentValue))
}

// Inc increments the value for the given combination of labels.
func (cv *CounterVec) Inc(labels map[string]string, v int64) {
	labelValues := cv.getOrderedValues(labels)
	if err := cv.recordLabels(labelValues); err != nil {
		return
	}
	cv.promVec.WithLabelValues(labelValues...).Add(float64(v))
}

// Count returns the current value of the counter for the given combination of
// labels.
func (cv *CounterVec) Count(labels map[string]string) int64 {
	m := prometheusgo.Metric{}
	labelValues := cv.getOrderedValues(labels)
	if err := cv.promVec.WithLabelValues(labelValues...).Write(&m); err != nil {
		panic(err)
	}

	return int64(m.Counter.GetValue())
}

// GetMetadata implements Iterable.
func (cv *CounterVec) GetMetadata() Metadata {
	md := cv.Metadata
	md.MetricType = prometheusgo.MetricType_COUNTER
	return md
}

// Inspect implements Iterable.
func (cv *CounterVec) Inspect(f func(interface{})) { f(cv) }

// GetType implements PrometheusExportable.
func (cv *CounterVec) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// ToPrometheusMetrics implements PrometheusExportable.
func (cv *CounterVec) ToPrometheusMetrics() []*prometheusgo.Metric {
	cv.RLock()
	defer cv.RUnlock()
	metrics := make([]*prometheusgo.Metric, 0, len(cv.encounteredLabelValues))

	for _, labels := range cv.encounteredLabelValues {
		m := &prometheusgo.Metric{}
		c := cv.promVec.WithLabelValues(labels...)

		if err := c.Write(m); err != nil {
			panic(err)
		}

		metrics = append(metrics, m)
	}

	return metrics
}

// Clear removes all child counters and resets the label tracking,
// preserving the metadata and configuration.
func (cv *CounterVec) Clear() {
	cv.vector.clear()
	cv.promVec.Reset()
}

// HistogramVec wraps a prometheus.HistogramVec; it is not aggregated or persisted.
type HistogramVec struct {
	Metadata
	vector
	promVec *prometheus.HistogramVec
}

// NewExportedHistogramVec creates a new HistogramVec containing labeled counters to
// be exported to an external collector; the contained histograms are not
// aggregated or persisted to the tsdb (see aggmetric.Histogram for a counter that
// persists the aggregation of n labeled child metrics).
func NewExportedHistogramVec(
	metadata Metadata, bucketConfig staticBucketConfig, labelNames []string,
) *HistogramVec {
	vec := newVector(labelNames)
	opts := prometheus.HistogramOpts{
		Buckets: bucketConfig.GetBucketsFromBucketConfig(),
		Name:    metadata.Name,
		Help:    metadata.Help,
	}
	promVec := prometheus.NewHistogramVec(opts, vec.orderedLabelNames)
	return &HistogramVec{
		Metadata: metadata,
		vector:   vec,
		promVec:  promVec,
	}
}

// Observe adds invokes prometheus.Observer Observe function for the given
// combination of labels.
func (hv *HistogramVec) Observe(labels map[string]string, v float64) {
	labelValues := hv.getOrderedValues(labels)
	if err := hv.recordLabels(labelValues); err != nil {
		return
	}
	hv.promVec.WithLabelValues(labelValues...).Observe(v)
}

// Clear removes all the metrics and the label vector, preserving the metadata and configuration.
func (hv *HistogramVec) Clear() {
	hv.vector.clear()
	hv.promVec.Reset()
}

// GetMetadata implements Iterable.
func (hv *HistogramVec) GetMetadata() Metadata {
	md := hv.Metadata
	md.MetricType = prometheusgo.MetricType_HISTOGRAM
	return md
}

// Inspect implements Iterable.
func (hv *HistogramVec) Inspect(f func(interface{})) { f(hv) }

// GetType implements PrometheusExportable.
func (hv *HistogramVec) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// Delete removes the metric with the given label set from the vector.
// Returns true if a metric was deleted.
func (hv *HistogramVec) Delete(labels map[string]string) bool {
	hv.deleteLabels(labels)
	return hv.promVec.Delete(labels)
}

// ToPrometheusMetrics implements PrometheusExportable.
func (hv *HistogramVec) ToPrometheusMetrics() []*prometheusgo.Metric {
	hv.RLock()
	defer hv.RUnlock()
	metrics := make([]*prometheusgo.Metric, 0, len(hv.encounteredLabelValues))

	for _, labels := range hv.encounteredLabelValues {
		m := &prometheusgo.Metric{}
		o := hv.promVec.WithLabelValues(labels...)
		histogram, ok := o.(prometheus.Histogram)
		if !ok {
			log.Dev.Errorf(context.TODO(), "Unable to convert Observer to prometheus.Histogram. Metric name=%s", hv.Name)
			continue
		}
		if err := histogram.Write(m); err != nil {
			panic(err)
		}

		metrics = append(metrics, m)
	}

	return metrics
}

func MakeLabelPairs(labelNamesAndValues ...string) []*LabelPair {
	if len(labelNamesAndValues)%2 != 0 {
		panic("labelNamesAndValues must be a list with even length of label names and values")
	}
	labelPairs := make([]*LabelPair, 0, len(labelNamesAndValues)/2)
	for i := 0; i < len(labelNamesAndValues); i += 2 {
		labelPairs = append(labelPairs, &LabelPair{
			Name:  &labelNamesAndValues[i],
			Value: &labelNamesAndValues[i+1],
		})
	}
	return labelPairs
}
