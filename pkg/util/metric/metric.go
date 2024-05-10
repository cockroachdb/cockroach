// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"encoding/json"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric/tick"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/rcrowley/go-metrics"
)

const (
	// TestSampleInterval is passed to histograms during tests which don't
	// want to concern themselves with supplying a "correct" interval.
	TestSampleInterval = time.Duration(math.MaxInt64)
	// WindowedHistogramWrapNum is the number of histograms to keep in rolling
	// window.
	WindowedHistogramWrapNum = 2
)

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// GetName returns the fully-qualified name of the metric.
	GetName() string
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

// PrometheusExportable is the standard interface for an individual metric
// that can be exported to prometheus.
type PrometheusExportable interface {
	// GetName is a method on Metadata
	GetName() string
	// GetHelp is a method on Metadata
	GetHelp() string
	// GetType returns the prometheus type enum for this metric.
	GetType() *prometheusgo.MetricType
	// GetLabels is a method on Metadata
	GetLabels() []*prometheusgo.LabelPair
	// ToPrometheusMetric returns a filled-in prometheus metric of the right type
	// for the given metric. It does not fill in labels.
	// The implementation must return thread-safe data to the caller, i.e.
	// usually a copy of internal state.
	// NB: For histogram metrics, ToPrometheusMetric should return the cumulative histogram.
	ToPrometheusMetric() *prometheusgo.Metric
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

// GetName returns the metric's name.
func (m *Metadata) GetName() string {
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
func (m *Metadata) GetLabels() []*prometheusgo.LabelPair {
	lps := make([]*prometheusgo.LabelPair, len(m.Labels))
	// x satisfies the field XXX_unrecognized in prometheusgo.LabelPair.
	var x []byte
	for i, v := range m.Labels {
		lps[i] = &prometheusgo.LabelPair{Name: v.Name, Value: v.Value, XXX_unrecognized: x}
	}
	return lps
}

// AddLabel adds a label/value pair for this metric.
func (m *Metadata) AddLabel(name, value string) {
	m.Labels = append(m.Labels,
		&LabelPair{
			Name:  proto.String(exportedLabel(name)),
			Value: proto.String(value),
		})
}

var _ Iterable = &Gauge{}
var _ Iterable = &GaugeFloat64{}
var _ Iterable = &Counter{}
var _ Iterable = &CounterFloat64{}

var _ json.Marshaler = &Gauge{}
var _ json.Marshaler = &GaugeFloat64{}
var _ json.Marshaler = &Counter{}
var _ json.Marshaler = &CounterFloat64{}
var _ json.Marshaler = &Registry{}

var _ PrometheusExportable = &Gauge{}
var _ PrometheusExportable = &GaugeFloat64{}
var _ PrometheusExportable = &Counter{}
var _ PrometheusExportable = &CounterFloat64{}

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
			h.windowed.prev = h.windowed.cur
			h.windowed.cur = prometheus.NewHistogram(opts)
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
	cum prometheus.Histogram

	// TODO(obs-inf): the way we implement windowed histograms is not great.
	// We could "just" double the rotation interval (so that the histogram really
	// collects for 20s when we expect to persist the contents every 10s).
	// Really it would make more sense to explicitly rotate the histogram
	// atomically with collecting its contents, but that is now how we have set
	// it up right now. It should be doable though, since there is only one
	// consumer of windowed histograms - our internal timeseries system.
	windowed struct {
		// prometheus.Histogram is thread safe, so we only
		// need an RLock to record into it. But write lock
		// is held while rotating.
		syncutil.RWMutex
		*tick.Ticker
		prev, cur prometheus.Histogram
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
	h.windowed.RLock()
	defer h.windowed.RUnlock()
	return h.windowed.NextTick()
}

// Tick triggers a tick of this Histogram, regardless of whether we've passed
// the next tick interval. Generally, this should not be used by any caller other
// than aggmetric.AggHistogram. Future work will remove the need to expose this function
// as part of the public API.
// TODO(obs-infra): remove this once pkg/util/aggmetric is merged with this package.
func (h *Histogram) Tick() {
	h.windowed.Lock()
	defer h.windowed.Unlock()
	h.windowed.Tick()
}

// Windowed returns a copy of the current windowed histogram.
func (h *Histogram) Windowed() prometheus.Histogram {
	h.windowed.RLock()
	defer h.windowed.RUnlock()
	return h.windowed.cur
}

// RecordValue adds the given value to the histogram.
func (h *Histogram) RecordValue(n int64) {
	v := float64(n)
	h.cum.Observe(v)

	h.windowed.RLock()
	defer h.windowed.RUnlock()
	h.windowed.cur.Observe(v)
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
	cur := &prometheusgo.Metric{}
	prev := &prometheusgo.Metric{}
	if err := h.windowed.cur.Write(cur); err != nil {
		panic(err)
	}
	if h.windowed.prev != nil {
		if err := h.windowed.prev.Write(prev); err != nil {
			panic(err)
		}
		MergeWindowedHistogram(cur.Histogram, prev.Histogram)
	}
	return MakeHistogramSnapshot(cur.Histogram)
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *Histogram) GetMetadata() Metadata {
	return h.Metadata
}

// Inspect calls the closure.
func (h *Histogram) Inspect(f func(interface{})) {
	func() {
		h.windowed.Lock()
		defer h.windowed.Unlock()
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
	metrics.Counter
}

// NewCounter creates a counter.
func NewCounter(metadata Metadata) *Counter {
	return &Counter{metadata, metrics.NewCounter()}
}

// Dec overrides the metric.Counter method. This method should NOT be
// used and serves only to prevent misuse of the metric type.
func (c *Counter) Dec(int64) {
	// From https://prometheus.io/docs/concepts/metric_types/#counter
	// > Counters should not be used to expose current counts of items
	// > whose number can also go down, e.g. the number of currently
	// > running goroutines. Use gauges for this use case.
	panic("Counter should not be decremented, use a Gauge instead")
}

// GetType returns the prometheus type enum for this metric.
func (c *Counter) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_COUNTER.Enum()
}

// Inspect calls the given closure with the empty string and itself.
func (c *Counter) Inspect(f func(interface{})) { f(c) }

// MarshalJSON marshals to JSON.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Counter.Count())
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (c *Counter) ToPrometheusMetric() *prometheusgo.Metric {
	return &prometheusgo.Metric{
		Counter: &prometheusgo.Counter{Value: proto.Float64(float64(c.Counter.Count()))},
	}
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (c *Counter) GetMetadata() Metadata {
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
	syncutil.StoreFloat64(&c.count, 0)
}

func (c *CounterFloat64) Count() float64 {
	return syncutil.LoadFloat64(&c.count)
}

func (c *CounterFloat64) Inc(i float64) {
	syncutil.AddFloat64(&c.count, i)
}

func (c *CounterFloat64) UpdateIfHigher(i float64) {
	syncutil.StoreFloat64IfHigher(&c.count, i)
}

func (c *CounterFloat64) Snapshot() *CounterFloat64 {
	newCounter := NewCounterFloat64(c.Metadata)
	syncutil.StoreFloat64(&newCounter.count, c.Count())
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
	value *int64
	fn    func() int64
}

// NewGauge creates a Gauge.
func NewGauge(metadata Metadata) *Gauge {
	return &Gauge{metadata, new(int64), nil}
}

// NewFunctionalGauge creates a Gauge metric whose value is determined when
// asked for by calling the provided function.
// Note that Update, Inc, and Dec should NOT be called on a Gauge returned
// from NewFunctionalGauge.
func NewFunctionalGauge(metadata Metadata, f func() int64) *Gauge {
	return &Gauge{metadata, nil, f}
}

// Snapshot returns a read-only copy of the gauge.
func (g *Gauge) Snapshot() metrics.Gauge {
	return metrics.GaugeSnapshot(g.Value())
}

// Update updates the gauge's value.
func (g *Gauge) Update(v int64) {
	atomic.StoreInt64(g.value, v)
}

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 {
	if g.fn != nil {
		return g.fn()
	}
	return atomic.LoadInt64(g.value)
}

// Inc increments the gauge's value.
func (g *Gauge) Inc(i int64) {
	atomic.AddInt64(g.value, i)
}

// Dec decrements the gauge's value.
func (g *Gauge) Dec(i int64) {
	atomic.AddInt64(g.value, -i)
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
	bits *uint64
}

// NewGaugeFloat64 creates a GaugeFloat64.
func NewGaugeFloat64(metadata Metadata) *GaugeFloat64 {
	return &GaugeFloat64{metadata, new(uint64)}
}

// Snapshot returns a read-only copy of the gauge.
func (g *GaugeFloat64) Snapshot() metrics.GaugeFloat64 {
	return metrics.GaugeFloat64Snapshot(g.Value())
}

// Update updates the gauge's value.
func (g *GaugeFloat64) Update(v float64) {
	atomic.StoreUint64(g.bits, math.Float64bits(v))
}

// Value returns the gauge's current value.
func (g *GaugeFloat64) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(g.bits))
}

// Inc increments the gauge's value.
func (g *GaugeFloat64) Inc(delta float64) {
	for {
		oldBits := atomic.LoadUint64(g.bits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + delta)
		if atomic.CompareAndSwapUint64(g.bits, oldBits, newBits) {
			return
		}
	}
}

// Dec decrements the gauge's value.
func (g *GaugeFloat64) Dec(delta float64) {
	g.Inc(-delta)
}

// GetType returns the prometheus type enum for this metric.
func (g *GaugeFloat64) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_GAUGE.Enum()
}

// Inspect calls the given closure with itself.
func (g *GaugeFloat64) Inspect(f func(interface{})) { f(g) }

// MarshalJSON marshals to JSON.
func (g *GaugeFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Value())
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

// Quantile is a quantile along with a string suffix to be attached to the metric
// name upon recording into the internal TSDB.
type Quantile struct {
	Suffix   string
	Quantile float64
}

// RecordHistogramQuantiles are the quantiles at which (windowed) histograms
// are recorded into the internal TSDB.
var RecordHistogramQuantiles = []Quantile{
	{"-max", 100},
	{"-p99.999", 99.999},
	{"-p99.99", 99.99},
	{"-p99.9", 99.9},
	{"-p99", 99},
	{"-p90", 90},
	{"-p75", 75},
	{"-p50", 50},
}
