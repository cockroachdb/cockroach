// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package telemetry

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Bucket10 buckets a number by order of magnitude base 10, eg 637 -> 100.
// This can be used in telemetry to get ballpark ideas of how users use a given
// feature, such as file sizes, qps, etc, without being as revealing as the
// raw numbers.
// The numbers 0-10 are reported unchanged.
func Bucket10(num int64) int64 {
	if num == math.MinInt64 {
		// This is needed to prevent overflow in the negation below.
		return -1000000000000000000
	}
	sign := int64(1)
	if num < 0 {
		sign = -1
		num = -num
	}
	if num < 10 {
		return num * sign
	}
	res := int64(10)
	for ; res < 1000000000000000000 && res*10 <= num; res *= 10 {
	}
	return res * sign
}

// CountBucketed counts the feature identified by prefix and the value, using
// the bucketed value to pick a feature bucket to increment, e.g. a prefix of
// "foo.bar" and value of 632 would be counted as "foo.bar.100".
func CountBucketed(prefix string, value int64) {
	Count(fmt.Sprintf("%s.%d", prefix, Bucket10(value)))
}

// Count retrieves and increments the usage counter for the passed feature.
// High-volume callers may want to instead use `GetCounter` and hold on to the
// returned Counter between calls to Inc, to avoid contention in the registry.
func Count(feature string) {
	Inc(GetCounter(feature))
}

// Counter represents the usage counter for a given 'feature'.
type Counter *int32

// Inc increments the counter.
func Inc(c Counter) {
	atomic.AddInt32(c, 1)
}

// Read reads the current value of the counter.
func Read(c Counter) int32 {
	return atomic.LoadInt32(c)
}

// GetCounterOnce returns a counter from the global registry,
// and asserts it didn't exist previously.
func GetCounterOnce(feature string) Counter {
	counters.RLock()
	_, ok := counters.m[feature]
	counters.RUnlock()
	if ok {
		panic("counter already exists: " + feature)
	}
	return GetCounter(feature)
}

// GetCounter returns a counter from the global registry.
func GetCounter(feature string) Counter {
	counters.RLock()
	i, ok := counters.m[feature]
	counters.RUnlock()
	if ok {
		return i
	}

	counters.Lock()
	defer counters.Unlock()
	i, ok = counters.m[feature]
	if !ok {
		i = new(int32)
		counters.m[feature] = i
	}
	return i
}

// CounterWithMetric combines a telemetry and a metrics counter.
type CounterWithMetric struct {
	telemetry Counter
	metric    *metric.Counter
}

// Necessary for metric metadata registration.
var _ metric.Iterable = CounterWithMetric{}

// NewCounterWithMetric creates a CounterWithMetric.
func NewCounterWithMetric(metadata metric.Metadata) CounterWithMetric {
	return CounterWithMetric{
		telemetry: GetCounter(metadata.Name),
		metric:    metric.NewCounter(metadata),
	}
}

// Inc increments both counters.
func (c CounterWithMetric) Inc() {
	Inc(c.telemetry)
	c.metric.Inc(1)
}

// Forward the metric.Iterable interface to the metric counter. We
// don't just embed the counter because our Inc() interface is a bit
// different.

// GetName implements metric.Iterable
func (c CounterWithMetric) GetName() string {
	return c.metric.GetName()
}

// GetHelp implements metric.Iterable
func (c CounterWithMetric) GetHelp() string {
	return c.metric.GetHelp()
}

// GetMeasurement implements metric.Iterable
func (c CounterWithMetric) GetMeasurement() string {
	return c.metric.GetMeasurement()
}

// GetUnit implements metric.Iterable
func (c CounterWithMetric) GetUnit() metric.Unit {
	return c.metric.GetUnit()
}

// GetMetadata implements metric.Iterable
func (c CounterWithMetric) GetMetadata() metric.Metadata {
	return c.metric.GetMetadata()
}

// Inspect implements metric.Iterable
func (c CounterWithMetric) Inspect(f func(interface{})) {
	c.metric.Inspect(f)
}

func init() {
	counters.m = make(map[string]Counter, approxFeatureCount)
}

var approxFeatureCount = 1500

// counters stores the registry of feature-usage counts.
// TODO(dt): consider a lock-free map.
var counters struct {
	syncutil.RWMutex
	m map[string]Counter
}

// QuantizeCounts controls if counts are quantized when fetched.
type QuantizeCounts bool

// ResetCounters controls if counts are reset when fetched.
type ResetCounters bool

const (
	// Quantized returns counts quantized to order of magnitude.
	Quantized QuantizeCounts = true
	// Raw returns the raw, unquantized counter values.
	Raw QuantizeCounts = false
	// ResetCounts resets the counter to zero after fetching its value.
	ResetCounts ResetCounters = true
	// ReadOnly leaves the counter value unchanged when reading it.
	ReadOnly ResetCounters = false
)

// GetRawFeatureCounts returns current raw, un-quanitzed feature counter values.
func GetRawFeatureCounts() map[string]int32 {
	return GetFeatureCounts(Raw, ReadOnly)
}

// GetFeatureCounts returns the current feature usage counts.
//
// It optionally quantizes quantizes the returned counts to just order of
// magnitude using the `Bucket10` helper, and optionally resets the counters to
// zero i.e. if flushing accumulated counts during a report.
func GetFeatureCounts(quantize QuantizeCounts, reset ResetCounters) map[string]int32 {
	counters.RLock()
	m := make(map[string]int32, len(counters.m))
	for k, cnt := range counters.m {
		var val int32
		if reset {
			val = atomic.SwapInt32(cnt, 0)
		} else {
			val = atomic.LoadInt32(cnt)
		}
		if val != 0 {
			m[k] = val
		}
	}
	counters.RUnlock()
	if quantize {
		for k := range m {
			m[k] = int32(Bucket10(int64(m[k])))
		}
	}
	return m
}

// ValidationTelemetryKeyPrefix is the prefix of telemetry keys pertaining to
// descriptor validation failures.
const ValidationTelemetryKeyPrefix = "sql.schema.validation_errors."

// RecordError takes an error and increments the corresponding count
// for its error code, and, if it is an unimplemented or internal
// error, the count for that feature or the internal error's shortened
// stack trace.
func RecordError(err error) {
	if err == nil {
		return
	}

	code := pgerror.GetPGCode(err)
	Count("errorcodes." + code.String())

	tkeys := errors.GetTelemetryKeys(err)
	if len(tkeys) > 0 {
		var prefix string
		switch code {
		case pgcode.FeatureNotSupported:
			prefix = "unimplemented."
		case pgcode.Internal:
			prefix = "internalerror."
		default:
			prefix = "othererror." + code.String() + "."
		}
		for _, tk := range tkeys {
			prefixedTelemetryKey := prefix + tk
			if strings.HasPrefix(tk, ValidationTelemetryKeyPrefix) {
				// Descriptor validation errors already have their own prefixing scheme.
				prefixedTelemetryKey = tk
			}
			Count(prefixedTelemetryKey)
		}
	}
}
