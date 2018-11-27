// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package telemetry

import (
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Bucket10 buckets a number by order of magnitude base 10, eg 637 -> 100.
// This can be used in telemetry to get ballpark ideas of how users use a given
// feature, such as file sizes, qps, etc, without being as revealing as the
// raw numbers.
// The numbers 0-10 are reported unchanged.
func Bucket10(num int64) int64 {
	if num <= 0 {
		return 0
	}
	if num < 10 {
		return num
	}
	res := int64(10)
	for ; res < 1000000000000000000 && res*10 < num; res *= 10 {
	}
	return res
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

// GetCounter returns a counter from the global registry.
func GetCounter(feature string) Counter {
	counters.RLock()
	i, ok := counters.m[feature]
	counters.RUnlock()

	if !ok {
		counters.Lock()
		var n int32
		counters.m[feature] = &n
		i = &n
		counters.Unlock()
	}
	return i
}

func init() {
	counters.m = make(map[string]Counter, approxFeatureCount)
}

var approxFeatureCount = 100

// counters stores the registry of feature-usage counts.
// TODO(dt): consider a lock-free map.
var counters struct {
	syncutil.RWMutex
	m map[string]Counter
}

// GetFeatureCounts returns the current feature usage counts. Used for
// inspection via SQL. They are not quantized! Thus not suitable for
// reporting.
func GetFeatureCounts() map[string]int32 {
	counters.RLock()
	defer counters.RUnlock()
	m := make(map[string]int32, len(counters.m))
	for k, cnt := range counters.m {
		m[k] = atomic.LoadInt32(cnt)
	}
	return m
}

// GetAndResetFeatureCounts returns the current feature usage counts and resets
// the counts for all features back to 0. If `quantize` is true, the returned
// counts are quantized to just order of magnitude using the `Bucket10` helper.
func GetAndResetFeatureCounts(quantize bool) map[string]int32 {
	counters.RLock()
	m := make(map[string]int32, len(counters.m))
	for k, cnt := range counters.m {
		val := atomic.SwapInt32(cnt, 0)
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

// RecordError takes an error and increments the corresponding count
// for its error code, and, if it is an unimplemented or internal
// error, the count for that feature or the internal error's shortened
// stack trace.
func RecordError(err error) {
	if err == nil {
		return
	}

	if pgErr, ok := pgerror.GetPGCause(err); ok {
		Count("errorcodes." + pgErr.Code)

		if details := pgErr.InternalCommand; details != "" {
			var prefix string
			switch pgErr.Code {
			case pgerror.CodeFeatureNotSupportedError:
				prefix = "unimplemented."
			case pgerror.CodeInternalError:
				prefix = "internalerror."
			default:
				prefix = "othererror." + pgErr.Code + "."
			}
			Count(prefix + details)
		}
	} else {
		typ := log.ErrorSource(err)
		if typ == "" {
			typ = "unknown"
		}
		Count("othererror." + typ)
	}
}
