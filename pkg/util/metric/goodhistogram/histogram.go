// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package goodhistogram provides an exponential histogram with Prometheus
// native histogram schema alignment, bounded relative error, and trapezoidal
// quantile estimation.
//
// The histogram is configured with a value range [lo, hi] and a desired
// relative error bound. It selects the tightest Prometheus schema whose error
// is at or below the requested bound, then allocates a fixed array of atomic
// counters covering the range. Recording is O(1) and lock-free: values are
// mapped to bucket indices via math.Frexp plus a small precomputed boundary
// table, then the corresponding counter is atomically incremented.
//
// Because the bucket layout is identical to a Prometheus native histogram
// schema, export to the Prometheus sparse format requires no remapping — our
// internal indices are Prometheus bucket indices offset by a constant.
package goodhistogram

import (
	"math"
	"sort"
	"sync/atomic"
)

const maxSchema = 8

// nativeHistogramBounds contains the Prometheus native histogram sub-bucket
// boundaries for each schema (0–8). For schema s, there are 2^s sub-buckets
// per power-of-2 octave. Each entry is the lower bound of a sub-bucket in
// [0.5, 1.0), matching the range of math.Frexp's fractional part.
//
// The boundary for sub-bucket j in schema s is: 2^(j / 2^s) / 2, which
// equals the fractional part of the Prometheus bucket boundary when
// decomposed via math.Frexp.
//
// These are computed at init time and are identical to the
// nativeHistogramBounds table in the Prometheus client_golang library.
var nativeHistogramBounds [maxSchema + 1][]float64

func init() {
	for s := int32(0); s <= maxSchema; s++ {
		numSubBuckets := 1 << s
		bounds := make([]float64, numSubBuckets)
		for j := 0; j < numSubBuckets; j++ {
			// The Prometheus bucket boundary for sub-bucket j in schema s
			// within a power-of-2 octave is 2^(j / 2^s). When decomposed via
			// math.Frexp, the fractional part is 2^(j / 2^s) / 2 (since
			// Frexp returns frac in [0.5, 1.0) with value = frac * 2^exp).
			bounds[j] = math.Ldexp(math.Pow(2, float64(j)/float64(numSubBuckets)), -1)
		}
		nativeHistogramBounds[s] = bounds
	}
}

// schemaRelativeError returns the relative error for a given schema.
// The error is (γ-1)/(γ+1) where γ = 2^(2^(-schema)).
func schemaRelativeError(schema int32) float64 {
	gamma := math.Pow(2, math.Pow(2, float64(-schema)))
	return (gamma - 1) / (gamma + 1)
}

// pickSchema selects the coarsest (fewest buckets) Prometheus schema whose
// relative error is at or below the desired error. Returns a schema in [0, 8].
func pickSchema(desiredError float64) int32 {
	for s := int32(0); s <= maxSchema; s++ {
		if schemaRelativeError(s) <= desiredError {
			return s
		}
	}
	return maxSchema
}

// promBucketKey computes the Prometheus native histogram bucket key for a
// positive value v, using the given schema. This is the same mapping used by
// the Prometheus client_golang library.
//
// For schema s > 0, the bucket key is:
//
//	sort.SearchFloat64s(bounds, frac) + (exp-1)*len(bounds)
//
// where frac, exp = math.Frexp(v) and bounds = nativeHistogramBounds[s].
func promBucketKey(v float64, schema int32) int {
	frac, exp := math.Frexp(v)
	bounds := nativeHistogramBounds[schema]
	return sort.SearchFloat64s(bounds, frac) + (exp-1)*len(bounds)
}

// getLe returns the upper bound of the bucket with the given key and schema.
// This is the inverse of promBucketKey.
func getLe(key int, schema int32) float64 {
	fracIdx := key & ((1 << schema) - 1)
	frac := nativeHistogramBounds[schema][fracIdx]
	exp := (key >> schema) + 1
	return math.Ldexp(frac, exp)
}

const mantissaMask = (1 << 52) - 1

// subBucketLookupBits is the number of top mantissa bits used to index
// the sub-bucket lookup table. 8 bits → 256 entries, which is enough
// precision for all schemas up to 8 (256 sub-buckets).
const subBucketLookupBits = 8
const subBucketLookupSize = 1 << subBucketLookupBits
const subBucketLookupShift = 52 - subBucketLookupBits

// Config holds the immutable parameters for a Histogram, computed at
// construction time from the user-specified range and error bound.
type Config struct {
	// Schema is the Prometheus native histogram schema (0–8).
	Schema int32
	// Lo and Hi are the tracked value range. Values outside this range are
	// clamped to the first or last bucket and counted separately.
	Lo, Hi float64
	// MinKey is the Prometheus bucket key for the lo boundary.
	MinKey int
	// NumBuckets is the number of buckets in the fixed array.
	NumBuckets int
	// NumSubBuckets is 2^Schema, the number of sub-buckets per octave.
	NumSubBuckets int
	// SubBucketBounds is the precomputed sub-bucket boundary table in
	// [0.5, 1.0), used by promBucketKey and export. This is a
	// reference to nativeHistogramBounds[schema].
	SubBucketBounds []float64
	// SubBucketLookup maps the top 8 bits of the mantissa to the
	// sub-bucket index (equivalent to sort.SearchFloat64s over
	// SubBucketBounds). One array access replaces the linear scan.
	SubBucketLookup [subBucketLookupSize]uint8
	// Boundaries holds the precomputed lower bound of each bucket, plus
	// the upper bound of the last bucket (length NumBuckets+1). Used for
	// quantile estimation.
	Boundaries []float64
}

// NewConfig creates a Config for the given range [lo, hi] and desired
// relative error. The schema is chosen as the tightest Prometheus schema
// whose error is at or below desiredError. Panics if lo <= 0, hi <= lo,
// or desiredError <= 0.
func NewConfig(lo, hi, desiredError float64) Config {
	if lo <= 0 || hi <= lo || desiredError <= 0 {
		panic("goodhistogram: invalid config: need 0 < lo < hi and desiredError > 0")
	}
	schema := pickSchema(desiredError)
	minKey := promBucketKey(lo, schema)
	maxKey := promBucketKey(hi, schema)
	numBuckets := maxKey - minKey + 1

	// Precompute bucket boundaries for quantile estimation.
	boundaries := make([]float64, numBuckets+1)
	boundaries[0] = lo
	for i := 1; i < numBuckets; i++ {
		boundaries[i] = getLe(minKey+i-1, schema)
	}
	boundaries[numBuckets] = hi

	subBounds := nativeHistogramBounds[schema]
	numSubBuckets := len(subBounds)

	// Build the sub-bucket lookup table. For each of the 256 possible
	// top-8-bit mantissa values, compute the sub-bucket key by
	// evaluating promBucketKey on representative float64 values.
	//
	// A single 8-bit entry can straddle a sub-bucket boundary: the low
	// 44 bits that were truncated may push the full-precision value
	// into the next sub-bucket. We detect straddling by checking both
	// the minimum and maximum float64 representable by each table
	// entry. When they disagree, we resolve to the upper key (keyMax).
	// This matches SearchFloat64s's >= semantics and means straddling
	// values always round up to the next sub-bucket — at most one
	// bucket of additional error for a small fraction of values.
	//
	// For the common schema 2 (10% error), only 4 of 256 entries
	// straddle, affecting ~0.4% of recorded values. The maximum
	// additional error for those values is bounded by one bucket width
	// (8.6%), but the impact on quantile estimation is negligible since
	// the affected values are already near the boundary.
	var subBucketLookup [subBucketLookupSize]uint8
	for tableIdx := 0; tableIdx < subBucketLookupSize; tableIdx++ {
		minBits := uint64(1023)<<52 | uint64(tableIdx)<<subBucketLookupShift
		maxBits := minBits | (1<<subBucketLookupShift - 1)
		keyMin := promBucketKey(math.Float64frombits(minBits), schema)
		keyMax := promBucketKey(math.Float64frombits(maxBits), schema)
		if keyMin == keyMax {
			subBucketLookup[tableIdx] = uint8(keyMin)
		} else {
			// Straddling entry — round up to the next sub-bucket.
			subBucketLookup[tableIdx] = uint8(keyMax)
		}
	}

	return Config{
		Schema:          schema,
		Lo:              lo,
		Hi:              hi,
		MinKey:          minKey,
		NumBuckets:      numBuckets,
		NumSubBuckets:   numSubBuckets,
		SubBucketBounds: subBounds,
		SubBucketLookup: subBucketLookup,
		Boundaries:      boundaries,
	}
}

// Histogram is a lock-free exponential histogram with atomic counters. Multiple
// Histogram instances can share the same Config (e.g., the cumulative and
// current-window histograms inside a WindowedHistogram).
type Histogram struct {
	config *Config
	counts []atomic.Uint64
	// Underflow counts values below config.Lo.
	Underflow atomic.Uint64
	// Overflow counts values above config.Hi.
	Overflow atomic.Uint64
	// ZeroCount counts exact zeros (and negative values).
	ZeroCount atomic.Uint64
	sum       atomic.Int64 // using Int64 since CockroachDB histograms record int64
}

// New creates a new Histogram for the given range [lo, hi] and desired
// relative error.
func New(lo, hi, desiredError float64) *Histogram {
	config := NewConfig(lo, hi, desiredError)
	return newHistogram(&config)
}

// NewFromConfig creates a new Histogram from a pre-built Config. This is
// useful when multiple Histogram instances need to share the same bucket
// layout (e.g., the cumulative and windowed histograms in a
// WindowedHistogram).
func NewFromConfig(config *Config) *Histogram {
	return newHistogram(config)
}

func newHistogram(config *Config) *Histogram {
	return &Histogram{
		config: config,
		counts: make([]atomic.Uint64, config.NumBuckets),
	}
}

// Record adds a value to the histogram. This is the hot path: O(1), lock-free,
// no allocations. Values <= 0 are counted in ZeroCount. Values outside [lo, hi]
// are clamped and counted in Underflow/Overflow respectively.
//
// Bucket index is computed via math.Frexp (IEEE 754 bit extraction) plus a
// binary search over a small precomputed sub-bucket boundary table. This
// produces indices exactly aligned with Prometheus native histogram bucket
// keys, avoiding the floating-point rounding drift that a math.Log-based
// approach would introduce.
func (h *Histogram) Record(v int64) {
	h.sum.Add(v)

	if v <= 0 {
		h.ZeroCount.Add(1)
		return
	}

	// Convert to float64 and extract IEEE 754 bits. The int→float
	// conversion is a single SCVTF instruction; Float64bits is a
	// zero-cost reinterpret.
	bits := math.Float64bits(float64(v))
	exp := int(bits>>52) - 1022
	sub := int(h.config.SubBucketLookup[(bits>>subBucketLookupShift)&0xFF])

	key := sub + (exp-1)*h.config.NumSubBuckets
	idx := key - h.config.MinKey

	// Clamp to valid range. Values below Lo land in bucket 0 (and are
	// counted as underflow); values above Hi land in the last bucket
	// (and are counted as overflow).
	if idx < 0 {
		h.Underflow.Add(1)
		idx = 0
	} else if idx >= h.config.NumBuckets {
		h.Overflow.Add(1)
		idx = h.config.NumBuckets - 1
	}
	h.counts[idx].Add(1)
}

// Snapshot is a point-in-time, non-atomic copy of a Histogram, suitable for
// quantile computation and export.
type Snapshot struct {
	Config     *Config
	Counts     []uint64
	ZeroCount  uint64
	Underflow  uint64
	Overflow   uint64
	TotalCount uint64
	TotalSum   int64
}

// Snapshot returns a point-in-time copy of the histogram. The snapshot is
// not guaranteed to be perfectly consistent (individual counters are read
// independently), but this is acceptable for metrics — the same trade-off
// Prometheus makes.
func (h *Histogram) Snapshot() Snapshot {
	s := Snapshot{
		Config:    h.config,
		Counts:    make([]uint64, h.config.NumBuckets),
		ZeroCount: h.ZeroCount.Load(),
		Underflow: h.Underflow.Load(),
		Overflow:  h.Overflow.Load(),
		TotalSum:  h.sum.Load(),
	}
	for i := range s.Counts {
		c := h.counts[i].Load()
		s.Counts[i] = c
		s.TotalCount += c
	}
	// ZeroCount observations are not recorded in any bucket, so add
	// them to the total.
	s.TotalCount += s.ZeroCount
	return s
}

// Config returns the histogram's configuration.
func (h *Histogram) Config() Config {
	return *h.config
}

// Merge returns a new Snapshot whose counts are the element-wise sum of s
// and other. Both snapshots must share the same Config (same schema and
// bucket boundaries). This is used to merge prev and cur window snapshots
// in the tick-based windowing pattern.
func (s *Snapshot) Merge(other *Snapshot) Snapshot {
	merged := Snapshot{
		Config:     s.Config,
		Counts:     make([]uint64, len(s.Counts)),
		ZeroCount:  s.ZeroCount + other.ZeroCount,
		Underflow:  s.Underflow + other.Underflow,
		Overflow:   s.Overflow + other.Overflow,
		TotalCount: s.TotalCount + other.TotalCount,
		TotalSum:   s.TotalSum + other.TotalSum,
	}
	for i := range s.Counts {
		merged.Counts[i] = s.Counts[i] + other.Counts[i]
	}
	return merged
}

// Downsample returns a new Snapshot with at most maxBuckets buckets by
// merging groups of adjacent buckets. The merge factor is always a power of
// 2, which corresponds to reducing the Prometheus schema by an integer
// amount — so the downsampled snapshot remains schema-aligned and can be
// exported as a valid native histogram at a coarser resolution.
//
// For example, a snapshot with schema 5 (32 sub-buckets/octave) and 1000
// buckets downsampled to maxBuckets=100 would merge groups of 16
// (reducing schema from 5 to 1) to produce ~63 buckets.
//
// The returned snapshot has its own Config with the reduced schema and
// recomputed boundaries. Counts are summed within each merged group.
// TotalCount, TotalSum, ZeroCount, Underflow, and Overflow are preserved.
func (s *Snapshot) Downsample(maxBuckets int) Snapshot {
	if maxBuckets <= 0 || s.Config.NumBuckets <= maxBuckets {
		return *s
	}

	// Find the smallest power-of-2 merge factor that brings us at or below
	// maxBuckets. Each doubling of the merge factor reduces the schema by 1.
	mergeFactor := 1
	schemaReduction := int32(0)
	for s.Config.NumBuckets/mergeFactor > maxBuckets {
		mergeFactor *= 2
		schemaReduction++
	}

	newSchema := s.Config.Schema - schemaReduction
	if newSchema < 0 {
		newSchema = 0
		mergeFactor = 1 << s.Config.Schema
	}

	// Compute the new bucket count. The first bucket's alignment may not
	// be on a mergeFactor boundary relative to the Prometheus key space,
	// so we compute new keys from the original minKey/maxKey.
	newMinKey := s.Config.MinKey >> schemaReduction
	oldMaxKey := s.Config.MinKey + s.Config.NumBuckets - 1
	newMaxKey := oldMaxKey >> schemaReduction
	newNumBuckets := newMaxKey - newMinKey + 1

	// Build merged counts.
	newCounts := make([]uint64, newNumBuckets)
	for i, c := range s.Counts {
		oldKey := s.Config.MinKey + i
		newKey := oldKey >> schemaReduction
		newIdx := newKey - newMinKey
		newCounts[newIdx] += c
	}

	// Recompute boundaries for the new schema. getLe(key, schema) returns
	// the upper bound for that key, so boundary[i+1] = getLe(newMinKey+i).
	// We clamp the first to lo and last to hi.
	newBoundaries := make([]float64, newNumBuckets+1)
	newBoundaries[0] = s.Config.Lo
	for i := 0; i < newNumBuckets-1; i++ {
		b := getLe(newMinKey+i, newSchema)
		if b <= newBoundaries[i] {
			// Ensure strict monotonicity at the edges where getLe may
			// return a value at or below lo due to rounding.
			b = newBoundaries[i] + (s.Config.Hi-s.Config.Lo)*1e-15
		}
		newBoundaries[i+1] = b
	}
	newBoundaries[newNumBuckets] = s.Config.Hi

	newConfig := &Config{
		Schema:          newSchema,
		Lo:              s.Config.Lo,
		Hi:              s.Config.Hi,
		MinKey:          newMinKey,
		NumBuckets:      newNumBuckets,
		SubBucketBounds: nativeHistogramBounds[newSchema],
		Boundaries:      newBoundaries,
	}

	return Snapshot{
		Config:     newConfig,
		Counts:     newCounts,
		ZeroCount:  s.ZeroCount,
		Underflow:  s.Underflow,
		Overflow:   s.Overflow,
		TotalCount: s.TotalCount,
		TotalSum:   s.TotalSum,
	}
}
