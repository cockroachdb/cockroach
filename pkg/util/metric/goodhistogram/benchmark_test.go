// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goodhistogram

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/RaduBerinde/tdigest"
	"github.com/cockroachdb/cockroach/pkg/util/metric/base2histogram"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/codahale/hdrhistogram"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"go.uber.org/atomic"
)

// tdigestDelta controls the compression factor. Higher delta = more centroids
// = better accuracy but more memory and slower merges. delta=100 is a common
// default; it produces ~130 centroids in practice.
const tdigestDelta = 100

// Latency range: 500ns to 1 minute (in nanoseconds).
const (
	latencyLo       = 500         // 500ns
	latencyHi       = 60e9        // 60s = 1 minute
	relErr          = 0.10        // 10% relative error for GoodHistogram, DDSketch, PromNative
	hdrMax          = int64(60e9) // HDR histogram max trackable value
	hdrSigFig       = 3           // HDR significant figures
	promBucketCount = 60          // CockroachDB's standard bucket count
)

// ---- Distribution generators ----
//
// All distributions produce values in nanoseconds clamped to [latencyLo, latencyHi].
// They model realistic query latency patterns.

type distributionGen struct {
	name  string
	genFn func(rng *rand.Rand, n int) []float64
}

func clampSort(vals []float64) []float64 {
	for i, v := range vals {
		if v < latencyLo {
			vals[i] = latencyLo
		} else if v > latencyHi {
			vals[i] = latencyHi
		}
	}
	sort.Float64s(vals)
	return vals
}

var distributions = []distributionGen{
	{
		name: "uniform",
		genFn: func(rng *rand.Rand, n int) []float64 {
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = rng.Float64()*(latencyHi-latencyLo) + latencyLo
			}
			return clampSort(vals)
		},
	},
	{
		name: "exponential",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Exponential with mean ~ 100ms. Typical for OLTP latencies.
			mean := 100e6 // 100ms
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = rng.ExpFloat64()*mean + latencyLo
			}
			return clampSort(vals)
		},
	},
	{
		name: "lognormal-narrow",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Log-normal with σ=1, centered around 10ms. Moderate tail.
			mu := math.Log(10e6) // 10ms
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = math.Exp(rng.NormFloat64()*1.0 + mu)
			}
			return clampSort(vals)
		},
	},
	{
		name: "lognormal-wide",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Log-normal with σ=2, centered around 1ms. Heavy tail — models
			// workloads where most queries are fast but some hit disk/network.
			mu := math.Log(1e6) // 1ms
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = math.Exp(rng.NormFloat64()*2.0 + mu)
			}
			return clampSort(vals)
		},
	},
	{
		name: "pareto-alpha1.5",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Pareto (alpha=1.5): heavy tail, finite mean and variance.
			// Models "most queries under 1ms, some take much longer."
			alpha := 1.5
			vals := make([]float64, n)
			for i := range vals {
				u := rng.Float64()
				vals[i] = latencyLo / math.Pow(1-u, 1.0/alpha)
			}
			return clampSort(vals)
		},
	},
	{
		name: "pareto-alpha1.0",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Pareto (alpha=1.0): very heavy tail, infinite mean.
			alpha := 1.0
			vals := make([]float64, n)
			for i := range vals {
				u := rng.Float64()
				vals[i] = latencyLo / math.Pow(1-u, 1.0/alpha)
			}
			return clampSort(vals)
		},
	},
	{
		name: "bimodal",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Two clusters: 80% fast (around 1ms), 20% slow (around 500ms).
			// Models a system with cache hits and cache misses.
			vals := make([]float64, n)
			for i := range vals {
				if rng.Float64() < 0.8 {
					vals[i] = math.Exp(rng.NormFloat64()*0.5 + math.Log(1e6)) // ~1ms
				} else {
					vals[i] = math.Exp(rng.NormFloat64()*0.5 + math.Log(500e6)) // ~500ms
				}
			}
			return clampSort(vals)
		},
	},
	{
		name: "trimodal",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Three clusters: 60% at ~500µs, 30% at ~50ms, 10% at ~5s.
			// Models a system with memory/disk/network tiers.
			vals := make([]float64, n)
			for i := range vals {
				r := rng.Float64()
				switch {
				case r < 0.6:
					vals[i] = math.Exp(rng.NormFloat64()*0.3 + math.Log(500e3)) // ~500µs
				case r < 0.9:
					vals[i] = math.Exp(rng.NormFloat64()*0.3 + math.Log(50e6)) // ~50ms
				default:
					vals[i] = math.Exp(rng.NormFloat64()*0.3 + math.Log(5e9)) // ~5s
				}
			}
			return clampSort(vals)
		},
	},
	{
		name: "chi-squared-k4",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Chi-squared(k=4), scaled so mean ≈ 50ms.
			// Right-skewed, bounded tail.
			scale := 50e6 / 4.0 // mean = k * scale
			vals := make([]float64, n)
			for i := range vals {
				var sum float64
				for j := 0; j < 4; j++ {
					z := rng.NormFloat64()
					sum += z * z
				}
				vals[i] = sum*scale + latencyLo
			}
			return clampSort(vals)
		},
	},
	{
		name: "weibull-k0.5",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Weibull(k=0.5): decreasing hazard, very heavy tail.
			k := 0.5
			lambda := 10e6 // scale ~ 10ms
			vals := make([]float64, n)
			for i := range vals {
				u := rng.Float64()
				vals[i] = latencyLo + lambda*math.Pow(-math.Log(1-u), 1.0/k)
			}
			return clampSort(vals)
		},
	},
	{
		name: "point-mass-with-tail",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// 90% at exactly ~1ms, 10% log-uniform across range.
			// Models a fast-path with occasional slow outliers.
			vals := make([]float64, n)
			for i := range vals {
				if rng.Float64() < 0.9 {
					vals[i] = 1e6 // 1ms
				} else {
					logLo := math.Log(latencyLo)
					logHi := math.Log(latencyHi)
					vals[i] = math.Exp(rng.Float64()*(logHi-logLo) + logLo)
				}
			}
			return clampSort(vals)
		},
	},
	{
		name: "log-uniform",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Each order of magnitude equally likely. Worst case for fixed-bucket
			// histograms that concentrate buckets in one region.
			logLo := math.Log(latencyLo)
			logHi := math.Log(latencyHi)
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = math.Exp(rng.Float64()*(logHi-logLo) + logLo)
			}
			return clampSort(vals)
		},
	},
}

var quantiles = []float64{50, 75, 90, 95, 99, 99.9}

// ---- Recording throughput benchmarks ----

func makeInt64Values(rng *rand.Rand, n int) []int64 {
	logLo := math.Log(latencyLo)
	logHi := math.Log(latencyHi)
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
	}
	return vals
}

func makeFloat64Values(rng *rand.Rand, n int) []float64 {
	logLo := math.Log(latencyLo)
	logHi := math.Log(latencyHi)
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = math.Exp(rng.Float64()*(logHi-logLo) + logLo)
	}
	return vals
}

func BenchmarkGoodHistogramRecord(b *testing.B) {
	h := New(latencyLo, latencyHi, relErr)
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Record(vals[i%len(vals)])
	}
}

func BenchmarkGoodHistogramRecordParallel(b *testing.B) {
	h := New(latencyLo, latencyHi, relErr)
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			h.Record(vals[i%len(vals)])
			i++
		}
	})
}

// BenchmarkSeqlockOverhead measures the cost of adding a generation counter
// (seqlock) to the Record hot path. This is a direct A/B comparison: the
// histogram is identical, the only difference is one extra atomic.Uint64.Add(1)
// per Record call.
func BenchmarkSeqlockOverhead(b *testing.B) {
	h := New(latencyLo, latencyHi, relErr)
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	var gen atomic.Uint64

	b.Run("without-seqlock", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h.Record(vals[i%len(vals)])
		}
	})
	b.Run("with-seqlock", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			gen.Add(1)
			h.Record(vals[i%len(vals)])
		}
	})
	b.Run("without-seqlock-parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Record(vals[i%len(vals)])
				i++
			}
		})
	})
	b.Run("with-seqlock-parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				gen.Add(1)
				h.Record(vals[i%len(vals)])
				i++
			}
		})
	})
}

func BenchmarkBase2HistogramRecord(b *testing.B) {
	h := base2histogram.New()
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Record(vals[i%len(vals)])
	}
}

func BenchmarkBase2HistogramRecordParallel(b *testing.B) {
	h := base2histogram.New()
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			h.Record(vals[i%len(vals)])
			i++
		}
	})
}

func BenchmarkHDRHistogramRecord(b *testing.B) {
	var mu syncutil.Mutex
	h := hdrhistogram.New(1, hdrMax, hdrSigFig)
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		_ = h.RecordValue(vals[i%len(vals)])
		mu.Unlock()
	}
}

func BenchmarkHDRHistogramRecordParallel(b *testing.B) {
	var mu syncutil.Mutex
	h := hdrhistogram.New(1, hdrMax, hdrSigFig)
	vals := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.Lock()
			_ = h.RecordValue(vals[i%len(vals)])
			mu.Unlock()
			i++
		}
	})
}

func BenchmarkPrometheusHistogramRecord(b *testing.B) {
	buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
	ph := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ph.Observe(vals[i%len(vals)])
	}
}

func BenchmarkPrometheusHistogramRecordParallel(b *testing.B) {
	buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
	ph := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ph.Observe(vals[i%len(vals)])
			i++
		}
	})
}

func BenchmarkPrometheusNativeHistogramRecord(b *testing.B) {
	buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
	ph := prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets:                        buckets,
		NativeHistogramBucketFactor:    1.2,
		NativeHistogramMaxBucketNumber: promBucketCount,
	})
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ph.Observe(vals[i%len(vals)])
	}
}

func BenchmarkPrometheusNativeHistogramRecordParallel(b *testing.B) {
	buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
	ph := prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets:                        buckets,
		NativeHistogramBucketFactor:    1.2,
		NativeHistogramMaxBucketNumber: promBucketCount,
	})
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ph.Observe(vals[i%len(vals)])
			i++
		}
	})
}

func BenchmarkDDSketchRecord(b *testing.B) {
	sketch, err := ddsketch.NewDefaultDDSketch(relErr)
	if err != nil {
		b.Fatal(err)
	}
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sketch.Add(vals[i%len(vals)])
	}
}

func BenchmarkDDSketchRecordParallel(b *testing.B) {
	// DDSketch is not thread-safe. Per-goroutine instances.
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		sketch, _ := ddsketch.NewDefaultDDSketch(relErr)
		i := 0
		for pb.Next() {
			_ = sketch.Add(vals[i%len(vals)])
			i++
		}
	})
}

func BenchmarkTDigestRecord(b *testing.B) {
	builder := tdigest.MakeBuilder(tdigestDelta)
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Add(vals[i%len(vals)], 1)
	}
}

func BenchmarkTDigestRecordParallel(b *testing.B) {
	// T-digest is not thread-safe. Per-goroutine instances.
	vals := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		builder := tdigest.MakeBuilder(tdigestDelta)
		i := 0
		for pb.Next() {
			builder.Add(vals[i%len(vals)], 1)
			i++
		}
	})
}

// ---- Quantile computation benchmarks ----

func BenchmarkGoodHistogramQuantile(b *testing.B) {
	h := New(latencyLo, latencyHi, relErr)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		h.Record(makeInt64Values(rng, 1)[0])
	}
	snap := h.Snapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = snap.ValueAtQuantile(99)
	}
}

func BenchmarkBase2HistogramQuantile(b *testing.B) {
	h := base2histogram.New()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		h.Record(makeInt64Values(rng, 1)[0])
	}
	snap := h.Snapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = snap.ValueAtQuantile(99)
	}
}

func BenchmarkDDSketchQuantile(b *testing.B) {
	sketch, _ := ddsketch.NewDefaultDDSketch(relErr)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		_ = sketch.Add(makeFloat64Values(rng, 1)[0])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sketch.GetValueAtQuantile(0.99)
	}
}

func BenchmarkTDigestQuantile(b *testing.B) {
	builder := tdigest.MakeBuilder(tdigestDelta)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		builder.Add(makeFloat64Values(rng, 1)[0], 1)
	}
	td := builder.Digest()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = td.Quantile(0.99)
	}
}

// ---- Full comparison report ----

// TestFullComparisonReport generates a comprehensive comparison of all five
// histogram implementations across all distributions, covering bucket counts,
// recording speed, and quantile accuracy. Results are written to
// reports/histogram_comparison.txt.
func TestFullComparisonReport(t *testing.T) {
	const n = 100000

	var out strings.Builder

	// Collect speed and accuracy data first so the overall summary can use both.
	speedData := collectSpeedData()
	accuracyData := collectAccuracyData(t, n)

	writeHeader(&out)
	writeOverallSummary(&out, speedData, accuracyData) // Section 1
	writeBucketComparison(&out)                        // Section 2
	writeSpeedReport(&out, speedData)                  // Section 3
	writeAccuracySummary(&out, accuracyData)           // Section 4
	writeAccuracyDetails(&out, accuracyData)           // Section 5

	// Write to reports file.
	reportsDir := filepath.Join("reports")
	if err := os.MkdirAll(reportsDir, 0755); err == nil {
		outPath := filepath.Join(reportsDir, "histogram_comparison.txt")
		if err := os.WriteFile(outPath, []byte(out.String()), 0644); err == nil {
			t.Logf("Wrote report to %s", outPath)
		}
	}
	t.Log("\n" + out.String())
}

func writeHeader(out *strings.Builder) {
	out.WriteString("Histogram Implementation Comparison Report\n")
	out.WriteString("==========================================\n\n")
	out.WriteString(fmt.Sprintf("Latency range: [%s, %s]\n",
		fmtDuration(latencyLo), fmtDuration(latencyHi)))
	out.WriteString("Sample count: 100,000 per distribution\n\n")
	out.WriteString("Implementations:\n")
	out.WriteString("  1. GoodHistogram    — Prometheus-schema-aligned exponential, trapezoidal quantiles, lock-free\n")
	out.WriteString("  2. Base2Histogram   — Integer bit-based exponential (WIDTH=3), slope-clamped trapezoidal, lock-free\n")
	out.WriteString("  3. HDR Histogram    — Sub-bucket per power-of-2 octave, mutex-wrapped (shown at 1 and 3 sig figs)\n")
	out.WriteString("  4. Prometheus       — 60 exponential buckets, linear interpolation, mutex-based\n")
	out.WriteString("  5. Prometheus Native— Exponential (factor=1.2), linear interpolation, mutex-based\n")
	out.WriteString("  6. DDSketch         — Log-based exponential (5% accuracy), NOT thread-safe\n")
	out.WriteString(fmt.Sprintf(
		"  7. T-Digest         — Adaptive centroid merging (delta=%d), NOT thread-safe\n", tdigestDelta))
	out.WriteString("\n")
	out.WriteString("Notes on thread safety:\n")
	out.WriteString("  - GoodHistogram and Base2Histogram use atomic increments — fully lock-free.\n")
	out.WriteString("  - Prometheus (legacy and native) and HDR Histogram use a mutex.\n")
	out.WriteString("  - DDSketch and T-Digest are NOT thread-safe; a mutex or per-goroutine\n")
	out.WriteString("    instances with periodic merging would be needed in production.\n")
	out.WriteString("\n")
	out.WriteString("Error convention: positive = overestimate, negative = underestimate.\n")
	out.WriteString("\n")
}

func writeBucketComparison(out *strings.Builder) {
	out.WriteString("=== SECTION 2: Bucket Count and Memory ===\n")
	out.WriteString(strings.Repeat("-", 50) + "\n")

	info := infoByImpl()
	names := []string{
		"GoodHistogram", "Base2Histogram",
		"HDR Histogram 1sf", "HDR Histogram 3sf",
		"Prometheus (60)", "Prometheus Native", "DDSketch", "T-Digest",
	}

	out.WriteString(fmt.Sprintf("%-25s  %8s  %8s\n", "Implementation", "Buckets", "Memory"))
	out.WriteString(fmt.Sprintf("%-25s  %8s  %8s\n", "-------------------------", "--------", "--------"))
	for _, name := range names {
		inf := info[name]
		out.WriteString(fmt.Sprintf("%-25s  %8s  %7.2fKB\n",
			name, inf.buckets, float64(inf.memBytes)/1024.0))
	}
	out.WriteString("\n")
}

// signedRelError returns (got - expected) / expected, preserving sign.
func signedRelError(got, expected float64) float64 {
	if expected == 0 {
		return 0
	}
	return (got - expected) / expected
}

// nativeHistogramQuantile computes a quantile from a Prometheus native
// histogram by decoding its spans and deltas into bucket boundaries and
// counts, then applying linear interpolation.
func nativeHistogramQuantile(h *prometheusgo.Histogram, q float64) float64 {
	schema := h.GetSchema()
	n := float64(h.GetSampleCount())
	if n == 0 {
		return 0
	}
	rank := (q / 100.0) * n

	// Decode positive spans + deltas into (bucket_key, count) pairs.
	// Spans encode runs of consecutive bucket keys with gaps between them.
	// Deltas are delta-encoded counts: actual_count[i] = sum(delta[0..i]).
	type bucket struct {
		upperBound float64
		count      int64
	}
	var buckets []bucket
	var runningCount int64
	deltaIdx := 0
	bucketKey := 0

	for spanIdx, span := range h.GetPositiveSpan() {
		if spanIdx == 0 {
			// First span: offset is absolute bucket key.
			bucketKey = int(span.GetOffset())
		} else {
			// Subsequent spans: offset is gap from end of previous span.
			bucketKey += int(span.GetOffset())
		}
		for i := uint32(0); i < span.GetLength(); i++ {
			if deltaIdx < len(h.GetPositiveDelta()) {
				runningCount += h.GetPositiveDelta()[deltaIdx]
				deltaIdx++
			}
			ub := getLe(bucketKey, schema)
			buckets = append(buckets, bucket{upperBound: ub, count: runningCount})
			bucketKey++
		}
	}

	// Linear interpolation within buckets (same as Prometheus).
	var cumCount float64
	for i, b := range buckets {
		cumCount += float64(b.count)
		if cumCount >= rank {
			var lo float64
			localCount := float64(b.count)
			localRank := rank - (cumCount - float64(b.count))
			if i > 0 {
				lo = buckets[i-1].upperBound
			}
			hi := b.upperBound
			if localCount == 0 {
				return hi
			}
			return lo + (hi-lo)*(localRank/localCount)
		}
	}
	if len(buckets) > 0 {
		return buckets[len(buckets)-1].upperBound
	}
	return 0
}

// accuracyData holds pre-collected accuracy results for all implementations.
const numAccuracyCols = 8

type accuracyData struct {
	cols       []string
	results    []distResult
	summaryByQ map[float64]*[numAccuracyCols]float64 // quantile -> sum of |error| per impl
}

type distResult struct {
	name   string
	trueQ  map[float64]float64
	errors map[float64][numAccuracyCols]float64
}

func collectAccuracyData(t *testing.T, n int) accuracyData {
	ad := accuracyData{
		cols:       []string{"GoodHist", "Base2", "HDR-1sf", "HDR-3sf", "Prom60", "PromNat", "DDSketch", "T-Digest"},
		summaryByQ: make(map[float64]*[numAccuracyCols]float64),
	}
	for _, q := range quantiles {
		ad.summaryByQ[q] = &[numAccuracyCols]float64{}
	}

	for _, dist := range distributions {
		rng := rand.New(rand.NewSource(42))
		sortedVals := dist.genFn(rng, n)

		trueQ := make(map[float64]float64)
		for _, q := range quantiles {
			idx := int(q / 100.0 * float64(n))
			if idx >= n {
				idx = n - 1
			}
			trueQ[q] = sortedVals[idx]
		}

		gh := New(latencyLo, latencyHi, relErr)
		b2h := base2histogram.New()
		hdr1 := hdrhistogram.New(1, hdrMax, 1)
		hdr3 := hdrhistogram.New(1, hdrMax, 3)
		promBuckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
		ph := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: promBuckets})
		phn := prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets:                        promBuckets,
			NativeHistogramBucketFactor:    1.2,
			NativeHistogramMaxBucketNumber: 1000,
		})
		sketch, err := ddsketch.NewDefaultDDSketch(relErr)
		if err != nil {
			t.Fatal(err)
		}
		tdBuilder := tdigest.MakeBuilder(tdigestDelta)

		for _, v := range sortedVals {
			iv := int64(v)
			gh.Record(iv)
			b2h.Record(iv)
			_ = hdr1.RecordValue(iv)
			_ = hdr3.RecordValue(iv)
			ph.Observe(v)
			phn.Observe(v)
			_ = sketch.Add(v)
			tdBuilder.Add(v, 1)
		}

		ghSnap := gh.Snapshot()
		b2hSnap := b2h.Snapshot()
		promMetric := &prometheusgo.Metric{}
		if err := ph.Write(promMetric); err != nil {
			t.Fatal(err)
		}
		promNativeMetric := &prometheusgo.Metric{}
		if err := phn.Write(promNativeMetric); err != nil {
			t.Fatal(err)
		}
		td := tdBuilder.Digest()

		dr := distResult{name: dist.name, trueQ: trueQ, errors: make(map[float64][numAccuracyCols]float64)}
		for _, q := range quantiles {
			tv := trueQ[q]
			ddVal, _ := sketch.GetValueAtQuantile(q / 100.0)
			errs := [numAccuracyCols]float64{
				signedRelError(ghSnap.ValueAtQuantile(q), tv),
				signedRelError(b2hSnap.ValueAtQuantile(q), tv),
				signedRelError(float64(hdr1.ValueAtQuantile(q)), tv),
				signedRelError(float64(hdr3.ValueAtQuantile(q)), tv),
				signedRelError(promValueAtQuantile(promMetric.Histogram, q), tv),
				signedRelError(nativeHistogramQuantile(promNativeMetric.Histogram, q), tv),
				signedRelError(ddVal, tv),
				signedRelError(td.Quantile(q/100.0), tv),
			}
			dr.errors[q] = errs
			for i := range errs {
				ad.summaryByQ[q][i] += math.Abs(errs[i])
			}
		}
		ad.results = append(ad.results, dr)
	}
	return ad
}

// meanError returns the mean |error| across the given quantiles for a column.
func (ad *accuracyData) meanError(colIdx int, qs []float64) float64 {
	nd := float64(len(ad.results))
	var sum float64
	for _, q := range qs {
		sum += ad.summaryByQ[q][colIdx] / nd
	}
	return sum / float64(len(qs))
}

// implInfo holds pre-computed metadata for each implementation.
type implInfo struct {
	memBytes int    // memory in bytes
	buckets  string // bucket count string
}

func infoByImpl() map[string]implInfo {
	ghCfg := NewConfig(latencyLo, latencyHi, relErr)
	hdr1 := hdrhistogram.New(1, hdrMax, 1)
	hdr3 := hdrhistogram.New(1, hdrMax, 3)

	promNativeBuckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
	phn := prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets:                        promNativeBuckets,
		NativeHistogramBucketFactor:    1.2,
		NativeHistogramMaxBucketNumber: 1000,
	})
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100000; i++ {
		phn.Observe(math.Exp(rng.Float64()*(math.Log(latencyHi)-math.Log(latencyLo)) + math.Log(latencyLo)))
	}
	phnMetric := &prometheusgo.Metric{}
	_ = phn.Write(phnMetric)
	var promNativeBucketCount int
	for _, span := range phnMetric.Histogram.GetPositiveSpan() {
		promNativeBucketCount += int(span.GetLength())
	}

	ddsk, _ := ddsketch.NewDefaultDDSketch(relErr)
	rng = rand.New(rand.NewSource(42))
	for i := 0; i < 100000; i++ {
		_ = ddsk.Add(math.Exp(rng.Float64()*(math.Log(latencyHi)-math.Log(latencyLo)) + math.Log(latencyLo)))
	}
	var ddBucketCount int
	ddsk.ForEach(func(_, _ float64) (stop bool) {
		ddBucketCount++
		return false
	})

	tdCentroids := int(float64(tdigestDelta) * 1.3)

	return map[string]implInfo{
		"GoodHistogram":       {ghCfg.NumBuckets * 8, fmt.Sprintf("%d", ghCfg.NumBuckets)},
		"Base2Histogram":      {base2histogram.NumBuckets * 8, fmt.Sprintf("%d", base2histogram.NumBuckets)},
		"HDR Histogram (1sf)": {hdr1.ByteSize(), fmt.Sprintf("%d", hdr1.ByteSize()/8)},
		"HDR Histogram (3sf)": {hdr3.ByteSize(), fmt.Sprintf("%d", hdr3.ByteSize()/8)},
		"HDR Histogram 1sf":   {hdr1.ByteSize(), fmt.Sprintf("%d", hdr1.ByteSize()/8)},
		"HDR Histogram 3sf":   {hdr3.ByteSize(), fmt.Sprintf("%d", hdr3.ByteSize()/8)},
		"Prometheus (60)":     {promBucketCount * 8, fmt.Sprintf("%d", promBucketCount)},
		// Prometheus Native uses a synchronized map for sparse buckets. Each entry
		// costs ~85 bytes (map entry struct + interface boxing + *int64 heap alloc),
		// and there are 2 copies (hot/cold swap). The conventional []uint64 array
		// is also always allocated (60 entries * 8 bytes * 2 copies = 960B).
		"Prometheus Native": {
			promNativeBucketCount*85*2 + promBucketCount*8*2,
			fmt.Sprintf("%d", promNativeBucketCount),
		},
		"DDSketch": {ddBucketCount * 8, fmt.Sprintf("%d", ddBucketCount)},
		"T-Digest": {tdCentroids * 16, fmt.Sprintf("~%d", tdCentroids)},
	}
}

func writeOverallSummary(out *strings.Builder, sd speedData, ad accuracyData) {
	out.WriteString("=== SECTION 1: Overall Summary ===\n")
	out.WriteString(strings.Repeat("-", 95) + "\n")

	type row struct {
		name      string
		speedName string
		colIdx    int
	}
	rows := []row{
		{"GoodHistogram", "GoodHistogram", 0},
		{"Base2Histogram", "Base2Histogram", 1},
		{"HDR Histogram (1sf)", "HDR Histogram", 2},
		{"HDR Histogram (3sf)", "HDR Histogram", 3},
		{"Prometheus (60)", "Prometheus (60)", 4},
		{"Prometheus Native", "Prometheus Native", 5},
		{"DDSketch", "DDSketch", 6},
		{"T-Digest", "T-Digest", 7},
	}

	info := infoByImpl()
	allQs := quantiles
	tailQs := []float64{90, 95, 99, 99.9}

	out.WriteString(fmt.Sprintf("%-22s  %8s  %8s  %8s  %8s  %9s  %9s\n",
		"Implementation", "Buckets", "Memory", "1-thread", "parallel", "mean err", "p90+ err"))
	out.WriteString(fmt.Sprintf("%-22s  %8s  %8s  %8s  %8s  %9s  %9s\n",
		"----------------------", "--------", "--------", "--------", "--------", "---------", "---------"))

	for _, r := range rows {
		inf := info[r.name]
		if inf.memBytes == 0 {
			inf = info[r.speedName]
		}
		memKB := fmt.Sprintf("%.2fKB", float64(inf.memBytes)/1024.0)
		singleStr := fmt.Sprintf("%.1fns", sd.single[r.speedName])
		parallelStr := "N/A"
		if pNs, ok := sd.parallel[r.speedName]; ok {
			parallelStr = fmt.Sprintf("%.1fns", pNs)
		}
		allErr := ad.meanError(r.colIdx, allQs)
		tailErr := ad.meanError(r.colIdx, tailQs)
		out.WriteString(fmt.Sprintf("%-22s  %8s  %8s  %8s  %8s  %8.2f%%  %8.2f%%\n",
			r.name, inf.buckets, memKB, singleStr, parallelStr, allErr*100, tailErr*100))
	}
	out.WriteString("\n")
}

func writeAccuracySummary(out *strings.Builder, ad accuracyData) {
	nd := float64(len(ad.results))
	out.WriteString("=== SECTION 4: Quantile Accuracy — Summary ===\n")
	out.WriteString("Mean |relative error| (%) across all distributions.\n\n")
	out.WriteString(fmt.Sprintf("%-6s", "Quant"))
	for _, c := range ad.cols {
		out.WriteString(fmt.Sprintf("  %8s", c))
	}
	out.WriteString("\n")
	out.WriteString(fmt.Sprintf("%-6s", "------"))
	for range ad.cols {
		out.WriteString(fmt.Sprintf("  %8s", "--------"))
	}
	out.WriteString("\n")
	for _, q := range quantiles {
		s := ad.summaryByQ[q]
		out.WriteString(fmt.Sprintf("p%-5.1f", q))
		for i := range ad.cols {
			out.WriteString(fmt.Sprintf("  %7.3f%%", s[i]/nd*100))
		}
		out.WriteString("\n")
	}
	out.WriteString("\n")
}

func writeAccuracyDetails(out *strings.Builder, ad accuracyData) {
	out.WriteString("=== SECTION 5: Quantile Accuracy — Per Distribution ===\n")
	out.WriteString("(signed relative error: +overestimate / -underestimate)\n\n")

	for _, dr := range ad.results {
		out.WriteString(fmt.Sprintf("Distribution: %s\n", dr.name))
		out.WriteString(fmt.Sprintf("%-6s  %12s", "Quant", "True"))
		for _, c := range ad.cols {
			out.WriteString(fmt.Sprintf("  %8s", c))
		}
		out.WriteString("\n")
		out.WriteString(fmt.Sprintf("%-6s  %12s", "------", "------------"))
		for range ad.cols {
			out.WriteString(fmt.Sprintf("  %8s", "--------"))
		}
		out.WriteString("\n")

		for _, q := range quantiles {
			errs := dr.errors[q]
			out.WriteString(fmt.Sprintf("p%-5.1f  %12s", q, fmtDuration(dr.trueQ[q])))
			for _, e := range errs {
				out.WriteString(fmt.Sprintf("  %+7.2f%%", e*100))
			}
			out.WriteString("\n")
		}
		out.WriteString("\n")
	}
}

// speedData holds pre-collected benchmark results for all implementations.
type speedData struct {
	single   map[string]float64 // name -> ns/op
	parallel map[string]float64 // name -> ns/op (thread-safe only)
	safe     map[string]string  // name -> thread-safety description
}

func collectSpeedData() speedData {
	vals64 := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	valsF := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)

	bench := func(f func(b *testing.B)) float64 {
		const runs = 5
		results := make([]float64, runs)
		for i := range results {
			r := testing.Benchmark(f)
			results[i] = float64(r.T.Nanoseconds()) / float64(r.N)
		}
		sort.Float64s(results)
		return results[runs/2] // median
	}

	sd := speedData{
		single:   make(map[string]float64),
		parallel: make(map[string]float64),
		safe:     make(map[string]string),
	}

	// Single-threaded.
	sd.single["GoodHistogram"] = bench(func(b *testing.B) {
		h := New(latencyLo, latencyHi, relErr)
		for i := 0; i < b.N; i++ {
			h.Record(vals64[i%len(vals64)])
		}
	})
	sd.single["Base2Histogram"] = bench(func(b *testing.B) {
		h := base2histogram.New()
		for i := 0; i < b.N; i++ {
			h.Record(vals64[i%len(vals64)])
		}
	})
	sd.single["HDR Histogram"] = bench(func(b *testing.B) {
		var mu syncutil.Mutex
		h := hdrhistogram.New(1, hdrMax, hdrSigFig)
		for i := 0; i < b.N; i++ {
			mu.Lock()
			_ = h.RecordValue(vals64[i%len(vals64)])
			mu.Unlock()
		}
	})
	sd.single["Prometheus (60)"] = bench(func(b *testing.B) {
		buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
		ph := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
		for i := 0; i < b.N; i++ {
			ph.Observe(valsF[i%len(valsF)])
		}
	})
	sd.single["Prometheus Native"] = bench(func(b *testing.B) {
		buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
		ph := prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets:                        buckets,
			NativeHistogramBucketFactor:    1.2,
			NativeHistogramMaxBucketNumber: 1000,
		})
		for i := 0; i < b.N; i++ {
			ph.Observe(valsF[i%len(valsF)])
		}
	})
	sd.single["DDSketch"] = bench(func(b *testing.B) {
		sketch, _ := ddsketch.NewDefaultDDSketch(relErr)
		for i := 0; i < b.N; i++ {
			_ = sketch.Add(valsF[i%len(valsF)])
		}
	})
	sd.single["T-Digest"] = bench(func(b *testing.B) {
		builder := tdigest.MakeBuilder(tdigestDelta)
		for i := 0; i < b.N; i++ {
			builder.Add(valsF[i%len(valsF)], 1)
		}
	})

	sd.safe["GoodHistogram"] = "YES (lock-free)"
	sd.safe["Base2Histogram"] = "YES (lock-free)"
	sd.safe["HDR Histogram"] = "YES (mutex)"
	sd.safe["Prometheus (60)"] = "YES (mutex)"
	sd.safe["Prometheus Native"] = "YES (mutex)"
	sd.safe["DDSketch"] = "NO"
	sd.safe["T-Digest"] = "NO"

	// Parallel — thread-safe implementations only.
	sd.parallel["GoodHistogram"] = bench(func(b *testing.B) {
		h := New(latencyLo, latencyHi, relErr)
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Record(vals64[i%len(vals64)])
				i++
			}
		})
	})
	sd.parallel["Base2Histogram"] = bench(func(b *testing.B) {
		h := base2histogram.New()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Record(vals64[i%len(vals64)])
				i++
			}
		})
	})
	sd.parallel["HDR Histogram"] = bench(func(b *testing.B) {
		var mu syncutil.Mutex
		h := hdrhistogram.New(1, hdrMax, hdrSigFig)
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				mu.Lock()
				_ = h.RecordValue(vals64[i%len(vals64)])
				mu.Unlock()
				i++
			}
		})
	})
	sd.parallel["Prometheus (60)"] = bench(func(b *testing.B) {
		buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
		ph := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				ph.Observe(valsF[i%len(valsF)])
				i++
			}
		})
	})
	sd.parallel["Prometheus Native"] = bench(func(b *testing.B) {
		buckets := prometheus.ExponentialBucketsRange(latencyLo, latencyHi, promBucketCount)
		ph := prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets:                        buckets,
			NativeHistogramBucketFactor:    1.2,
			NativeHistogramMaxBucketNumber: 1000,
		})
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				ph.Observe(valsF[i%len(valsF)])
				i++
			}
		})
	})

	return sd
}

func writeSpeedReport(out *strings.Builder, sd speedData) {
	out.WriteString("=== SECTION 3: Recording Speed ===\n")
	out.WriteString("(median of 5 runs per implementation)\n\n")

	order := []string{
		"GoodHistogram", "Base2Histogram", "HDR Histogram",
		"Prometheus (60)", "Prometheus Native", "DDSketch", "T-Digest",
	}

	out.WriteString(fmt.Sprintf("%-22s  %10s  %10s  %s\n",
		"Implementation", "1-thread", "parallel", "Thread-safe?"))
	out.WriteString(fmt.Sprintf("%-22s  %10s  %10s  %s\n",
		"----------------------", "----------", "----------", "---------------"))
	for _, name := range order {
		singleNs := sd.single[name]
		parallelStr := "       N/A"
		if pNs, ok := sd.parallel[name]; ok {
			parallelStr = fmt.Sprintf("%8.1fns", pNs)
		}
		out.WriteString(fmt.Sprintf("%-22s  %8.1fns  %s  %s\n",
			name, singleNs, parallelStr, sd.safe[name]))
	}
	out.WriteString("\n")
}

// ---- Helpers ----

// promValueAtQuantile replicates the standard Prometheus quantile estimation
// (linear interpolation within buckets) used by CockroachDB.
func promValueAtQuantile(h *prometheusgo.Histogram, q float64) float64 {
	buckets := h.Bucket
	n := float64(h.GetSampleCount())
	if n == 0 {
		return 0
	}
	rank := uint64(((q / 100) * n) + 0.5)
	b := sort.Search(len(buckets)-1, func(i int) bool {
		return *buckets[i].CumulativeCount >= rank
	})
	var (
		bucketStart float64
		bucketEnd   = *buckets[b].UpperBound
		count       = *buckets[b].CumulativeCount
	)
	if b > 0 {
		bucketStart = *buckets[b-1].UpperBound
		count -= *buckets[b-1].CumulativeCount
		rank -= *buckets[b-1].CumulativeCount
	}
	if count == 0 {
		if rank == 0 {
			return bucketStart
		}
		return bucketEnd
	}
	return bucketStart + (bucketEnd-bucketStart)*(float64(rank)/float64(count))
}

// fmtDuration formats a nanosecond value as a human-readable duration.
func fmtDuration(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2fµs", ns/1e3)
	default:
		return fmt.Sprintf("%.0fns", ns)
	}
}
