// Package hdrhistogram provides an implementation of Gil Tene's HDR Histogram
// data structure. The HDR Histogram allows for fast and accurate analysis of
// the extreme ranges of data with non-normal distributions, like latency.
package hdrhistogram

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"sort"
)

// A Bracket is a part of a cumulative distribution.
type Bracket struct {
	Quantile       float64
	Count, ValueAt int64
}

// A Snapshot is an exported view of a Histogram, useful for serializing them.
// A Histogram can be constructed from it by passing it to Import.
type Snapshot struct {
	LowestTrackableValue  int64
	HighestTrackableValue int64
	SignificantFigures    int64
	Counts                []int64
}

// A Histogram is a lossy data structure used to record the distribution of
// non-normally distributed data (like latency) with a high degree of accuracy
// and a bounded degree of precision.
type Histogram struct {
	lowestDiscernibleValue      int64
	highestTrackableValue       int64
	unitMagnitude               int64
	significantFigures          int64
	subBucketHalfCountMagnitude int32
	subBucketHalfCount          int32
	subBucketMask               int64
	subBucketCount              int32
	bucketCount                 int32
	countsLen                   int32
	totalCount                  int64
	counts                      []int64
	startTimeMs                 int64
	endTimeMs                   int64
	tag                         string
}

func (h *Histogram) Tag() string {
	return h.tag
}

func (h *Histogram) SetTag(tag string) {
	h.tag = tag
}

func (h *Histogram) EndTimeMs() int64 {
	return h.endTimeMs
}

func (h *Histogram) SetEndTimeMs(endTimeMs int64) {
	h.endTimeMs = endTimeMs
}

func (h *Histogram) StartTimeMs() int64 {
	return h.startTimeMs
}

func (h *Histogram) SetStartTimeMs(startTimeMs int64) {
	h.startTimeMs = startTimeMs
}

// Construct a Histogram given the Lowest and Highest values to be tracked and a number of significant decimal digits.
//
// Providing a lowestDiscernibleValue is useful in situations where the units used for the histogram's values are
// much smaller that the minimal accuracy required.
// E.g. when tracking time values stated in nanosecond units, where the minimal accuracy required is a microsecond,
// the proper value for lowestDiscernibleValue would be 1000.
//
// Note: the numberOfSignificantValueDigits must be [1,5]. If lower than 1 the numberOfSignificantValueDigits will be
// forced to 1, and if higher than 5 the numberOfSignificantValueDigits will be forced to 5.
func New(lowestDiscernibleValue, highestTrackableValue int64, numberOfSignificantValueDigits int) *Histogram {
	if numberOfSignificantValueDigits < 1 {
		numberOfSignificantValueDigits = 1
	} else if numberOfSignificantValueDigits > 5 {
		numberOfSignificantValueDigits = 5
	}
	if lowestDiscernibleValue < 1 {
		lowestDiscernibleValue = 1
	}

	// Given a 3 decimal point accuracy, the expectation is obviously for "+/- 1 unit at 1000". It also means that
	// it's "ok to be +/- 2 units at 2000". The "tricky" thing is that it is NOT ok to be +/- 2 units at 1999. Only
	// starting at 2000. So internally, we need to maintain single unit resolution to 2x 10^decimalPoints.
	largestValueWithSingleUnitResolution := 2 * math.Pow10(numberOfSignificantValueDigits)

	// We need to maintain power-of-two subBucketCount (for clean direct indexing) that is large enough to
	// provide unit resolution to at least largestValueWithSingleUnitResolution. So figure out
	// largestValueWithSingleUnitResolution's nearest power-of-two (rounded up), and use that:
	subBucketCountMagnitude := int32(math.Ceil(math.Log2(float64(largestValueWithSingleUnitResolution))))
	subBucketHalfCountMagnitude := subBucketCountMagnitude
	if subBucketHalfCountMagnitude < 1 {
		subBucketHalfCountMagnitude = 1
	}
	subBucketHalfCountMagnitude--

	unitMagnitude := int32(math.Floor(math.Log2(float64(lowestDiscernibleValue))))
	if unitMagnitude < 0 {
		unitMagnitude = 0
	}

	subBucketCount := int32(math.Pow(2, float64(subBucketHalfCountMagnitude)+1))

	subBucketHalfCount := subBucketCount / 2
	subBucketMask := int64(subBucketCount-1) << uint(unitMagnitude)

	// determine exponent range needed to support the trackable value with no
	// overflow:
	smallestUntrackableValue := int64(subBucketCount) << uint(unitMagnitude)
	bucketsNeeded := getBucketsNeededToCoverValue(smallestUntrackableValue, highestTrackableValue)

	bucketCount := bucketsNeeded
	countsLen := (bucketCount + 1) * (subBucketCount / 2)

	return &Histogram{
		lowestDiscernibleValue:      lowestDiscernibleValue,
		highestTrackableValue:       highestTrackableValue,
		unitMagnitude:               int64(unitMagnitude),
		significantFigures:          int64(numberOfSignificantValueDigits),
		subBucketHalfCountMagnitude: subBucketHalfCountMagnitude,
		subBucketHalfCount:          subBucketHalfCount,
		subBucketMask:               subBucketMask,
		subBucketCount:              subBucketCount,
		bucketCount:                 bucketCount,
		countsLen:                   countsLen,
		totalCount:                  0,
		counts:                      make([]int64, countsLen),
		startTimeMs:                 0,
		endTimeMs:                   0,
		tag:                         "",
	}
}

func getBucketsNeededToCoverValue(smallestUntrackableValue int64, maxValue int64) int32 {
	// always have at least 1 bucket
	bucketsNeeded := int32(1)
	for smallestUntrackableValue < maxValue {
		if smallestUntrackableValue > (math.MaxInt64 / 2) {
			// next shift will overflow, meaning that bucket could represent values up to ones greater than
			// math.MaxInt64, so it's the last bucket
			return bucketsNeeded + 1
		}
		smallestUntrackableValue <<= 1
		bucketsNeeded++
	}
	return bucketsNeeded
}

// ByteSize returns an estimate of the amount of memory allocated to the
// histogram in bytes.
//
// N.B.: This does not take into account the overhead for slices, which are
// small, constant, and specific to the compiler version.
func (h *Histogram) ByteSize() int {
	return 6*8 + 5*4 + len(h.counts)*8
}

func (h *Histogram) getNormalizingIndexOffset() int32 {
	return 1
}

// Merge merges the data stored in the given histogram with the receiver,
// returning the number of recorded values which had to be dropped.
func (h *Histogram) Merge(from *Histogram) (dropped int64) {
	i := from.rIterator()
	for i.next() {
		v := i.valueFromIdx
		c := i.countAtIdx

		if h.RecordValues(v, c) != nil {
			dropped += c
		}
	}

	return
}

// TotalCount returns total number of values recorded.
func (h *Histogram) TotalCount() int64 {
	return h.totalCount
}

// Max returns the approximate maximum recorded value.
func (h *Histogram) Max() int64 {
	var max int64
	i := h.iterator()
	for i.next() {
		if i.countAtIdx != 0 {
			max = i.highestEquivalentValue
		}
	}
	return h.highestEquivalentValue(max)
}

// Min returns the approximate minimum recorded value.
func (h *Histogram) Min() int64 {
	var min int64
	i := h.iterator()
	for i.next() {
		if i.countAtIdx != 0 && min == 0 {
			min = i.highestEquivalentValue
			break
		}
	}
	return h.lowestEquivalentValue(min)
}

// Mean returns the approximate arithmetic mean of the recorded values.
func (h *Histogram) Mean() float64 {
	if h.totalCount == 0 {
		return 0
	}
	var total int64
	i := h.iterator()
	for i.next() {
		if i.countAtIdx != 0 {
			total += i.countAtIdx * h.medianEquivalentValue(i.valueFromIdx)
		}
	}
	return float64(total) / float64(h.totalCount)
}

// StdDev returns the approximate standard deviation of the recorded values.
func (h *Histogram) StdDev() float64 {
	if h.totalCount == 0 {
		return 0
	}

	mean := h.Mean()
	geometricDevTotal := 0.0

	i := h.iterator()
	for i.next() {
		if i.countAtIdx != 0 {
			dev := float64(h.medianEquivalentValue(i.valueFromIdx)) - mean
			geometricDevTotal += (dev * dev) * float64(i.countAtIdx)
		}
	}

	return math.Sqrt(geometricDevTotal / float64(h.totalCount))
}

// Reset deletes all recorded values and restores the histogram to its original
// state.
func (h *Histogram) Reset() {
	h.totalCount = 0
	for i := range h.counts {
		h.counts[i] = 0
	}
}

// RecordValue records the given value, returning an error if the value is out
// of range.
func (h *Histogram) RecordValue(v int64) error {
	return h.RecordValues(v, 1)
}

// RecordCorrectedValue records the given value, correcting for stalls in the
// recording process. This only works for processes which are recording values
// at an expected interval (e.g., doing jitter analysis). Processes which are
// recording ad-hoc values (e.g., latency for incoming requests) can't take
// advantage of this.
func (h *Histogram) RecordCorrectedValue(v, expectedInterval int64) error {
	if err := h.RecordValue(v); err != nil {
		return err
	}

	if expectedInterval <= 0 || v <= expectedInterval {
		return nil
	}

	missingValue := v - expectedInterval
	for missingValue >= expectedInterval {
		if err := h.RecordValue(missingValue); err != nil {
			return err
		}
		missingValue -= expectedInterval
	}

	return nil
}

// RecordValues records n occurrences of the given value, returning an error if
// the value is out of range.
func (h *Histogram) RecordValues(v, n int64) error {
	idx := h.countsIndexFor(v)
	if idx < 0 || int(h.countsLen) <= idx {
		return fmt.Errorf("value %d is too large to be recorded", v)
	}
	h.setCountAtIndex(idx, n)

	return nil
}

func (h *Histogram) setCountAtIndex(idx int, n int64) {
	h.counts[idx] += n
	h.totalCount += n
}

// ValueAtQuantile returns the largest value that (100% - percentile) of the overall recorded value entries
// in the histogram are either larger than or equivalent to.
//
// The passed quantile must be a float64 value in [0.0 .. 100.0]
// Note that two values are "equivalent" if `ValuesAreEquivalent(value1,value2)` would return true.
//
// Returns 0 if no recorded values exist.
func (h *Histogram) ValueAtQuantile(q float64) int64 {
	return h.ValueAtPercentile(q)
}

// ValueAtPercentile returns the largest value that (100% - percentile) of the overall recorded value entries
// in the histogram are either larger than or equivalent to.
//
// The passed percentile must be a float64 value in [0.0 .. 100.0]
// Note that two values are "equivalent" if `ValuesAreEquivalent(value1,value2)` would return true.
//
// Returns 0 if no recorded values exist.
func (h *Histogram) ValueAtPercentile(percentile float64) int64 {
	if percentile > 100 {
		percentile = 100
	}

	countAtPercentile := int64(((percentile / 100) * float64(h.totalCount)) + 0.5)
	valueFromIdx := h.getValueFromIdxUpToCount(countAtPercentile)
	if percentile == 0.0 {
		return h.lowestEquivalentValue(valueFromIdx)
	}
	return h.highestEquivalentValue(valueFromIdx)
}

func (h *Histogram) getValueFromIdxUpToCount(countAtPercentile int64) int64 {
	var countToIdx int64
	var valueFromIdx int64
	var subBucketIdx int32 = -1
	var bucketIdx int32
	bucketBaseIdx := h.getBucketBaseIdx(bucketIdx)

	for {
		if countToIdx >= countAtPercentile {
			break
		}
		// increment bucket
		subBucketIdx++
		if subBucketIdx >= h.subBucketCount {
			subBucketIdx = h.subBucketHalfCount
			bucketIdx++
			bucketBaseIdx = h.getBucketBaseIdx(bucketIdx)
		}

		countToIdx += h.getCountAtIndexGivenBucketBaseIdx(bucketBaseIdx, subBucketIdx)
		valueFromIdx = int64(subBucketIdx) << uint(int64(bucketIdx)+h.unitMagnitude)
	}
	return valueFromIdx
}

// ValueAtPercentiles, given an slice of percentiles returns a map containing for each passed percentile,
// the largest value that (100% - percentile) of the overall recorded value entries
// in the histogram are either larger than or equivalent to.
//
// Each element in the given an slice of percentiles must be a float64 value in [0.0 .. 100.0]
// Note that two values are "equivalent" if `ValuesAreEquivalent(value1,value2)` would return true.
//
// Returns a map of 0's if no recorded values exist.
func (h *Histogram) ValueAtPercentiles(percentiles []float64) (values map[float64]int64) {
	sort.Float64s(percentiles)
	totalQuantilesToCalculate := len(percentiles)
	values = make(map[float64]int64, totalQuantilesToCalculate)
	countAtPercentiles := make([]int64, totalQuantilesToCalculate)
	for i, percentile := range percentiles {
		if percentile > 100 {
			percentile = 100
		}
		values[percentile] = 0
		countAtPercentiles[i] = int64(((percentile / 100) * float64(h.totalCount)) + 0.5)
	}

	total := int64(0)
	currentQuantileSlicePos := 0
	i := h.iterator()
	for currentQuantileSlicePos < totalQuantilesToCalculate && i.nextCountAtIdx(h.totalCount) {
		total += i.countAtIdx
		for currentQuantileSlicePos < totalQuantilesToCalculate && total >= countAtPercentiles[currentQuantileSlicePos] {
			currentPercentile := percentiles[currentQuantileSlicePos]
			if currentPercentile == 0.0 {
				values[currentPercentile] = h.lowestEquivalentValue(i.valueFromIdx)
			} else {
				values[currentPercentile] = h.highestEquivalentValue(i.valueFromIdx)
			}
			currentQuantileSlicePos++
		}
	}
	return
}

// Determine if two values are equivalent with the histogram's resolution.
// Where "equivalent" means that value samples recorded for any two
// equivalent values are counted in a common total count.
func (h *Histogram) ValuesAreEquivalent(value1, value2 int64) (result bool) {
	result = h.lowestEquivalentValue(value1) == h.lowestEquivalentValue(value2)
	return
}

// CumulativeDistribution returns an ordered list of brackets of the
// distribution of recorded values.
func (h *Histogram) CumulativeDistribution() []Bracket {
	var result []Bracket

	i := h.pIterator(1)
	for i.next() {
		result = append(result, Bracket{
			Quantile: i.percentile,
			Count:    i.countToIdx,
			ValueAt:  i.highestEquivalentValue,
		})
	}

	return result
}

// SignificantFigures returns the significant figures used to create the
// histogram
func (h *Histogram) SignificantFigures() int64 {
	return h.significantFigures
}

// LowestTrackableValue returns the lower bound on values that will be added
// to the histogram
func (h *Histogram) LowestTrackableValue() int64 {
	return h.lowestDiscernibleValue
}

// HighestTrackableValue returns the upper bound on values that will be added
// to the histogram
func (h *Histogram) HighestTrackableValue() int64 {
	return h.highestTrackableValue
}

// Histogram bar for plotting
type Bar struct {
	From, To, Count int64
}

// Pretty print as csv for easy plotting
func (b Bar) String() string {
	return fmt.Sprintf("%v, %v, %v\n", b.From, b.To, b.Count)
}

// Distribution returns an ordered list of bars of the
// distribution of recorded values, counts can be normalized to a probability
func (h *Histogram) Distribution() (result []Bar) {
	i := h.iterator()
	for i.next() {
		result = append(result, Bar{
			Count: i.countAtIdx,
			From:  h.lowestEquivalentValue(i.valueFromIdx),
			To:    i.highestEquivalentValue,
		})
	}

	return result
}

// Equals returns true if the two Histograms are equivalent, false if not.
func (h *Histogram) Equals(other *Histogram) bool {
	switch {
	case
		h.lowestDiscernibleValue != other.lowestDiscernibleValue,
		h.highestTrackableValue != other.highestTrackableValue,
		h.unitMagnitude != other.unitMagnitude,
		h.significantFigures != other.significantFigures,
		h.subBucketHalfCountMagnitude != other.subBucketHalfCountMagnitude,
		h.subBucketHalfCount != other.subBucketHalfCount,
		h.subBucketMask != other.subBucketMask,
		h.subBucketCount != other.subBucketCount,
		h.bucketCount != other.bucketCount,
		h.countsLen != other.countsLen,
		h.totalCount != other.totalCount:
		return false
	default:
		for i, c := range h.counts {
			if c != other.counts[i] {
				return false
			}
		}
	}
	return true
}

// Export returns a snapshot view of the Histogram. This can be later passed to
// Import to construct a new Histogram with the same state.
func (h *Histogram) Export() *Snapshot {
	return &Snapshot{
		LowestTrackableValue:  h.lowestDiscernibleValue,
		HighestTrackableValue: h.highestTrackableValue,
		SignificantFigures:    h.significantFigures,
		Counts:                append([]int64(nil), h.counts...), // copy
	}
}

// Import returns a new Histogram populated from the Snapshot data (which the
// caller must stop accessing).
func Import(s *Snapshot) *Histogram {
	h := New(s.LowestTrackableValue, s.HighestTrackableValue, int(s.SignificantFigures))
	h.counts = s.Counts
	totalCount := int64(0)
	for i := int32(0); i < h.countsLen; i++ {
		countAtIndex := h.counts[i]
		if countAtIndex > 0 {
			totalCount += countAtIndex
		}
	}
	h.totalCount = totalCount
	return h
}

func (h *Histogram) iterator() *iterator {
	return &iterator{
		h:            h,
		subBucketIdx: -1,
	}
}

func (h *Histogram) rIterator() *rIterator {
	return &rIterator{
		iterator: iterator{
			h:            h,
			subBucketIdx: -1,
		},
	}
}

func (h *Histogram) pIterator(ticksPerHalfDistance int32) *pIterator {
	return &pIterator{
		iterator: iterator{
			h:            h,
			subBucketIdx: -1,
		},
		ticksPerHalfDistance: ticksPerHalfDistance,
	}
}

func (h *Histogram) sizeOfEquivalentValueRange(v int64) int64 {
	bucketIdx := h.getBucketIndex(v)
	return h.sizeOfEquivalentValueRangeGivenBucketIdx(v, bucketIdx)
}

func (h *Histogram) sizeOfEquivalentValueRangeGivenBucketIdx(v int64, bucketIdx int32) int64 {
	subBucketIdx := h.getSubBucketIdx(v, bucketIdx)
	adjustedBucket := bucketIdx
	if subBucketIdx >= h.subBucketCount {
		adjustedBucket++
	}
	return int64(1) << uint(h.unitMagnitude+int64(adjustedBucket))
}

func (h *Histogram) valueFromIndex(bucketIdx, subBucketIdx int32) int64 {
	return int64(subBucketIdx) << uint(int64(bucketIdx)+h.unitMagnitude)
}

func (h *Histogram) lowestEquivalentValue(v int64) int64 {
	bucketIdx := h.getBucketIndex(v)
	return h.lowestEquivalentValueGivenBucketIdx(v, bucketIdx)
}

func (h *Histogram) lowestEquivalentValueGivenBucketIdx(v int64, bucketIdx int32) int64 {
	subBucketIdx := h.getSubBucketIdx(v, bucketIdx)
	return h.valueFromIndex(bucketIdx, subBucketIdx)
}

func (h *Histogram) nextNonEquivalentValue(v int64) int64 {
	bucketIdx := h.getBucketIndex(v)
	return h.lowestEquivalentValueGivenBucketIdx(v, bucketIdx) + h.sizeOfEquivalentValueRangeGivenBucketIdx(v, bucketIdx)
}

func (h *Histogram) highestEquivalentValue(v int64) int64 {
	return h.nextNonEquivalentValue(v) - 1
}

func (h *Histogram) medianEquivalentValue(v int64) int64 {
	return h.lowestEquivalentValue(v) + (h.sizeOfEquivalentValueRange(v) >> 1)
}

func (h *Histogram) getCountAtIndex(bucketIdx, subBucketIdx int32) int64 {
	return h.counts[h.countsIndex(bucketIdx, subBucketIdx)]
}

func (h *Histogram) getCountAtIndexGivenBucketBaseIdx(bucketBaseIdx, subBucketIdx int32) int64 {
	return h.counts[bucketBaseIdx+subBucketIdx-h.subBucketHalfCount]
}

func (h *Histogram) countsIndex(bucketIdx, subBucketIdx int32) int32 {
	return h.getBucketBaseIdx(bucketIdx) + subBucketIdx - h.subBucketHalfCount
}

func (h *Histogram) getBucketBaseIdx(bucketIdx int32) int32 {
	return (bucketIdx + 1) << uint(h.subBucketHalfCountMagnitude)
}

// return the lowest (and therefore highest precision) bucket index that can represent the value
// Calculates the number of powers of two by which the value is greater than the biggest value that fits in
// bucket 0. This is the bucket index since each successive bucket can hold a value 2x greater.
func (h *Histogram) getBucketIndex(v int64) int32 {
	var pow2Ceiling = int64(64 - bits.LeadingZeros64(uint64(v|h.subBucketMask)))
	return int32(pow2Ceiling - int64(h.unitMagnitude) -
		int64(h.subBucketHalfCountMagnitude+1))
}

// For bucketIndex 0, this is just value, so it may be anywhere in 0 to subBucketCount.
// For other bucketIndex, this will always end up in the top half of subBucketCount: assume that for some bucket
// k > 0, this calculation will yield a value in the bottom half of 0 to subBucketCount. Then, because of how
// buckets overlap, it would have also been in the top half of bucket k-1, and therefore would have
// returned k-1 in getBucketIndex(). Since we would then shift it one fewer bits here, it would be twice as big,
// and therefore in the top half of subBucketCount.
func (h *Histogram) getSubBucketIdx(v int64, idx int32) int32 {
	return int32(v >> uint(int64(idx)+int64(h.unitMagnitude)))
}

func (h *Histogram) countsIndexFor(v int64) int {
	bucketIdx := h.getBucketIndex(v)
	subBucketIdx := h.getSubBucketIdx(v, bucketIdx)
	return int(h.countsIndex(bucketIdx, subBucketIdx))
}

func (h *Histogram) getIntegerToDoubleValueConversionRatio() float64 {
	return 1.0
}

type iterator struct {
	h                                    *Histogram
	bucketIdx, subBucketIdx              int32
	countAtIdx, countToIdx, valueFromIdx int64
	highestEquivalentValue               int64
}

// nextCountAtIdx does not update the iterator highestEquivalentValue in order to optimize cpu usage.
func (i *iterator) nextCountAtIdx(limit int64) bool {
	if i.countToIdx >= limit {
		return false
	}
	// increment bucket
	i.subBucketIdx++
	if i.subBucketIdx >= i.h.subBucketCount {
		i.subBucketIdx = i.h.subBucketHalfCount
		i.bucketIdx++
	}

	if i.bucketIdx >= i.h.bucketCount {
		return false
	}

	i.countAtIdx = i.h.getCountAtIndex(i.bucketIdx, i.subBucketIdx)
	i.countToIdx += i.countAtIdx
	i.valueFromIdx = i.h.valueFromIndex(i.bucketIdx, i.subBucketIdx)
	return true
}

// Returns the next element in the iteration.
func (i *iterator) next() bool {
	if !i.nextCountAtIdx(i.h.totalCount) {
		return false
	}
	i.highestEquivalentValue = i.h.highestEquivalentValue(i.valueFromIdx)
	return true
}

type rIterator struct {
	iterator
	countAddedThisStep int64
}

func (r *rIterator) next() bool {
	for r.iterator.next() {
		if r.countAtIdx != 0 {
			r.countAddedThisStep = r.countAtIdx
			return true
		}
	}
	return false
}

type pIterator struct {
	iterator
	seenLastValue          bool
	ticksPerHalfDistance   int32
	percentileToIteratorTo float64
	percentile             float64
}

func (p *pIterator) next() bool {
	if !(p.countToIdx < p.h.totalCount) {
		if p.seenLastValue {
			return false
		}

		p.seenLastValue = true
		p.percentile = 100

		return true
	}

	if p.subBucketIdx == -1 && !p.iterator.next() {
		return false
	}

	var done = false
	for !done {
		currentPercentile := (100.0 * float64(p.countToIdx)) / float64(p.h.totalCount)
		if p.countAtIdx != 0 && p.percentileToIteratorTo <= currentPercentile {
			p.percentile = p.percentileToIteratorTo
			halfDistance := math.Trunc(math.Pow(2, math.Trunc(math.Log2(100.0/(100.0-p.percentileToIteratorTo)))+1))
			percentileReportingTicks := float64(p.ticksPerHalfDistance) * halfDistance
			p.percentileToIteratorTo += 100.0 / percentileReportingTicks
			return true
		}
		done = !p.iterator.next()
	}

	return true
}

// CumulativeDistribution returns an ordered list of brackets of the
// distribution of recorded values.
func (h *Histogram) CumulativeDistributionWithTicks(ticksPerHalfDistance int32) []Bracket {
	var result []Bracket

	i := h.pIterator(ticksPerHalfDistance)
	for i.next() {
		result = append(result, Bracket{
			Quantile: i.percentile,
			Count:    i.countToIdx,
			ValueAt:  int64(i.highestEquivalentValue),
		})
	}

	return result
}

// Output the percentiles distribution in a text format
func (h *Histogram) PercentilesPrint(writer io.Writer, ticksPerHalfDistance int32, valueScale float64) (outputWriter io.Writer, err error) {
	outputWriter = writer
	dist := h.CumulativeDistributionWithTicks(ticksPerHalfDistance)
	_, err = outputWriter.Write([]byte(" Value\tPercentile\tTotalCount\t1/(1-Percentile)\n\n"))
	if err != nil {
		return
	}
	for _, slice := range dist {
		percentile := slice.Quantile / 100.0
		inverted_percentile := 1.0 / (1.0 - percentile)
		var inverted_percentile_string = fmt.Sprintf("%12.2f", inverted_percentile)
		// Given that other language implementations display inf (instead of Go's +Inf)
		// we want to be as close as possible to them
		if math.IsInf(inverted_percentile, 1) {
			inverted_percentile_string = fmt.Sprintf("%12s", "inf")
		}
		_, err = outputWriter.Write([]byte(fmt.Sprintf("%12.3f %12f %12d %s\n", float64(slice.ValueAt)/valueScale, percentile, slice.Count, inverted_percentile_string)))
		if err != nil {
			return
		}
	}

	footer := fmt.Sprintf("#[Mean    = %12.3f, StdDeviation   = %12.3f]\n#[Max     = %12.3f, Total count    = %12d]\n#[Buckets = %12d, SubBuckets     = %12d]\n",
		h.Mean()/valueScale,
		h.StdDev()/valueScale,
		float64(h.Max())/valueScale,
		h.TotalCount(),
		h.bucketCount,
		h.subBucketCount,
	)
	_, err = outputWriter.Write([]byte(footer))
	return
}
