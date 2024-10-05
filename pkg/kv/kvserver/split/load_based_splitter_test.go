// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package split

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/ycsb"
	"github.com/cockroachdb/datadriven"
	"golang.org/x/exp/rand"
)

// This is a testing framework to benchmark load-based splitters, enabling
// experimenting and comparison of different designs and settings.
//
// This testing framework inputs:
// 1. Generator settings to generate the requests for the load splitter to
// record:
//   - Start key generator type (zipfian or uniform) and iMax
//   - Span length generator type (zipfian or uniform) and iMax
//   - Weight generator type (zipfian or uniform) and iMax.
// 2. Range request percent (percent of range requests [startKey, endKey) as
// opposed to point requests with just a start key).
// 3. Load-based splitter constructor.
// 4. Random seed.
//
// This testing framework performs the following work in runTest:
// 1. Calls generateRequests to generate the requests for the load splitter to
// record. The start key, span length, and weight of requests are generated
// using the input generator types and iMax, as well as the range request
// percent to determine if we have a point or range request.
// 2. Calls getOptimalKey to calculate the optimal split key assuming we are an
// oracle with access to all the request weighted keys, the total weight of
// request keys left of this key, and the total weight of request keys right of
// this key. The optimal key is found by sorting all the request keys and
// finding the request key that minimizes the difference between the total
// weight of the right side and the total weight of the left side.
// 3. Calls getKey to evaluate the split key found by the load-based splitter.
// This is done by constructing the load splitter, invoking the load splitter's
// Record function on all the generated requests, invoking the load splitter's
// Key function to get the split key, and calculating the total weight on the
// left and right sides. We also maintain the times it took to execute the load
// splitter's Record and Key functions.
//
// This testing framework also supports runTestRepeated, which repeats the test
// multiple times with different random numbers and calculating the average /
// max percentage differences of left / right weights and average execution
// times. This testing framework also supports runTestMultipleSettings, which
// runs tests with different settings and prints the test results for each
// setting.
//
// This testing framework is intended for benchmarking different load splitter
// approaches and settings in terms of quality of split key found and execution
// time, and will probably be in the code for at least a month after the time
// of writing and possibly longer if experimenting load splitter designs is
// ongoing or the testing framework is deemed useful in the future. If the load
// splitter's found split key becomes consistent, we may be able to assert on
// the split key quality of the load splitter.

const (
	zipfGenerator     = 0
	uniformGenerator  = 1
	defaultIterations = 200
)

func genToString(gen int) string {
	switch gen {
	case zipfGenerator:
		return "zip"
	case uniformGenerator:
		return "uni"
	default:
		panic("unknown gen")
	}
}

var startTime = time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)

type request struct {
	span   roachpb.Span
	weight float64
}

type weightedKey struct {
	key    uint32
	weight float64
}

type generator interface {
	Uint64() uint64
}

func newGenerator(randSource *rand.Rand, generatorType int, iMax uint64) generator {
	var g generator
	var err error
	if generatorType == zipfGenerator {
		g, err = ycsb.NewZipfGenerator(randSource, 1, iMax, 0.99, false)
	} else if generatorType == uniformGenerator {
		g, err = ycsb.NewUniformGenerator(randSource, 1, iMax)
	} else {
		panic("generatorType must be zipfGenerator or uniformGenerator")
	}
	if err != nil {
		panic(err)
	}
	return g
}

// We want to apply a weight distribution over the keyspace, where the
// weight(key)/sum(weight(keys)) is some fraction representing the relative
// load on the key. We also want to apply an access distribution over the
// keyspace, where keys are accessed with some probability, semi-independently
// of their weights. If a key is never accessed it cannot have weight, so the
// weight and access are not independent. For example, the weight and access
// distributions may look something like this:
//
// weight distribution   access distribution
//
//	  |x                 |
//	  |x x               |
//	w |x x x x         w |
//	  |x x x x x x       |x x x x x x
//	  +-----------       +-----------
//	       k                  k
//
// In order to get these two distributions, multiple request configs are
// necessary. Which can then be mixed with mixGenerators.
type requestConfig struct {
	startKeyGeneratorType   int
	startKeyGeneratorIMax   uint64
	spanLengthGeneratorType int
	spanLengthGeneratorIMax uint64
	weightGeneratorType     int
	weightGeneratorIMax     uint64
	rangeRequestPercent     float64
	numRequests             int
}

func (rc requestConfig) String() string {
	return fmt.Sprintf("w=%s(%d)/k=%s(%d)/s=%s(%d)/s(%%)=%d/%d",
		genToString(rc.weightGeneratorType), rc.weightGeneratorIMax,
		genToString(rc.startKeyGeneratorType), rc.startKeyGeneratorIMax,
		genToString(rc.spanLengthGeneratorType), rc.spanLengthGeneratorIMax,
		int(rc.rangeRequestPercent*100),
		rc.numRequests,
	)
}

func (rc requestConfig) makeGenerator(randSource *rand.Rand) requestGenerator {
	return requestGenerator{
		startKeyGenerator:   newGenerator(randSource, rc.startKeyGeneratorType, rc.startKeyGeneratorIMax),
		spanLengthGenerator: newGenerator(randSource, rc.spanLengthGeneratorType, rc.spanLengthGeneratorIMax),
		weightGenerator:     newGenerator(randSource, rc.weightGeneratorType, rc.weightGeneratorIMax),
		rangeRequestPercent: rc.rangeRequestPercent,
		numRequests:         rc.numRequests,
	}
}

type multiReqConfig struct {
	reqConfigs []requestConfig
	mix        mixType
}

func (mrc multiReqConfig) makeGenerator(randSource *rand.Rand) multiRequestGenerator {
	gens := make([]requestGenerator, 0, len(mrc.reqConfigs))
	var numRequests int
	for _, reqConfig := range mrc.reqConfigs {
		gen := reqConfig.makeGenerator(randSource)
		numRequests += gen.numRequests
		gens = append(gens, gen)
	}

	return multiRequestGenerator{
		requestGenerators: gens,
		mix:               mrc.mix,
		rand:              randSource,
		numRequests:       numRequests,
	}
}

type mixType int

const (
	sequential mixType = iota
	permute
)

func (mrc multiReqConfig) String() string {
	var buf bytes.Buffer
	for _, rc := range mrc.reqConfigs {
		fmt.Fprintf(&buf, "%s\n", rc)
	}
	return buf.String()
}

type requestGenerator struct {
	startKeyGenerator   generator
	spanLengthGenerator generator
	weightGenerator     generator
	rangeRequestPercent float64
	numRequests         int
}

func (rg requestGenerator) generate() ([]request, []weightedKey, float64) {
	var totalWeight float64
	requests := make([]request, 0, rg.numRequests)
	weightedKeys := make([]weightedKey, 0, 2*rg.numRequests)

	for i := 0; i < rg.numRequests; i++ {
		startKey := uint32(rg.startKeyGenerator.Uint64())
		spanLength := uint32(rg.spanLengthGenerator.Uint64())
		weight := float64(rg.weightGenerator.Uint64())

		var span roachpb.Span
		span.Key = uint32ToKey(startKey)
		if rand.Float64() < rg.rangeRequestPercent {
			endKey := startKey + spanLength
			span.EndKey = uint32ToKey(endKey)

			weightedKeys = append(weightedKeys, weightedKey{
				key:    startKey,
				weight: weight / 2,
			})
			weightedKeys = append(weightedKeys, weightedKey{
				key:    endKey,
				weight: weight / 2,
			})
		} else {
			weightedKeys = append(weightedKeys, weightedKey{
				key:    startKey,
				weight: weight,
			})
		}
		requests = append(requests, request{
			span:   span,
			weight: weight,
		})
		totalWeight += weight
	}
	return requests, weightedKeys, totalWeight
}

type multiRequestGenerator struct {
	requestGenerators []requestGenerator
	mix               mixType
	rand              *rand.Rand
	numRequests       int
}

func (mrg multiRequestGenerator) generate() ([]request, []weightedKey, float64) {
	numRequests := 0
	for _, gen := range mrg.requestGenerators {
		numRequests += gen.numRequests
	}

	var totalWeight float64
	requests := make([]request, 0, numRequests)
	weightedKeys := make([]weightedKey, 0, numRequests*2)
	for _, gen := range mrg.requestGenerators {
		reqs, keys, weight := gen.generate()
		requests = append(requests, reqs...)
		weightedKeys = append(weightedKeys, keys...)
		totalWeight += weight
	}

	switch mrg.mix {
	case sequential:
		// Nothing to do.
	case permute:
		perms := mrg.rand.Perm(numRequests)
		permutedRequests := make([]request, numRequests)
		for i, idx := range perms {
			permutedRequests[idx] = requests[i]
		}
		requests = permutedRequests
	default:
		panic("unknown mix type")
	}

	return requests, weightedKeys, totalWeight
}

type finderConfig struct {
	weighted bool
}

func (fc finderConfig) makeFinder(randSource rand.Source) LoadBasedSplitter {
	if fc.weighted {
		return NewWeightedFinder(startTime, rand.New(randSource))
	}
	return NewUnweightedFinder(startTime, rand.New(randSource))
}

type deciderConfig struct {
	threshold float64
	retention time.Duration
	objective SplitObjective
	duration  time.Duration
}

func (dc deciderConfig) makeDecider(randSource rand.Source) *Decider {
	d := &Decider{}
	loadSplitConfig := testLoadSplitConfig{
		randSource:    rand.New(randSource),
		useWeighted:   dc.objective == SplitCPU,
		statRetention: dc.retention,
		statThreshold: dc.threshold,
	}

	Init(d, &loadSplitConfig, &LoadSplitterMetrics{
		PopularKeyCount: metric.NewCounter(metric.Metadata{}),
		NoSplitKeyCount: metric.NewCounter(metric.Metadata{}),
	}, dc.objective)
	return d
}

type lbsTestSettings struct {
	requestConfig multiReqConfig
	deciderConfig *deciderConfig
	finderConfig  *finderConfig
	seed          uint64
	iterations    int
	showLastState bool
}

func uint32ToKey(key uint32) roachpb.Key {
	return keys.SystemSQLCodec.TablePrefix(key)
}

func getOptimalKey(
	weightedKeys []weightedKey, totalWeight float64,
) (optimalKey uint32, optimalLeftWeight, optimalRightWeight float64) {
	var optimalKeyPtr *uint32
	var leftWeight float64
	sort.Slice(weightedKeys, func(i, j int) bool {
		return weightedKeys[i].key < weightedKeys[j].key
	})
	for i := range weightedKeys {
		rightWeight := totalWeight - leftWeight
		// Find the split key that results in the smallest difference between the
		// total weight of keys on the right side and the total weight of keys on
		// the left side.
		if optimalKeyPtr == nil ||
			math.Abs(rightWeight-leftWeight) < math.Abs(optimalRightWeight-optimalLeftWeight) {
			optimalKeyPtr = &weightedKeys[i].key
			optimalLeftWeight = leftWeight
			optimalRightWeight = rightWeight
		}
		leftWeight += weightedKeys[i].weight
	}
	optimalKey = *optimalKeyPtr
	return
}

func getKey(
	recordFn func(span roachpb.Span, weight int), keyFn func() roachpb.Key, requests []request,
) (
	key uint32,
	leftWeight, rightWeight float64,
	recordExecutionTime, keyExecutionTime time.Duration,
	noKeyFound bool,
) {
	recordStart := timeutil.Now()
	for _, request := range requests {
		recordFn(request.span, int(request.weight))
	}
	recordExecutionTime = timeutil.Since(recordStart)
	keyStart := timeutil.Now()
	keyRoachpbKey := keyFn()
	keyExecutionTime = timeutil.Since(keyStart)
	noKeyFound = keyRoachpbKey.Equal(keys.MinKey)

	_, key, _ = keys.SystemSQLCodec.DecodeTablePrefix(keyRoachpbKey)
	return
}

type result struct {
	key, optimalKey                                                uint32
	leftWeight, rightWeight, optimalLeftWeight, optimalRightWeight float64
	recordExecutionTime, keyExecutionTime                          time.Duration
	noKeyFound                                                     bool
}

func runTest(
	recordFn func(span roachpb.Span, weight int),
	keyFn func() roachpb.Key,
	randSource rand.Source,
	mrg multiRequestGenerator,
) result {
	requests, weightedKeys, totalWeight := mrg.generate()
	optimalKey, optimalLeftWeight, optimalRightWeight := getOptimalKey(
		weightedKeys,
		totalWeight,
	)
	key, leftWeight, rightWeight, recordExecutionTime,
		keyExecutionTime, noKeyFound := getKey(
		recordFn,
		keyFn,
		requests,
	)
	for _, weightedKey := range weightedKeys {
		if weightedKey.key < key {
			leftWeight += weightedKey.weight
		} else {
			rightWeight += weightedKey.weight
		}
	}
	return result{
		key:                 key,
		optimalKey:          optimalKey,
		leftWeight:          leftWeight,
		rightWeight:         rightWeight,
		optimalLeftWeight:   optimalLeftWeight,
		optimalRightWeight:  optimalRightWeight,
		recordExecutionTime: recordExecutionTime,
		keyExecutionTime:    keyExecutionTime,
		noKeyFound:          noKeyFound,
	}
}

type repeatedResult struct {
	avgPercentDifference, maxPercentDifference,
	avgOptimalPercentDifference, maxOptimalPercentDifference float64
	avgRecordExecutionTime, avgKeyExecutionTime time.Duration
	noKeyFoundPercent                           float64
	lastStateString                             string
}

func resultTable(configs []multiReqConfig, rr []repeatedResult, showTiming bool) string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "description\t"+
		"no_key(%)\t"+
		"avg_diff(%)\t"+
		"max_diff(%)\t"+
		"avg_optimal_diff(%)\t"+
		"max_optimal_diff(%)",
	)
	if showTiming {
		_, _ = fmt.Fprintln(w,
			"\tavg_record_time\t"+
				"avg_key_time",
		)
	}

	for i, r := range rr {
		n := len(configs[i].reqConfigs)
		var desc string
		// When there are multiple descriptions, print out the results in the
		// first row and the rest of the descriptions in subsequent rows with
		// empty results.
		if n > 1 {
			desc = fmt.Sprintf("mixed_requests(%d)", n)
		} else {
			desc = configs[i].reqConfigs[0].String()
		}

		_, _ = fmt.Fprintf(w, "%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f",
			desc,
			r.noKeyFoundPercent,
			r.avgPercentDifference,
			r.maxPercentDifference,
			r.avgOptimalPercentDifference,
			r.maxOptimalPercentDifference,
		)

		if showTiming {
			_, _ = fmt.Fprintf(w, "\t%s\t%s",
				r.avgRecordExecutionTime,
				r.avgKeyExecutionTime,
			)
		}

		_, _ = fmt.Fprintln(w)

		// We already printed the results and description for the single config
		// case.
		if n == 1 {
			continue
		}
		// Print the descriptions for the multi-config case.
		for _, reqConfig := range configs[i].reqConfigs {
			_, _ = fmt.Fprintf(w, "%s\t\t\t\t\t\n", reqConfig)
		}
	}
	_ = w.Flush()

	return buf.String()
}

func runTestRepeated(settings *lbsTestSettings) repeatedResult {
	var (
		avgPercentDifference, maxPercentDifference,
		avgOptimalPercentDifference, maxOptimalPercentDifference float64
		avgRecordExecutionTime, avgKeyExecutionTime time.Duration
		noKeyFoundPercent                           float64
		lastStateString                             string
	)
	randSource := rand.New(rand.NewSource(settings.seed))
	requestGen := settings.requestConfig.makeGenerator(randSource)

	for i := 0; i < settings.iterations; i++ {
		var recordFn func(span roachpb.Span, weight int)
		var keyFn func() roachpb.Key
		var stringFn func() string

		if settings.deciderConfig != nil {
			d := settings.deciderConfig.makeDecider(randSource)
			now := startTime
			duration := settings.deciderConfig.duration
			requestInterval := duration / time.Duration(requestGen.numRequests)

			ctx := context.Background()
			recordFn = func(span roachpb.Span, weight int) {
				d.Record(ctx, now,
					func(SplitObjective) int { return weight },
					func() roachpb.Span { return span },
				)
				now = now.Add(requestInterval)
			}
			keyFn = func() roachpb.Key {
				return d.MaybeSplitKey(ctx, now)
			}
			stringFn = (*lockedDecider)(d).String
		} else {
			f := settings.finderConfig.makeFinder(randSource)
			recordFn = func(span roachpb.Span, weight int) {
				f.Record(span, float64(weight))
			}
			keyFn = func() roachpb.Key {
				return f.Key()
			}
			stringFn = f.String
		}
		ret := runTest(recordFn, keyFn, randSource, requestGen)

		if !ret.noKeyFound {
			percentDifference := 100 * math.Abs(ret.leftWeight-ret.rightWeight) / (ret.leftWeight + ret.rightWeight)
			avgPercentDifference += percentDifference
			if maxPercentDifference < percentDifference {
				maxPercentDifference = percentDifference
			}
			optimalPercentDifference := 100 *
				math.Abs(ret.optimalLeftWeight-ret.optimalRightWeight) /
				(ret.optimalLeftWeight + ret.optimalRightWeight)

			avgOptimalPercentDifference += optimalPercentDifference
			if maxOptimalPercentDifference < optimalPercentDifference {
				maxOptimalPercentDifference = optimalPercentDifference
			}
		} else {
			noKeyFoundPercent++
		}
		avgRecordExecutionTime += ret.recordExecutionTime
		avgKeyExecutionTime += ret.keyExecutionTime

		if i == settings.iterations-1 && settings.showLastState {
			lastStateString = stringFn()
		}
	}

	nRuns := settings.iterations
	keyRuns := nRuns - int(noKeyFoundPercent)

	avgRecordExecutionTime = time.Duration(avgRecordExecutionTime.Nanoseconds() / int64(nRuns))
	avgKeyExecutionTime = time.Duration(avgKeyExecutionTime.Nanoseconds() / int64(nRuns))
	avgPercentDifference /= float64(keyRuns)
	avgOptimalPercentDifference /= float64(keyRuns)
	noKeyFoundPercent /= float64(nRuns)
	// We want a percent (0-100).
	noKeyFoundPercent *= 100

	return repeatedResult{
		avgPercentDifference:        avgPercentDifference,
		maxPercentDifference:        maxPercentDifference,
		avgOptimalPercentDifference: avgOptimalPercentDifference,
		maxOptimalPercentDifference: maxOptimalPercentDifference,
		avgRecordExecutionTime:      avgRecordExecutionTime,
		avgKeyExecutionTime:         avgKeyExecutionTime,
		noKeyFoundPercent:           noKeyFoundPercent,
		lastStateString:             lastStateString,
	}
}

func cartesianProduct(lists [][]int) [][]int {
	// Base case: if there's only one list, return it as a nested list.
	if len(lists) == 1 {
		result := [][]int{}
		for _, elem := range lists[0] {
			result = append(result, []int{elem})
		}
		return result
	}

	// Recursive case: compute the cartesian product of the first list and the
	// product of the rest of the lists
	rest := cartesianProduct(lists[1:])
	result := [][]int{}
	for _, elem := range lists[0] {
		for _, partial := range rest {
			result = append(result, append([]int{elem}, partial...))
		}
	}
	return result
}

func enumerateBasicRequestConfigs() []requestConfig {
	distributions := []int{uniformGenerator, zipfGenerator}
	spanLengths := []int{1, 1000}
	startKeyMaxes := []int{10000, 1000000}
	weightMaxes := []int{100, 10000}
	rangePercents := []int{0, 20, 95}

	cartesian := cartesianProduct([][]int{
		distributions, // Weight distribution.
		weightMaxes,   // Weight max.
		distributions, // Start key distribution.
		startKeyMaxes, // Start key max.
		distributions, // Span distribution.
		spanLengths,   // Span length max.
		rangePercents, // Range request percent.
	})

	configMap := map[requestConfig]struct{}{}

	configs := make([]requestConfig, 0, len(cartesian)/2)
	for _, c := range cartesian {
		config := requestConfig{
			weightGeneratorType:     c[0],
			weightGeneratorIMax:     uint64(c[1]),
			startKeyGeneratorType:   c[2],
			startKeyGeneratorIMax:   uint64(c[3]),
			spanLengthGeneratorType: c[4],
			spanLengthGeneratorIMax: uint64(c[5]),
			rangeRequestPercent:     float64(c[6]) / 100.0,
			// Always use 10k requests for simplicity.
			numRequests: 10000,
		}

		// Don't include configurations where there's a non-zero span percent but
		// no span length or vice versa.
		if config.spanLengthGeneratorIMax > 1 && config.rangeRequestPercent == 0 ||
			(config.spanLengthGeneratorIMax <= 1 && config.rangeRequestPercent > 0) {
			continue
		}
		// No point running both zipfian and uniform for unweighted, only use
		// uniform.
		if config.weightGeneratorIMax == 1 && config.weightGeneratorType == zipfGenerator {
			continue
		}

		// No point running both zipfian and uniform spans for 1 span length.
		if config.spanLengthGeneratorIMax == 1 &&
			config.spanLengthGeneratorType == zipfGenerator {
			continue
		}

		// Don't include duplicate entries.
		if _, present := configMap[config]; present {
			continue
		}
		configMap[config] = struct{}{}
		configs = append(configs, config)
	}
	return configs
}

func makeMultiRequestConfigs(
	seed uint64, mixCount int, mix mixType, requestConfigs ...requestConfig,
) []multiReqConfig {
	ret := []multiReqConfig{}
	if mixCount == 0 {
		for _, reqConfig := range requestConfigs {
			ret = append(ret, multiReqConfig{
				reqConfigs: []requestConfig{reqConfig},
				mix:        mix,
			})
		}
	} else {
		// This is expensive, we could instead pass in the source.
		n := len(requestConfigs)
		rand := rand.New(rand.NewSource(seed))
		perms := rand.Perm(n)
		for i := 0; i < n; i += mixCount {
			mrc := multiReqConfig{
				mix:        mix,
				reqConfigs: make([]requestConfig, 0, mixCount),
			}
			for j := 0; j < mixCount && i+j < n; j++ {
				mrc.reqConfigs = append(mrc.reqConfigs, requestConfigs[perms[i+j]])
			}
			ret = append(ret, mrc)
		}
	}
	return ret
}

// TestDataDriven is a data-driven test for testing the integrated load
// splitter. It allows testing the results of different request distributions
// and sampling durations against the optimal split key. The commands provided
// are:
//
//   - "requests" key_dist=(zipfian|uniform) key_max=<int>
//     span_dist=(zipfian|uniform) span_max=<int> weight_dist=(zipfian|uniform)
//     weight_max=<int> range_request_percent=<float> request_count=<int>
//     key_dist is the distribution of start keys, key_max is the maximum start
//     key, span_dist is the distribution of span lengths (offset by the start
//     key), span_max is the maximum span length, weight_dist is the distribution
//     of span weights, weight_max is the maximum span weight,
//     range_request_percent is the percentage of requests which will be spanning
//     i.e. non-point requests and request_count is the number of requests to
//     generate.
//
//   - "decider" duration=<int> threshold=<int> retention=<int>
//     objective=(cpu|qps) duration is the time in seconds the decider will
//     receive requests over, threshold is number above which the decider will
//     instantiate a key finder, retention is the time that requests are tracked
//     for in the sliding max window and objective is the split objective - which
//     acts the same as weighted vs unweighted for CPU and QPS.
//
//     The decider is the container and manager of the finder, if this is
//     specified, then a finder will be instantiated on the next call to eval. It
//     is added here for completeness and e2e testing of the pkg.
//
//   - "finder" weighted=<bool> weighted indicates the finder should be the
//     weighted variation.
//
//     The finder, resolves finding an actual split key given incoming requests.
//     The finder does not have notions of timing, unlike the decider.
//
//   - "eval" seed=<int> iterations=<int> cartesian=<bool>
//     [mix=(sequential|perm) mix_count=<int>] seed is the seed to instantiate
//     the current finder/decider and request generators with, iterations is the
//     number of iterations to repeatedly run before synthesizing a result,
//     cartesian evaluates a list of configurations with every different
//     distribution and a few maximum values for span length, start key and
//     weight, mix and mix_count mix loaded requests together and evaluate them
//     in one test; mixing can be used to generate different access and weight
//     distributions.
//
// To test multiple concurrent splitters in a cluster, see
// asim/tests/testdata/example_splitting, which enables larger testing.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t, "takes 20s")
	skip.UnderRace(t, "takes 20s")

	parseGeneratorType := func(dist string) int {
		switch dist {
		case "zipfian":
			return zipfGenerator
		case "uniform":
			return uniformGenerator
		default:
			panic("unknown gen type")
		}
	}

	parseObjType := func(obj string) SplitObjective {
		switch obj {
		case SplitCPU.String():
			return SplitCPU
		case SplitQPS.String():
			return SplitQPS
		default:
			panic("unknown split objective")
		}
	}

	dir := datapathutils.TestDataPath(t, "")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		reqConfigs := []requestConfig{}
		var decConfig *deciderConfig
		var findConfig *finderConfig
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "requests":
				var keyDist, spanDist, weightDist string
				var keyIMax, spanIMax, weightIMax int
				var rangeRequestPercent, requestCount int

				d.ScanArgs(t, "key_dist", &keyDist)
				d.ScanArgs(t, "span_dist", &spanDist)
				d.ScanArgs(t, "weight_dist", &weightDist)

				d.ScanArgs(t, "key_max", &keyIMax)
				d.ScanArgs(t, "span_max", &spanIMax)
				d.ScanArgs(t, "weight_max", &weightIMax)

				d.ScanArgs(t, "range_request_percent", &rangeRequestPercent)
				d.ScanArgs(t, "request_count", &requestCount)

				reqConfigs = append(reqConfigs, requestConfig{
					startKeyGeneratorType:   parseGeneratorType(keyDist),
					startKeyGeneratorIMax:   uint64(keyIMax),
					spanLengthGeneratorType: parseGeneratorType(spanDist),
					spanLengthGeneratorIMax: uint64(spanIMax),
					weightGeneratorType:     parseGeneratorType(weightDist),
					weightGeneratorIMax:     uint64(weightIMax),
					rangeRequestPercent:     float64(rangeRequestPercent) / 100.0,
					numRequests:             requestCount,
				})
				return ""
			case "finder":
				var weighted bool
				d.ScanArgs(t, "weighted", &weighted)

				findConfig = &finderConfig{
					weighted: weighted,
				}
				decConfig = nil
			case "decider":
				var duration int
				var threshold int
				var retentionSeconds, durationSeconds int
				var objective string

				d.ScanArgs(t, "duration", &duration)
				d.ScanArgs(t, "retention", &retentionSeconds)
				d.ScanArgs(t, "duration", &durationSeconds)
				d.ScanArgs(t, "objective", &objective)
				d.ScanArgs(t, "threshold", &threshold)
				splitObj := parseObjType(objective)

				decConfig = &deciderConfig{
					threshold: float64(threshold),
					retention: time.Duration(retentionSeconds) * time.Second,
					objective: splitObj,
					duration:  time.Duration(duration) * time.Second,
				}
				findConfig = nil
			case "eval":
				var seed uint64
				var iterations, mixCount int
				var showTiming, cartesian, all, showLastState bool
				var mix string
				var mixT mixType

				d.ScanArgs(t, "seed", &seed)
				d.ScanArgs(t, "iterations", &iterations)
				d.MaybeScanArgs(t, "timing", &showTiming)
				d.MaybeScanArgs(t, "cartesian", &cartesian)
				d.MaybeScanArgs(t, "all", &all)
				d.MaybeScanArgs(t, "show_last", &showLastState)
				if d.HasArg("mix") {
					d.ScanArgs(t, "mix", &mix)
					d.ScanArgs(t, "mix_count", &mixCount)
					switch mix {
					case "sequential":
						mixT = sequential
					case "perm":
						mixT = permute
					default:
						panic("unknown mix type")
					}
				}

				n := len(reqConfigs)
				var requestConfigs []requestConfig
				if cartesian {
					requestConfigs = enumerateBasicRequestConfigs()
				} else if all {
					requestConfigs = reqConfigs
				} else {
					if n == 0 {
						panic("no request config specified")
					}
					if mixCount > 0 {
						requestConfigs = reqConfigs[n-mixCount : n]
					} else {
						requestConfigs = reqConfigs[n-1 : n]
					}
				}

				evalRequestConfigs := makeMultiRequestConfigs(
					seed, mixCount, mixT, requestConfigs...)

				repeatedResults := make([]repeatedResult, len(evalRequestConfigs))
				for i, r := range evalRequestConfigs {
					repeatedResults[i] = runTestRepeated(&lbsTestSettings{
						requestConfig: r,
						deciderConfig: decConfig,
						finderConfig:  findConfig,
						seed:          seed,
						iterations:    iterations,
						showLastState: showLastState,
					})
				}
				retTable := resultTable(evalRequestConfigs, repeatedResults, showTiming)
				if showLastState {
					showRequestConfgDesc := len(evalRequestConfigs) > 1
					var buf strings.Builder
					for i := range evalRequestConfigs {
						if i > 0 {
							buf.WriteString("\n")
						}
						if showRequestConfgDesc {
							buf.WriteString(evalRequestConfigs[i].String())
						}
						fmt.Fprintf(&buf, "\t%s",
							repeatedResults[i].lastStateString)
					}
					return fmt.Sprintf("%s%s", retTable, buf.String())
				}
				return retTable
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
