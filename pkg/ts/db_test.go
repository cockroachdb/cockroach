// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/ts/testmodel"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// testModelRunner is a model-based testing structure used to verify that time
// series data sent to the Cockroach time series DB is stored correctly.
//
// This structure maintains a single ts.DB instance which stores data in a
// monolithic Cockroach Store. It additionally maintains a test-model, a fully
// in-memory implementation of the CockroachDB storage and query system. The
// test model is unoptimized and in-memory, making it easier to understand than
// the distributed and highly-optimized system used by real queries. The model
// is used to generate expected results for test cases automatically.
//
// Each test should send a series of commands to the testModelRunner. Commands
// are dispatched to both the ts.DB instance and the test model. Queries are
// executed against both, and the results should match exactly.
//
// In addition, the test model can be used to generate an expecation of the
// on-disk layout in the ts.DB instance; the tests should periodically assert
// that the expectation matches reality.
//
// Finally, testModelRunner provides a small number of sanity checks
// (assertKeyCount) that ensure that the real data does not trivially match the
// model due to an improperly constructed test case.
type testModelRunner struct {
	*localtestcluster.LocalTestCluster
	t                 testing.TB
	DB                *DB
	model             *testmodel.ModelDB
	workerMemMonitor  *mon.BytesMonitor
	resultMemMonitor  *mon.BytesMonitor
	queryMemoryBudget int64
	// firstColumnarTimestamp is a map from a string name for a series to the
	// first timestamp at which columnar data was inserted into that timestamp.
	// This is used when computing the expected on-disk layout from the model.
	firstColumnarTimestamp map[string]int64
}

// newTestModelRunner creates a new testModel instance. The Start() method must
// be called before using it.
func newTestModelRunner(t *testing.T) testModelRunner {
	st := cluster.MakeTestingClusterSettings()
	workerMonitor := mon.NewUnlimitedMonitor(
		context.Background(),
		"timeseries-test-worker",
		mon.MemoryResource,
		nil,
		nil,
		math.MaxInt64,
		st,
	)
	resultMonitor := mon.NewUnlimitedMonitor(
		context.Background(),
		"timeseries-test-result",
		mon.MemoryResource,
		nil,
		nil,
		math.MaxInt64,
		st,
	)
	return testModelRunner{
		t:                      t,
		model:                  testmodel.NewModelDB(),
		LocalTestCluster:       &localtestcluster.LocalTestCluster{},
		workerMemMonitor:       workerMonitor,
		resultMemMonitor:       resultMonitor,
		queryMemoryBudget:      math.MaxInt64,
		firstColumnarTimestamp: make(map[string]int64),
	}
}

// Start constructs and starts the local test server and creates a
// time series DB.
func (tm *testModelRunner) Start() {
	tm.LocalTestCluster.Start(tm.t, testutils.NewNodeTestBaseContext(),
		kvcoord.InitFactoryForLocalTestCluster)
	tm.DB = NewDB(tm.LocalTestCluster.DB, tm.Cfg.Settings)
}

// getActualData returns the actual value of all time series keys in the
// underlying engine. Data is returned as a map of strings to roachpb.Values.
func (tm *testModelRunner) getActualData() map[string]roachpb.Value {
	// Scan over all TS Keys stored in the engine
	startKey := keys.TimeseriesPrefix
	endKey := startKey.PrefixEnd()
	res, err := storage.MVCCScan(context.Background(), tm.Eng, startKey, endKey, tm.Clock.Now(), storage.MVCCScanOptions{})
	if err != nil {
		tm.t.Fatalf("error scanning TS data from engine: %s", err)
	}

	kvMap := make(map[string]roachpb.Value)
	for _, kv := range res.KVs {
		kvMap[string(kv.Key)] = kv.Value
	}

	return kvMap
}

// assertModelCorrect asserts that the model data being maintained by this
// testModel is equivalent to the actual time series data stored in the
// engine. If the actual data does not match the model, this method will print
// out detailed information about the differences between the two data sets.
func (tm *testModelRunner) assertModelCorrect() {
	tm.t.Helper()
	actualData := tm.getActualData()
	modelDisk := tm.getModelDiskLayout()
	if a, e := actualData, modelDisk; !reflect.DeepEqual(a, e) {
		for _, diff := range pretty.Diff(a, e) {
			tm.t.Error(diff)
		}
	}
}

func (tm *testModelRunner) getModelDiskLayout() map[string]roachpb.Value {
	result := make(map[string]roachpb.Value)
	tm.model.VisitAllSeries(func(name, source string, data testmodel.DataSeries) (testmodel.DataSeries, bool) {
		// For computing the expected disk layout, only consider resolution-specific
		// series.
		resolution, seriesName, valid := getResolutionFromKey(name)
		if !valid {
			return data, false
		}

		// The on-disk model discards all samples in each sample period except for
		// the last one.
		if !resolution.IsRollup() {
			data = data.GroupByResolution(resolution.SampleDuration(), testmodel.AggregateLast)
		}

		// Depending on when column-based storage was activated, some slabs will
		// be in row format and others in column format. Find the dividing line
		// and generate two sets of slabs.
		var allSlabs []roachpb.InternalTimeSeriesData
		addSlabs := func(datapoints testmodel.DataSeries, columnar bool) {
			tsdata := tspb.TimeSeriesData{
				Name:       seriesName,
				Source:     source,
				Datapoints: datapoints,
			}
			// Convert rollup resolutions before converting to slabs.
			var slabs []roachpb.InternalTimeSeriesData
			var err error
			if resolution.IsRollup() {
				rollup := computeRollupsFromData(tsdata, resolution.SampleDuration())
				slabs, err = rollup.toInternal(resolution.SlabDuration(), resolution.SampleDuration())
			} else {
				slabs, err = tsdata.ToInternal(resolution.SlabDuration(), resolution.SampleDuration(), columnar)
			}
			if err != nil {
				tm.t.Fatalf("error converting testmodel data to internal format: %s", err.Error())
			}
			allSlabs = append(allSlabs, slabs...)
		}

		if resolution.IsRollup() {
			addSlabs(data, true)
		} else {
			firstColumnTime, hasColumns := tm.firstColumnarTimestamp[name]
			if !hasColumns {
				addSlabs(data, false)
			} else {
				firstColumnTime = resolution.normalizeToSlab(firstColumnTime)
				addSlabs(data.TimeSlice(math.MinInt64, firstColumnTime), false)
				addSlabs(data.TimeSlice(firstColumnTime, math.MaxInt64), true)
			}
		}

		for _, slab := range allSlabs {
			key := MakeDataKey(seriesName, source, resolution, slab.StartTimestampNanos)
			keyStr := string(key)
			var val roachpb.Value
			if err := val.SetProto(&slab); err != nil {
				tm.t.Fatal(err)
			}
			result[keyStr] = val
		}

		return data, false
	})

	return result
}

// assertKeyCount asserts that the model contains the expected number of keys.
// This is used to ensure that data is actually being generated in the test
// model.
func (tm *testModelRunner) assertKeyCount(expected int) {
	tm.t.Helper()
	if a, e := len(tm.getModelDiskLayout()), expected; a != e {
		tm.t.Errorf("model data key count did not match expected value: %d != %d", a, e)
	}
}

func (tm *testModelRunner) storeInModel(r Resolution, data tspb.TimeSeriesData) {
	if !TimeseriesStorageEnabled.Get(&tm.Cfg.Settings.SV) {
		return
	}

	key := resolutionModelKey(data.Name, r)
	if tm.DB.WriteColumnar() {
		firstColumar, ok := tm.firstColumnarTimestamp[key]
		if candidate := data.Datapoints[0].TimestampNanos; !ok || candidate < firstColumar {
			tm.firstColumnarTimestamp[key] = candidate
		}
	}
	tm.model.Record(key, data.Source, data.Datapoints)
}

// resolutionModelKey returns a string to store resolution-specific data in
// the test model.
func resolutionModelKey(name string, r Resolution) string {
	return fmt.Sprintf("@%d.%s", r, name)
}

func getResolutionFromKey(key string) (Resolution, string, bool) {
	if len(key) < 3 || !strings.HasPrefix(key, "@") {
		return 0, key, false
	}

	parts := strings.SplitN(key[1:], ".", 2)
	if len(parts) != 2 {
		return 0, key, false
	}

	val, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, key, false
	}

	return Resolution(val), parts[1], true
}

// storeTimeSeriesData instructs the model to store the given time series data
// in both the model and the system under test.
func (tm *testModelRunner) storeTimeSeriesData(r Resolution, data []tspb.TimeSeriesData) {
	// Store data in the system under test.
	if r.IsRollup() {
		// For rollup resolutions, compute the rollupData from the time series
		// data and store the rollup data.
		var rdata []rollupData
		for _, d := range data {
			rdata = append(rdata, computeRollupsFromData(d, r.SampleDuration()))
		}
		if err := tm.DB.storeRollup(context.Background(), r, rdata); err != nil {
			tm.t.Fatalf("error storing time series rollups: %s", err)
		}
	} else {
		if err := tm.DB.StoreData(context.Background(), r, data); err != nil {
			tm.t.Fatalf("error storing time series data: %s", err)
		}
	}

	// store data in the model. Even for rollup resolutoins we store the original
	// data points in the model, with the expectation that queries will be
	// identical to those based on rollups.
	for _, d := range data {
		tm.storeInModel(r, d)
	}
}

// prune time series from the model. "nowNanos" represents the current time,
// and is used to compute threshold ages. Only time series in the provided list
// of time series/resolution pairs will be considered for deletion.
func (tm *testModelRunner) prune(nowNanos int64, timeSeries ...timeSeriesResolutionInfo) {
	// Prune time series from the system under test.
	if err := tm.DB.pruneTimeSeries(
		context.Background(),
		tm.LocalTestCluster.DB,
		timeSeries,
		hlc.Timestamp{
			WallTime: nowNanos,
			Logical:  0,
		},
	); err != nil {
		tm.t.Fatalf("error pruning time series data: %s", err)
	}

	// Prune the appropriate resolution-specific series from the test model using
	// VisitSeries.
	thresholds := tm.DB.computeThresholds(nowNanos)
	for _, ts := range timeSeries {
		tm.model.VisitSeries(
			resolutionModelKey(ts.Name, ts.Resolution),
			func(name, source string, data testmodel.DataSeries) (testmodel.DataSeries, bool) {
				pruned := data.TimeSlice(thresholds[ts.Resolution], math.MaxInt64)
				if len(pruned) != len(data) {
					return pruned, true
				}
				return data, false
			},
		)
	}
}

// rollup time series from the model. "nowNanos" represents the current time,
// and is used to compute threshold ages. Only time series in the provided list
// of time series/resolution pairs will be considered for rollup.
func (tm *testModelRunner) rollup(nowNanos int64, timeSeries ...timeSeriesResolutionInfo) {
	// Rollup time series from the system under test.
	qmc := MakeQueryMemoryContext(tm.workerMemMonitor, tm.resultMemMonitor, QueryMemoryOptions{
		// Large budget, but not maximum to avoid overflows.
		BudgetBytes:             math.MaxInt64,
		EstimatedSources:        1, // Not needed for rollups
		InterpolationLimitNanos: 0,
		Columnar:                tm.DB.WriteColumnar(),
	})
	tm.rollupWithMemoryContext(qmc, nowNanos, timeSeries...)
}

// rollupWithMemoryContext performs the rollup operation using a custom memory
// context).
func (tm *testModelRunner) rollupWithMemoryContext(
	qmc QueryMemoryContext, nowNanos int64, timeSeries ...timeSeriesResolutionInfo,
) {
	if err := tm.DB.rollupTimeSeries(
		context.Background(),
		timeSeries,
		hlc.Timestamp{
			WallTime: nowNanos,
			Logical:  0,
		},
		qmc,
	); err != nil {
		tm.t.Fatalf("error rolling up time series data: %s", err)
	}

	// Prune the appropriate resolution-specific series from the test model using
	// VisitSeries.
	thresholds := tm.DB.computeThresholds(nowNanos)
	for _, ts := range timeSeries {
		// Track any data series which are pruned from the original resolution -
		// they will be recorded into the rollup resolution.
		type sourceDataPair struct {
			source string
			data   testmodel.DataSeries
		}
		var toRecord []sourceDataPair

		// Visit each data series for the given name and resolution (may have multiple
		// sources). Prune the data down to *only* time periods after the pruning
		// thresholds - additionally, record any pruned data into the target rollup
		// resolution for this resolution.
		tm.model.VisitSeries(
			resolutionModelKey(ts.Name, ts.Resolution),
			func(name, source string, data testmodel.DataSeries) (testmodel.DataSeries, bool) {
				if rollupData := data.TimeSlice(0, thresholds[ts.Resolution]); len(rollupData) > 0 {
					toRecord = append(toRecord, sourceDataPair{
						source: source,
						data:   rollupData,
					})
				}
				return data, false
			},
		)
		for _, data := range toRecord {
			targetResolution, _ := ts.Resolution.TargetRollupResolution()
			tm.model.Record(
				resolutionModelKey(ts.Name, targetResolution),
				data.source,
				data.data,
			)
		}
	}
}

// maintain calls the same operation called by the TS maintenance queue,
// simulating the effects in the model at the same time.
func (tm *testModelRunner) maintain(nowNanos int64) {
	snap := tm.Store.Engine().NewSnapshot()
	defer snap.Close()
	if err := tm.DB.MaintainTimeSeries(
		context.Background(),
		snap,
		roachpb.RKey(keys.TimeseriesPrefix),
		roachpb.RKey(keys.TimeseriesKeyMax),
		tm.LocalTestCluster.DB,
		tm.workerMemMonitor,
		math.MaxInt64,
		hlc.Timestamp{
			WallTime: nowNanos,
			Logical:  0,
		},
	); err != nil {
		tm.t.Fatalf("error maintaining time series data: %s", err)
	}

	// Prune the appropriate resolution-specific series from the test model using
	// VisitSeries.
	thresholds := tm.DB.computeThresholds(nowNanos)

	// Track any data series which has been marked for rollup, and record it into
	// the correct target resolution.
	type rollupRecordingData struct {
		name   string
		source string
		res    Resolution
		data   testmodel.DataSeries
	}
	var toRecord []rollupRecordingData

	// Visit each data series in the model, pruning and computing rollups.
	tm.model.VisitAllSeries(
		func(name, source string, data testmodel.DataSeries) (testmodel.DataSeries, bool) {
			res, seriesName, ok := getResolutionFromKey(name)
			if !ok {
				return data, false
			}
			targetResolution, hasRollup := res.TargetRollupResolution()
			if hasRollup && tm.DB.WriteRollups() {
				pruned := data.TimeSlice(thresholds[res], math.MaxInt64)
				if len(pruned) != len(data) {
					toRecord = append(toRecord, rollupRecordingData{
						name:   seriesName,
						source: source,
						res:    targetResolution,
						data:   data.TimeSlice(0, thresholds[res]),
					})
					return pruned, true
				}
			} else if !hasRollup || !tm.DB.WriteRollups() {
				pruned := data.TimeSlice(thresholds[res], math.MaxInt64)
				if len(pruned) != len(data) {
					return pruned, true
				}
			}
			return data, false
		},
	)
	for _, data := range toRecord {
		tm.model.Record(
			resolutionModelKey(data.name, data.res),
			data.source,
			data.data,
		)
	}
}

// modelQuery encapsulates all of the parameters to execute a query along with
// some context for executing that query. This structure is a useful abstraction
// for tests, when tests utilize default values for most query fields but
// *all* fields are modified in at least one test.
type modelQuery struct {
	tspb.Query
	QueryTimespan
	QueryMemoryOptions
	diskResolution   Resolution
	workerMemMonitor *mon.BytesMonitor
	resultMemMonitor *mon.BytesMonitor
	modelRunner      *testModelRunner
}

// makeQuery creates a new modelQuery which executes using this testModelRunner.
// The new query executes against the given named metric and diskResolution,
// querying between the provided start and end bounds. Useful defaults are set
// for all other fields.
func (tm *testModelRunner) makeQuery(
	name string, diskResolution Resolution, startNanos, endNanos int64,
) modelQuery {
	currentEstimatedSources := tm.model.UniqueSourceCount()
	if currentEstimatedSources == 0 {
		currentEstimatedSources = 1
	}

	return modelQuery{
		Query: tspb.Query{
			Name: name,
		},
		QueryTimespan: QueryTimespan{
			StartNanos:          startNanos,
			EndNanos:            endNanos,
			SampleDurationNanos: diskResolution.SampleDuration(),
			NowNanos:            math.MaxInt64,
		},
		QueryMemoryOptions: QueryMemoryOptions{
			// Large budget, but not maximum to avoid overflows.
			BudgetBytes:             math.MaxInt64,
			EstimatedSources:        currentEstimatedSources,
			InterpolationLimitNanos: 0,
			Columnar:                tm.DB.WriteColumnar(),
		},
		diskResolution:   diskResolution,
		workerMemMonitor: tm.workerMemMonitor,
		resultMemMonitor: tm.resultMemMonitor,
		modelRunner:      tm,
	}
}

// setSourceAggregator sets the source aggregator of the query. This is a
// convenience method to avoid having to call Enum().
func (mq *modelQuery) setSourceAggregator(agg tspb.TimeSeriesQueryAggregator) {
	mq.SourceAggregator = agg.Enum()
}

// setDownsampler sets the downsampler of the query. This is a convenience
// method to avoid having to call Enum().
func (mq *modelQuery) setDownsampler(agg tspb.TimeSeriesQueryAggregator) {
	mq.Downsampler = agg.Enum()
}

// setDerivative sets the derivative function of the query. This is a
// convenience method to avoid having to call Enum().
func (mq *modelQuery) setDerivative(deriv tspb.TimeSeriesQueryDerivative) {
	mq.Derivative = deriv.Enum()
}

// queryDB queries the actual database using the configured parameters of the
// model query.
func (mq *modelQuery) queryDB() ([]tspb.TimeSeriesDatapoint, []string, error) {
	// Query the actual server.
	memContext := MakeQueryMemoryContext(
		mq.workerMemMonitor, mq.resultMemMonitor, mq.QueryMemoryOptions,
	)
	defer memContext.Close(context.Background())
	return mq.modelRunner.DB.Query(
		context.Background(), mq.Query, mq.diskResolution, mq.QueryTimespan, memContext,
	)
}

func (mq *modelQuery) queryModel() testmodel.DataSeries {
	var result testmodel.DataSeries
	startTime := mq.StartNanos
	if rollupResolution, ok := mq.diskResolution.TargetRollupResolution(); ok &&
		mq.verifyDiskResolution(rollupResolution) == nil {
		result = mq.modelRunner.model.Query(
			resolutionModelKey(mq.Name, rollupResolution),
			mq.Sources,
			mq.GetDownsampler(),
			mq.GetSourceAggregator(),
			mq.GetDerivative(),
			rollupResolution.SlabDuration(),
			mq.SampleDurationNanos,
			mq.StartNanos,
			mq.EndNanos,
			mq.InterpolationLimitNanos,
			mq.NowNanos,
		)
		if len(result) > 0 {
			startTime = result[len(result)-1].TimestampNanos
		}
	}
	result = append(result, mq.modelRunner.model.Query(
		resolutionModelKey(mq.Name, mq.diskResolution),
		mq.Sources,
		mq.GetDownsampler(),
		mq.GetSourceAggregator(),
		mq.GetDerivative(),
		mq.diskResolution.SlabDuration(),
		mq.SampleDurationNanos,
		startTime,
		mq.EndNanos,
		mq.InterpolationLimitNanos,
		mq.NowNanos,
	)...)
	return result
}

// assertSuccess runs the query against both the real database and the model
// database, ensuring that the query succeeds and that the real result matches
// the model result. The two supplied parameters are a form of sanity check,
// ensuring that the query actually performed the expected work (to avoid a
// situation where both the model and the real database return the same
// unexpected result because the query was incorrectly constructed).
func (mq *modelQuery) assertSuccess(expectedDatapointCount, expectedSourceCount int) {
	mq.modelRunner.t.Helper()

	// Query the real DB.
	actualDatapoints, actualSources, err := mq.queryDB()
	if err != nil {
		mq.modelRunner.t.Fatal(err)
	}

	// Query the model.
	modelDatapoints := mq.queryModel()
	if a, e := testmodel.DataSeries(actualDatapoints), modelDatapoints; !testmodel.DataSeriesEquivalent(a, e) {
		for _, diff := range pretty.Diff(a, e) {
			mq.modelRunner.t.Error(diff)
		}
	}
	if a, e := len(actualDatapoints), expectedDatapointCount; a != e {
		mq.modelRunner.t.Logf("actual datapoints: %v", actualDatapoints)
		mq.modelRunner.t.Logf("model datapoints: %v", modelDatapoints)
		mq.modelRunner.t.Fatal(errors.Errorf("query got %d datapoints, wanted %d", a, e))
	}
	if a, e := len(actualSources), expectedSourceCount; a != e {
		mq.modelRunner.t.Logf("actual sources: %v", actualSources)
		mq.modelRunner.t.Fatal(errors.Errorf("query got %d sources, wanted %d", a, e))
	}
}

// assertMatchesModel asserts that the results of the query are identical when
// executed against the real database and the model. This is the same as
// assertSuccess, but does not include the sanity checks for datapoint count and
// source count. This method is intended for use in tests which are generated
// procedurally.
func (mq *modelQuery) assertMatchesModel() {
	mq.modelRunner.t.Helper()
	// Query the real DB.
	actualDatapoints, _, err := mq.queryDB()
	if err != nil {
		mq.modelRunner.t.Fatal(err)
	}

	// Query the model.
	modelDatapoints := mq.queryModel()
	if a, e := testmodel.DataSeries(actualDatapoints), modelDatapoints; !testmodel.DataSeriesEquivalent(a, e) {
		mq.modelRunner.t.Errorf("actual %v expected %v", a, e)
		for _, diff := range pretty.Diff(a, e) {
			mq.modelRunner.t.Error(diff)
		}
	}
}

// assertError runs the query against the real database and asserts that the
// database returns an error. The error's message must match the supplied
// string.
func (mq *modelQuery) assertError(errString string) {
	mq.modelRunner.t.Helper()
	_, _, err := mq.queryDB()
	if err == nil {
		mq.modelRunner.t.Fatalf(
			"query got no error, wanted error with message matching  \"%s\"", errString,
		)
	}
	if !testutils.IsError(err, errString) {
		mq.modelRunner.t.Fatalf(
			"query got error \"%s\", wanted error with message matching \"%s\"", err.Error(), errString,
		)
	}
}

// modelDataSource is used to create a mock DataSource. It returns a
// deterministic set of data to GetTimeSeriesData, storing the returned data in
// the model whenever GetTimeSeriesData is called. Data is returned until all
// sets are exhausted, at which point the supplied stop.Stopper is stopped.
type modelDataSource struct {
	model       testModelRunner
	datasets    [][]tspb.TimeSeriesData
	r           Resolution
	stopper     *stop.Stopper
	calledCount int
	once        sync.Once
}

// GetTimeSeriesData implements the DataSource interface, returning a predefined
// set of TimeSeriesData to subsequent calls. It stores each TimeSeriesData
// object in the test model before returning it. If all TimeSeriesData objects
// have been returned, this method will stop the provided Stopper.
func (mds *modelDataSource) GetTimeSeriesData() []tspb.TimeSeriesData {
	if len(mds.datasets) == 0 {
		// Stop on goroutine to prevent deadlock.
		go mds.once.Do(func() { mds.stopper.Stop(context.Background()) })
		return nil
	}
	mds.calledCount++
	data := mds.datasets[0]
	mds.datasets = mds.datasets[1:]

	for _, d := range data {
		mds.model.storeInModel(mds.r, d)
	}
	return data
}

// TestStoreTimeSeries is a simple test of the Time Series module, ensuring that
// it is storing time series correctly.
func TestStoreTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {

		// Basic storage operation: one data point.
		tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
			tsd("test.metric", "",
				tsdp(440000000000000000, 100),
			),
		})
		tm.assertKeyCount(1)
		tm.assertModelCorrect()

		// Store data with different sources, and with multiple data points that
		// aggregate into the same key.
		tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
			tsd("test.metric.float", "cpu01",
				tsdp(1428713843000000000, 100.0),
				tsdp(1428713843000000001, 50.2),
				tsdp(1428713843000000002, 90.9),
			),
		})
		tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
			tsd("test.metric.float", "cpu02",
				tsdp(1428713843000000000, 900.8),
				tsdp(1428713843000000001, 30.12),
				tsdp(1428713843000000002, 72.324),
			),
		})
		tm.assertKeyCount(3)
		tm.assertModelCorrect()

		// A single storage operation that stores to multiple keys, including an
		// existing key.
		tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
			tsd("test.metric", "",
				tsdp(440000000000000000, 200),
				tsdp(450000000000000001, 1),
				tsdp(460000000000000000, 777),
			),
		})
		tm.assertKeyCount(5)
		tm.assertModelCorrect()
	})
}

// TestPollSource verifies that polled data sources are called as expected.
func TestPollSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		testSource := modelDataSource{
			model:   tm,
			r:       Resolution10s,
			stopper: stop.NewStopper(),
			datasets: [][]tspb.TimeSeriesData{
				{
					tsd("test.metric.float", "cpu01",
						tsdp(1428713843000000000, 100.0),
						tsdp(1428713843000000001, 50.2),
						tsdp(1428713843000000002, 90.9),
					),
					tsd("test.metric.float", "cpu02",
						tsdp(1428713843000000000, 900.8),
						tsdp(1428713843000000001, 30.12),
						tsdp(1428713843000000002, 72.324),
					),
				},
				{
					tsd("test.metric", "",
						tsdp(1428713843000000000, 100),
					),
				},
			},
		}

		ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
		tm.DB.PollSource(ambient, &testSource, time.Millisecond, Resolution10s, testSource.stopper)
		<-testSource.stopper.IsStopped()
		if a, e := testSource.calledCount, 2; a != e {
			t.Errorf("testSource was called %d times, expected %d", a, e)
		}
		tm.assertKeyCount(3)
		tm.assertModelCorrect()
	})
}

// TestDisableStorage verifies that disabling timeseries storage via the cluster
// setting works properly.
func TestDisableStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		TimeseriesStorageEnabled.Override(ctx, &tm.Cfg.Settings.SV, false)

		// Basic storage operation: one data point.
		tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
			tsd("test.metric", "",
				tsdp(440000000000000000, 100),
			),
		})
		tm.assertKeyCount(0)
		tm.assertModelCorrect()

		testSource := modelDataSource{
			model:   tm,
			r:       Resolution10s,
			stopper: stop.NewStopper(),
			datasets: [][]tspb.TimeSeriesData{
				{
					tsd("test.metric.float", "cpu01",
						tsdp(1428713843000000000, 100.0),
						tsdp(1428713843000000001, 50.2),
						tsdp(1428713843000000002, 90.9),
					),
					tsd("test.metric.float", "cpu02",
						tsdp(1428713843000000000, 900.8),
						tsdp(1428713843000000001, 30.12),
						tsdp(1428713843000000002, 72.324),
					),
				},
				{
					tsd("test.metric", "",
						tsdp(1428713843000000000, 100),
					),
				},
			},
		}

		ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
		tm.DB.PollSource(ambient, &testSource, time.Millisecond, Resolution10s, testSource.stopper)
		select {
		case <-testSource.stopper.IsStopped():
			t.Error("testSource data exhausted when polling should have been enabled")
		case <-time.After(50 * time.Millisecond):
			testSource.stopper.Stop(context.Background())
		}
		if a, e := testSource.calledCount, 0; a != e {
			t.Errorf("testSource was called %d times, expected %d", a, e)
		}
		tm.assertKeyCount(0)
		tm.assertModelCorrect()
	})
}

// TestPruneThreshold verifies that `PruneThreshold` returns correct result in nanoseconds
func TestPruneThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTestCaseMultipleFormats(t, func(t *testing.T, tm testModelRunner) {
		db := NewDB(nil, tm.Cfg.Settings)
		var expected int64
		if db.WriteRollups() {
			expected = resolution10sDefaultRollupThreshold.Nanoseconds()
		} else {
			expected = deprecatedResolution10sDefaultPruneThreshold.Nanoseconds()
		}
		result := db.PruneThreshold(Resolution10s)
		if expected != result {
			t.Errorf("prune threshold did not match expected value: %d != %d", expected, result)
		}
	})
}
