// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/vecann"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

const defaultDataset = "unsplash-512-euclidean"
const tempDir = "tmp"
const minPartitionSize = 16
const maxPartitionSize = 128

const (
	Reset   = "\033[0m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
	Grey    = "\033[37m"
	White   = "\033[97m"
)

const (
	HideCursor = "\033[?25l"
	ShowCursor = "\033[?25h"
)

var flagHideProgress = flag.Bool("hide-progress", false, "Hide progress during index build.")

// Insert command options.
var flagBatchSize = flag.Int("batch-size", 3, "Number of vectors to insert in a single batch.")

// Search command options.
var flagMaxResults = flag.Int("k", 10, "Number of search results, used in recall calculation.")
var flagBeamSize = flag.Int(
	"build-beam-size",
	8,
	"Default beam size used for building the index.")
var flagSearchBeamSizes = flag.String(
	"search-beam-sizes",
	"1,2,4,8,16,32,64,128,256,512",
	"List of beam sizes used for search.")

// Store options.
var flagMemStore = flag.Bool("memstore", false, "Use in-memory store instead of CockroachDB")
var flagDBConnStr = flag.String("db", "postgresql://root@localhost:26257",
	"Database connection string (when not using --memstore)")

// vecbench benchmarks vector index in-memory build and search performance on a
// variety of datasets. Datasets are downloaded from the
// cockroach-fixtures-us-east1 GCP bucket (vecindex directory). Here is a list
// of available datasets, most of which are derived from datasets on
// ann-benchmarks.com:
//
//	fashion-mnist-784-euclidean (60K vectors, 784 dims)
//	gist-960-euclidean (1M vectors, 960 dims)
//	random-s-100-euclidean (90K vectors, 100 dims)
//	random-xs-20-euclidean (9K vectors, 20 dims)
//	sift-128-euclidean (1M vectors, 128 dims)
//	dbpedia-openai-100k-angular (100K vectors, 1536 dims)
//
// After download, the datasets are cached in a local tmp directory and a vector
// index is created. The built vector index is also cached in the tmp directory
// so that it can be rapidly reconstituted across benchmark runs.
//
// The search benchmark runs over a set of test vectors that are not part of the
// indexed vectors. It outputs average recall rates across the test vectors for
// different beam sizes, as well as statistics for each run.
//
// The build benchmark can operate over a subset of available vectors for
// shorter build times.

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Printf(Yellow + "usage: vecbench [search | build] [<dataset-name>]\n" + Reset)
		os.Exit(1)
	}

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Hide the cursor, but ensure it's restored on exit, including Ctrl+C.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	stopper.RunAsyncTask(ctx, "Ctrl+C", func(context.Context) {
		select {
		case <-c:
			fmt.Print(ShowCursor)
			fmt.Println()
			os.Exit(0)

		case <-stopper.ShouldQuiesce():
			break
		}
	})
	fmt.Print(HideCursor)
	defer fmt.Print(ShowCursor)

	// Start pprof server at http://localhost:8080/debug/pprof/
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	switch flag.Arg(0) {
	// Search an index that has been built and cached on disk.
	case "search":
		var datasetNames []string
		if flag.NArg() == 1 {
			// Use default dataset.
			datasetNames = []string{defaultDataset}
		} else {
			datasetNames = flag.Args()[1:]
		}

		for i, datasetName := range datasetNames {
			if i != 0 {
				fmt.Println("----------")
			}

			vb := newVectorBench(ctx, stopper, datasetName)
			vb.SearchIndex()
		}

	// Build the vector index from scratch, possibly on a subset of available
	// vectors.
	case "build":
		var datasetName string
		if flag.NArg() == 1 {
			// Use default dataset.
			datasetName = defaultDataset
		} else {
			datasetName = flag.Args()[1]
		}

		vb := newVectorBench(ctx, stopper, datasetName)
		vb.BuildIndex()

	default:
		fmt.Printf(Red+"unknown command '%s'\n"+Reset, flag.Arg(0))
		return
	}
}

// vectorBench encapsulates the state and operations for vector benchmarking.
type vectorBench struct {
	ctx         context.Context
	stopper     *stop.Stopper
	datasetName string
	provider    VectorProvider
	buildData   vecann.Dataset
	searchData  vecann.SearchDataset
}

// newVectorBench creates a new VecBench instance for the given dataset.
func newVectorBench(ctx context.Context, stopper *stop.Stopper, datasetName string) *vectorBench {
	return &vectorBench{
		ctx:         ctx,
		stopper:     stopper,
		datasetName: datasetName,
	}
}

// SearchIndex downloads, builds, and searches an index for the given dataset.
func (vb *vectorBench) SearchIndex() {
	// Ensure the data needed for the search is available.
	vb.ensureDataset(vb.ctx, true /* forSearch */)

	var err error
	vb.provider, err = newVectorProvider(vb.stopper, vb.datasetName, vb.searchData.Test.Dims)
	if err != nil {
		panic(err)
	}

	// Try to reuse an index that was previously saved.
	loaded, err := vb.provider.Load(vb.ctx)
	if err != nil {
		panic(err)
	}
	if !loaded {
		// Index is not yet built, do so now.
		vb.BuildIndex()
	}

	doSearch := func(beamSize int) {
		// Prepare to search.
		maxResults := *flagMaxResults
		state, err := vb.provider.SetupSearch(vb.ctx, maxResults, beamSize)
		if err != nil {
			panic(err)
		}

		start := timeutil.Now()

		// Search for test vectors.
		var sumRecall, sumVectors, sumLeafVectors, sumFullVectors, sumPartitions float64
		count := vb.searchData.Test.Count
		for i := range count {
			// Calculate truth set for the vector.
			queryVector := vb.searchData.Test.At(i)

			var stats cspann.SearchStats
			prediction, err := vb.provider.Search(vb.ctx, state, queryVector, &stats)
			if err != nil {
				panic(err)
			}

			primaryKeys := make([]byte, maxResults*4)
			truth := make([]cspann.KeyBytes, maxResults)
			for neighbor := range maxResults {
				primaryKey := primaryKeys[neighbor*4 : neighbor*4+4]
				binary.BigEndian.PutUint32(primaryKey, uint32(vb.searchData.Neighbors[i][neighbor]))
				truth[neighbor] = primaryKey
			}

			sumRecall += calculateRecall(prediction, truth)
			sumVectors += float64(stats.QuantizedVectorCount)
			sumLeafVectors += float64(stats.QuantizedLeafVectorCount)
			sumFullVectors += float64(stats.FullVectorCount)
			sumPartitions += float64(stats.PartitionCount)
		}

		elapsed := timeutil.Since(start)
		fmt.Printf("%d\t%0.2f%%\t%0.0f\t%0.0f\t%0.2f\t%0.2f\t%0.2f\n",
			beamSize, sumRecall/float64(count)*100,
			sumLeafVectors/float64(count), sumVectors/float64(count),
			sumFullVectors/float64(count), sumPartitions/float64(count),
			float64(count)/elapsed.Seconds())
	}

	fmt.Println()
	fmt.Printf(White+"%s\n"+Reset, vb.datasetName)
	fmt.Printf(
		White+"%d train vectors, %d test vectors, %d dimensions, %d/%d min/max partitions, build beam size %d\n"+Reset,
		vb.searchData.Count, vb.searchData.Test.Count, vb.searchData.Test.Dims,
		minPartitionSize, maxPartitionSize, *flagBeamSize)
	fmt.Println(vb.provider.FormatStats())

	fmt.Printf("beam\trecall\tleaf\tall\tfull\tpartns\tqps\n")

	// Search multiple times with different search beam sizes.
	beamSizeStrs := strings.Split(*flagSearchBeamSizes, ",")
	for i := range beamSizeStrs {
		beamSize, err := strconv.Atoi(beamSizeStrs[i])
		if err != nil {
			panic(err)
		}
		doSearch(beamSize)
	}
}

// BuildIndex builds a vector index for the given dataset. If it already exists,
// it is rebuilt from scratch.
func (vb *vectorBench) BuildIndex() {
	// Ensure dataset is downloaded and cached.
	vb.ensureDataset(vb.ctx, false /* forSearch */)

	// Construct the vector provider.
	var err error
	vb.provider, err = newVectorProvider(vb.stopper, vb.datasetName, vb.buildData.Train.Dims)
	if err != nil {
		panic(err)
	}

	// Start fresh.
	err = vb.provider.New(vb.ctx)
	if err != nil {
		panic(err)
	}

	// Create unique primary key for each vector.
	primaryKeys := make([]cspann.KeyBytes, vb.buildData.Train.Count)
	keyBuf := make(cspann.KeyBytes, vb.buildData.Train.Count*4)
	for i := range vb.buildData.Train.Count {
		primaryKeys[i] = keyBuf[i*4 : i*4+4]
		binary.BigEndian.PutUint32(primaryKeys[i], uint32(i))
	}

	// Compute percentile latencies.
	estimator := NewPercentileEstimator(1000)

	// Set up time series charts.
	cp := chartPrinter{Footer: 2, Hide: *flagHideProgress}
	throughput := cp.AddChart("ops/sec")
	p50 := cp.AddChart("p50 ms latency")
	p90 := cp.AddChart("p90 ms latency")
	p99 := cp.AddChart("p99 ms latency")

	// Add additional provider-specific metrics.
	metrics, err := vb.provider.GetMetrics()
	if err != nil {
		panic(err)
	}
	metricIds := make([]int, len(metrics))
	for i, metric := range metrics {
		metricIds[i] = cp.AddChart(metric.Name)
	}

	fmt.Printf(White+"Building index for dataset: %s\n"+Reset, vb.datasetName)
	startAt := crtime.NowMono()

	// Insert vectors into the provider on multiple goroutines.
	var insertCount atomic.Uint64
	procs := runtime.GOMAXPROCS(-1)
	countPerProc := (vb.buildData.Train.Count + procs) / procs
	batchSize := *flagBatchSize
	for i := 0; i < vb.buildData.Train.Count; i += countPerProc {
		end := min(i+countPerProc, vb.buildData.Train.Count)
		go func(start, end int) {
			// Break vector group into batches that each insert a batch of vectors.
			for j := start; j < end; j += batchSize {
				startMono := crtime.NowMono()
				vectors := vb.buildData.Train.Slice(j, min(j+batchSize, end)-j)
				err := vb.provider.InsertVectors(vb.ctx, primaryKeys[j:j+vectors.Count], vectors)
				if err != nil {
					panic(err)
				}
				estimator.Add(startMono.Elapsed().Seconds() / float64(vectors.Count))
				insertCount.Add(uint64(vectors.Count))
			}
		}(i, end)
	}

	// Compute ops per second.
	var lastInserted int

	// Update progress every second.
	lastProgressAt := startAt
	for {
		time.Sleep(time.Second)

		// Calculate exactly how long it's been since last progress update.
		now := crtime.NowMono()
		sinceProgress := now.Sub(lastProgressAt)
		lastProgressAt = now

		// Calculate ops per second over the last second.
		totalInserted := int(insertCount.Load())
		opsPerSec := float64(totalInserted-lastInserted) / sinceProgress.Seconds()
		lastInserted = totalInserted

		cp.AddSample(throughput, opsPerSec)
		cp.AddSample(p50, estimator.Estimate(0.50)*1000)
		cp.AddSample(p90, estimator.Estimate(0.90)*1000)
		cp.AddSample(p99, estimator.Estimate(0.99)*1000)

		// Add provider-specific metric samples.
		metrics, err := vb.provider.GetMetrics()
		if err != nil {
			panic(err)
		}
		for i, metric := range metrics {
			cp.AddSample(metricIds[i], metric.Value)
		}
		cp.Plot()

		if !*flagHideProgress {
			sinceStart := now.Sub(startAt)
			fmt.Printf(White+"\rInserted %d / %d vectors (%.2f%%) in %v"+Reset,
				totalInserted, vb.buildData.Train.Count,
				(float64(totalInserted)/float64(vb.buildData.Train.Count))*100,
				sinceStart.Truncate(time.Second))
		}

		if totalInserted >= vb.buildData.Train.Count {
			break
		}
	}

	fmt.Printf(White+"\nBuilt index in %v\n"+Reset, roundDuration(startAt.Elapsed()))

	// Ensure that index is persisted so it can be reused.
	if err = vb.provider.Save(vb.ctx); err != nil {
		panic(err)
	}
}

// ensureDataset ensures that the dataset has been downloaded and cached to
// disk. It also loads the data into memory. If "forSearch" is true, then only
// test vectors are loaded, not train vectors.
func (vb *vectorBench) ensureDataset(ctx context.Context, forSearch bool) {
	loader := vecann.DatasetLoader{
		DatasetName: vb.datasetName,
		OnProgress: func(ctx context.Context, format string, args ...any) {
			fmt.Printf(Cyan+"%s\n"+Reset, fmt.Sprintf(format, args...))
		},
		OnDownloadProgress: func(downloaded, total int64, elapsed time.Duration) {
			fmt.Printf(Cyan+"\rDownloaded %s / %s (%.2f%%) in %v          "+Reset,
				humanizeutil.IBytes(downloaded), humanizeutil.IBytes(total),
				(float64(downloaded)/float64(total))*100, elapsed.Truncate(time.Second))
		},
	}

	if forSearch {
		// Only need to load the search data, not the insert data. Loading
		// the insert data can take 10+ seconds for large datasets.
		if err := loader.LoadForSearch(ctx); err != nil {
			panic(err)
		}
	} else {
		if err := loader.Load(ctx); err != nil {
			panic(err)
		}
	}

	vb.buildData = loader.Data
	vb.searchData = loader.SearchData
}

// newVectorProvider creates a new in-memory or SQL based vector provider that
// indexes the given dataset.
func newVectorProvider(
	stopper *stop.Stopper, datasetName string, dims int,
) (VectorProvider, error) {
	options := cspann.IndexOptions{
		MinPartitionSize: minPartitionSize,
		MaxPartitionSize: maxPartitionSize,
		BaseBeamSize:     *flagBeamSize,
	}

	if *flagMemStore {
		return NewMemProvider(stopper, datasetName, dims, options), nil
	}

	// Use SQL-based provider with connection string from flags.
	provider, err := NewSQLProvider(context.Background(), datasetName, dims, options)
	if err != nil {
		return nil, errors.Wrap(err, "creating SQL provider")
	}

	return provider, nil
}

// calculateRecall returns the percentage overlap of the predicted set with the
// truth set. If the predicted set has fewer items than the truth set, it is
// treated as if the predicted set has missing/incorrect items that reduce the
// recall rate.
func calculateRecall(prediction, truth []cspann.KeyBytes) float64 {
	predictionMap := make(map[string]bool, len(prediction))
	for _, p := range prediction {
		predictionMap[string(p)] = true
	}

	var intersect float64
	for _, t := range truth {
		_, ok := predictionMap[string(t)]
		if ok {
			intersect++
		}
	}
	return intersect / float64(len(truth))
}

func roundDuration(duration time.Duration) time.Duration {
	return duration.Truncate(time.Millisecond)
}
