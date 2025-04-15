// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
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

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const bucketName = "cockroach-fixtures-us-east1"
const bucketDirName = "vecindex"
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

// Search command options.
var flagMaxResults = flag.Int("k", 10, "Number of search results, used in recall calculation.")
var flagBeamSize = flag.Int(
	"default-beam-size",
	8,
	"Default beam size used for building and searching the index.")
var flagSearchBeamSizes = flag.String(
	"search-beam-sizes",
	"1,2,4,8,16,32,64,128,256,512",
	"List of beam sizes used for search.")

// Build command options.
var flagBuildCount = flag.Int(
	"build-count",
	0,
	"Subset of vectors to use for building the index.")

// Store options.
var flagMemStore = flag.Bool("memstore", false, "Use in-memory store instead of CockroachDB")
var flagDBConnStr = flag.String("db", "postgresql://root@localhost:26257",
	"Database connection string (when not using --memstore)")

// dataset describes a set of vectors to be benchmarked.
type dataset struct {
	// Train is the set of vectors to insert into the index.
	Train vector.Set
	// Test is the set of vectors to search for in the index. This is typically
	// a disjoint set from Train, in order to ensure that the index works well
	// on unseen data.
	Test vector.Set
	// Neighbors is the set of N nearest neighbors for each vector in the Test
	// set. Its length is equal to Test.Count, and each entry contains a slice of
	// N int64 values. These int64 values are offsets into the Train set that
	// point to the nearest neighbors of that test vector.
	Neighbors [][]int64
}

// searchData stores the subset of information from "dataset" that's needed to
// benchmark the performance of searching the index. In particular, it assumes
// that the index has already been built, and so does not contain the Train
// vectors. This allows "searchData" to be loaded from disk much faster.
type searchData struct {
	// Count is the number of Train vectors in the original dataset.
	Count int
	// Test is the the same as dataset.Test.
	Test vector.Set
	// Neighbors is the same as dataset.Neighbors.
	Neighbors [][]int64
}

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
//	unsplash-512-euclidean (1M vectors, 512 dims)
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
	data := ensureSearchData(vb.ctx, vb.datasetName)

	var err error
	vb.provider, err = newVectorProvider(vb.stopper, vb.datasetName, data.Test.Dims)
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
		start := timeutil.Now()

		// Search for test vectors.
		var sumMAP, sumVectors, sumLeafVectors, sumFullVectors, sumPartitions float64
		count := data.Test.Count
		for i := 0; i < count; i++ {
			// Calculate truth set for the vector.
			queryVector := data.Test.At(i)

			var stats cspann.SearchStats
			prediction, err := vb.provider.Search(vb.ctx, queryVector, *flagMaxResults, beamSize, &stats)
			if err != nil {
				panic(err)
			}

			primaryKeys := make([]byte, len(prediction)*4)
			truth := make([]cspann.KeyBytes, len(prediction))
			for neighbor := 0; neighbor < len(prediction); neighbor++ {
				primaryKey := primaryKeys[neighbor*4 : neighbor*4+4]
				binary.BigEndian.PutUint32(primaryKey, uint32(data.Neighbors[i][neighbor]))
				truth[neighbor] = primaryKey
			}

			sumMAP += findMAP(prediction, truth)
			sumVectors += float64(stats.QuantizedVectorCount)
			sumLeafVectors += float64(stats.QuantizedLeafVectorCount)
			sumFullVectors += float64(stats.FullVectorCount)
			sumPartitions += float64(stats.PartitionCount)
		}

		elapsed := timeutil.Since(start)
		fmt.Printf("%d\t%0.2f%%\t%0.0f\t%0.0f\t%0.2f\t%0.2f\t%0.2f\n",
			beamSize, sumMAP/float64(count)*100,
			sumLeafVectors/float64(count), sumVectors/float64(count),
			sumFullVectors/float64(count), sumPartitions/float64(count),
			float64(count)/elapsed.Seconds())
	}

	fmt.Println()
	fmt.Printf(White+"%s\n"+Reset, vb.datasetName)
	fmt.Printf(
		White+"%d train vectors, %d test vectors, %d dimensions, %d/%d min/max partitions, base beam size %d\n"+Reset,
		data.Count, data.Test.Count, data.Test.Dims,
		minPartitionSize, maxPartitionSize, *flagBeamSize)
	fmt.Println(vb.provider.FormatStats())

	fmt.Printf("beam\trecall\tleaf\tall\tfull\tpartns\tqps\n")

	// Search multiple times with different beam sizes.
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
	data := ensureDataset(vb.ctx, vb.datasetName)

	// Construct the vector provider.
	var err error
	vb.provider, err = newVectorProvider(vb.stopper, vb.datasetName, data.Train.Dims)
	if err != nil {
		panic(err)
	}

	// Start fresh.
	vb.provider.New(vb.ctx)

	// Create unique primary key for each vector.
	primaryKeys := make([]cspann.KeyBytes, data.Train.Count)
	keyBuf := make(cspann.KeyBytes, data.Train.Count*4)
	for i := range data.Train.Count {
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
	countPerProc := (data.Train.Count + procs) / procs
	blockSize := minPartitionSize
	for i := 0; i < data.Train.Count; i += countPerProc {
		end := min(i+countPerProc, data.Train.Count)
		go func(start, end int) {
			// Break vector group into batches that each insert a block of vectors.
			for j := start; j < end; j += blockSize {
				startMono := crtime.NowMono()
				vectors := data.Train.Slice(j, min(j+blockSize, end)-j)
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
				totalInserted, data.Train.Count,
				(float64(totalInserted)/float64(data.Train.Count))*100,
				sinceStart.Truncate(time.Second))
		}

		if totalInserted >= data.Train.Count {
			break
		}
	}

	fmt.Printf(White+"\nBuilt index in %v\n"+Reset, roundDuration(startAt.Elapsed()))

	// Ensure that index is persisted so it can be reused.
	if err = vb.provider.Save(vb.ctx); err != nil {
		panic(err)
	}
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

// ensureDataset checks whether the given dataset has been downloaded. If not,
// it downloads it from the GCP bucket and unzips it into the tmp directory. It
// returns the dataset once it's cached locally.
func ensureDataset(ctx context.Context, datasetName string) dataset {
	objectName := fmt.Sprintf("%s/%s.zip", bucketDirName, datasetName)
	datasetFileName := fmt.Sprintf("%s/%s.gob", tempDir, datasetName)
	searchFileName := fmt.Sprintf("%s/%s.search.gob", tempDir, datasetName)

	// If dataset file has already been downloaded, then just return it.
	_, err := os.Stat(datasetFileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			panic(err)
		}
	} else {
		return loadDataset(datasetFileName)
	}

	fmt.Printf(Cyan+"Downloading %s/%s\n"+Reset, bucketName, objectName)

	// Create a GCS client using Application Default Credentials.
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Get a handle to the object.
	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)

	// Get object attributes to determine its size
	attrs, err := object.Attrs(ctx)
	if err != nil {
		panic(err)
	}

	// Read the object with progress tracking.
	reader, err := object.NewReader(ctx)
	if err != nil {
		log.Fatalf("Failed to create object reader: %v", err)
	}
	defer reader.Close()

	// Use progressWriter to track download progress
	var buf bytes.Buffer
	writer := makeProgressWriter(&buf, attrs.Size)

	if _, err = io.Copy(&writer, reader); err != nil {
		log.Fatalf("Failed to copy object data: %v", err)
	}

	// Open the zip archive.
	fmt.Printf(Cyan+"\nUnzipping to %s\n"+Reset, datasetFileName)
	zipReader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		panic(err)
	}

	// Create temp directory.
	if err = os.MkdirAll(tempDir, os.ModePerm); err != nil {
		panic(err)
	}

	// Extract files to the temp directory.
	for _, file := range zipReader.File {
		// Open the file inside the zip archive
		zippedFile, err := file.Open()
		if err != nil {
			panic(err)
		}

		// Create the output file
		path := fmt.Sprintf("%s/%s", tempDir, file.Name)
		outputFile, err := os.Create(path)
		if err != nil {
			panic(err)
		}

		// Copy the contents of the zipped file to the output file
		if _, err := io.Copy(outputFile, zippedFile); err != nil {
			panic(err)
		}
		if err := outputFile.Close(); err != nil {
			panic(err)
		}
		if err := zippedFile.Close(); err != nil {
			panic(err)
		}
	}

	// Separately store test vectors for searching, so the training data does not
	// need to be loaded every time.
	data := loadDataset(datasetFileName)
	searchData := searchData{Count: data.Train.Count, Test: data.Test, Neighbors: data.Neighbors}

	writeFile, err := os.Create(searchFileName)
	if err != nil {
		panic(err)
	}
	defer writeFile.Close()

	encoder := gob.NewEncoder(writeFile)
	if err = encoder.Encode(&searchData); err != nil {
		panic(err)
	}

	return data
}

// ensureSearchData ensures that the dataset has been downloaded and cached
// locally. However, rather than loading the full training data, which can take
// time, it only loads the subset of data needed for search (e.g. the test
// vectors and nearest neighbors).
func ensureSearchData(ctx context.Context, datasetName string) searchData {
	fileName := fmt.Sprintf("%s/%s.search.gob", tempDir, datasetName)

	// If search data is not cached, download it now.
	_, err := os.Stat(fileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			panic(err)
		}
		ensureDataset(ctx, datasetName)
	}

	readFile, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer readFile.Close()

	decoder := gob.NewDecoder(readFile)
	var data searchData
	if err = decoder.Decode(&data); err != nil {
		panic(err)
	}

	return data
}

// loadDataset deserializes a dataset saved as a gob file.
func loadDataset(fileName string) dataset {
	startTime := timeutil.Now()
	fmt.Printf(Cyan+"Loading train and test data from %s\n"+Reset, fileName)

	readFile, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer readFile.Close()

	decoder := gob.NewDecoder(readFile)
	var data dataset
	if err = decoder.Decode(&data); err != nil {
		panic(err)
	}

	if *flagBuildCount != 0 {
		// Load subset of data - first N vectors.
		n := *flagBuildCount
		if n > data.Train.Count {
			n = data.Train.Count
		}
		data.Train.SplitAt(n)
	}

	elapsed := timeutil.Since(startTime)
	fmt.Printf(Cyan+"Loaded %s in %v          \n"+Reset, fileName, roundDuration(elapsed))
	return data
}

// findMAP returns mean average precision, which compares a set of predicted
// results with the true set of results. Both sets are expected to be of equal
// length. It returns the percentage overlap of the predicted set with the truth
// set.
func findMAP(prediction, truth []cspann.KeyBytes) float64 {
	if len(prediction) != len(truth) {
		panic(errors.AssertionFailedf("prediction and truth sets are not same length"))
	}

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
