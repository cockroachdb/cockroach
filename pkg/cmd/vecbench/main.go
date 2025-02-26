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
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/memstore"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
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
const seed = 42

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

var flagHideCharts = flag.Bool("hide-charts", false, "Hide time series charts during index build.")

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

type dataset struct {
	Train     vector.Set
	Test      vector.Set
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

	switch flag.Arg(0) {
	// Search an index that has been built and cached on disk.
	case "search":
		if flag.NArg() == 1 {
			// Default to default dataset.
			searchIndex(ctx, stopper, defaultDataset)
		} else {
			for i, datasetName := range flag.Args()[1:] {
				if i != 0 {
					fmt.Println("----------")
				}
				searchIndex(ctx, stopper, datasetName)
			}
		}

	// Build the index from scratch, possibly on a subset of available vectors.
	case "build":
		if flag.NArg() == 1 {
			// Default to default dataset.
			_, _ = buildIndex(ctx, stopper, defaultDataset)
		} else {
			_, _ = buildIndex(ctx, stopper, flag.Args()[1])
		}

	default:
		fmt.Printf(Red+"unknown command '%s'\n"+Reset, flag.Arg(0))
		return
	}
}

// searchIndex downloads, builds, and searches an index for the given dataset.
func searchIndex(ctx context.Context, stopper *stop.Stopper, datasetName string) {
	indexFileName := fmt.Sprintf("%s/%s.idx", tempDir, datasetName)
	searchFileName := fmt.Sprintf("%s/%s.search.gob", tempDir, datasetName)

	// If index file has not been built, then do so now. Otherwise, load it from
	// disk.
	var memStore *memstore.Store
	var index *cspann.Index
	_, err := os.Stat(indexFileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			panic(err)
		}

		memStore, index = buildIndex(ctx, stopper, datasetName)
		saveStore(memStore, indexFileName)
	} else {
		memStore = loadStore(indexFileName)
		index = createIndex(ctx, stopper, memStore)
	}

	// Load test data.
	data := loadDataset(searchFileName)
	fmt.Println()

	var idxCtx cspann.Context
	doSearch := func(beamSize int) {
		start := timeutil.Now()

		txn := beginTransaction(ctx, index.Store())
		defer commitTransaction(ctx, index.Store(), txn)
		idxCtx.Init(txn)

		// Search for test vectors.
		var sumMAP, sumVectors, sumLeafVectors, sumFullVectors, sumPartitions float64
		count := data.Test.Count
		for i := 0; i < count; i++ {
			// Calculate truth set for the vector.
			queryVector := data.Test.At(i)

			searchSet := cspann.SearchSet{MaxResults: *flagMaxResults}
			searchOptions := cspann.SearchOptions{BaseBeamSize: beamSize}

			// Calculate prediction set for the vector.
			err = index.Search(ctx, &idxCtx, nil /* treeKey */, queryVector, &searchSet, searchOptions)
			if err != nil {
				panic(err)
			}
			results := searchSet.PopResults()

			prediction := make([]cspann.KeyBytes, searchSet.MaxResults)
			for res := 0; res < len(results); res++ {
				prediction[res] = results[res].ChildKey.KeyBytes
			}

			primaryKeys := make([]byte, searchSet.MaxResults*4)
			truth := make([]cspann.KeyBytes, searchSet.MaxResults)
			for neighbor := 0; neighbor < searchSet.MaxResults; neighbor++ {
				primaryKey := primaryKeys[neighbor*4 : neighbor*4+4]
				binary.BigEndian.PutUint32(primaryKey, uint32(data.Neighbors[i][neighbor]))
				truth[neighbor] = primaryKey
			}

			sumMAP += findMAP(prediction, truth)
			sumVectors += float64(searchSet.Stats.QuantizedVectorCount)
			sumLeafVectors += float64(searchSet.Stats.QuantizedLeafVectorCount)
			sumFullVectors += float64(searchSet.Stats.FullVectorCount)
			sumPartitions += float64(searchSet.Stats.PartitionCount)
		}

		elapsed := timeutil.Since(start)
		fmt.Printf("%d\t%0.2f%%\t%0.0f\t%0.0f\t%0.2f\t%0.2f\t%0.2f\n",
			beamSize, sumMAP/float64(count)*100,
			sumLeafVectors/float64(count), sumVectors/float64(count),
			sumFullVectors/float64(count), sumPartitions/float64(count),
			float64(count)/elapsed.Seconds())
	}

	fmt.Printf(White+"%s\n"+Reset, datasetName)
	trainVectors := memStore.GetAllVectors()
	fmt.Printf(
		White+"%d train vectors, %d test vectors, %d dimensions, %d/%d min/max partitions, base beam size %d\n"+Reset,
		len(trainVectors), data.Test.Count, data.Test.Dims,
		index.Options().MinPartitionSize, index.Options().MaxPartitionSize,
		index.Options().BaseBeamSize)
	fmt.Println(index.FormatStats())

	fmt.Printf(White + "beam\trecall\tleaf\tall\tfull\tpartns\tqps\n" + Reset)

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

// downloadDataset downloads the given dataset from the GCP bucket and unzips
// it into the tmp directory.
func downloadDataset(ctx context.Context, datasetName string) dataset {
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
		//nolint:deferloop TODO(#137605)
		defer zippedFile.Close()

		// Create the output file
		path := fmt.Sprintf("%s/%s", tempDir, file.Name)
		outputFile, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		//nolint:deferloop TODO(#137605)
		defer outputFile.Close()

		// Copy the contents of the zipped file to the output file
		if _, err = io.Copy(outputFile, zippedFile); err != nil {
			panic(err)
		}
	}

	// Separately store test vectors for searching, so the training data does not
	// need to be loaded every time.
	data := loadDataset(datasetFileName)
	searchData := data
	searchData.Train = vector.Set{}

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

// buildIndex builds a vector index for the given dataset and serializes the
// built index to the tmp directory.
func buildIndex(
	ctx context.Context, stopper *stop.Stopper, datasetName string,
) (*memstore.Store, *cspann.Index) {
	// Ensure dataset file has been downloaded.
	data := downloadDataset(ctx, datasetName)
	if *flagBuildCount != 0 {
		// Build index from subset of data - first N vectors.
		n := *flagBuildCount
		if n > data.Train.Count {
			n = data.Train.Count
		}
		data.Train.SplitAt(n)
	}

	// Create index.
	store := memstore.New(data.Train.Dims, seed)
	index := createIndex(ctx, stopper, store)

	// Create unique primary key for each vector in a single large byte buffer.
	primaryKeys := make([]byte, data.Train.Count*4)
	for i := 0; i < data.Train.Count; i++ {
		binary.BigEndian.PutUint32(primaryKeys[i*4:], uint32(i))
	}

	// Compute percentile latencies.
	estimator := NewPercentileEstimator(1000)

	// Insert block of vectors within the scope of a transaction.
	var insertCount atomic.Uint64
	insertBlock := func(idxCtx *cspann.Context, start, end int) {
		txn := beginTransaction(ctx, store)
		defer commitTransaction(ctx, store, txn)
		idxCtx.Init(txn)

		for i := start; i < end; i++ {
			key := primaryKeys[i*4 : i*4+4]
			vec := data.Train.At(i)
			store.InsertVector(key, vec)
			startMono := crtime.NowMono()
			if err := index.Insert(ctx, idxCtx, nil /* treeKey */, vec, key); err != nil {
				panic(err)
			}
			estimator.Add(startMono.Elapsed().Seconds())
		}
		insertCount.Add(uint64(end - start))
	}

	// Set up time series charts.
	cp := chartPrinter{Footer: 2, Hide: *flagHideCharts}
	throughput := cp.AddChart("ops/sec")
	fixups := cp.AddChart("fixup queue size")
	throttled := cp.AddChart("pacer ops/sec")
	p50 := cp.AddChart("p50 ms latency")
	p90 := cp.AddChart("p90 ms latency")
	p99 := cp.AddChart("p99 ms latency")

	fmt.Printf(White+"Building index for dataset: %s\n"+Reset, datasetName)
	startAt := crtime.NowMono()

	// Insert vectors into the store on multiple goroutines.
	procs := runtime.GOMAXPROCS(-1)
	countPerProc := (data.Train.Count + procs) / procs
	blockSize := index.Options().MinPartitionSize
	for i := 0; i < data.Train.Count; i += countPerProc {
		end := min(i+countPerProc, data.Train.Count)
		go func(start, end int) {
			var indexCtx cspann.Context
			// Break vector group into individual transactions that each insert a
			// block of vectors. Run any pending fixups after each block.
			for j := start; j < end; j += blockSize {
				insertBlock(&indexCtx, j, min(j+blockSize, end))
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
		cp.AddSample(fixups, float64(index.Fixups().QueueSize()))
		cp.AddSample(throttled, index.Fixups().AllowedOpsPerSec())
		cp.AddSample(p50, estimator.Estimate(0.50)*1000)
		cp.AddSample(p90, estimator.Estimate(0.90)*1000)
		cp.AddSample(p99, estimator.Estimate(0.99)*1000)
		cp.Plot()

		sinceStart := now.Sub(startAt)
		fmt.Printf(White+"\rInserted %d / %d vectors (%.2f%%) in %v"+Reset,
			totalInserted, data.Train.Count, (float64(totalInserted)/float64(data.Train.Count))*100,
			sinceStart.Truncate(time.Second))

		if totalInserted >= data.Train.Count {
			break
		}
	}

	// Wait for any remaining background fixups to be processed.
	index.ProcessFixups()

	fmt.Printf(White+"\nBuilt index in %v\n"+Reset, roundDuration(startAt.Elapsed()))
	return store, index
}

// createIndex returns a vector index created using the given store.
func createIndex(ctx context.Context, stopper *stop.Stopper, store cspann.Store) *cspann.Index {
	memStore := store.(*memstore.Store)
	quantizer := quantize.NewRaBitQuantizer(memStore.Dims(), seed)
	options := cspann.IndexOptions{
		MinPartitionSize: minPartitionSize,
		MaxPartitionSize: maxPartitionSize,
		BaseBeamSize:     *flagBeamSize,
	}
	index, err := cspann.NewIndex(ctx, store, quantizer, seed, &options, stopper)
	if err != nil {
		panic(err)
	}
	return index
}

// saveStore serializes the store as a protobuf and saves it to the given file.
func saveStore(memStore *memstore.Store, fileName string) {
	startTime := timeutil.Now()

	indexBytes, err := memStore.MarshalBinary()
	if err != nil {
		panic(err)
	}

	indexFile, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()

	_, err = indexFile.Write(indexBytes)
	if err != nil {
		panic(err)
	}

	elapsed := timeutil.Since(startTime)
	fmt.Printf(Cyan+"Saved index to disk in %v\n"+Reset, roundDuration(elapsed))
}

// loadStore deserializes a previously saved protobuf of a vector store.
func loadStore(fileName string) *memstore.Store {
	startTime := timeutil.Now()

	data, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	memStore, err := memstore.Load(data)
	if err != nil {
		panic(err)
	}

	elapsed := timeutil.Since(startTime)
	fmt.Printf(Cyan+"Loaded %s index from disk in %v\n"+Reset, fileName, roundDuration(elapsed))
	return memStore
}

// loadDataset deserializes a dataset saved as a gob file.
func loadDataset(fileName string) dataset {
	startTime := timeutil.Now()
	fmt.Printf(Cyan+"Loading data from %s\n"+Reset, fileName)

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

	elapsed := timeutil.Since(startTime)
	fmt.Printf(Cyan+"Loaded %s in %v          \n"+Reset, fileName, roundDuration(elapsed))
	return data
}

func beginTransaction(ctx context.Context, store cspann.Store) cspann.Txn {
	txn, err := store.BeginTransaction(ctx)
	if err != nil {
		panic(err)
	}
	return txn
}

func commitTransaction(ctx context.Context, store cspann.Store, txn cspann.Txn) {
	if err := store.CommitTransaction(ctx, txn); err != nil {
		panic(err)
	}
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

// progressWriter tracks download progress.
type progressWriter struct {
	startTime  time.Time
	writer     io.Writer
	total      int64
	downloaded int64
}

func makeProgressWriter(writer io.Writer, total int64) progressWriter {
	return progressWriter{
		startTime: timeutil.Now(),
		writer:    writer,
		total:     total,
	}
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.writer.Write(p)
	pw.downloaded += int64(n)
	pw.printProgress()
	return n, err
}

func (pw *progressWriter) printProgress() {
	elapsed := timeutil.Since(pw.startTime)
	fmt.Printf(Cyan+"\rDownloaded %s / %s (%.2f%%) in %v          "+Reset,
		formatSize(pw.downloaded), formatSize(pw.total),
		(float64(pw.downloaded)/float64(pw.total))*100, elapsed.Truncate(time.Second))
}

func roundDuration(duration time.Duration) time.Duration {
	return duration.Truncate(time.Millisecond)
}

func formatSize(size int64) string {
	const (
		_           = iota // ignore first value by assigning to blank identifier
		KiB float64 = 1 << (10 * iota)
		MiB
		GiB
		TiB
		PiB
	)

	switch {
	case size < int64(KiB):
		return fmt.Sprintf("%d B", size)
	case size < int64(MiB):
		return fmt.Sprintf("%.2f KiB", float64(size)/KiB)
	case size < int64(GiB):
		return fmt.Sprintf("%.2f MiB", float64(size)/MiB)
	case size < int64(TiB):
		return fmt.Sprintf("%.2f GiB", float64(size)/GiB)
	case size < int64(PiB):
		return fmt.Sprintf("%.2f TiB", float64(size)/TiB)
	default:
		return fmt.Sprintf("%.2f PiB", float64(size)/PiB)
	}
}
