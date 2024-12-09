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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
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

var flagMaxResults = flag.Int("k", 10, "Number of search results, used in recall calculation.")
var flagBeamSize = flag.Int(
	"default-beam-size",
	8,
	"Default beam size used for building and searching the index.")
var flagSearchBeamSizes = flag.String(
	"search-beam-sizes",
	"1,2,4,8,16,32,64,128,256,512",
	"List of beam sizes used for search.")

type dataset struct {
	Train     vector.Set
	Test      vector.Set
	Neighbors [][]int64
}

// vecbench benchmarks vector index in-memory search performance on a variety of
// datasets. Datasets are downloaded from the cockroach-fixtures-us-east1 GCP
// bucket (vecindex directory). Here is a list of available datasets, most of
// which are derived from datasets on ann-benchmarks.com:
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

func main() {
	flag.Parse()

	ctx := context.Background()

	if flag.NArg() < 1 {
		fmt.Printf("usage: vecbench search [<dataset-name>]\n")
		os.Exit(1)
	}

	switch flag.Arg(0) {
	case "search":
		if flag.NArg() == 1 {
			// Default to default dataset.
			searchIndex(ctx, defaultDataset)
		} else {
			for i, datasetName := range flag.Args()[1:] {
				if i != 0 {
					fmt.Println("----------")
				}
				searchIndex(ctx, datasetName)
			}
		}

	default:
		log.Fatalf("unknown command '%s", flag.Arg(0))
	}
}

// searchIndex downloads, builds, and searches an index for the given dataset.
func searchIndex(ctx context.Context, datasetName string) {
	indexFileName := fmt.Sprintf("%s/%s.idx", tempDir, datasetName)
	datasetFileName := fmt.Sprintf("%s/%s.gob", tempDir, datasetName)

	// If index is not yet built, do so now.
	buildIndex(ctx, datasetName)

	// Load the store from disk file.
	store := loadStore(indexFileName)

	// Load test data.
	data := loadDataset(datasetFileName)

	quantizer := quantize.NewRaBitQuantizer(data.Test.Dims, seed)
	indexOptions := vecindex.VectorIndexOptions{
		MinPartitionSize: minPartitionSize,
		MaxPartitionSize: maxPartitionSize,
		BaseBeamSize:     *flagBeamSize,
		Seed:             seed,
	}
	index, err := vecindex.NewVectorIndex(ctx, store, quantizer, &indexOptions, nil /* stopper */)
	if err != nil {
		panic(err)
	}

	doSearch := func(beamSize int) {
		start := timeutil.Now()

		txn := beginTransaction(ctx, store)
		defer commitTransaction(ctx, store, txn)

		// Search for test vectors.
		var sumMAP, sumVectors, sumLeafVectors, sumFullVectors, sumPartitions float64
		count := data.Test.Count
		for i := 0; i < count; i++ {
			// Calculate truth set for the vector.
			queryVector := data.Test.At(i)

			searchSet := vecstore.SearchSet{MaxResults: *flagMaxResults}
			searchOptions := vecindex.SearchOptions{BaseBeamSize: beamSize}

			// Calculate prediction set for the vector.
			err = index.Search(ctx, txn, queryVector, &searchSet, searchOptions)
			if err != nil {
				panic(err)
			}
			results := searchSet.PopResults()

			prediction := make([]vecstore.PrimaryKey, searchSet.MaxResults)
			for res := 0; res < len(results); res++ {
				prediction[res] = results[res].ChildKey.PrimaryKey
			}

			primaryKeys := make([]byte, searchSet.MaxResults*4)
			truth := make([]vecstore.PrimaryKey, searchSet.MaxResults)
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

	fmt.Printf("%s\n", datasetName)
	fmt.Printf("%d train vectors, %d test vectors, %d dimensions, %d/%d min/max partitions, base beam size %d\n",
		data.Train.Count, data.Test.Count, data.Test.Dims,
		indexOptions.MinPartitionSize, indexOptions.MaxPartitionSize, indexOptions.BaseBeamSize)
	fmt.Println(index.FormatStats())

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

// downloadDataset downloads the given dataset from the GCP bucket and unzips
// it into the tmp directory.
func downloadDataset(ctx context.Context, datasetName string) {
	objectName := fmt.Sprintf("%s/%s.zip", bucketDirName, datasetName)
	datasetFileName := fmt.Sprintf("%s/%s.gob", tempDir, datasetName)

	// If dataset file has already been downloaded, then nothing to do.
	_, err := os.Stat(datasetFileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			panic(err)
		}
	} else {
		return
	}

	fmt.Printf("Downloading %s/%s\n", bucketName, objectName)

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
	writer := &progressWriter{
		Writer: &buf,
		Total:  attrs.Size,
	}

	if _, err = io.Copy(writer, reader); err != nil {
		log.Fatalf("Failed to copy object data: %v", err)
	}

	// Open the zip archive.
	fmt.Printf("\nUnzipping to %s\n", datasetFileName)
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
		defer zippedFile.Close()

		// Create the output file
		path := fmt.Sprintf("%s/%s", tempDir, file.Name)
		outputFile, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		defer outputFile.Close()

		// Copy the contents of the zipped file to the output file
		if _, err = io.Copy(outputFile, zippedFile); err != nil {
			panic(err)
		}
	}
}

// buildIndex builds a vector index for the given dataset and serializes the
// built index to the tmp directory.
func buildIndex(ctx context.Context, datasetName string) {
	indexFileName := fmt.Sprintf("%s/%s.idx", tempDir, datasetName)
	datasetFileName := fmt.Sprintf("%s/%s.gob", tempDir, datasetName)

	// If index file has already been built, then nothing to do.
	_, err := os.Stat(indexFileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			panic(err)
		}
	} else {
		return
	}

	// Ensure dataset file has been downloaded.
	downloadDataset(ctx, datasetName)

	fmt.Printf("Building index for dataset: %s\n", datasetName)

	// Load data from the downloaded .gob file.
	startTime := timeutil.Now()

	readFile, err := os.Open(datasetFileName)
	if err != nil {
		panic(err)
	}
	defer readFile.Close()

	decoder := gob.NewDecoder(readFile)
	var data dataset
	if err = decoder.Decode(&data); err != nil {
		panic(err)
	}

	readTime := timeutil.Now()
	fmt.Printf("Loaded %s in %v\n", datasetFileName, readTime.Sub(startTime))

	// Create index.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	store := vecstore.NewInMemoryStore(data.Train.Dims, seed)
	quantizer := quantize.NewRaBitQuantizer(data.Train.Dims, seed)
	options := vecindex.VectorIndexOptions{
		MinPartitionSize: minPartitionSize,
		MaxPartitionSize: maxPartitionSize,
		BaseBeamSize:     *flagBeamSize,
		Seed:             seed,
	}
	index, err := vecindex.NewVectorIndex(ctx, store, quantizer, &options, stopper)
	if err != nil {
		panic(err)
	}

	// Create unique primary key for each vector in a single large byte buffer.
	primaryKeys := make([]byte, data.Train.Count*4)
	for i := 0; i < data.Train.Count; i++ {
		binary.BigEndian.PutUint32(primaryKeys[i*4:], uint32(i))
	}

	// Insert block of vectors within the scope of a transaction.
	var insertCount atomic.Uint64
	insertBlock := func(start, end int) {
		txn := beginTransaction(ctx, store)
		defer commitTransaction(ctx, store, txn)

		for i := start; i < end; i++ {
			key := primaryKeys[i*4 : i*4+4]
			vec := data.Train.At(i)
			store.InsertVector(key, vec)
			if err = index.Insert(ctx, txn, vec, key); err != nil {
				panic(err)
			}
		}

		inserted := insertCount.Add(uint64(end - start))
		fmt.Printf("\rInserted %d / %d vectors (%.2f%%)", inserted, data.Train.Count,
			(float64(inserted)/float64(data.Train.Count))*100)
	}

	// Insert vectors into the store on multiple goroutines.
	var wait sync.WaitGroup
	procs := runtime.GOMAXPROCS(-1)
	countPerProc := (data.Train.Count + procs) / procs
	blockSize := options.MinPartitionSize
	for i := 0; i < data.Train.Count; i += countPerProc {
		end := min(i+countPerProc, data.Train.Count)
		wait.Add(1)
		go func(start, end int) {
			// Break vector group into individual transactions that each insert a
			// block of vectors. Run any pending fixups after each block.
			for j := start; j < end; j += blockSize {
				insertBlock(j, min(j+blockSize, end))
				index.ProcessFixups()
			}

			wait.Done()
		}(i, end)
	}
	wait.Wait()

	buildTime := timeutil.Now()
	fmt.Printf("\nBuilt index in %v\n", buildTime.Sub(readTime))

	saveStore(store, indexFileName)

	writeTime := timeutil.Now()
	fmt.Printf("Wrote index to disk in %v\n", writeTime.Sub(buildTime))
}

// saveStore serializes the store as a protobuf and saves it to the given file.
func saveStore(store *vecstore.InMemoryStore, fileName string) {
	indexBytes, err := store.MarshalBinary()
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
}

// loadStore deserializes a previously saved protobuf of a vector store.
func loadStore(fileName string) *vecstore.InMemoryStore {
	data, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}

	inMemStore, err := vecstore.LoadInMemoryStore(data)
	if err != nil {
		panic(err)
	}

	return inMemStore
}

// loadDataset deserializes a dataset saved as a gob file.
func loadDataset(fileName string) dataset {
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

	return data
}

func beginTransaction(ctx context.Context, store vecstore.Store) vecstore.Txn {
	txn, err := store.Begin(ctx)
	if err != nil {
		panic(err)
	}
	return txn
}

func commitTransaction(ctx context.Context, store vecstore.Store, txn vecstore.Txn) {
	if err := store.Commit(ctx, txn); err != nil {
		panic(err)
	}
}

// findMAP returns mean average precision, which compares a set of predicted
// results with the true set of results. Both sets are expected to be of equal
// length. It returns the percentage overlap of the predicted set with the truth
// set.
func findMAP(prediction, truth []vecstore.PrimaryKey) float64 {
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
	Writer     io.Writer
	Total      int64
	Downloaded int64
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.Writer.Write(p)
	pw.Downloaded += int64(n)
	pw.printProgress()
	return n, err
}

func (pw *progressWriter) printProgress() {
	fmt.Printf("\rDownloaded %d / %d bytes (%.2f%%)", pw.Downloaded, pw.Total, (float64(pw.Downloaded)/float64(pw.Total))*100)
}
