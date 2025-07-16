// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecann

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

const bucketName = "cockroach-fixtures-us-east1"
const bucketDirName = "vecindex"

// Dataset describes a set of vectors to be benchmarked.
type Dataset struct {
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

// SearchDataset stores the subset of information from "dataset" that's needed
// to benchmark the performance of searching the index. In particular, it
// assumes that the index has already been built, and so does not contain the
// Train vectors. This allows the dataset to be loaded from disk much faster.
type SearchDataset struct {
	// Count is the number of Train vectors in the original dataset.
	Count int
	// Test is the the same as dataset.Test.
	Test vector.Set
	// Neighbors is the same as dataset.Neighbors.
	Neighbors [][]int64
}

// DatasetLoader downloads a vector dataset from a GCP bucket, stores it in a
// local cache folder, and loads it from disk into memory.
type DatasetLoader struct {
	// DatasetName is the name of the dataset (e.g. dbpedia-openai-100k-angular).
	DatasetName string
	// CacheFolder is the path to the temporary folder where datasets will be
	// cached. It defaults to ~/.cache/workload-datasets.
	CacheFolder string

	// OnProgress logs the progress of the loading process.
	OnProgress func(ctx context.Context, format string, args ...any)
	// OnDownloadProgress logs the progress of downloading from the GCP bucket.
	// This is called at a granular level over the course of the download.
	OnDownloadProgress func(downloaded, total int64, elapsed time.Duration)

	// Data is the dataset loaded into memory, including both test and train
	// vectors. This is set by the Load method, but not by LoadForSearch.
	Data Dataset
	// SearchData is the dataset loaded into memory, but only including the test
	// vectors, not the train vectors. This is set by both the Load and
	// LoadForSearch methods.
	SearchData SearchDataset
}

// Load checks whether the given dataset has been downloaded. If not, it
// downloads it from the GCP bucket and unzips it into the fixtures folder. It
// returns the dataset once it's cached locally.
func (dl *DatasetLoader) Load(ctx context.Context) (err error) {
	dl.CacheFolder, err = EnsureCacheFolder(dl.CacheFolder)
	if err != nil {
		return err
	}
	return dl.loadFiles(ctx, true)
}

// LoadForSearch loads only the test vectors and neighbors.
func (dl *DatasetLoader) LoadForSearch(ctx context.Context) (err error) {
	dl.CacheFolder, err = EnsureCacheFolder(dl.CacheFolder)
	if err != nil {
		return err
	}
	return dl.loadFiles(ctx, false)
}

// loadFiles loads the required files for the dataset. If loadTrain is true, it
// loads train vectors as well.
func (dl *DatasetLoader) loadFiles(ctx context.Context, loadTrain bool) error {
	baseName, metric, err := parseDatasetName(dl.DatasetName)
	if err != nil {
		return err
	}

	baseDir := fmt.Sprintf("%s/%s", dl.CacheFolder, baseName)
	if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "creating cache subdir %s", baseDir)
	}

	train := fmt.Sprintf("%s/%s.fbin", baseDir, baseName)
	test := fmt.Sprintf("%s/%s-test.fbin", baseDir, baseName)
	neighborsIbin := fmt.Sprintf("%s/%s-neighbors-%s.ibin", baseDir, baseName, metric)

	// Download all files if missing.
	if !fileExists(train) {
		if err := dl.downloadAndUnzip(ctx, baseName, baseName+".fbin.zip", train); err != nil {
			return err
		}
	}
	if !fileExists(test) {
		if err := dl.downloadAndUnzip(ctx, baseName, baseName+"-test.fbin.zip", test); err != nil {
			return err
		}
	}
	if !fileExists(neighborsIbin) {
		fileName := baseName + "-neighbors-" + metric + ".ibin.zip"
		if err := dl.downloadAndUnzip(ctx, baseName, fileName, neighborsIbin); err != nil {
			return err
		}
	}

	var trainSet vector.Set
	var trainCount int
	if loadTrain {
		trainSet, err = readFbin(train)
		if err != nil {
			return err
		}
		trainCount = trainSet.Count
	} else {
		// Only read the header to get the train vector count.
		f, err := os.Open(train)
		if err != nil {
			return errors.Wrapf(err, "opening %s", train)
		}
		defer f.Close()
		var numVec uint32
		if err := binary.Read(f, binary.LittleEndian, &numVec); err != nil {
			return errors.Wrapf(err, "reading numVec from %s", train)
		}
		trainCount = int(numVec)
	}
	testSet, err := readFbin(test)
	if err != nil {
		return err
	}
	neighbors, err := readNeighbors(neighborsIbin)
	if err != nil {
		return err
	}

	if loadTrain {
		dl.Data = Dataset{
			Train:     trainSet,
			Test:      testSet,
			Neighbors: neighbors,
		}
	}
	dl.SearchData = SearchDataset{
		Count:     trainCount,
		Test:      testSet,
		Neighbors: neighbors,
	}

	return nil
}

// downloadAndUnzip downloads a zip file from GCP and extracts the contained
// file to destPath.
func (dl *DatasetLoader) downloadAndUnzip(
	ctx context.Context, baseName, objectFile, destPath string,
) (err error) {
	objectName := fmt.Sprintf("%s/%s/%s", bucketDirName, baseName, objectFile)
	tempZipFile := destPath + ".zip"
	defer func() {
		err = errors.CombineErrors(err, os.Remove(tempZipFile))
	}()

	dl.OnProgress(ctx, "Downloading %s from %s", objectName, bucketName)
	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrapf(err, "creating GCS client")
	}
	defer func() {
		err = errors.CombineErrors(err, client.Close())
	}()

	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return errors.Wrapf(err, "getting attributes for %s/%s", bucketName, objectName)
	}
	tempZip, err := os.Create(tempZipFile)
	if err != nil {
		return errors.Wrapf(err, "creating temp zip file %s", tempZipFile)
	}
	defer func() {
		err = errors.CombineErrors(err, tempZip.Close())
	}()

	reader, err := object.NewReader(ctx)
	if err != nil {
		return errors.Wrapf(err, "creating reader for %s/%s", bucketName, objectName)
	}
	defer func() {
		err = errors.CombineErrors(err, reader.Close())
	}()

	writer := makeProgressWriter(tempZip, attrs.Size)
	writer.OnProgress = dl.OnDownloadProgress
	if _, err := io.Copy(&writer, reader); err != nil {
		return errors.Wrapf(err, "downloading to file %s", tempZipFile)
	}

	// Unzip the file
	zipR, err := zip.OpenReader(tempZipFile)
	if err != nil {
		return errors.Wrapf(err, "opening zip file %s", tempZipFile)
	}
	defer func() {
		err = errors.CombineErrors(err, zipR.Close())
	}()

	if len(zipR.File) == 0 {
		return errors.Newf("zip file %s is empty", tempZipFile)
	}
	zfile := zipR.File[0]
	zreader, err := zfile.Open()
	if err != nil {
		return errors.Wrapf(err, "opening zipped file %s", zfile.Name)
	}
	defer zreader.Close()
	out, err := os.Create(destPath)
	if err != nil {
		return errors.Wrapf(err, "creating output file %s", destPath)
	}
	defer out.Close()
	if _, err := io.Copy(out, zreader); err != nil {
		return errors.Wrapf(err, "extracting to %s", destPath)
	}
	return nil
}

// readFbin reads a .fbin file into a vector.Set. The .fbin format is:
//
//	[num_vectors (uint32), vector_dim (uint32), vector_array (float32)]
//
// where vector_array contains num_vectors * vector_dim float32 values in
// row-major order.
func readFbin(path string) (vector.Set, error) {
	f, err := os.Open(path)
	if err != nil {
		return vector.Set{}, errors.Wrapf(err, "opening %s", path)
	}
	defer f.Close()
	var numVec, dim uint32
	if err := binary.Read(f, binary.LittleEndian, &numVec); err != nil {
		return vector.Set{}, errors.Wrapf(err, "reading numVec from %s", path)
	}
	if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
		return vector.Set{}, errors.Wrapf(err, "reading dim from %s", path)
	}
	data := make([]float32, int(numVec)*int(dim))
	if err := binary.Read(f, binary.LittleEndian, data); err != nil {
		return vector.Set{}, errors.Wrapf(err, "reading data from %s", path)
	}
	return vector.MakeSetFromRawData(data, int(dim)), nil
}

// readNeighbors reads an .ibin file into [][]int64. The .ibin format is:
//
//	[num_vectors (uint32), num_neighbors_per_vector (uint32), neighbor_array (int32)]
//
// where neighbor_array contains num_vectors * num_neighbors_per_vector int32
// values in row-major order. Each row represents the neighbor indices for one
// query vector.
func readNeighbors(path string) ([][]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "opening %s", path)
	}
	defer f.Close()
	var numVec, numNeighbors uint32
	if err := binary.Read(f, binary.LittleEndian, &numVec); err != nil {
		return nil, errors.Wrapf(err, "reading numVec from %s", path)
	}
	if err := binary.Read(f, binary.LittleEndian, &numNeighbors); err != nil {
		return nil, errors.Wrapf(err, "reading numNeighbors from %s", path)
	}
	data := make([]int32, int(numVec)*int(numNeighbors))
	if err := binary.Read(f, binary.LittleEndian, data); err != nil {
		return nil, errors.Wrapf(err, "reading data from %s", path)
	}
	neighbors := make([][]int64, numVec)
	for i := range neighbors {
		neighbors[i] = make([]int64, numNeighbors)
		for j := range neighbors[i] {
			neighbors[i][j] = int64(data[i*int(numNeighbors)+j])
		}
	}
	return neighbors, nil
}

// parseDatasetName splits <base-name>-<metric> into baseName, metric.
func parseDatasetName(name string) (string, string, error) {
	idx := strings.LastIndex(name, "-")
	if idx == -1 || idx == len(name)-1 {
		return "", "", errors.Newf("invalid dataset name: %s", name)
	}
	return name[:idx], name[idx+1:], nil
}

// fileExists returns true if the file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// EnsureCacheFolder creates the given directory path if it is not already
// present. If the path is the empty string, then ~/.cache/workload-datasets is
// used as the default. EnsureCacheFolder returns the path of the cache folder.
func EnsureCacheFolder(path string) (string, error) {
	if path == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrapf(err, "getting home directory")
		}
		path = fmt.Sprintf("%s/.cache/workload-datasets", homeDir)
	}

	// Create the cache folder.
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return "", errors.Wrapf(err, "creating cache folder")
	}

	return path, nil
}
