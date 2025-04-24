// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecann

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
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

	// cacheFolder is the name of the temporary folder that will cache the
	// downloaded dataset files.
	cacheFolder string
}

// Load checks whether the given dataset has been downloaded. If not, it
// downloads it from the GCP bucket and unzips it into the fixtures folder. It
// returns the dataset once it's cached locally.
func (dl *DatasetLoader) Load(ctx context.Context) error {
	if err := dl.ensureCacheFolder(); err != nil {
		return err
	}

	dataFileName := fmt.Sprintf("%s/%s.gob", dl.cacheFolder, dl.DatasetName)
	searchDataFileName := fmt.Sprintf("%s/%s.search.gob", dl.cacheFolder, dl.DatasetName)

	// If dataset file has already been downloaded, load it from disk into memory.
	_, err := os.Stat(dataFileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			return errors.Wrapf(err, "getting OS stats for %s", dataFileName)
		}
	} else {
		return dl.loadFromDisk(ctx)
	}

	// Download the dataset from the GCP bucket.
	if err = dl.download(ctx); err != nil {
		return err
	}

	// Load the data from disk into memory.
	if err = dl.loadFromDisk(ctx); err != nil {
		return err
	}

	// Separately save test vectors for searching, so the full training data does
	// not need to be loaded every time.
	writeFile, err := os.Create(searchDataFileName)
	if err != nil {
		return errors.Wrapf(err, "creating search file %s", dataFileName)
	}
	defer writeFile.Close()

	encoder := gob.NewEncoder(writeFile)
	if err = encoder.Encode(&dl.SearchData); err != nil {
		return errors.Wrapf(err, "encoding search data")
	}

	return nil
}

// LoadForSearch is similar to Load, except that it only loads up the subset of
// data needed to search the index, not to build it. This includes the Test
// vectors and neighbor information, but not the Train vectors.
func (dl *DatasetLoader) LoadForSearch(ctx context.Context) error {
	if err := dl.ensureCacheFolder(); err != nil {
		return err
	}

	fileName := fmt.Sprintf("%s/%s.search.gob", dl.cacheFolder, dl.DatasetName)

	// If search data is not cached, download it now.
	_, err := os.Stat(fileName)
	if err != nil {
		if !oserror.IsNotExist(err) {
			return errors.Wrapf(err, "getting search data file %s", fileName)
		}
		return dl.Load(ctx)
	}

	// Load the search data from disk.
	readFile, err := os.Open(fileName)
	if err != nil {
		return errors.Wrapf(err, "opening search data file %s", fileName)
	}
	defer readFile.Close()

	decoder := gob.NewDecoder(readFile)
	if err = decoder.Decode(&dl.SearchData); err != nil {
		return errors.Wrapf(err, "decoding search data from file %s", fileName)
	}

	return nil
}

// download downloads the dataset from the GCP bucket, unzips it, and stores it
// in the cache folder.
func (dl *DatasetLoader) download(ctx context.Context) error {
	objectName := fmt.Sprintf("%s/%s.zip", bucketDirName, dl.DatasetName)

	dl.OnProgress(ctx, "Downloading %s from %s", objectName, bucketName)

	// Create a GCS client using Application Default Credentials.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrapf(err, "creating GCS client")
	}
	defer func() {
		_ = client.Close()
	}()

	// Get a handle to the object.
	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)

	// Get object attributes to determine its size.
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return errors.Wrapf(err, "getting attributes for %s/%s", bucketName, objectName)
	}

	// Read the object with progress tracking.
	reader, err := object.NewReader(ctx)
	if err != nil {
		return errors.Wrapf(err, "creating reader for %s/%s", bucketName, objectName)
	}
	defer func() {
		_ = reader.Close()
	}()

	// Use progressWriter to track download progress.
	var buf bytes.Buffer
	writer := makeProgressWriter(&buf, attrs.Size)
	writer.OnProgress = dl.OnDownloadProgress

	if _, err = io.Copy(&writer, reader); err != nil {
		return errors.Wrapf(err, "copying data for %s/%s", bucketName, objectName)
	}

	// Open the zip archive.
	zipReader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		return errors.Wrapf(err, "unzipping %s.zip", dl.DatasetName)
	}

	// Create temp folder.
	if err = os.MkdirAll(dl.cacheFolder, os.ModePerm); err != nil {
		return errors.Wrapf(err, "creating cache folder")
	}

	// Extract files to the cache folder.
	for _, file := range zipReader.File {
		// Open the file inside the zip archive
		zippedFile, err := file.Open()
		if err != nil {
			return errors.Wrapf(err, "extracting file %s from zip archive", file.Name)
		}

		// Create the output file
		path := fmt.Sprintf("%s/%s", dl.cacheFolder, file.Name)
		dl.OnProgress(ctx, "Unzipping to %s", path)
		outputFile, err := os.Create(path)
		if err != nil {
			return errors.Wrapf(err, "creating output file %s/%s", dl.cacheFolder, file.Name)
		}

		// Copy the contents of the zipped file to the output file
		if _, err := io.Copy(outputFile, zippedFile); err != nil {
			return errors.Wrapf(err, "creating output file %s/%s", dl.cacheFolder, file.Name)
		}
		if err := outputFile.Close(); err != nil {
			return errors.Wrapf(err, "closing output file %s", outputFile.Name())
		}
		if err := zippedFile.Close(); err != nil {
			return errors.Wrapf(err, "closing zipped file")
		}
	}

	return nil
}

// loadFromDisk deserializes a dataset saved as a gob file.
func (dl *DatasetLoader) loadFromDisk(ctx context.Context) error {
	fileName := fmt.Sprintf("%s/%s.gob", dl.cacheFolder, dl.DatasetName)

	startTime := timeutil.Now()
	dl.OnProgress(ctx, "Loading train and test data from %s", fileName)

	readFile, err := os.Open(fileName)
	if err != nil {
		return errors.Wrapf(err, "opening file %s", fileName)
	}
	defer readFile.Close()

	decoder := gob.NewDecoder(readFile)
	if err = decoder.Decode(&dl.Data); err != nil {
		return errors.Wrapf(err, "decoding file %s", fileName)
	}

	elapsed := timeutil.Since(startTime)
	dl.OnProgress(ctx, "Loaded %s in %v", fileName, roundDuration(elapsed))

	// Also set SearchData.
	dl.SearchData = SearchDataset{
		Count:     dl.Data.Train.Count,
		Test:      dl.Data.Test,
		Neighbors: dl.Data.Neighbors,
	}

	return nil
}

// ensureCacheFolder ensures that the dl.cacheFolder field has been set to the
// directory that caches workload datasets, at ~/.cache/workload-datasets/
func (dl *DatasetLoader) ensureCacheFolder() error {
	if dl.cacheFolder == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrapf(err, "getting home directory")
		}
		dl.cacheFolder = fmt.Sprintf("%s/.cache/workload-datasets/", homeDir)
	}
	return nil
}
