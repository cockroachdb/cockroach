// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecann

import (
	"archive/zip"
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

	dataFileName := fmt.Sprintf("%s/%s.gob", dl.CacheFolder, dl.DatasetName)
	searchDataFileName := fmt.Sprintf("%s/%s.search.gob", dl.CacheFolder, dl.DatasetName)

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

	fileName := fmt.Sprintf("%s/%s.search.gob", dl.CacheFolder, dl.DatasetName)

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
func (dl *DatasetLoader) download(ctx context.Context) (err error) {
	objectName := fmt.Sprintf("%s/%s.zip", bucketDirName, dl.DatasetName)
	tempZipFileName := fmt.Sprintf("%s/%s.zip", dl.CacheFolder, dl.DatasetName)

	defer func() {
		// Delete the temporary zip file if it exists.
		_ = os.Remove(tempZipFileName)
	}()

	dl.OnProgress(ctx, "Downloading %s from %s", objectName, bucketName)

	// Download the zipped dataset file from the GCP bucket, to a temporary file.
	err = func() (err error) {
		// Create a GCS client using Application Default Credentials.
		client, err := storage.NewClient(ctx)
		if err != nil {
			return errors.Wrapf(err, "creating GCS client")
		}
		defer func() {
			err = errors.CombineErrors(err, client.Close())
		}()

		// Get a handle to the GCS object.
		bucket := client.Bucket(bucketName)
		object := bucket.Object(objectName)

		// Get GCS object attributes to determine its size.
		attrs, err := object.Attrs(ctx)
		if err != nil {
			return errors.Wrapf(err, "getting attributes for %s/%s", bucketName, objectName)
		}

		// Create a temporary file to download the zip to.
		tempZipFile, err := os.Create(tempZipFileName)
		if err != nil {
			return errors.Wrapf(err, "creating temporary zip file %s", tempZipFileName)
		}
		defer func() {
			closeErr := errors.Wrapf(tempZipFile.Close(), "closing temporary zip file")
			err = errors.CombineErrors(err, closeErr)
		}()

		// Read the GCS object with progress tracking.
		reader, err := object.NewReader(ctx)
		if err != nil {
			return errors.Wrapf(err, "creating reader for %s/%s", bucketName, objectName)
		}
		defer func() {
			_ = reader.Close()
		}()

		// Use progressWriter to track download progress.
		writer := makeProgressWriter(tempZipFile, attrs.Size)
		writer.OnProgress = dl.OnDownloadProgress

		if _, err = io.Copy(&writer, reader); err != nil {
			return errors.Wrapf(err, "downloading to file %s", tempZipFileName)
		}

		return nil
	}()
	if err != nil {
		return err
	}

	// Open the temporary zipped file.
	dl.OnProgress(ctx, "Extracting files from zipped file %s", tempZipFileName)
	zipFile, err := os.Open(tempZipFileName)
	if err != nil {
		return errors.Wrapf(err, "opening downloaded zip file %s", tempZipFileName)
	}
	defer func() {
		err = errors.CombineErrors(err, zipFile.Close())
	}()

	// Get the file size for the zip reader.
	zipInfo, err := zipFile.Stat()
	if err != nil {
		return errors.Wrapf(err, "getting zip file stats")
	}

	// Create a zip reader from the file.
	zipReader, err := zip.NewReader(zipFile, zipInfo.Size())
	if err != nil {
		return errors.Wrapf(err, "creating zip reader")
	}

	// Extract files to the cache folder.
	for _, file := range zipReader.File {
		err = func() (err error) {
			// Open the file inside the zip archive.
			zippedFile, err := file.Open()
			if err != nil {
				return errors.Wrapf(err, "extracting file %s from zip file", file.Name)
			}
			defer func() {
				closeErr := errors.Wrapf(zippedFile.Close(), "closing file %s from zip file", file.Name)
				err = errors.CombineErrors(err, closeErr)
			}()

			// Create the output file.
			path := fmt.Sprintf("%s/%s", dl.CacheFolder, file.Name)
			dl.OnProgress(ctx, "Unzipping to %s", path)
			outputFile, err := os.Create(path)
			if err != nil {
				return errors.Wrapf(err, "creating output file %s", path)
			}
			defer func() {
				closeErr := errors.Wrapf(outputFile.Close(), "closing output file %s", path)
				err = errors.CombineErrors(err, closeErr)
			}()

			// Copy the contents of the zipped file to the output file.
			if _, err := io.Copy(outputFile, zippedFile); err != nil {
				return errors.Wrapf(err, "writing to output file %s", path)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

// loadFromDisk deserializes a dataset saved as a gob file.
func (dl *DatasetLoader) loadFromDisk(ctx context.Context) error {
	fileName := fmt.Sprintf("%s/%s.gob", dl.CacheFolder, dl.DatasetName)

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

// ensureCacheFolder ensures that the dl.CacheFolder field has been set to the
// directory that caches workload datasets. The default is set to
// ~/.cache/workload-datasets, if not already specified. The directory is
// created, it not already present.
func (dl *DatasetLoader) ensureCacheFolder() error {
	if dl.CacheFolder == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrapf(err, "getting home directory")
		}
		dl.CacheFolder = fmt.Sprintf("%s/.cache/workload-datasets", homeDir)
	}

	// Create the cache folder.
	if err := os.MkdirAll(dl.CacheFolder, os.ModePerm); err != nil {
		return errors.Wrapf(err, "creating cache folder")
	}

	return nil
}
