// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
)

var client *storage.Client

func initGCS() error {
	var err error
	ctx := context.Background()
	client, err = storage.NewClient(ctx)
	return err
}

func createWriter(path string) (io.WriteCloser, error) {
	if strings.HasPrefix(path, "gs://") {
		return createGCSWriter(path)
	} else {
		return os.Create(path)
	}
}

func createGCSWriter(path string) (io.WriteCloser, error) {
	if client == nil {
		if err := initGCS(); err != nil {
			return nil, err
		}
	}
	bucket, obj, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}
	return bucket.Object(obj).NewWriter(context.Background()), nil
}

func parseGCSPath(path string) (*storage.BucketHandle, string, error) {
	parts := strings.SplitN(path, "/", 4)
	if len(parts) != 4 {
		return nil, "", fmt.Errorf("invalid GCS path")
	}
	return client.Bucket(parts[2]), parts[3], nil
}

func createReader(path string) (io.ReadCloser, error) {
	if strings.HasPrefix(path, "gs://") {
		return createGCSReader(path)
	} else {
		return os.Open(path)
	}
}

func createGCSReader(path string) (io.ReadCloser, error) {
	if client == nil {
		if err := initGCS(); err != nil {
			return nil, err
		}
	}
	bucket, obj, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}
	return bucket.Object(obj).NewReader(context.Background())
}

func joinPath(elem ...string) string {
	if len(elem) == 0 {
		return ""
	}
	if strings.HasPrefix(elem[0], "gs://") {
		elem[0] = strings.TrimPrefix(elem[0], "gs://")
		return "gs://" + filepath.Join(elem...)
	}
	return filepath.Join(elem...)
}

func publishDirectory(localSrcDir, dstDir string) error {
	files, readErr := os.ReadDir(localSrcDir)
	if readErr != nil {
		return readErr
	}
	var wg sync.WaitGroup
	errorsFound := false
	wg.Add(len(files))
	for index := range files {
		go func(index int) {
			defer wg.Done()
			path := files[index]
			reader, wErr := createReader(filepath.Join(localSrcDir, path.Name()))
			if wErr != nil {
				l.Errorf("Failed to create reader for %s - %v", path.Name(), wErr)
				errorsFound = true
			}
			writer, wErr := createWriter(joinPath(dstDir, path.Name()))
			if wErr != nil {
				l.Errorf("Failed to create writer for %s - %v", path.Name(), wErr)
				errorsFound = true
			}
			_, cErr := io.Copy(writer, reader)
			if cErr != nil {
				l.Errorf("Failed to copy %s - %v", path.Name(), cErr)
				errorsFound = true
			}
			cErr = writer.Close()
			if cErr != nil {
				l.Errorf("Failed to close writer for %s - %v", path.Name(), cErr)
				errorsFound = true
			}
			cErr = reader.Close()
			if cErr != nil {
				l.Errorf("Failed to close reader for %s - %v", path.Name(), cErr)
				errorsFound = true
			}
		}(index)
	}
	wg.Wait()
	if errorsFound {
		return fmt.Errorf("failed to publish directory %s", localSrcDir)
	}
	return nil
}

func isNotFoundError(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, os.ErrNotExist)
}
