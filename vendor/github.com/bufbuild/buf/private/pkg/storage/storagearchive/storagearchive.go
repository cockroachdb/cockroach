// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package storagearchive implements archive utilities.
package storagearchive

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageutil"
	"github.com/klauspost/compress/zip"
	"go.uber.org/multierr"
)

// Tar tars the given bucket to the writer.
//
// Only regular files are added to the writer.
// All files are written as 0644.
func Tar(
	ctx context.Context,
	readBucket storage.ReadBucket,
	writer io.Writer,
) (retErr error) {
	tarWriter := tar.NewWriter(writer)
	defer func() {
		retErr = multierr.Append(retErr, tarWriter.Close())
	}()
	return storage.WalkReadObjects(
		ctx,
		readBucket,
		"",
		func(readObject storage.ReadObject) error {
			data, err := io.ReadAll(readObject)
			if err != nil {
				return err
			}
			if err := tarWriter.WriteHeader(
				&tar.Header{
					Typeflag: tar.TypeReg,
					Name:     readObject.Path(),
					Size:     int64(len(data)),
					// If we ever use this outside of testing, we will want to do something about this
					Mode: 0644,
				},
			); err != nil {
				return err
			}
			_, err = tarWriter.Write(data)
			return err
		},
	)
}

// Untar untars the given tar archive from the reader into the bucket.
//
// Only regular files are added to the bucket.
//
// Paths from the tar archive will be mapped before adding to the bucket.
// Mapper can be nil.
// StripComponents happens before the mapper.
func Untar(
	ctx context.Context,
	reader io.Reader,
	writeBucket storage.WriteBucket,
	mapper storage.Mapper,
	stripComponentCount uint32,
) error {
	tarReader := tar.NewReader(reader)
	walkChecker := storageutil.NewWalkChecker()
	for tarHeader, err := tarReader.Next(); err != io.EOF; tarHeader, err = tarReader.Next() {
		if err != nil {
			return err
		}
		if err := walkChecker.Check(ctx); err != nil {
			return err
		}
		path, ok, err := unmapArchivePath(tarHeader.Name, mapper, stripComponentCount)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if tarHeader.FileInfo().Mode().IsRegular() {
			if tarHeader.Size < 0 {
				return fmt.Errorf("invalid size for tar file %s: %d", tarHeader.Name, tarHeader.Size)
			}
			if err := storage.CopyReader(ctx, writeBucket, tarReader, path); err != nil {
				return err
			}
		}
	}
	return nil
}

// Zip zips the given bucket to the writer.
//
// Only regular files are added to the writer.
func Zip(
	ctx context.Context,
	readBucket storage.ReadBucket,
	writer io.Writer,
	compressed bool,
) (retErr error) {
	zipWriter := zip.NewWriter(writer)
	defer func() {
		retErr = multierr.Append(retErr, zipWriter.Close())
	}()
	return storage.WalkReadObjects(
		ctx,
		readBucket,
		"",
		func(readObject storage.ReadObject) error {
			method := zip.Store
			if compressed {
				method = zip.Deflate
			}
			header := &zip.FileHeader{
				Name:   readObject.Path(),
				Method: method,
			}
			writer, err := zipWriter.CreateHeader(header)
			if err != nil {
				return err
			}
			_, err = io.Copy(writer, readObject)
			return err
		},
	)
}

// Unzip unzips the given zip archive from the reader into the bucket.
//
// Only regular files are added to the bucket.
//
// Paths from the zip archive will be mapped before adding to the bucket.
// Mapper can be nil.
// StripComponents happens before the mapper.
func Unzip(
	ctx context.Context,
	readerAt io.ReaderAt,
	size int64,
	writeBucket storage.WriteBucket,
	mapper storage.Mapper,
	stripComponentCount uint32,
) error {
	if size < 0 {
		return fmt.Errorf("unknown size to unzip: %d", int(size))
	}
	if size == 0 {
		return nil
	}
	zipReader, err := zip.NewReader(readerAt, size)
	if err != nil {
		return err
	}
	walkChecker := storageutil.NewWalkChecker()
	// reads can be done concurrently in the future
	for _, zipFile := range zipReader.File {
		if err := walkChecker.Check(ctx); err != nil {
			return err
		}
		path, ok, err := unmapArchivePath(zipFile.Name, mapper, stripComponentCount)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if zipFile.FileInfo().Mode().IsRegular() {
			if err := copyZipFile(ctx, writeBucket, zipFile, path); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyZipFile(
	ctx context.Context,
	writeBucket storage.WriteBucket,
	zipFile *zip.File,
	path string,
) (retErr error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, readCloser.Close())
	}()
	return storage.CopyReader(ctx, writeBucket, readCloser, path)
}

func unmapArchivePath(
	archivePath string,
	mapper storage.Mapper,
	stripComponentCount uint32,
) (string, bool, error) {
	if archivePath == "" {
		return "", false, errors.New("empty archive file name")
	}
	fullPath, err := normalpath.NormalizeAndValidate(archivePath)
	if err != nil {
		return "", false, err
	}
	if fullPath == "." {
		return "", false, nil
	}
	fullPath, ok := normalpath.StripComponents(fullPath, stripComponentCount)
	if !ok {
		return "", false, nil
	}
	if mapper != nil {
		return mapper.UnmapFullPath(fullPath)
	}
	return fullPath, true, nil
}
