// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memstorage

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/errors"
)

type memStorage struct {
	files    map[string][]byte
	settings *cluster.Settings
}

// This test uses a singleton instance that is reused.
var memStorageSingleton = &memStorage{}

func makeMemStorage(
	ctx context.Context, args cloud.ExternalStorageContext, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	memStorageSingleton.settings = args.Settings
	if memStorageSingleton.files == nil {
		memStorageSingleton.files = make(map[string][]byte)
	}
	return memStorageSingleton, nil
}

// Conf implements cloud.ExternalStorage.
func (s *memStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider: roachpb.ExternalStorageProvider_mem,
	}
}

// Conf implements cloud.ExternalStorage.
func (s *memStorage) ExternalIOConf() base.ExternalIODirConfig {
	panic("not implemented")
}

// Settings implements cloud.ExternalStorage.
func (s *memStorage) Settings() *cluster.Settings {
	return s.settings
}

type testWriter struct {
	basename string
	f        map[string][]byte
}

func (w testWriter) Write(p []byte) (n int, err error) {
	w.f[w.basename] = p
	return len(p), nil
}

func (w testWriter) Close() error { return nil }

func (s *memStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	w := testWriter{
		basename: basename,
		f:        s.files,
	}
	return w, nil
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (s *memStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	time.Sleep(100 * time.Millisecond)
	r, _, err := s.ReadFileAt(ctx, basename, 0)
	return r, err
}

// ReadFileAt opens a reader at the requested offset.
func (s *memStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (io.ReadCloser, int64, error) {
	size, err := s.Size(ctx, basename)
	if err != nil {
		return nil, 0, err
	}
	return &cloud.ResumingReader{
		Ctx: ctx,
		Opener: func(ctx context.Context, pos int64) (io.ReadCloser, error) {
			return s.openReaderAt(basename, pos)
		},
		Pos: offset,
	}, size, nil
}

func (s *memStorage) openReaderAt(basename string, pos int64) (io.ReadCloser, error) {
	data, ok := s.files[basename]
	if !ok {
		existingFiles := make([]string, len(s.files))
		for fileName := range s.files {
			existingFiles = append(existingFiles, fileName)
		}
		return nil, errors.Wrapf(cloud.ErrFileDoesNotExist, "could not find %s %v", basename, existingFiles)
	}
	if pos != 0 {
		data = data[pos:]
	}

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (s *memStorage) ListFiles(ctx context.Context, pattern string) ([]string, error) {
	panic("not implemented")
}

func (s *memStorage) Delete(ctx context.Context, basename string) error {
	s.files[basename] = nil
	return nil
}

func (s *memStorage) Size(ctx context.Context, basename string) (int64, error) {
	return int64(len(s.files[basename])), nil
}

func (s *memStorage) Close() error {
	return nil
}

func parseTestURL(_ cloud.ExternalStorageURIContext, _ *url.URL) (roachpb.ExternalStorage, error) {
	return roachpb.ExternalStorage{Provider: roachpb.ExternalStorageProvider_mem}, nil
}

func init() {
	if !build.IsRelease() {
		cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_mem,
			parseTestURL, makeMemStorage, cloud.RedactedParams(), "mem-test")
	}
}
