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

package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/git"
	"github.com/bufbuild/buf/private/pkg/httpauth"
	"github.com/bufbuild/buf/private/pkg/ioextended"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/osextended"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storagearchive"
	"github.com/bufbuild/buf/private/pkg/storage/storagemem"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type reader struct {
	logger            *zap.Logger
	storageosProvider storageos.Provider

	localEnabled bool
	stdioEnabled bool

	httpEnabled       bool
	httpClient        *http.Client
	httpAuthenticator httpauth.Authenticator

	gitEnabled bool
	gitCloner  git.Cloner

	moduleEnabled  bool
	moduleReader   bufmodule.ModuleReader
	moduleResolver bufmodule.ModuleResolver
}

func newReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	options ...ReaderOption,
) *reader {
	reader := &reader{
		logger:            logger,
		storageosProvider: storageosProvider,
	}
	for _, option := range options {
		option(reader)
	}
	return reader
}

func (r *reader) GetFile(
	ctx context.Context,
	container app.EnvStdinContainer,
	fileRef FileRef,
	options ...GetFileOption,
) (io.ReadCloser, error) {
	getFileOptions := newGetFileOptions()
	for _, option := range options {
		option(getFileOptions)
	}
	switch t := fileRef.(type) {
	case SingleRef:
		return r.getSingle(
			ctx,
			container,
			t,
			getFileOptions.keepFileCompression,
		)
	case ArchiveRef:
		return r.getArchiveFile(
			ctx,
			container,
			t,
			getFileOptions.keepFileCompression,
		)
	default:
		return nil, fmt.Errorf("unknown FileRef type: %T", fileRef)
	}
}

func (r *reader) GetBucket(
	ctx context.Context,
	container app.EnvStdinContainer,
	bucketRef BucketRef,
	options ...GetBucketOption,
) (ReadBucketCloser, error) {
	getBucketOptions := newGetBucketOptions()
	for _, option := range options {
		option(getBucketOptions)
	}
	switch t := bucketRef.(type) {
	case ArchiveRef:
		return r.getArchiveBucket(
			ctx,
			container,
			t,
			getBucketOptions.terminateFileNames,
		)
	case DirRef:
		return r.getDirBucket(
			ctx,
			container,
			t,
			getBucketOptions.terminateFileNames,
		)
	case GitRef:
		return r.getGitBucket(
			ctx,
			container,
			t,
			getBucketOptions.terminateFileNames,
		)
	default:
		return nil, fmt.Errorf("unknown BucketRef type: %T", bucketRef)
	}
}

func (r *reader) GetModule(
	ctx context.Context,
	container app.EnvStdinContainer,
	moduleRef ModuleRef,
	_ ...GetModuleOption,
) (bufmodule.Module, error) {
	switch t := moduleRef.(type) {
	case ModuleRef:
		return r.getModule(
			ctx,
			container,
			t,
		)
	default:
		return nil, fmt.Errorf("unknown ModuleRef type: %T", moduleRef)
	}
}

func (r *reader) getSingle(
	ctx context.Context,
	container app.EnvStdinContainer,
	singleRef SingleRef,
	keepFileCompression bool,
) (io.ReadCloser, error) {
	readCloser, _, err := r.getFileReadCloserAndSize(ctx, container, singleRef, keepFileCompression)
	return readCloser, err
}

func (r *reader) getArchiveFile(
	ctx context.Context,
	container app.EnvStdinContainer,
	archiveRef ArchiveRef,
	keepFileCompression bool,
) (io.ReadCloser, error) {
	readCloser, _, err := r.getFileReadCloserAndSize(ctx, container, archiveRef, keepFileCompression)
	return readCloser, err
}

func (r *reader) getArchiveBucket(
	ctx context.Context,
	container app.EnvStdinContainer,
	archiveRef ArchiveRef,
	terminateFileNames []string,
) (_ ReadBucketCloser, retErr error) {
	subDirPath, err := normalpath.NormalizeAndValidate(archiveRef.SubDirPath())
	if err != nil {
		return nil, err
	}
	readCloser, size, err := r.getFileReadCloserAndSize(ctx, container, archiveRef, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = multierr.Append(retErr, readCloser.Close())
	}()
	readBucketBuilder := storagemem.NewReadBucketBuilder()
	ctx, span := trace.StartSpan(ctx, "unarchive")
	defer span.End()
	switch archiveType := archiveRef.ArchiveType(); archiveType {
	case ArchiveTypeTar:
		if err := storagearchive.Untar(
			ctx,
			readCloser,
			readBucketBuilder,
			nil,
			archiveRef.StripComponents(),
		); err != nil {
			return nil, err
		}
	case ArchiveTypeZip:
		var readerAt io.ReaderAt
		if size < 0 {
			data, err := io.ReadAll(readCloser)
			if err != nil {
				return
			}
			readerAt = bytes.NewReader(data)
			size = int64(len(data))
		} else {
			readerAt, err = ioextended.ReaderAtForReader(readCloser)
			if err != nil {
				return nil, err
			}
		}
		if err := storagearchive.Unzip(
			ctx,
			readerAt,
			size,
			readBucketBuilder,
			nil,
			archiveRef.StripComponents(),
		); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown ArchiveType: %v", archiveType)
	}
	readBucket, err := readBucketBuilder.ToReadBucket()
	if err != nil {
		return nil, err
	}
	terminateFileDirectoryPath, err := findTerminateFileDirectoryPathFromBucket(ctx, readBucket, subDirPath, terminateFileNames)
	if err != nil {
		return nil, err
	}
	if terminateFileDirectoryPath != "" {
		relativeSubDirPath, err := normalpath.Rel(terminateFileDirectoryPath, subDirPath)
		if err != nil {
			return nil, err
		}
		return newReadBucketCloser(
			storage.NopReadBucketCloser(storage.MapReadBucket(readBucket, storage.MapOnPrefix(terminateFileDirectoryPath))),
			terminateFileDirectoryPath,
			relativeSubDirPath,
		)
	}
	if subDirPath != "." {
		readBucket = storage.MapReadBucket(readBucket, storage.MapOnPrefix(subDirPath))
	}
	return newReadBucketCloser(
		storage.NopReadBucketCloser(readBucket),
		"",
		"",
	)
}

func (r *reader) getDirBucket(
	ctx context.Context,
	container app.EnvStdinContainer,
	dirRef DirRef,
	terminateFileNames []string,
) (ReadBucketCloser, error) {
	if !r.localEnabled {
		return nil, NewReadLocalDisabledError()
	}
	terminateFileDirectoryAbsPath, err := findTerminateFileDirectoryPathFromOS(dirRef.Path(), terminateFileNames)
	if err != nil {
		return nil, err
	}
	if terminateFileDirectoryAbsPath != "" {
		// If the terminate file exists, we need to determine the relative path from the
		// terminateFileDirectoryAbsPath to the target DirRef.Path.
		wd, err := osextended.Getwd()
		if err != nil {
			return nil, err
		}
		terminateFileRelativePath, err := normalpath.Rel(wd, terminateFileDirectoryAbsPath)
		if err != nil {
			return nil, err
		}
		dirRefAbsPath, err := normalpath.NormalizeAndAbsolute(dirRef.Path())
		if err != nil {
			return nil, err
		}
		dirRefRelativePath, err := normalpath.Rel(terminateFileDirectoryAbsPath, dirRefAbsPath)
		if err != nil {
			return nil, err
		}
		// It should be impossible for the dirRefRelativePath to be outside of the context
		// diretory, but we validate just to make sure.
		dirRefRelativePath, err = normalpath.NormalizeAndValidate(dirRefRelativePath)
		if err != nil {
			return nil, err
		}
		rootPath := terminateFileRelativePath
		if filepath.IsAbs(dirRef.Path()) {
			// If the input was provided as an absolute path,
			// we preserve it by initializing the workspace
			// bucket with an absolute path.
			rootPath = terminateFileDirectoryAbsPath
		}
		readWriteBucket, err := r.storageosProvider.NewReadWriteBucket(
			rootPath,
			storageos.ReadWriteBucketWithSymlinksIfSupported(),
		)
		if err != nil {
			return nil, err
		}
		// Verify that the subDirPath exists too.
		if _, err := r.storageosProvider.NewReadWriteBucket(
			normalpath.Join(rootPath, dirRefRelativePath),
			storageos.ReadWriteBucketWithSymlinksIfSupported(),
		); err != nil {
			return nil, err
		}
		return newReadWriteBucketCloser(
			storage.NopReadWriteBucketCloser(readWriteBucket),
			rootPath,
			dirRefRelativePath,
		)
	}
	readWriteBucket, err := r.storageosProvider.NewReadWriteBucket(
		dirRef.Path(),
		storageos.ReadWriteBucketWithSymlinksIfSupported(),
	)
	if err != nil {
		return nil, err
	}
	return newReadWriteBucketCloser(
		storage.NopReadWriteBucketCloser(readWriteBucket),
		"",
		"",
	)
}

func (r *reader) getGitBucket(
	ctx context.Context,
	container app.EnvStdinContainer,
	gitRef GitRef,
	terminateFileNames []string,
) (_ ReadBucketCloser, retErr error) {
	if !r.gitEnabled {
		return nil, NewReadGitDisabledError()
	}
	if r.gitCloner == nil {
		return nil, errors.New("git cloner is nil")
	}
	subDirPath, err := normalpath.NormalizeAndValidate(gitRef.SubDirPath())
	if err != nil {
		return nil, err
	}
	gitURL, err := getGitURL(gitRef)
	if err != nil {
		return nil, err
	}
	readBucketBuilder := storagemem.NewReadBucketBuilder()
	if err := r.gitCloner.CloneToBucket(
		ctx,
		container,
		gitURL,
		gitRef.Depth(),
		readBucketBuilder,
		git.CloneToBucketOptions{
			Name:              gitRef.GitName(),
			RecurseSubmodules: gitRef.RecurseSubmodules(),
		},
	); err != nil {
		return nil, fmt.Errorf("could not clone %s: %v", gitURL, err)
	}
	readBucket, err := readBucketBuilder.ToReadBucket()
	if err != nil {
		return nil, err
	}
	terminateFileDirectoryPath, err := findTerminateFileDirectoryPathFromBucket(ctx, readBucket, subDirPath, terminateFileNames)
	if err != nil {
		return nil, err
	}
	if terminateFileDirectoryPath != "" {
		relativeSubDirPath, err := normalpath.Rel(terminateFileDirectoryPath, subDirPath)
		if err != nil {
			return nil, err
		}
		return newReadBucketCloser(
			storage.NopReadBucketCloser(storage.MapReadBucket(readBucket, storage.MapOnPrefix(terminateFileDirectoryPath))),
			terminateFileDirectoryPath,
			relativeSubDirPath,
		)
	}
	if subDirPath != "." {
		readBucket = storage.MapReadBucket(readBucket, storage.MapOnPrefix(subDirPath))
	}
	return newReadBucketCloser(
		storage.NopReadBucketCloser(readBucket),
		"",
		"",
	)
}

func (r *reader) getModule(
	ctx context.Context,
	container app.EnvStdinContainer,
	moduleRef ModuleRef,
) (bufmodule.Module, error) {
	if !r.moduleEnabled {
		return nil, NewReadModuleDisabledError()
	}
	if r.moduleReader == nil {
		return nil, errors.New("module reader is nil")
	}
	if r.moduleResolver == nil {
		return nil, errors.New("module resolver is nil")
	}
	modulePin, err := r.moduleResolver.GetModulePin(ctx, moduleRef.ModuleReference())
	if err != nil {
		return nil, err
	}
	return r.moduleReader.GetModule(ctx, modulePin)
}

func (r *reader) getFileReadCloserAndSize(
	ctx context.Context,
	container app.EnvStdinContainer,
	fileRef FileRef,
	keepFileCompression bool,
) (_ io.ReadCloser, _ int64, retErr error) {
	readCloser, size, err := r.getFileReadCloserAndSizePotentiallyCompressed(ctx, container, fileRef)
	if err != nil {
		return nil, -1, err
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, readCloser.Close())
		}
	}()
	if keepFileCompression {
		return readCloser, size, nil
	}
	switch compressionType := fileRef.CompressionType(); compressionType {
	case CompressionTypeNone:
		return readCloser, size, nil
	case CompressionTypeGzip:
		gzipReadCloser, err := pgzip.NewReader(readCloser)
		if err != nil {
			return nil, -1, err
		}
		return ioextended.CompositeReadCloser(
			gzipReadCloser,
			ioextended.ChainCloser(
				gzipReadCloser,
				readCloser,
			),
		), -1, nil
	case CompressionTypeZstd:
		zstdDecoder, err := zstd.NewReader(readCloser)
		if err != nil {
			return nil, -1, err
		}
		zstdReadCloser := zstdDecoder.IOReadCloser()
		return ioextended.CompositeReadCloser(
			zstdReadCloser,
			ioextended.ChainCloser(
				zstdReadCloser,
				readCloser,
			),
		), -1, nil
	default:
		return nil, -1, fmt.Errorf("unknown CompressionType: %v", compressionType)
	}
}

// returns -1 if size unknown
func (r *reader) getFileReadCloserAndSizePotentiallyCompressed(
	ctx context.Context,
	container app.EnvStdinContainer,
	fileRef FileRef,
) (io.ReadCloser, int64, error) {
	switch fileScheme := fileRef.FileScheme(); fileScheme {
	case FileSchemeHTTP:
		if !r.httpEnabled {
			return nil, -1, NewReadHTTPDisabledError()
		}
		return r.getFileReadCloserAndSizePotentiallyCompressedHTTP(ctx, container, "http://"+fileRef.Path())
	case FileSchemeHTTPS:
		if !r.httpEnabled {
			return nil, -1, NewReadHTTPDisabledError()
		}
		return r.getFileReadCloserAndSizePotentiallyCompressedHTTP(ctx, container, "https://"+fileRef.Path())
	case FileSchemeLocal:
		if !r.localEnabled {
			return nil, -1, NewReadLocalDisabledError()
		}
		file, err := os.Open(fileRef.Path())
		if err != nil {
			return nil, -1, err
		}
		fileInfo, err := file.Stat()
		if err != nil {
			return nil, -1, err
		}
		return file, fileInfo.Size(), nil
	case FileSchemeStdio, FileSchemeStdin:
		if !r.stdioEnabled {
			return nil, -1, NewReadStdioDisabledError()
		}
		return io.NopCloser(container.Stdin()), -1, nil
	case FileSchemeStdout:
		return nil, -1, errors.New("cannot read from stdout")
	case FileSchemeNull:
		return ioextended.DiscardReadCloser, 0, nil
	default:
		return nil, -1, fmt.Errorf("unknown FileScheme: %v", fileScheme)
	}
}

// the httpPath must have the scheme attached
func (r *reader) getFileReadCloserAndSizePotentiallyCompressedHTTP(
	ctx context.Context,
	container app.EnvStdinContainer,
	httpPath string,
) (io.ReadCloser, int64, error) {
	if r.httpClient == nil {
		return nil, 0, errors.New("http client is nil")
	}
	if r.httpAuthenticator == nil {
		return nil, 0, errors.New("http authenticator is nil")
	}
	request, err := http.NewRequestWithContext(ctx, "GET", httpPath, nil)
	if err != nil {
		return nil, -1, err
	}
	if _, err := r.httpAuthenticator.SetAuth(container, request); err != nil {
		return nil, -1, err
	}
	response, err := r.httpClient.Do(request)
	if err != nil {
		return nil, -1, err
	}
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("got HTTP status code %d", response.StatusCode)
		if response.Body != nil {
			return nil, -1, multierr.Append(err, response.Body.Close())
		}
		return nil, -1, err
	}
	// ContentLength is -1 if unknown, which is what we want
	return response.Body, response.ContentLength, nil
}

func getGitURL(gitRef GitRef) (string, error) {
	switch gitScheme := gitRef.GitScheme(); gitScheme {
	case GitSchemeHTTP:
		return "http://" + gitRef.Path(), nil
	case GitSchemeHTTPS:
		return "https://" + gitRef.Path(), nil
	case GitSchemeSSH:
		return "ssh://" + gitRef.Path(), nil
	case GitSchemeGit:
		return "git://" + gitRef.Path(), nil
	case GitSchemeLocal:
		absPath, err := filepath.Abs(normalpath.Unnormalize(gitRef.Path()))
		if err != nil {
			return "", err
		}
		return "file://" + absPath, nil
	default:
		return "", fmt.Errorf("unknown GitScheme: %v", gitScheme)
	}
}

// findTerminateFileDirectoryPathFromBucket returns the directory path that contains
// one of the terminateFileNames, starting with the subDirPath and ascending until the root
// of the bucket.
func findTerminateFileDirectoryPathFromBucket(
	ctx context.Context,
	readBucket storage.ReadBucket,
	subDirPath string,
	terminateFileNames []string,
) (string, error) {
	if len(terminateFileNames) == 0 {
		return "", nil
	}
	terminateFileDirectoryPath := normalpath.Normalize(subDirPath)
	for {
		fullTerminateFileNames := make([]string, len(terminateFileNames))
		for i, terminateFileName := range terminateFileNames {
			fullTerminateFileNames[i] = normalpath.Join(terminateFileDirectoryPath, terminateFileName)
		}
		exists, err := anyExistsBucket(ctx, readBucket, fullTerminateFileNames)
		if err != nil {
			return "", err
		}
		if exists {
			return terminateFileDirectoryPath, nil
		}
		parent := normalpath.Dir(terminateFileDirectoryPath)
		if parent == terminateFileDirectoryPath {
			return "", nil
		}
		terminateFileDirectoryPath = parent
	}
}

func anyExistsBucket(
	ctx context.Context,
	readBucket storage.ReadBucket,
	paths []string,
) (bool, error) {
	for _, path := range paths {
		exists, err := storage.Exists(ctx, readBucket, path)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}

// findTerminateFileDirectoryPathFromOS returns the directory that contains
// the terminateFileName, starting with the subDirPath and ascending until
// the root of the local filesysem.
func findTerminateFileDirectoryPathFromOS(subDirPath string, terminateFileNames []string) (string, error) {
	if len(terminateFileNames) == 0 {
		return "", nil
	}
	fileInfo, err := os.Stat(normalpath.Unnormalize(subDirPath))
	if err != nil {
		if os.IsNotExist(err) {
			return "", storage.NewErrNotExist(subDirPath)
		}
		return "", err
	}
	if !fileInfo.IsDir() {
		return "", normalpath.NewError(normalpath.Unnormalize(subDirPath), errors.New("not a directory"))
	}
	terminateFileDirectoryPath, err := normalpath.NormalizeAndAbsolute(subDirPath)
	if err != nil {
		return "", err
	}
	for {
		fullTerminateFileNames := make([]string, len(terminateFileNames))
		for i, terminateFileName := range terminateFileNames {
			fullTerminateFileNames[i] = normalpath.Unnormalize(normalpath.Join(terminateFileDirectoryPath, terminateFileName))
		}
		exists, err := anyExistsOS(fullTerminateFileNames)
		if err != nil {
			return "", err
		}
		if exists {
			return terminateFileDirectoryPath, nil
		}
		parent := normalpath.Dir(terminateFileDirectoryPath)
		if parent == terminateFileDirectoryPath {
			return "", nil
		}
		terminateFileDirectoryPath = parent
	}
}

func anyExistsOS(paths []string) (bool, error) {
	for _, path := range paths {
		fileInfo, err := os.Stat(path)
		if err != nil && !os.IsNotExist(err) {
			return false, err
		}
		if fileInfo != nil && !fileInfo.IsDir() {
			return true, nil
		}
	}
	return false, nil
}

type getFileOptions struct {
	keepFileCompression bool
}

func newGetFileOptions() *getFileOptions {
	return &getFileOptions{}
}

type getBucketOptions struct {
	terminateFileNames []string
}

func newGetBucketOptions() *getBucketOptions {
	return &getBucketOptions{}
}

type getModuleOptions struct{}
