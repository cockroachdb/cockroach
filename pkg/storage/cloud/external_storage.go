// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// This file is for interfaces only and should not contain any implementation
// code. All concrete implementations should be added to pkg/storage/cloudimpl.

// ExternalStorage provides an API to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
// Generally an implementation is instantiated pointing to some base path or
// prefix and then gets and puts files using the various methods to interact
// with individual files contained within that path or prefix. However,
// implementations must also allow callers to provide the full path to a given
// file as the "base" path, and then read or write it with the methods below by
// simply passing an empty filename. Implementations that use stdlib's
// `filepath.Join` to concatenate their base path with the provided filename
// will find its semantics well suited to this -- it elides empty components and
// does not append surplus slashes.
type ExternalStorage interface {
	io.Closer

	// Conf should return the serializable configuration required to reconstruct
	// this ExternalStorage implementation.
	Conf() roachpb.ExternalStorage

	// ReadFile should return a Reader for requested name.
	// ErrFileDoesNotExist is raised if `basename` cannot be located in storage.
	// This can be leveraged for an existence check.
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)

	// WriteFile should write the content to requested name.
	WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error

	// ListFiles returns files that match a globs-style pattern. The returned
	// results are usually relative to the base path, meaning an ExternalStorage
	// instance can be initialized with some base path, used to query for files,
	// then pass those results to its other methods.
	//
	// As a special-case, if the passed patternSuffix is empty, the base path used
	// to initialize the storage connection is treated as a pattern. In this case,
	// as the connection is not really reusable for interacting with other files
	// and there is no clear definition of what it would mean to be relative to
	// that, the results are fully-qualified absolute URIs. The base URI is *only*
	// allowed to contain globs-patterns when the explicit patternSuffix is "".
	ListFiles(ctx context.Context, patternSuffix string) ([]string, error)

	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error

	// Size returns the length of the named file in bytes.
	Size(ctx context.Context, basename string) (int64, error)
}

// ExternalStorageFactory describes a factory function for ExternalStorage.
type ExternalStorageFactory func(ctx context.Context, dest roachpb.ExternalStorage) (ExternalStorage, error)

// ExternalStorageFromURIFactory describes a factory function for ExternalStorage given a URI.
type ExternalStorageFromURIFactory func(ctx context.Context, uri string,
	user string) (ExternalStorage, error)
