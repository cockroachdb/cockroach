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
	"database/sql/driver"
	"io"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

// This file is for interfaces only and should not contain any implementation
// code. All concrete implementations should be added to pkg/cloud/impl.

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
	Conf() cloudpb.ExternalStorage

	// ExternalIOConf should return the configuration containing several server
	// configured options pertaining to an ExternalStorage implementation.
	ExternalIOConf() base.ExternalIODirConfig

	// RequiresExternalIOAccounting should return true if the
	// ExternalStorage implementation should be accounted for when
	// calculating resource usage.
	RequiresExternalIOAccounting() bool

	// Settings should return the cluster settings used to configure the
	// ExternalStorage implementation.
	Settings() *cluster.Settings

	// ReadFile returns a Reader for requested name reading at opts.Offset, along
	// with the total size of the file (unless opts.NoFileSize is true).
	//
	// ErrFileDoesNotExist is raised if `basename` cannot be located in storage.
	// This can be leveraged for an existence check.
	ReadFile(ctx context.Context, basename string, opts ReadOptions) (_ ioctx.ReadCloserCtx, fileSize int64, _ error)

	// Writer returns a writer for the requested name.
	//
	// A Writer *must* be closed via either Close, and if closing returns a
	// non-nil error, that error should be handled or reported to the user -- an
	// implementation may buffer written data until Close and only then return
	// an error, or Write may return an opaque io.EOF with the underlying cause
	// returned by the subsequent Close().
	Writer(ctx context.Context, basename string) (io.WriteCloser, error)

	// List enumerates files within the supplied prefix, calling the passed
	// function with the name of each file found, relative to the external storage
	// destination's configured prefix. If the passed function returns a non-nil
	// error, iteration is stopped it is returned. If delimiter is non-empty
	// names which have the same prefix, prior to the delimiter, are grouped
	// into a single result which is that prefix. The order that results are
	// passed to the callback is undefined.
	List(ctx context.Context, prefix, delimiter string, fn ListingFn) error

	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error

	// Size returns the length of the named file in bytes.
	Size(ctx context.Context, basename string) (int64, error)
}

type ReadOptions struct {
	Offset int64

	// LengthHint is set when the caller will not read more than this many bytes
	// from the returned ReadCloserCtx. This allows backend implementation to make
	// more efficient limited requests.
	//
	// There is no guarantee that the reader won't produce more bytes; backends
	// are free to ignore this value.
	LengthHint int64

	// NoFileSize is set if the ReadFile caller is not interested in the fileSize
	// return value (potentially making the call more efficient).
	NoFileSize bool
}

// ListingFn describes functions passed to ExternalStorage.ListFiles.
type ListingFn func(string) error

// ExternalStorageFactory describes a factory function for ExternalStorage.
type ExternalStorageFactory func(ctx context.Context, dest cloudpb.ExternalStorage, opts ...ExternalStorageOption) (ExternalStorage, error)

// ExternalStorageFromURIFactory describes a factory function for ExternalStorage given a URI.
type ExternalStorageFromURIFactory func(ctx context.Context, uri string,
	user username.SQLUsername, opts ...ExternalStorageOption) (ExternalStorage, error)

// SQLConnI encapsulates the interfaces which will be implemented by the network
// backed SQLConn which is used to interact with the userfile tables.
type SQLConnI interface {
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...interface{}) error
}

// ErrFileDoesNotExist is a sentinel error for indicating that a specified
// bucket/object/key/file (depending on storage terminology) does not exist.
// This error is raised by the ReadFile method.
var ErrFileDoesNotExist = errors.New("external_storage: file doesn't exist")

// ErrListingUnsupported is a marker for indicating listing is unsupported.
var ErrListingUnsupported = errors.New("listing is not supported")

// ErrListingDone is a marker for indicating listing is done.
var ErrListingDone = errors.New("listing is done")

// RedactedParams is a helper for making a set of param names to redact in URIs.
func RedactedParams(strs ...string) map[string]struct{} {
	if len(strs) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(strs))
	for i := range strs {
		m[strs[i]] = struct{}{}
	}
	return m
}

// ExternalStorageURIContext contains arguments needed to parse external storage
// URIs.
type ExternalStorageURIContext struct {
	CurrentUser username.SQLUsername
}

// ExternalStorageURIParser functions parses a URL into a structured
// ExternalStorage configuration.
type ExternalStorageURIParser func(ExternalStorageURIContext, *url.URL) (cloudpb.ExternalStorage, error)

// ExternalStorageContext contains the dependencies passed to external storage
// implementations during creation.
type ExternalStorageContext struct {
	IOConf            base.ExternalIODirConfig
	Settings          *cluster.Settings
	BlobClientFactory blobs.BlobClientFactory
	DB                isql.DB
	Options           []ExternalStorageOption
	Limiters          Limiters
	MetricsRecorder   *Metrics
}

// ExternalStorageOptions holds dependencies and values that can be
// overridden by callers of an ExternalStorageFactory via a passed
// ExternalStorageOption.
type ExternalStorageOptions struct {
	ioAccountingInterceptor  ReadWriterInterceptor
	AzureStorageTestingKnobs base.ModuleTestingKnobs
}

// ExternalStorageConstructor is a function registered to create instances
// of a given external storage implementation.
type ExternalStorageConstructor func(
	context.Context, ExternalStorageContext, cloudpb.ExternalStorage,
) (ExternalStorage, error)

// NewExternalStorageAccessor creates an uninitialized ExternalStorageAccessor.
func NewExternalStorageAccessor() *ExternalStorageAccessor {
	return &ExternalStorageAccessor{ready: make(chan struct{})}
}

// ExternalStorageAccessor is a container for accessing the ExternalStorage
// factory methods once they are initialized. Attempts to access them prior to
// initialization will block.
type ExternalStorageAccessor struct {
	ready   chan struct{}
	factory ExternalStorageFactory
	byURI   ExternalStorageFromURIFactory
}

// Init initializes the ExternalStorageAccessor with the passed factories.
func (a *ExternalStorageAccessor) Init(
	factory ExternalStorageFactory, uriFactory ExternalStorageFromURIFactory,
) error {
	a.factory = factory
	a.byURI = uriFactory
	close(a.ready)
	return nil
}

// Open opens an ExternalStorage.
func (a *ExternalStorageAccessor) Open(
	ctx context.Context, dest cloudpb.ExternalStorage, opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	<-a.ready
	return a.factory(ctx, dest, opts...)
}

// OpenURL opens an ExternalStorage using a URI spec.
func (a *ExternalStorageAccessor) OpenURL(
	ctx context.Context, uri string, user username.SQLUsername, opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	<-a.ready
	return a.byURI(ctx, uri, user, opts...)
}
