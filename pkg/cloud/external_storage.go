// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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

	// Delete removes the named file from the store. If the file does not exist,
	// Delete returns nil.
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

// ExternalStorageFromURIFactory describes a factory function for ExternalStorage given a URI.
type EarlyBootExternalStorageFromURIFactory func(ctx context.Context, uri string, opts ...ExternalStorageOption) (ExternalStorage, error)

// SQLConnI encapsulates the interfaces which will be implemented by the network
// backed SQLConn which is used to interact with the userfile tables.
type SQLConnI interface {
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...interface{}) error
}

// ErrFileDoesNotExist is a sentinel error for indicating that a specified
// bucket/object/key/file (depending on storage terminology) does not exist.
// This error is raised by the ReadFile method.
var ErrFileDoesNotExist = errors.New("external_storage: file does not exist")

// WrapErrFileDoesNotExist wraps an error with ErrFileDoesNotExist.
func WrapErrFileDoesNotExist(err error, msg string) error {
	//nolint:errwrap
	return errors.Wrapf(ErrFileDoesNotExist, "%s: %s", err.Error(), msg)
}

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
type EarlyBootExternalStorageURIParser func(*url.URL) (cloudpb.ExternalStorage, error)

// ExternalStorageContext contains the dependencies passed to external storage
// implementations during creation.
type ExternalStorageContext struct {
	EarlyBootExternalStorageContext

	BlobClientFactory blobs.BlobClientFactory
	DB                isql.DB
}

// ExternalStorageContext contains the dependencies passed to external storage
// implementations during creation.
type EarlyBootExternalStorageContext struct {
	IOConf base.ExternalIODirConfig
	// TODO(ssd): We provide settings to early-boot external
	// storage, but I am rather uncertain it is a good idea. We
	// may be using this provider before we've even read our
	// cached settings.
	Settings        *cluster.Settings
	Options         []ExternalStorageOption
	Limiters        Limiters
	MetricsRecorder *Metrics
}

// ExternalStorageOptions rolls up the Options into a struct.
func (e *EarlyBootExternalStorageContext) ExternalStorageOptions() ExternalStorageOptions {
	var options ExternalStorageOptions
	for _, option := range e.Options {
		option(&options)
	}
	return options
}

// ExternalStorageOptions holds dependencies and values that can be
// overridden by callers of an ExternalStorageFactory via a passed
// ExternalStorageOption.
type ExternalStorageOptions struct {
	ioAccountingInterceptor  ReadWriterInterceptor
	AzureStorageTestingKnobs base.ModuleTestingKnobs
	ClientName               string
}

// ExternalStorageConstructor is a function registered to create instances
// of a given external storage implementation.
type ExternalStorageConstructor func(
	context.Context, ExternalStorageContext, cloudpb.ExternalStorage,
) (ExternalStorage, error)

type EarlyBootExternalStorageConstructor func(
	context.Context, EarlyBootExternalStorageContext, cloudpb.ExternalStorage,
) (ExternalStorage, error)

// NewEarlyBootExternalStorageAccessor creates an
// EarlyBootExternalStorageAccessor
func NewEarlyBootExternalStorageAccessor(
	st *cluster.Settings, conf base.ExternalIODirConfig, lookup *cidr.Lookup,
) *EarlyBootExternalStorageAccessor {
	return &EarlyBootExternalStorageAccessor{
		conf:     conf,
		settings: st,
		limiters: MakeLimiters(&st.SV),
		metrics:  MakeMetrics(lookup),
	}
}

// EarlyBootExternalStorageAccessor provides access to external
// storage providers that can be accessed from the very start of node
// startup. Such providers do not depend on SQL access, node IDs, or
// other dependencies that are only available later in the startup
// process.
type EarlyBootExternalStorageAccessor struct {
	conf     base.ExternalIODirConfig
	settings *cluster.Settings
	limiters Limiters
	metrics  metric.Struct
}

// Metrics returns the metrics struct so that it can be consumed by
// the external storage builder.
func (a *EarlyBootExternalStorageAccessor) Metrics() metric.Struct {
	return a.metrics
}

// Limiters returns the limiters slice so that it can be consumed by
// the external storage builder.
func (a *EarlyBootExternalStorageAccessor) Limiters() Limiters {
	return a.limiters
}

// OpenURL opens an ExternalStorage using a URI spec.
func (a *EarlyBootExternalStorageAccessor) OpenURL(
	ctx context.Context, uri string, opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	return EarlyBootExternalStorageFromURI(ctx, uri, a.conf, a.settings, a.limiters, a.metrics, opts...)
}
