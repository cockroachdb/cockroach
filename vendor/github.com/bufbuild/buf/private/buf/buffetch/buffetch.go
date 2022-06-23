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

package buffetch

import (
	"context"
	"io"
	"net/http"

	"github.com/bufbuild/buf/private/buf/buffetch/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/git"
	"github.com/bufbuild/buf/private/pkg/httpauth"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"go.uber.org/zap"
)

const (
	// ImageEncodingBin is the binary image encoding.
	ImageEncodingBin ImageEncoding = iota + 1
	// ImageEncodingJSON is the JSON image encoding.
	ImageEncodingJSON
)

var (
	// ImageFormatsString is the string representation of all image formats.
	//
	// This does not include deprecated formats.
	ImageFormatsString = stringutil.SliceToString(imageFormatsNotDeprecated)
	// SourceFormatsString is the string representation of all source formats.
	//
	// This does not include deprecated formats.
	SourceFormatsString = stringutil.SliceToString(sourceFormatsNotDeprecated)
	// ModuleFormatsString is the string representation of all module formats.
	//
	// Module formats are also source formats.
	//
	// This does not include deprecated formats.
	ModuleFormatsString = stringutil.SliceToString(moduleFormatsNotDeprecated)
	// SourceOrModuleFormatsString is the string representation of all source or module formats.
	//
	// This does not include deprecated formats.
	SourceOrModuleFormatsString = stringutil.SliceToString(sourceOrModuleFormatsNotDeprecated)
	// AllFormatsString is the string representation of all formats.
	//
	// This does not include deprecated formats.
	AllFormatsString = stringutil.SliceToString(allFormatsNotDeprecated)
)

// ImageEncoding is the encoding of the image.
type ImageEncoding int

// PathResolver resolves external paths to paths.
type PathResolver interface {
	// PathForExternalPath takes a path external to the asset and converts it to
	// a path that is relative to the asset.
	//
	// The returned path will be normalized and validated.
	//
	// Example:
	//   Directory: /foo/bar
	//   ExternalPath: /foo/bar/baz/bat.proto
	//   Path: baz/bat.proto
	//
	// Example:
	//   Directory: .
	//   ExternalPath: baz/bat.proto
	//   Path: baz/bat.proto
	PathForExternalPath(externalPath string) (string, error)
}

// Ref is an image file or source bucket reference.
type Ref interface {
	PathResolver

	internalRef() internal.Ref
}

// ImageRef is an image file reference.
type ImageRef interface {
	Ref
	ImageEncoding() ImageEncoding
	IsNull() bool
	internalFileRef() internal.FileRef
}

// SourceOrModuleRef is a source bucket or module reference.
type SourceOrModuleRef interface {
	Ref
	isSourceOrModuleRef()
}

// SourceRef is a source bucket reference.
type SourceRef interface {
	SourceOrModuleRef
	internalBucketRef() internal.BucketRef
}

// ModuleRef is a module reference.
type ModuleRef interface {
	SourceOrModuleRef
	internalModuleRef() internal.ModuleRef
}

// ImageRefParser is an image ref parser for Buf.
type ImageRefParser interface {
	// GetImageRef gets the reference for the image file.
	GetImageRef(ctx context.Context, value string) (ImageRef, error)
}

// SourceRefParser is a source ref parser for Buf.
type SourceRefParser interface {
	// GetSourceRef gets the reference for the source file.
	GetSourceRef(ctx context.Context, value string) (SourceRef, error)
}

// ModuleRefParser is a source ref parser for Buf.
type ModuleRefParser interface {
	// GetModuleRef gets the reference for the source file.
	//
	// A module is a special type of source with additional properties.
	GetModuleRef(ctx context.Context, value string) (ModuleRef, error)
}

// SourceOrModuleRefParser is a source or module ref parser for Buf.
type SourceOrModuleRefParser interface {
	SourceRefParser
	ModuleRefParser

	// GetSourceOrModuleRef gets the reference for the image file or source bucket.
	GetSourceOrModuleRef(ctx context.Context, value string) (SourceOrModuleRef, error)
}

// RefParser is a ref parser for Buf.
type RefParser interface {
	ImageRefParser
	SourceOrModuleRefParser

	// GetRef gets the reference for the image file, source bucket, or module.
	GetRef(ctx context.Context, value string) (Ref, error)
}

// NewRefParser returns a new RefParser.
//
// This defaults to dir or module.
func NewRefParser(logger *zap.Logger) RefParser {
	return newRefParser(logger)
}

// NewImageRefParser returns a new RefParser for images only.
//
// This defaults to binary.
func NewImageRefParser(logger *zap.Logger) ImageRefParser {
	return newImageRefParser(logger)
}

// NewSourceRefParser returns a new RefParser for sources only.
//
// This defaults to dir or module.
func NewSourceRefParser(logger *zap.Logger) SourceRefParser {
	return newSourceRefParser(logger)
}

// NewModuleRefParser returns a new RefParser for modules only.
func NewModuleRefParser(logger *zap.Logger) ModuleRefParser {
	return newModuleRefParser(logger)
}

// NewSourceOrModuleRefParser returns a new RefParser for sources or modules only.
//
// This defaults to dir or module.
func NewSourceOrModuleRefParser(logger *zap.Logger) SourceOrModuleRefParser {
	return newSourceOrModuleRefParser(logger)
}

// ReadBucketCloser is a bucket returned from GetBucket.
// We need to surface the internal.ReadBucketCloser
// interface to other packages, so we use a type
// declaration to do so.
type ReadBucketCloser internal.ReadBucketCloser

// ReadWriteBucketCloser is a bucket returned from GetBucket.
// We need to surface the internal.ReadWriteBucketCloser
// interface to other packages, so we use a type
// declaration to do so.
type ReadWriteBucketCloser internal.ReadWriteBucketCloser

// ImageReader is an image reader.
type ImageReader interface {
	// GetImageFile gets the image file.
	//
	// The returned file will be uncompressed.
	GetImageFile(
		ctx context.Context,
		container app.EnvStdinContainer,
		imageRef ImageRef,
	) (io.ReadCloser, error)
}

// SourceReader is a source reader.
type SourceReader interface {
	// GetSource gets the source bucket.
	//
	// The returned bucket will only have .proto and configuration files.
	// The returned bucket may be upgradeable to a ReadWriteBucketCloser.
	GetSourceBucket(
		ctx context.Context,
		container app.EnvStdinContainer,
		sourceRef SourceRef,
		options ...GetSourceBucketOption,
	) (ReadBucketCloser, error)
}

// GetSourceBucketOption is an option for GetSourceBucket.
type GetSourceBucketOption func(*getSourceBucketOptions)

// GetSourceBucketWithWorkspacesDisabled disables workspace mode.
func GetSourceBucketWithWorkspacesDisabled() GetSourceBucketOption {
	return func(o *getSourceBucketOptions) {
		o.workspacesDisabled = true
	}
}

// ModuleFetcher is a module fetcher.
type ModuleFetcher interface {
	// GetModule gets the module.
	// Unresolved ModuleRef's are automatically resolved.
	GetModule(
		ctx context.Context,
		container app.EnvStdinContainer,
		moduleRef ModuleRef,
	) (bufmodule.Module, error)
}

// Reader is a reader for Buf.
type Reader interface {
	ImageReader
	SourceReader
	ModuleFetcher
}

// NewReader returns a new Reader.
func NewReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	httpClient *http.Client,
	httpAuthenticator httpauth.Authenticator,
	gitCloner git.Cloner,
	moduleResolver bufmodule.ModuleResolver,
	moduleReader bufmodule.ModuleReader,
) Reader {
	return newReader(
		logger,
		storageosProvider,
		httpClient,
		httpAuthenticator,
		gitCloner,
		moduleResolver,
		moduleReader,
	)
}

// NewImageReader returns a new ImageReader.
func NewImageReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	httpClient *http.Client,
	httpAuthenticator httpauth.Authenticator,
	gitCloner git.Cloner,
) ImageReader {
	return newImageReader(
		logger,
		storageosProvider,
		httpClient,
		httpAuthenticator,
		gitCloner,
	)
}

// NewSourceReader returns a new SourceReader.
func NewSourceReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	httpClient *http.Client,
	httpAuthenticator httpauth.Authenticator,
	gitCloner git.Cloner,
) SourceReader {
	return newSourceReader(
		logger,
		storageosProvider,
		httpClient,
		httpAuthenticator,
		gitCloner,
	)
}

// NewModuleFetcher returns a new ModuleFetcher.
func NewModuleFetcher(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	moduleResolver bufmodule.ModuleResolver,
	moduleReader bufmodule.ModuleReader,
) ModuleFetcher {
	return newModuleFetcher(
		logger,
		storageosProvider,
		moduleResolver,
		moduleReader,
	)
}

// Writer is a writer for Buf.
type Writer interface {
	// PutImageFile puts the image file.
	PutImageFile(
		ctx context.Context,
		container app.EnvStdoutContainer,
		imageRef ImageRef,
	) (io.WriteCloser, error)
}

// NewWriter returns a new Writer.
func NewWriter(
	logger *zap.Logger,
) Writer {
	return newWriter(
		logger,
	)
}

type getSourceBucketOptions struct {
	workspacesDisabled bool
}
