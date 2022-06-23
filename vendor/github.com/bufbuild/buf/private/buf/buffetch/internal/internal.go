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
	"context"
	"io"
	"net/http"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/git"
	"github.com/bufbuild/buf/private/pkg/httpauth"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.uber.org/zap"
)

const (
	// FileSchemeHTTP is the http file scheme.
	FileSchemeHTTP FileScheme = iota + 1
	// FileSchemeHTTPS is the https file scheme.
	FileSchemeHTTPS
	// FileSchemeLocal is the local file scheme.
	FileSchemeLocal
	// FileSchemeStdio is the stdio file scheme.
	//
	// This can reference either stdin or stdout depending on if we are
	// reading or writing.
	FileSchemeStdio
	// FileSchemeStdin is the stdin file scheme.
	FileSchemeStdin
	// FileSchemeStdout is the stdout file scheme.
	FileSchemeStdout
	// FileSchemeNull is the null file scheme.
	FileSchemeNull

	// GitSchemeHTTP is the http git scheme.
	GitSchemeHTTP GitScheme = iota + 1
	// GitSchemeHTTPS is the https git scheme.
	GitSchemeHTTPS
	// GitSchemeLocal is the local git scheme.
	GitSchemeLocal
	// GitSchemeSSH is the ssh git scheme.
	GitSchemeSSH
	// GitSchemeGit is the git git scheme.
	GitSchemeGit

	// ArchiveTypeTar is a tar archive.
	ArchiveTypeTar ArchiveType = iota + 1
	// ArchiveTypeZip is a zip archive.
	ArchiveTypeZip

	// CompressionTypeNone is no compression.
	CompressionTypeNone CompressionType = iota + 1
	// CompressionTypeGzip is gzip compression.
	CompressionTypeGzip
	// CompressionTypeZstd is zstd compression.
	CompressionTypeZstd
)

// FileScheme is a file scheme.
type FileScheme int

// GitScheme is a git scheme.
type GitScheme int

// ArchiveType is a archive type.
type ArchiveType int

// CompressionType is a compression type.
type CompressionType int

// Ref is a reference.
type Ref interface {
	ref()
}

// FileRef is a file reference.
type FileRef interface {
	Ref
	// Path is the path to the reference.
	//
	// This will be the non-empty path minus the scheme for http and https files.
	// This will be the non-empty normalized file path for local files.
	// This will be empty for stdio and null files.
	Path() string
	FileScheme() FileScheme
	CompressionType() CompressionType
	fileRef()
}

// BucketRef is a bucket reference.
type BucketRef interface {
	Ref
	bucketRef()
}

// SingleRef is a non-archive file reference.
type SingleRef interface {
	FileRef
	singleRef()
}

// NewSingleRef returns a new SingleRef.
func NewSingleRef(path string, compressionType CompressionType) (SingleRef, error) {
	return newSingleRef("", path, compressionType)
}

// ArchiveRef is an archive reference.
//
// An ArchiveRef is a special type of reference that can be either a FileRef or a BucketRef.
// Note that if ArchiveType is ArchiveTypeZip, CompressionType will always be CompressionTypeNone.
type ArchiveRef interface {
	FileRef
	BucketRef
	ArchiveType() ArchiveType
	// Applied before subdir
	StripComponents() uint32
	// Will be empty instead of "." for root directory
	SubDirPath() string
	archiveRef()
}

// NewArchiveRef returns a new ArchiveRef.
func NewArchiveRef(
	path string,
	archiveType ArchiveType,
	compressionType CompressionType,
	stripComponents uint32,
	subDirPath string,
) (ArchiveRef, error) {
	return newArchiveRef("", path, archiveType, compressionType, stripComponents, subDirPath)
}

// DirRef is a local directory reference.
type DirRef interface {
	// Path is the path to the reference.
	//
	// This will be the non-empty normalized directory path for directories.
	Path() string
	BucketRef
	dirRef()
}

// NewDirRef returns a new DirRef.
func NewDirRef(path string) (DirRef, error) {
	return newDirRef("", path)
}

// GitRef is a git reference.
type GitRef interface {
	// Path is the path to the reference.
	//
	// This will be the non-empty path minus the scheme for http, https, ssh, and git repositories.
	// This will be the non-empty normalized directory path for local git repositories.
	Path() string
	BucketRef
	GitScheme() GitScheme
	// Optional. May be nil, in which case clone the default branch.
	GitName() git.Name
	// Will always be >= 1
	Depth() uint32
	RecurseSubmodules() bool
	// Will be empty instead of "." for root directory
	SubDirPath() string
	gitRef()
}

// NewGitRef returns a new GitRef.
func NewGitRef(
	path string,
	gitName git.Name,
	depth uint32,
	recurseSubmodules bool,
	subDirPath string,
) (GitRef, error) {
	return newGitRef("", path, gitName, depth, recurseSubmodules, subDirPath)
}

// ModuleRef is a module reference.
type ModuleRef interface {
	Ref
	ModuleReference() bufmodule.ModuleReference
	moduleRef()
}

// NewModuleRef returns a new ModuleRef.
//
// The path must be in the form server/owner/repository/branch[:digest].
func NewModuleRef(path string) (ModuleRef, error) {
	return newModuleRef("", path)
}

// HasFormat is an object that has a format.
type HasFormat interface {
	Format() string
}

// ParsedRef is a parsed Ref.
type ParsedRef interface {
	Ref
	HasFormat
}

// ParsedFileRef is a parsed FileRef.
type ParsedFileRef interface {
	FileRef
	HasFormat
}

// ParsedBucketRef is a parsed BucketRef.
type ParsedBucketRef interface {
	BucketRef
	HasFormat
}

// ParsedSingleRef is a parsed SingleRef.
type ParsedSingleRef interface {
	SingleRef
	HasFormat
}

// NewDirectParsedSingleRef returns a new ParsedSingleRef with no validation checks.
//
// This should only be used for testing.
func NewDirectParsedSingleRef(
	format string,
	path string,
	fileScheme FileScheme,
	compressionType CompressionType,
) ParsedSingleRef {
	return newDirectSingleRef(
		format,
		path,
		fileScheme,
		compressionType,
	)
}

// ParsedArchiveRef is a parsed ArchiveRef.
type ParsedArchiveRef interface {
	ArchiveRef
	HasFormat
}

// NewDirectParsedArchiveRef returns a new ParsedArchiveRef with no validation checks.
//
// This should only be used for testing.
func NewDirectParsedArchiveRef(
	format string,
	path string,
	fileScheme FileScheme,
	archiveType ArchiveType,
	compressionType CompressionType,
	stripComponents uint32,
	subDirPath string,
) ParsedArchiveRef {
	return newDirectArchiveRef(
		format,
		path,
		fileScheme,
		archiveType,
		compressionType,
		stripComponents,
		subDirPath,
	)
}

// ParsedDirRef is a parsed DirRef.
type ParsedDirRef interface {
	DirRef
	HasFormat
}

// NewDirectParsedDirRef returns a new ParsedDirRef with no validation checks.
//
// This should only be used for testing.
func NewDirectParsedDirRef(format string, path string) ParsedDirRef {
	return newDirectDirRef(format, path)
}

// ParsedGitRef is a parsed GitRef.
type ParsedGitRef interface {
	GitRef
	HasFormat
}

// NewDirectParsedGitRef returns a new ParsedGitRef with no validation checks.
//
// This should only be used for testing.
func NewDirectParsedGitRef(
	format string,
	path string,
	gitScheme GitScheme,
	gitName git.Name,
	recurseSubmodules bool,
	depth uint32,
	subDirPath string,
) ParsedGitRef {
	return newDirectGitRef(
		format,
		path,
		gitScheme,
		gitName,
		recurseSubmodules,
		depth,
		subDirPath,
	)
}

// ParsedModuleRef is a parsed ModuleRef.
type ParsedModuleRef interface {
	ModuleRef
	HasFormat
}

// NewDirectParsedModuleRef returns a new ParsedModuleRef with no validation checks.
//
// This should only be used for testing.
func NewDirectParsedModuleRef(
	format string,
	moduleReference bufmodule.ModuleReference,
) ParsedModuleRef {
	return newDirectModuleRef(
		format,
		moduleReference,
	)
}

// RefParser parses references.
type RefParser interface {
	// GetParsedRef gets the ParsedRef for the value.
	//
	// The returned ParsedRef will be either a ParsedSingleRef, ParsedArchiveRef, ParsedDirRef, ParsedGitRef, or ParsedModuleRef.
	//
	// The options should be used to validate that you are getting one of the correct formats.
	GetParsedRef(ctx context.Context, value string, options ...GetParsedRefOption) (ParsedRef, error)
}

// NewRefParser returns a new RefParser.
func NewRefParser(logger *zap.Logger, options ...RefParserOption) RefParser {
	return newRefParser(logger, options...)
}

// ReadBucketCloser is a bucket returned from GetBucket.
type ReadBucketCloser interface {
	storage.ReadBucketCloser

	// RelativeRootPath is the relative path to the root of the bucket
	// based on the current working directory.
	//
	// This will be set if a terminate filename was specified and found.
	RelativeRootPath() string
	// SubDirPath is the subdir within the Bucket of the actual asset.
	//
	// This will be set if a terminate filename was specified and found.
	// If so, the actual Bucket will be the directory that contained
	// this terminate file, and the subdir will be the subdir of
	// the actual asset relative to the terminate file.
	SubDirPath() string
}

// ReadWriteBucketCloser is a bucket potentially returned from GetBucket.
//
// The returned ReadBucketCloser may be upgradeable to a ReadWriteBucketCloser.
type ReadWriteBucketCloser interface {
	ReadBucketCloser
	storage.WriteBucket
}

// Reader is a reader.
type Reader interface {
	// GetFile gets the file.
	// SingleRefs and ArchiveRefs will result in decompressed files unless KeepFileCompression is set.
	GetFile(
		ctx context.Context,
		container app.EnvStdinContainer,
		fileRef FileRef,
		options ...GetFileOption,
	) (io.ReadCloser, error)
	// GetBucket gets the bucket.
	//
	// The returned ReadBucketCloser may actually be upgradeable to a ReadWriteBucketCloser.
	GetBucket(
		ctx context.Context,
		container app.EnvStdinContainer,
		bucketRef BucketRef,
		options ...GetBucketOption,
	) (ReadBucketCloser, error)
	// GetModule gets the module.
	GetModule(
		ctx context.Context,
		container app.EnvStdinContainer,
		moduleRef ModuleRef,
		options ...GetModuleOption,
	) (bufmodule.Module, error)
}

// NewReader returns a new Reader.
func NewReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	options ...ReaderOption,
) Reader {
	return newReader(
		logger,
		storageosProvider,
		options...,
	)
}

// Writer is a writer.
type Writer interface {
	// PutFile puts the file.
	PutFile(
		ctx context.Context,
		container app.EnvStdoutContainer,
		fileRef FileRef,
		options ...PutFileOption,
	) (io.WriteCloser, error)
}

// NewWriter returns a new Writer.
func NewWriter(
	logger *zap.Logger,
	options ...WriterOption,
) Writer {
	return newWriter(
		logger,
		options...,
	)
}

// RawRef is an unprocessed ref used for WithRefProcessor.
//
// A RawRefProcessor will allow modifications to a RawRef before continuing parsing.
// This allows defaults to be inferred from the path.
//
// The Path will be the only value set when the RawRefProcessor is invoked, and is not normalized.
// After the RawRefProcessor is called, options will be parsed.
type RawRef struct {
	// Will always be set
	// Not normalized yet
	Path string
	// Will always be set
	// Set via RawRefProcessor if not explicitly set
	Format string
	// Only set for single, archive formats
	// Cannot be set for zip archives
	CompressionType CompressionType
	// Only set for archive, git formats
	SubDirPath string
	// Only set for git formats
	// Only one of GitBranch and GitTag will be set
	GitBranch string
	// Only set for git formats
	// Only one of GitBranch and GitTag will be set
	GitTag string
	// Only set for git formats
	// Specifies an exact git reference to use with git checkout.
	// Can be used on its own or with GitBranch. Not allowed with GitTag.
	// This is defined as anything that can be given to git checkout.
	GitRef string
	// Only set for git formats
	GitRecurseSubmodules bool
	// Only set for git formats.
	// The depth to use when cloning a repository. Only allowed when GitRef
	// is set. Defaults to 50 if unset.
	GitDepth uint32
	// Only set for archive formats
	ArchiveStripComponents uint32
}

// RefParserOption is an RefParser option.
type RefParserOption func(*refParser)

// WithRawRefProcessor attaches the given RawRefProcessor.
//
// If format is not manually specified, the RefParser will use this format parser
// with the raw path, that is not normalized.
func WithRawRefProcessor(rawRefProcessor func(*RawRef) error) RefParserOption {
	return func(refParser *refParser) {
		refParser.rawRefProcessor = rawRefProcessor
	}
}

// WithSingleFormat attaches the given format as a single format.
//
// It is up to the user to not incorrectly attached a format twice.
func WithSingleFormat(format string, options ...SingleFormatOption) RefParserOption {
	return func(refParser *refParser) {
		format = normalizeFormat(format)
		if format == "" {
			return
		}
		singleFormatInfo := newSingleFormatInfo()
		for _, option := range options {
			option(singleFormatInfo)
		}
		refParser.singleFormatToInfo[format] = singleFormatInfo
	}
}

// WithArchiveFormat attaches the given format as an archive format.
//
// It is up to the user to not incorrectly attached a format twice.
func WithArchiveFormat(format string, archiveType ArchiveType, options ...ArchiveFormatOption) RefParserOption {
	return func(refParser *refParser) {
		format = normalizeFormat(format)
		if format == "" {
			return
		}
		archiveFormatInfo := newArchiveFormatInfo(archiveType)
		for _, option := range options {
			option(archiveFormatInfo)
		}
		refParser.archiveFormatToInfo[format] = archiveFormatInfo
	}
}

// WithDirFormat attaches the given format as a dir format.
//
// It is up to the user to not incorrectly attached a format twice.
func WithDirFormat(format string, options ...DirFormatOption) RefParserOption {
	return func(refParser *refParser) {
		format = normalizeFormat(format)
		if format == "" {
			return
		}
		dirFormatInfo := newDirFormatInfo()
		for _, option := range options {
			option(dirFormatInfo)
		}
		refParser.dirFormatToInfo[format] = dirFormatInfo
	}
}

// WithGitFormat attaches the given format as a git format.
//
// It is up to the user to not incorrectly attached a format twice.
func WithGitFormat(format string, options ...GitFormatOption) RefParserOption {
	return func(refParser *refParser) {
		format = normalizeFormat(format)
		if format == "" {
			return
		}
		gitFormatInfo := newGitFormatInfo()
		for _, option := range options {
			option(gitFormatInfo)
		}
		refParser.gitFormatToInfo[format] = gitFormatInfo
	}
}

// WithModuleFormat attaches the given format as a module format.
//
// It is up to the user to not incorrectly attach a format twice.
func WithModuleFormat(format string, options ...ModuleFormatOption) RefParserOption {
	return func(refParser *refParser) {
		format = normalizeFormat(format)
		if format == "" {
			return
		}
		moduleFormatInfo := newModuleFormatInfo()
		for _, option := range options {
			option(moduleFormatInfo)
		}
		refParser.moduleFormatToInfo[format] = moduleFormatInfo
	}
}

// SingleFormatOption is a single format option.
type SingleFormatOption func(*singleFormatInfo)

// WithSingleDefaultCompressionType sets the default compression type.
func WithSingleDefaultCompressionType(defaultCompressionType CompressionType) SingleFormatOption {
	return func(singleFormatInfo *singleFormatInfo) {
		singleFormatInfo.defaultCompressionType = defaultCompressionType
	}
}

// ArchiveFormatOption is a archive format option.
type ArchiveFormatOption func(*archiveFormatInfo)

// WithArchiveDefaultCompressionType sets the default compression type.
//
// Note this should never be set for zip.
func WithArchiveDefaultCompressionType(defaultCompressionType CompressionType) ArchiveFormatOption {
	return func(archiveFormatInfo *archiveFormatInfo) {
		archiveFormatInfo.defaultCompressionType = defaultCompressionType
	}
}

// DirFormatOption is a dir format option.
type DirFormatOption func(*dirFormatInfo)

// GitFormatOption is a git format option.
type GitFormatOption func(*gitFormatInfo)

// ModuleFormatOption is a module format option.
type ModuleFormatOption func(*moduleFormatInfo)

// ReaderOption is an Reader option.
type ReaderOption func(*reader)

// WithReaderHTTP enables HTTP.
func WithReaderHTTP(httpClient *http.Client, httpAuthenticator httpauth.Authenticator) ReaderOption {
	return func(reader *reader) {
		reader.httpEnabled = true
		reader.httpClient = httpClient
		reader.httpAuthenticator = httpAuthenticator
	}
}

// WithReaderGit enables Git.
func WithReaderGit(gitCloner git.Cloner) ReaderOption {
	return func(reader *reader) {
		reader.gitEnabled = true
		reader.gitCloner = gitCloner
	}
}

// WithReaderModule enables modules.
func WithReaderModule(
	moduleResolver bufmodule.ModuleResolver,
	moduleReader bufmodule.ModuleReader,
) ReaderOption {
	return func(reader *reader) {
		reader.moduleEnabled = true
		reader.moduleResolver = moduleResolver
		reader.moduleReader = moduleReader
	}
}

// WithReaderLocal enables local.
func WithReaderLocal() ReaderOption {
	return func(reader *reader) {
		reader.localEnabled = true
	}
}

// WithReaderStdio enables stdio.
func WithReaderStdio() ReaderOption {
	return func(reader *reader) {
		reader.stdioEnabled = true
	}
}

// WriterOption is an Writer option.
type WriterOption func(*writer)

// WithWriterLocal enables local.
func WithWriterLocal() WriterOption {
	return func(writer *writer) {
		writer.localEnabled = true
	}
}

// WithWriterStdio enables stdio.
func WithWriterStdio() WriterOption {
	return func(writer *writer) {
		writer.stdioEnabled = true
	}
}

// GetParsedRefOption is a GetParsedRef option.
type GetParsedRefOption func(*getParsedRefOptions)

// WithAllowedFormats limits the allowed formats to the given formats.
func WithAllowedFormats(formats ...string) GetParsedRefOption {
	return func(getParsedRefOptions *getParsedRefOptions) {
		for _, format := range formats {
			getParsedRefOptions.allowedFormats[normalizeFormat(format)] = struct{}{}
		}
	}
}

// GetFileOption is a GetFile option.
type GetFileOption func(*getFileOptions)

// WithGetFileKeepFileCompression says to return compressed.
func WithGetFileKeepFileCompression() GetFileOption {
	return func(getFileOptions *getFileOptions) {
		getFileOptions.keepFileCompression = true
	}
}

// GetBucketOption is a GetBucket option.
type GetBucketOption func(*getBucketOptions)

// WithGetBucketTerminateFileNames only applies if subdir is specified.
//
// This says that if given a subdir, ascend directories until you reach
// a file one of these names, and if you do, the returned bucket will be
// for the directory with this filename, while SubDirPath on the
// returned bucket will be set to the original subdir relative
// to the terminate file.
//
// This is used for workspaces. So if you have i.e. "proto/foo"
// subdir, and terminate file "proto/buf.work.yaml", the returned bucket will
// be for "proto", and the SubDirPath will be "foo".
//
// The terminateFileNames are expected to be valid and have no slashes.
func WithGetBucketTerminateFileNames(terminateFileNames ...string) GetBucketOption {
	return func(getBucketOptions *getBucketOptions) {
		getBucketOptions.terminateFileNames = terminateFileNames
	}
}

// PutFileOption is a PutFile option.
type PutFileOption func(*putFileOptions)

// WithPutFileNoFileCompression says to put s uncompressed.
func WithPutFileNoFileCompression() PutFileOption {
	return func(putFileOptions *putFileOptions) {
		putFileOptions.noFileCompression = true
	}
}

// GetModuleOption is a GetModule option.
type GetModuleOption func(*getModuleOptions)
