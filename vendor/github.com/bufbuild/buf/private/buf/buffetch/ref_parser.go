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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bufbuild/buf/private/buf/buffetch/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
)

type refParser struct {
	logger         *zap.Logger
	fetchRefParser internal.RefParser
}

func newRefParser(logger *zap.Logger) *refParser {
	return &refParser{
		logger: logger.Named("buffetch"),
		fetchRefParser: internal.NewRefParser(
			logger,
			internal.WithRawRefProcessor(processRawRef),
			internal.WithSingleFormat(formatBin),
			internal.WithSingleFormat(formatJSON),
			internal.WithSingleFormat(
				formatBingz,
				internal.WithSingleDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
			internal.WithSingleFormat(
				formatJSONGZ,
				internal.WithSingleDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
			internal.WithArchiveFormat(
				formatTar,
				internal.ArchiveTypeTar,
			),
			internal.WithArchiveFormat(
				formatTargz,
				internal.ArchiveTypeTar,
				internal.WithArchiveDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
			internal.WithArchiveFormat(
				formatZip,
				internal.ArchiveTypeZip,
			),
			internal.WithGitFormat(formatGit),
			internal.WithDirFormat(formatDir),
			internal.WithModuleFormat(formatMod),
		),
	}
}

func newImageRefParser(logger *zap.Logger) *refParser {
	return &refParser{
		logger: logger.Named("buffetch"),
		fetchRefParser: internal.NewRefParser(
			logger,
			internal.WithRawRefProcessor(processRawRefImage),
			internal.WithSingleFormat(formatBin),
			internal.WithSingleFormat(formatJSON),
			internal.WithSingleFormat(
				formatBingz,
				internal.WithSingleDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
			internal.WithSingleFormat(
				formatJSONGZ,
				internal.WithSingleDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
		),
	}
}

func newSourceRefParser(logger *zap.Logger) *refParser {
	return &refParser{
		logger: logger.Named("buffetch"),
		fetchRefParser: internal.NewRefParser(
			logger,
			internal.WithRawRefProcessor(processRawRefSource),
			internal.WithArchiveFormat(
				formatTar,
				internal.ArchiveTypeTar,
			),
			internal.WithArchiveFormat(
				formatTargz,
				internal.ArchiveTypeTar,
				internal.WithArchiveDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
			internal.WithArchiveFormat(
				formatZip,
				internal.ArchiveTypeZip,
			),
			internal.WithGitFormat(formatGit),
			internal.WithDirFormat(formatDir),
		),
	}
}

func newModuleRefParser(logger *zap.Logger) *refParser {
	return &refParser{
		logger: logger.Named("buffetch"),
		fetchRefParser: internal.NewRefParser(
			logger,
			internal.WithRawRefProcessor(processRawRefModule),
			internal.WithModuleFormat(formatMod),
		),
	}
}

func newSourceOrModuleRefParser(logger *zap.Logger) *refParser {
	return &refParser{
		logger: logger.Named("buffetch"),
		fetchRefParser: internal.NewRefParser(
			logger,
			internal.WithRawRefProcessor(processRawRefSourceOrModule),
			internal.WithArchiveFormat(
				formatTar,
				internal.ArchiveTypeTar,
			),
			internal.WithArchiveFormat(
				formatTargz,
				internal.ArchiveTypeTar,
				internal.WithArchiveDefaultCompressionType(
					internal.CompressionTypeGzip,
				),
			),
			internal.WithArchiveFormat(
				formatZip,
				internal.ArchiveTypeZip,
			),
			internal.WithGitFormat(formatGit),
			internal.WithDirFormat(formatDir),
			internal.WithModuleFormat(formatMod),
		),
	}
}

func (a *refParser) GetRef(
	ctx context.Context,
	value string,
) (Ref, error) {
	ctx, span := trace.StartSpan(ctx, "get_ref")
	defer span.End()
	parsedRef, err := a.getParsedRef(ctx, value, allFormats)
	if err != nil {
		return nil, err
	}
	switch t := parsedRef.(type) {
	case internal.ParsedSingleRef:
		imageEncoding, err := parseImageEncoding(t.Format())
		if err != nil {
			return nil, err
		}
		return newImageRef(t, imageEncoding), nil
	case internal.ParsedArchiveRef:
		return newSourceRef(t), nil
	case internal.ParsedDirRef:
		return newSourceRef(t), nil
	case internal.ParsedGitRef:
		return newSourceRef(t), nil
	case internal.ParsedModuleRef:
		return newModuleRef(t), nil
	default:
		return nil, fmt.Errorf("unknown ParsedRef type: %T", parsedRef)
	}
}

func (a *refParser) GetSourceOrModuleRef(
	ctx context.Context,
	value string,
) (SourceOrModuleRef, error) {
	ctx, span := trace.StartSpan(ctx, "get_source_or_module_ref")
	defer span.End()
	parsedRef, err := a.getParsedRef(ctx, value, sourceOrModuleFormats)
	if err != nil {
		return nil, err
	}
	switch t := parsedRef.(type) {
	case internal.ParsedSingleRef:
		// this should never happen
		return nil, fmt.Errorf("invalid ParsedRef type for source or module: %T", parsedRef)
	case internal.ParsedArchiveRef:
		return newSourceRef(t), nil
	case internal.ParsedDirRef:
		return newSourceRef(t), nil
	case internal.ParsedGitRef:
		return newSourceRef(t), nil
	case internal.ParsedModuleRef:
		return newModuleRef(t), nil
	default:
		return nil, fmt.Errorf("unknown ParsedRef type: %T", parsedRef)
	}
}

func (a *refParser) GetImageRef(
	ctx context.Context,
	value string,
) (ImageRef, error) {
	ctx, span := trace.StartSpan(ctx, "get_image_ref")
	defer span.End()
	parsedRef, err := a.getParsedRef(ctx, value, imageFormats)
	if err != nil {
		return nil, err
	}
	parsedSingleRef, ok := parsedRef.(internal.ParsedSingleRef)
	if !ok {
		// this should never happen
		return nil, fmt.Errorf("invalid ParsedRef type for image: %T", parsedRef)
	}
	imageEncoding, err := parseImageEncoding(parsedSingleRef.Format())
	if err != nil {
		return nil, err
	}
	return newImageRef(parsedSingleRef, imageEncoding), nil
}

func (a *refParser) GetSourceRef(
	ctx context.Context,
	value string,
) (SourceRef, error) {
	ctx, span := trace.StartSpan(ctx, "get_source_ref")
	defer span.End()
	parsedRef, err := a.getParsedRef(ctx, value, sourceFormats)
	if err != nil {
		return nil, err
	}
	parsedBucketRef, ok := parsedRef.(internal.ParsedBucketRef)
	if !ok {
		// this should never happen
		return nil, fmt.Errorf("invalid ParsedRef type for source: %T", parsedRef)
	}
	return newSourceRef(parsedBucketRef), nil
}

func (a *refParser) GetModuleRef(
	ctx context.Context,
	value string,
) (ModuleRef, error) {
	ctx, span := trace.StartSpan(ctx, "get_source_ref")
	defer span.End()
	parsedRef, err := a.getParsedRef(ctx, value, moduleFormats)
	if err != nil {
		return nil, err
	}
	parsedModuleRef, ok := parsedRef.(internal.ParsedModuleRef)
	if !ok {
		// this should never happen
		return nil, fmt.Errorf("invalid ParsedRef type for source: %T", parsedRef)
	}
	return newModuleRef(parsedModuleRef), nil
}

func (a *refParser) getParsedRef(
	ctx context.Context,
	value string,
	allowedFormats []string,
) (internal.ParsedRef, error) {
	parsedRef, err := a.fetchRefParser.GetParsedRef(
		ctx,
		value,
		internal.WithAllowedFormats(allowedFormats...),
	)
	if err != nil {
		return nil, err
	}
	a.checkDeprecated(parsedRef)
	return parsedRef, nil
}

func (a *refParser) checkDeprecated(parsedRef internal.ParsedRef) {
	format := parsedRef.Format()
	if replacementFormat, ok := deprecatedCompressionFormatToReplacementFormat[format]; ok {
		a.logger.Sugar().Warnf(
			`Format %q is deprecated. Use "format=%s,compression=gzip" instead. This will continue to work forever, but updating is recommended.`,
			format,
			replacementFormat,
		)
	}
}

func processRawRef(rawRef *internal.RawRef) error {
	// if format option is not set and path is "-", default to bin
	var format string
	var compressionType internal.CompressionType
	if rawRef.Path == "-" || app.IsDevNull(rawRef.Path) || app.IsDevStdin(rawRef.Path) || app.IsDevStdout(rawRef.Path) {
		format = formatBin
	} else {
		switch filepath.Ext(rawRef.Path) {
		case ".bin":
			format = formatBin
		case ".json":
			format = formatJSON
		case ".tar":
			format = formatTar
		case ".zip":
			format = formatZip
		case ".gz":
			compressionType = internal.CompressionTypeGzip
			switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
			case ".bin":
				format = formatBin
			case ".json":
				format = formatJSON
			case ".tar":
				format = formatTar
			default:
				return fmt.Errorf("path %q had .gz extension with unknown format", rawRef.Path)
			}
		case ".zst":
			compressionType = internal.CompressionTypeZstd
			switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
			case ".bin":
				format = formatBin
			case ".json":
				format = formatJSON
			case ".tar":
				format = formatTar
			default:
				return fmt.Errorf("path %q had .zst extension with unknown format", rawRef.Path)
			}
		case ".tgz":
			format = formatTar
			compressionType = internal.CompressionTypeGzip
		case ".git":
			format = formatGit
		default:
			var err error
			format, err = assumeModuleOrDir(rawRef.Path)
			if err != nil {
				return err
			}
		}
	}
	rawRef.Format = format
	rawRef.CompressionType = compressionType
	return nil
}

func processRawRefSource(rawRef *internal.RawRef) error {
	// if format option is not set and path is "-", default to bin
	var format string
	var compressionType internal.CompressionType
	switch filepath.Ext(rawRef.Path) {
	case ".tar":
		format = formatTar
	case ".zip":
		format = formatZip
	case ".gz":
		compressionType = internal.CompressionTypeGzip
		switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
		case ".tar":
			format = formatTar
		default:
			return fmt.Errorf("path %q had .gz extension with unknown format", rawRef.Path)
		}
	case ".zst":
		compressionType = internal.CompressionTypeZstd
		switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
		case ".tar":
			format = formatTar
		default:
			return fmt.Errorf("path %q had .zst extension with unknown format", rawRef.Path)
		}
	case ".tgz":
		format = formatTar
		compressionType = internal.CompressionTypeGzip
	case ".git":
		format = formatGit
	default:
		format = formatDir
	}
	rawRef.Format = format
	rawRef.CompressionType = compressionType
	return nil
}

func processRawRefSourceOrModule(rawRef *internal.RawRef) error {
	// if format option is not set and path is "-", default to bin
	var format string
	var compressionType internal.CompressionType
	switch filepath.Ext(rawRef.Path) {
	case ".tar":
		format = formatTar
	case ".zip":
		format = formatZip
	case ".gz":
		compressionType = internal.CompressionTypeGzip
		switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
		case ".tar":
			format = formatTar
		default:
			return fmt.Errorf("path %q had .gz extension with unknown format", rawRef.Path)
		}
	case ".zst":
		compressionType = internal.CompressionTypeZstd
		switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
		case ".tar":
			format = formatTar
		default:
			return fmt.Errorf("path %q had .zst extension with unknown format", rawRef.Path)
		}
	case ".tgz":
		format = formatTar
		compressionType = internal.CompressionTypeGzip
	case ".git":
		format = formatGit
	default:
		var err error
		format, err = assumeModuleOrDir(rawRef.Path)
		if err != nil {
			return err
		}
	}
	rawRef.Format = format
	rawRef.CompressionType = compressionType
	return nil
}

func processRawRefImage(rawRef *internal.RawRef) error {
	// if format option is not set and path is "-", default to bin
	var format string
	var compressionType internal.CompressionType
	if rawRef.Path == "-" || app.IsDevNull(rawRef.Path) || app.IsDevStdin(rawRef.Path) || app.IsDevStdout(rawRef.Path) {
		format = formatBin
	} else {
		switch filepath.Ext(rawRef.Path) {
		case ".bin":
			format = formatBin
		case ".json":
			format = formatJSON
		case ".gz":
			compressionType = internal.CompressionTypeGzip
			switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
			case ".bin":
				format = formatBin
			case ".json":
				format = formatJSON
			default:
				return fmt.Errorf("path %q had .gz extension with unknown format", rawRef.Path)
			}
		case ".zst":
			compressionType = internal.CompressionTypeZstd
			switch filepath.Ext(strings.TrimSuffix(rawRef.Path, filepath.Ext(rawRef.Path))) {
			case ".bin":
				format = formatBin
			case ".json":
				format = formatJSON
			default:
				return fmt.Errorf("path %q had .zst extension with unknown format", rawRef.Path)
			}
		default:
			format = formatBin
		}
	}
	rawRef.Format = format
	rawRef.CompressionType = compressionType
	return nil
}

func processRawRefModule(rawRef *internal.RawRef) error {
	rawRef.Format = formatMod
	return nil
}

func parseImageEncoding(format string) (ImageEncoding, error) {
	switch format {
	case formatBin, formatBingz:
		return ImageEncodingBin, nil
	case formatJSON, formatJSONGZ:
		return ImageEncodingJSON, nil
	default:
		return 0, fmt.Errorf("invalid format for image: %q", format)
	}
}

// TODO: this is a terrible heuristic, and we shouldn't be using what amounts
// to heuristics here (technically this is a documentable rule, but still)
func assumeModuleOrDir(path string) (string, error) {
	if path == "" {
		return "", errors.New("assumeModuleOrDir: no path given")
	}
	if _, err := bufmodule.ModuleReferenceForString(path); err == nil {
		// this is possible to be a module, check if it is a directory though
		// OK to use os.Stat instead of os.Lstat here
		fileInfo, err := os.Stat(path)
		if err == nil && fileInfo.IsDir() {
			// if we have a directory, assume this is a directory
			return formatDir, nil
		}
		// not a directory, assume module
		return formatMod, nil
	}
	// cannot be parsed into a module, assume dir for here
	return formatDir, nil
}
