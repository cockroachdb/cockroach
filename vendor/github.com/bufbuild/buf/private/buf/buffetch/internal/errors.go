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
	"errors"
	"fmt"
	"strings"
)

var (
	knownCompressionTypeStrings = []string{
		"none",
		"gzip",
		"zstd",
	}
)

// NewValueEmptyError is a fetch error.
func NewValueEmptyError() error {
	return errors.New("required")
}

// NewValueMultipleHashtagsError is a fetch error.
func NewValueMultipleHashtagsError(value string) error {
	return fmt.Errorf("%q has multiple #s which is invalid", value)
}

// NewValueStartsWithHashtagError is a fetch error.
func NewValueStartsWithHashtagError(value string) error {
	return fmt.Errorf("%q starts with # which is invalid", value)
}

// NewValueEndsWithHashtagError is a fetch error.
func NewValueEndsWithHashtagError(value string) error {
	return fmt.Errorf("%q ends with # which is invalid", value)
}

// NewFormatNotAllowedError is a fetch error.
func NewFormatNotAllowedError(format string, allowedFormats map[string]struct{}) error {
	return fmt.Errorf("format was %q but must be one of %s", format, formatsToString(allowedFormats))
}

// NewFormatCannotBeDeterminedError is a fetch error.
func NewFormatCannotBeDeterminedError(value string) error {
	return fmt.Errorf("format cannot be determined from %q", value)
}

// NewCannotSpecifyGitBranchAndTagError is a fetch error.
func NewCannotSpecifyGitBranchAndTagError() error {
	return fmt.Errorf(`must specify only one of "branch", "tag"`)
}

// NewCannotSpecifyTagWithRefError is a fetch error.
func NewCannotSpecifyTagWithRefError() error {
	return fmt.Errorf(`cannot specify "tag" with "ref"`)
}

// NewDepthParseError is a fetch error.
func NewDepthParseError(s string) error {
	return fmt.Errorf(`could not parse "depth" value %q`, s)
}

// NewDepthZeroError is a fetch error.
func NewDepthZeroError() error {
	return fmt.Errorf(`"depth" must be >0 if specified`)
}

// NewPathUnknownGzError is a fetch error.
func NewPathUnknownGzError(path string) error {
	return fmt.Errorf("path %q had .gz extension with unknown format", path)
}

// NewCompressionUnknownError is a fetch error.
func NewCompressionUnknownError(compression string) error {
	return fmt.Errorf("unknown compression: %q (valid values are %q)", compression, strings.Join(knownCompressionTypeStrings, ","))
}

// NewCannotSpecifyCompressionForZipError is a fetch error.
func NewCannotSpecifyCompressionForZipError() error {
	return errors.New("cannot specify compression type for zip files")
}

// NewNoPathError is a fetch error.
func NewNoPathError() error {
	return errors.New("value has no path once processed")
}

// NewOptionsInvalidError is a fetch error.
func NewOptionsInvalidError(s string) error {
	return fmt.Errorf("invalid options: %q", s)
}

// NewOptionsInvalidKeyError is a fetch error.
func NewOptionsInvalidKeyError(key string) error {
	return fmt.Errorf("invalid options key: %q", key)
}

// NewOptionsDuplicateKeyError is a fetch error.
func NewOptionsDuplicateKeyError(key string) error {
	return fmt.Errorf("duplicate options key: %q", key)
}

// NewOptionsInvalidForFormatError is a fetch error.
func NewOptionsInvalidForFormatError(format string, s string) error {
	return fmt.Errorf("invalid options for format %q: %q", format, s)
}

// NewOptionsCouldNotParseStripComponentsError is a fetch error.
func NewOptionsCouldNotParseStripComponentsError(s string) error {
	return fmt.Errorf("could not parse strip_components value %q", s)
}

// NewOptionsCouldNotParseRecurseSubmodulesError is a fetch error.
func NewOptionsCouldNotParseRecurseSubmodulesError(s string) error {
	return fmt.Errorf("could not parse recurse_submodules value %q", s)
}

// NewFormatOverrideNotAllowedForDevNullError is a fetch error.
func NewFormatOverrideNotAllowedForDevNullError(devNull string) error {
	return fmt.Errorf("not allowed if path is %s", devNull)
}

// NewInvalidPathError is a fetch error.
func NewInvalidPathError(format string, path string) error {
	if format != "" {
		format = format + " "
	}
	return fmt.Errorf("invalid %spath: %q", format, path)
}

// NewRealCleanPathError is a fetch error.
func NewRealCleanPathError(path string) error {
	return fmt.Errorf("could not clean relative path %q", path)
}

// NewFormatUnknownError is a fetch error.
func NewFormatUnknownError(formatString string) error {
	return fmt.Errorf("unknown format: %q", formatString)
}

// NewReadDisabledError is a fetch error.
func NewReadDisabledError(scheme string) error {
	return fmt.Errorf("reading assets from %s disabled", scheme)
}

// NewReadHTTPDisabledError is a fetch error.
func NewReadHTTPDisabledError() error {
	return NewReadDisabledError("http")
}

// NewReadGitDisabledError is a fetch error.
func NewReadGitDisabledError() error {
	return NewReadDisabledError("git")
}

// NewReadLocalDisabledError is a fetch error.
func NewReadLocalDisabledError() error {
	return NewReadDisabledError("local")
}

// NewReadStdioDisabledError is a fetch error.
func NewReadStdioDisabledError() error {
	return NewReadDisabledError("stdin")
}

// NewReadModuleDisabledError is a fetch error.
func NewReadModuleDisabledError() error {
	return NewReadDisabledError("module")
}

// NewWriteDisabledError is a fetch error.
func NewWriteDisabledError(scheme string) error {
	return fmt.Errorf("writing assets to %s disabled", scheme)
}

// NewWriteHTTPDisabledError is a fetch error.
func NewWriteHTTPDisabledError() error {
	return NewWriteDisabledError("http")
}

// NewWriteLocalDisabledError is a fetch error.
func NewWriteLocalDisabledError() error {
	return NewWriteDisabledError("local")
}

// NewWriteStdioDisabledError is a fetch error.
func NewWriteStdioDisabledError() error {
	return NewWriteDisabledError("stdout")
}
