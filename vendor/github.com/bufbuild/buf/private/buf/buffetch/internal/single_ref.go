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
	"strings"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/normalpath"
)

var (
	_ ParsedSingleRef = &singleRef{}

	fileSchemePrefixToFileScheme = map[string]FileScheme{
		"http://":  FileSchemeHTTP,
		"https://": FileSchemeHTTPS,
		"file://":  FileSchemeLocal,
	}
)

type singleRef struct {
	format          string
	path            string
	fileScheme      FileScheme
	compressionType CompressionType
}

func newSingleRef(
	format string,
	path string,
	compressionType CompressionType,
) (*singleRef, error) {
	if path == "" {
		return nil, NewNoPathError()
	}
	if app.IsDevStderr(path) {
		return nil, NewInvalidPathError(format, path)
	}
	if path == "-" {
		return newDirectSingleRef(
			format,
			"",
			FileSchemeStdio,
			compressionType,
		), nil
	}
	if app.IsDevStdin(path) {
		return newDirectSingleRef(
			format,
			"",
			FileSchemeStdin,
			compressionType,
		), nil
	}
	if app.IsDevStdout(path) {
		return newDirectSingleRef(
			format,
			"",
			FileSchemeStdout,
			compressionType,
		), nil
	}
	if app.IsDevNull(path) {
		return newDirectSingleRef(
			format,
			"",
			FileSchemeNull,
			compressionType,
		), nil
	}
	for prefix, fileScheme := range fileSchemePrefixToFileScheme {
		if strings.HasPrefix(path, prefix) {
			path = strings.TrimPrefix(path, prefix)
			if fileScheme == FileSchemeLocal {
				path = normalpath.Normalize(path)
			}
			if path == "" {
				return nil, NewNoPathError()
			}
			return newDirectSingleRef(
				format,
				path,
				fileScheme,
				compressionType,
			), nil
		}
	}
	if strings.Contains(path, "://") {
		return nil, NewInvalidPathError(format, path)
	}
	return newDirectSingleRef(
		format,
		normalpath.Normalize(path),
		FileSchemeLocal,
		compressionType,
	), nil
}

func newDirectSingleRef(
	format string,
	path string,
	fileScheme FileScheme,
	compressionType CompressionType,
) *singleRef {
	return &singleRef{
		format:          format,
		path:            path,
		fileScheme:      fileScheme,
		compressionType: compressionType,
	}
}

func (r *singleRef) Format() string {
	return r.format
}

func (r *singleRef) Path() string {
	return r.path
}

func (r *singleRef) FileScheme() FileScheme {
	return r.fileScheme
}

func (r *singleRef) CompressionType() CompressionType {
	return r.compressionType
}

func (*singleRef) ref()       {}
func (*singleRef) fileRef()   {}
func (*singleRef) singleRef() {}
