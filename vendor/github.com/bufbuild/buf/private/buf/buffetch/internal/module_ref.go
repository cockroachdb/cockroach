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

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
)

var (
	_ ParsedModuleRef = &moduleRef{}
)

type moduleRef struct {
	format          string
	moduleReference bufmodule.ModuleReference
}

func newModuleRef(
	format string,
	path string,
) (*moduleRef, error) {
	if path == "" {
		return nil, NewNoPathError()
	}
	if app.IsDevStderr(path) {
		return nil, NewInvalidPathError(format, path)
	}
	if path == "-" || app.IsDevNull(path) || app.IsDevStdin(path) || app.IsDevStdout(path) {
		return nil, NewInvalidPathError(format, path)
	}
	if strings.Contains(path, "://") {
		return nil, NewInvalidPathError(format, path)
	}
	moduleReference, err := bufmodule.ModuleReferenceForString(path)
	if err != nil {
		// TODO: this is dumb
		return nil, NewInvalidPathError(format, path)
	}
	return newDirectModuleRef(format, moduleReference), nil
}

func newDirectModuleRef(format string, moduleReference bufmodule.ModuleReference) *moduleRef {
	return &moduleRef{
		format:          format,
		moduleReference: moduleReference,
	}
}

func (r *moduleRef) Format() string {
	return r.format
}

func (r *moduleRef) ModuleReference() bufmodule.ModuleReference {
	return r.moduleReference
}

func (*moduleRef) ref()       {}
func (*moduleRef) bucketRef() {}
func (*moduleRef) moduleRef() {}
