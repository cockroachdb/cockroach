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

package bufimagebuild

import (
	"context"

	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"go.uber.org/zap"
)

// Builder builds Protobuf files into Images.
type Builder interface {
	// Build runs compilation.
	//
	// The FileRefs are assumed to have been created by a FileRefProvider, that is
	// they are unique relative to the roots.
	//
	// If an error is returned, it is a system error.
	// Only one of Image and FileAnnotations will be returned.
	//
	// FileAnnotations will use external file paths.
	Build(
		ctx context.Context,
		moduleFileSet bufmodule.ModuleFileSet,
		options ...BuildOption,
	) (bufimage.Image, []bufanalysis.FileAnnotation, error)
}

// NewBuilder returns a new Builder.
func NewBuilder(logger *zap.Logger) Builder {
	return newBuilder(logger)
}

// BuildOption is an option for Build.
type BuildOption func(*buildOptions)

// WithExcludeSourceCodeInfo returns a BuildOption that excludes sourceCodeInfo.
func WithExcludeSourceCodeInfo() BuildOption {
	return func(buildOptions *buildOptions) {
		buildOptions.excludeSourceCodeInfo = true
	}
}
