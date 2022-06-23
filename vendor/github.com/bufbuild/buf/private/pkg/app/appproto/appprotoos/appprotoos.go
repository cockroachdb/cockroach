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

// Package appprotoos does OS-specific generation.
package appprotoos

import (
	"context"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/pluginpb"
)

type Generator interface {
	// Generate generates to the os filesystem, switching on the file extension.
	// If there is a .jar extension, this generates a jar. If there is a .zip
	// extension, this generates a zip. If there is no extension, this outputs
	// to the directory.
	Generate(
		ctx context.Context,
		container app.EnvStderrContainer,
		pluginName string,
		pluginOut string,
		requests []*pluginpb.CodeGeneratorRequest,
		options ...GenerateOption,
	) error
}

// NewGenerator returns a new Generator.
func NewGenerator(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
) Generator {
	return newGenerator(logger, storageosProvider)
}

// GenerateOption is an option for Generate.
type GenerateOption func(*generateOptions)

// GenerateWithPluginPath returns a new GenerateOption that uses the given
// path to the plugin.
func GenerateWithPluginPath(pluginPath string) GenerateOption {
	return func(generateOptions *generateOptions) {
		generateOptions.pluginPath = pluginPath
	}
}

// GenerateWithCreateOutDirIfNotExists returns a new GenerateOption that creates
// the directory if it does not exist.
func GenerateWithCreateOutDirIfNotExists() GenerateOption {
	return func(generateOptions *generateOptions) {
		generateOptions.createOutDirIfNotExists = true
	}
}
