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

// Package bufapimodule provides bufmodule types based on bufapi types.
package bufapimodule

import (
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/gen/proto/apiclient/buf/alpha/registry/v1alpha1/registryv1alpha1apiclient"
	"go.uber.org/zap"
)

// NewModuleReader returns a new ModuleReader backed by the download service.
func NewModuleReader(
	downloadServiceProvider registryv1alpha1apiclient.DownloadServiceProvider,
) bufmodule.ModuleReader {
	return newModuleReader(
		downloadServiceProvider,
	)
}

// NewModuleResolver returns a new ModuleResolver backed by the resolve service.
func NewModuleResolver(
	logger *zap.Logger,
	resolveServiceProvider registryv1alpha1apiclient.ResolveServiceProvider,
) bufmodule.ModuleResolver {
	return newModuleResolver(logger, resolveServiceProvider)
}
