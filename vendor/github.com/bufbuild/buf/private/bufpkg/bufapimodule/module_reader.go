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

package bufapimodule

import (
	"context"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/gen/proto/apiclient/buf/alpha/registry/v1alpha1/registryv1alpha1apiclient"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/storage"
)

type moduleReader struct {
	downloadServiceProvider registryv1alpha1apiclient.DownloadServiceProvider
}

func newModuleReader(
	downloadServiceProvider registryv1alpha1apiclient.DownloadServiceProvider,
) *moduleReader {
	return &moduleReader{
		downloadServiceProvider: downloadServiceProvider,
	}
}

func (m *moduleReader) GetModule(ctx context.Context, modulePin bufmodule.ModulePin) (bufmodule.Module, error) {
	downloadService, err := m.downloadServiceProvider.NewDownloadService(ctx, modulePin.Remote())
	if err != nil {
		return nil, err
	}
	module, err := downloadService.Download(
		ctx,
		modulePin.Owner(),
		modulePin.Repository(),
		modulePin.Commit(),
	)
	if err != nil {
		if rpc.GetErrorCode(err) == rpc.ErrorCodeNotFound {
			// Required by ModuleReader interface spec
			return nil, storage.NewErrNotExist(modulePin.String())
		}
		return nil, err
	}
	moduleIdentity, err := bufmodule.NewModuleIdentity(
		modulePin.Remote(),
		modulePin.Owner(),
		modulePin.Repository(),
	)
	if err != nil {
		return nil, err
	}
	return bufmodule.NewModuleForProto(
		ctx, module,
		bufmodule.ModuleWithModuleIdentityAndCommit(moduleIdentity, modulePin.Commit()),
	)
}
