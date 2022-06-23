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
	"fmt"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/gen/proto/apiclient/buf/alpha/registry/v1alpha1/registryv1alpha1apiclient"
	modulev1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/module/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/storage"
	"go.uber.org/zap"
)

type moduleResolver struct {
	logger                 *zap.Logger
	resolveServiceProvider registryv1alpha1apiclient.ResolveServiceProvider
}

func newModuleResolver(logger *zap.Logger, resolveServiceProvider registryv1alpha1apiclient.ResolveServiceProvider) *moduleResolver {
	return &moduleResolver{
		logger:                 logger,
		resolveServiceProvider: resolveServiceProvider,
	}
}

func (m *moduleResolver) GetModulePin(ctx context.Context, moduleReference bufmodule.ModuleReference) (bufmodule.ModulePin, error) {
	resolveService, err := m.resolveServiceProvider.NewResolveService(ctx, moduleReference.Remote())
	if err != nil {
		return nil, err
	}
	protoModulePins, err := resolveService.GetModulePins(
		ctx,
		[]*modulev1alpha1.ModuleReference{
			bufmodule.NewProtoModuleReferenceForModuleReference(moduleReference),
		},
		nil,
	)
	if err != nil {
		if rpc.GetErrorCode(err) == rpc.ErrorCodeNotFound {
			// Required by ModuleResolver interface spec
			return nil, storage.NewErrNotExist(moduleReference.String())
		}
		return nil, err
	}
	var targetModulePin bufmodule.ModulePin
	moduleIdentity := getModuleIdentity(moduleReference)
	for _, protoModulePin := range protoModulePins {
		modulePin, err := bufmodule.NewModulePinForProto(protoModulePin)
		if err != nil {
			return nil, err
		}
		if getModuleIdentity(modulePin) == moduleIdentity {
			if targetModulePin != nil {
				return nil, fmt.Errorf("resolved multiple module pins for %q", moduleIdentity)
			}
			targetModulePin = modulePin
		}
	}
	if targetModulePin == nil {
		return nil, fmt.Errorf("resolved module pins did not contain module %q", moduleIdentity)
	}
	return targetModulePin, nil
}

func getModuleIdentity(moduleIdentity bufmodule.ModuleIdentity) string {
	return moduleIdentity.Remote() + "/" + moduleIdentity.Owner() + "/" + moduleIdentity.Repository()
}
