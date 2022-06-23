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

package bufmodulecache

import (
	"context"

	"github.com/bufbuild/buf/private/bufpkg/buflock"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/storage"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type moduleCacher struct {
	logger              *zap.Logger
	dataReadWriteBucket storage.ReadWriteBucket
	sumReadWriteBucket  storage.ReadWriteBucket
}

func newModuleCacher(
	logger *zap.Logger,
	dataReadWriteBucket storage.ReadWriteBucket,
	sumReadWriteBucket storage.ReadWriteBucket,
) *moduleCacher {
	return &moduleCacher{
		logger:              logger,
		dataReadWriteBucket: dataReadWriteBucket,
		sumReadWriteBucket:  sumReadWriteBucket,
	}
}

func (m *moduleCacher) GetModule(
	ctx context.Context,
	modulePin bufmodule.ModulePin,
) (bufmodule.Module, error) {
	modulePath := newCacheKey(modulePin)
	// We do not want the external path of the cache to be propagated to the user.
	dataReadWriteBucket := storage.NoExternalPathReadBucket(
		storage.MapReadWriteBucket(
			m.dataReadWriteBucket,
			storage.MapOnPrefix(modulePath),
		),
	)
	exists, err := storage.Exists(ctx, dataReadWriteBucket, buflock.ExternalConfigFilePath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, storage.NewErrNotExist(modulePath)
	}
	module, err := bufmodule.NewModuleForBucket(
		ctx,
		dataReadWriteBucket,
		bufmodule.ModuleWithModuleIdentityAndCommit(modulePin, modulePin.Commit()),
	)
	if err != nil {
		return nil, err
	}
	storedDigestData, err := storage.ReadPath(ctx, m.sumReadWriteBucket, modulePath)
	if err != nil {
		// This can happen if we couldn't find the sum file, which means
		// we are in an invalid state
		if storage.IsNotExist(err) {
			m.logger.Sugar().Warnf(
				"Module %q has invalid cache state: no stored digest could be found. The cache will attempt to self-correct.",
				modulePin.String(),
			)
			// We want to return ErrNotExist so that the ModuleReader can re-download
			return nil, storage.NewErrNotExist(modulePath)
		}
		return nil, err
	}
	storedDigest := string(storedDigestData)
	// This can happen if we couldn't find the sum file, which means
	// we are in an invalid state
	if storedDigest == "" {
		m.logger.Sugar().Warnf(
			"Module %q has invalid cache state: no stored digest could be found. The cache will attempt to self-correct.",
			modulePin.String(),
		)
		// We want to return ErrNotExist so that the ModuleReader can re-download
		// Note that we deal with invalid data in the cache at the ModuleReader level by overwriting via PutModule
		return nil, storage.NewErrNotExist(modulePath)
	}
	digest, err := bufmodule.ModuleDigestB2(ctx, module)
	if err != nil {
		return nil, err
	}
	if digest != storedDigest {
		m.logger.Sugar().Warnf(
			"Module %q has invalid cache state: calculated digest %q does not match stored digest %q. The cache will attempt to self-correct.",
			modulePin.String(),
			digest,
			storedDigest,
		)
		// We want to return ErrNotExist so that the ModuleReader can re-download
		// Note that we deal with invalid data in the cache at the ModuleReader level by overwriting via PutModule
		return nil, storage.NewErrNotExist(modulePath)
	}
	return module, nil
}

func (m *moduleCacher) PutModule(
	ctx context.Context,
	modulePin bufmodule.ModulePin,
	module bufmodule.Module,
) error {
	modulePath := newCacheKey(modulePin)
	digest, err := bufmodule.ModuleDigestB2(ctx, module)
	if err != nil {
		return err
	}
	dataReadWriteBucket := storage.MapReadWriteBucket(
		m.dataReadWriteBucket,
		storage.MapOnPrefix(modulePath),
	)
	exists, err := storage.Exists(ctx, dataReadWriteBucket, buflock.ExternalConfigFilePath)
	if err != nil {
		return err
	}
	if exists {
		// If the module already exists in the cache, we want to make sure we delete it
		// before putting new data
		if err := dataReadWriteBucket.DeleteAll(ctx, ""); err != nil {
			return err
		}
	}
	if err := bufmodule.ModuleToBucket(ctx, module, dataReadWriteBucket); err != nil {
		return err
	}
	// This will overwrite if necessary
	if err := storage.PutPath(ctx, m.sumReadWriteBucket, modulePath, []byte(digest)); err != nil {
		return multierr.Append(
			err,
			// Try to clean up after ourselves.
			dataReadWriteBucket.DeleteAll(ctx, ""),
		)
	}
	return nil
}
