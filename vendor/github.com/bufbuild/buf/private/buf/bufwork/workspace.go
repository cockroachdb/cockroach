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

package bufwork

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/bufbuild/buf/private/buf/bufconfig"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule/bufmodulebuild"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
)

type workspace struct {
	// bufmodule.ModuleIdentity -> bufmodule.Module
	namedModules map[string]bufmodule.Module
	allModules   []bufmodule.Module
}

func newWorkspace(
	ctx context.Context,
	workspaceConfig *Config,
	readBucket storage.ReadBucket,
	configProvider bufconfig.Provider,
	moduleBucketBuilder bufmodulebuild.ModuleBucketBuilder,
	relativeRootPath string,
	targetSubDirPath string,
	configOverride string,
	externalDirOrFilePaths []string,
	externalDirOrFilePathsAllowNotExist bool,
) (*workspace, error) {
	if workspaceConfig == nil {
		return nil, errors.New("received a nil workspace config")
	}
	// We know that if the file is actually buf.work for legacy reasons, this will be wrong,
	// but we accept that as this shouldn't happen often anymore and this is just
	// used for error messages.
	workspaceID := filepath.Join(normalpath.Unnormalize(relativeRootPath), ExternalConfigV1FilePath)
	namedModules := make(map[string]bufmodule.Module, len(workspaceConfig.Directories))
	allModules := make([]bufmodule.Module, 0, len(workspaceConfig.Directories))
	for _, directory := range workspaceConfig.Directories {
		if err := validateWorkspaceDirectoryNonEmpty(ctx, readBucket, directory, workspaceID); err != nil {
			return nil, err
		}
		if directory == targetSubDirPath {
			// We don't want to include the module found at the targetSubDirPath
			// since it would otherwise be included twice.
			continue
		}
		if err := validateInputOverlap(directory, targetSubDirPath, workspaceID); err != nil {
			return nil, err
		}
		readBucketForDirectory := storage.MapReadBucket(readBucket, storage.MapOnPrefix(directory))
		moduleConfig, err := bufconfig.ReadConfig(
			ctx,
			configProvider,
			readBucketForDirectory,
			bufconfig.ReadConfigWithOverride(configOverride),
		)
		if err != nil {
			return nil, fmt.Errorf(
				`failed to get module config for directory "%s" listed in %s: %w`,
				normalpath.Unnormalize(directory),
				workspaceID,
				err,
			)
		}
		buildOptions, err := BuildOptionsForWorkspaceDirectory(
			ctx,
			workspaceConfig,
			moduleConfig,
			relativeRootPath,
			directory,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
		if err != nil {
			return nil, err
		}
		module, err := moduleBucketBuilder.BuildForBucket(
			ctx,
			readBucketForDirectory,
			moduleConfig.Build,
			buildOptions...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				`failed to initialize module for directory "%s" listed in %s: %w`,
				normalpath.Unnormalize(directory),
				workspaceID,
				err,
			)
		}
		if moduleIdentity := moduleConfig.ModuleIdentity; moduleIdentity != nil {
			if _, ok := namedModules[moduleIdentity.IdentityString()]; ok {
				return nil, fmt.Errorf(
					"module %q is provided by multiple workspace directories listed in %s",
					moduleIdentity.IdentityString(),
					workspaceID,
				)
			}
			namedModules[moduleIdentity.IdentityString()] = module
		}
		allModules = append(allModules, module)
	}
	return &workspace{
		namedModules: namedModules,
		allModules:   allModules,
	}, nil
}

func (w *workspace) GetModule(moduleIdentity bufmodule.ModuleIdentity) (bufmodule.Module, bool) {
	module, ok := w.namedModules[moduleIdentity.IdentityString()]
	return module, ok
}

func (w *workspace) GetModules() []bufmodule.Module {
	return w.allModules
}

func validateWorkspaceDirectoryNonEmpty(
	ctx context.Context,
	readBucket storage.ReadBucket,
	workspaceDirectory string,
	workspaceID string,
) error {
	isEmpty, err := storage.IsEmpty(
		ctx,
		storage.MapReadBucket(readBucket, storage.MatchPathExt(".proto")),
		"",
	)
	if err != nil {
		return err
	}
	if isEmpty {
		return fmt.Errorf(
			`directory "%s" listed in %s contains no .proto files`,
			normalpath.Unnormalize(workspaceDirectory),
			workspaceID,
		)
	}
	return nil
}

// validateInputOverlap returns a non-nil error if the given directories
// overlap in either direction. The last argument is only used for
// error reporting.
//
//  validateInputOverlap("foo", "bar", "buf.work.yaml")     -> OK
//  validateInputOverlap("foo/bar", "foo", "buf.work.yaml") -> NOT OK
//  validateInputOverlap("foo", "foo/bar", "buf.work.yaml") -> NOT OK
func validateInputOverlap(
	workspaceDirectory string,
	targetSubDirPath string,
	workspaceID string,
) error {
	if normalpath.ContainsPath(workspaceDirectory, targetSubDirPath, normalpath.Relative) {
		return fmt.Errorf(
			`failed to build input "%s" because it is contained by directory "%s" listed in %s`,
			normalpath.Unnormalize(targetSubDirPath),
			normalpath.Unnormalize(workspaceDirectory),
			workspaceID,
		)
	}

	if normalpath.ContainsPath(targetSubDirPath, workspaceDirectory, normalpath.Relative) {
		return fmt.Errorf(
			`failed to build input "%s" because it contains directory "%s" listed in %s`,
			normalpath.Unnormalize(targetSubDirPath),
			normalpath.Unnormalize(workspaceDirectory),
			workspaceID,
		)
	}
	return nil
}
