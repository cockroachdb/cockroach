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

package bufwire

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/buf/private/buf/bufconfig"
	"github.com/bufbuild/buf/private/buf/buffetch"
	"github.com/bufbuild/buf/private/buf/bufwork"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule/bufmodulebuild"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type moduleConfigReader struct {
	logger                  *zap.Logger
	storageosProvider       storageos.Provider
	fetchReader             buffetch.Reader
	configProvider          bufconfig.Provider
	workspaceConfigProvider bufwork.Provider
	moduleBucketBuilder     bufmodulebuild.ModuleBucketBuilder
}

func newModuleConfigReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	fetchReader buffetch.Reader,
	configProvider bufconfig.Provider,
	workspaceConfigProvider bufwork.Provider,
	moduleBucketBuilder bufmodulebuild.ModuleBucketBuilder,
) *moduleConfigReader {
	return &moduleConfigReader{
		logger:                  logger,
		storageosProvider:       storageosProvider,
		fetchReader:             fetchReader,
		configProvider:          configProvider,
		workspaceConfigProvider: workspaceConfigProvider,
		moduleBucketBuilder:     moduleBucketBuilder,
	}
}

func (m *moduleConfigReader) GetModuleConfigs(
	ctx context.Context,
	container app.EnvStdinContainer,
	sourceOrModuleRef buffetch.SourceOrModuleRef,
	configOverride string,
	externalDirOrFilePaths []string,
	externalDirOrFilePathsAllowNotExist bool,
) ([]ModuleConfig, error) {
	ctx, span := trace.StartSpan(ctx, "get_module_config")
	defer span.End()
	switch t := sourceOrModuleRef.(type) {
	case buffetch.SourceRef:
		return m.getSourceModuleConfigs(
			ctx,
			container,
			t,
			configOverride,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
	case buffetch.ModuleRef:
		moduleConfig, err := m.getModuleModuleConfig(
			ctx,
			container,
			t,
			configOverride,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
		if err != nil {
			return nil, err
		}
		return []ModuleConfig{
			moduleConfig,
		}, nil
	default:
		return nil, fmt.Errorf("invalid ref: %T", sourceOrModuleRef)
	}
}

func (m *moduleConfigReader) getSourceModuleConfigs(
	ctx context.Context,
	container app.EnvStdinContainer,
	sourceRef buffetch.SourceRef,
	configOverride string,
	externalDirOrFilePaths []string,
	externalDirOrFilePathsAllowNotExist bool,
) (_ []ModuleConfig, retErr error) {
	readBucketCloser, err := m.fetchReader.GetSourceBucket(ctx, container, sourceRef)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = multierr.Append(retErr, readBucketCloser.Close())
	}()
	existingConfigFilePath, err := bufwork.ExistingConfigFilePath(ctx, readBucketCloser)
	if err != nil {
		return nil, err
	}
	if existingConfigFilePath != "" {
		return m.getWorkspaceModuleConfigs(
			ctx,
			sourceRef,
			readBucketCloser,
			readBucketCloser.RelativeRootPath(),
			readBucketCloser.SubDirPath(),
			configOverride,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
	}
	moduleConfig, err := m.getSourceModuleConfig(
		ctx,
		sourceRef,
		readBucketCloser,
		readBucketCloser.RelativeRootPath(),
		readBucketCloser.SubDirPath(),
		configOverride,
		nil,
		nil,
		externalDirOrFilePaths,
		externalDirOrFilePathsAllowNotExist,
	)
	if err != nil {
		return nil, err
	}
	return []ModuleConfig{
		moduleConfig,
	}, nil
}

func (m *moduleConfigReader) getModuleModuleConfig(
	ctx context.Context,
	container app.EnvStdinContainer,
	moduleRef buffetch.ModuleRef,
	configOverride string,
	externalDirOrFilePaths []string,
	externalDirOrFilePathsAllowNotExist bool,
) (_ ModuleConfig, retErr error) {
	module, err := m.fetchReader.GetModule(ctx, container, moduleRef)
	if err != nil {
		return nil, err
	}
	if len(externalDirOrFilePaths) > 0 {
		targetPaths := make([]string, len(externalDirOrFilePaths))
		for i, externalDirOrFilePath := range externalDirOrFilePaths {
			targetPath, err := moduleRef.PathForExternalPath(externalDirOrFilePath)
			if err != nil {
				return nil, err
			}
			targetPaths[i] = targetPath
		}
		if externalDirOrFilePathsAllowNotExist {
			module, err = bufmodule.ModuleWithTargetPathsAllowNotExist(module, targetPaths)
			if err != nil {
				return nil, err
			}
		} else {
			module, err = bufmodule.ModuleWithTargetPaths(module, targetPaths)
			if err != nil {
				return nil, err
			}
		}
	}
	// TODO: we should read the config from the module when configuration
	// is added to modules
	readWriteBucket, err := m.storageosProvider.NewReadWriteBucket(
		".",
		storageos.ReadWriteBucketWithSymlinksIfSupported(),
	)
	if err != nil {
		return nil, err
	}
	config, err := bufconfig.ReadConfig(
		ctx,
		m.configProvider,
		readWriteBucket,
		bufconfig.ReadConfigWithOverride(configOverride),
	)
	if err != nil {
		return nil, err
	}
	return newModuleConfig(module, config, nil /* Workspaces aren't supported for ModuleRefs */), nil
}

func (m *moduleConfigReader) getWorkspaceModuleConfigs(
	ctx context.Context,
	sourceRef buffetch.SourceRef,
	readBucket storage.ReadBucket,
	relativeRootPath string,
	subDirPath string,
	configOverride string,
	externalDirOrFilePaths []string,
	externalDirOrFilePathsAllowNotExist bool,
) ([]ModuleConfig, error) {
	workspaceConfig, err := m.workspaceConfigProvider.GetConfig(ctx, readBucket, relativeRootPath)
	if err != nil {
		return nil, err
	}
	if subDirPath != "." {
		// There's only a single ModuleConfig based on the subDirPath,
		// so we only need to create a single workspace.
		workspace, err := bufwork.NewWorkspace(
			ctx,
			workspaceConfig,
			readBucket,
			m.configProvider,
			m.moduleBucketBuilder,
			relativeRootPath,
			subDirPath,
			configOverride,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
		if err != nil {
			return nil, err
		}
		moduleConfig, err := m.getSourceModuleConfig(
			ctx,
			sourceRef,
			readBucket,
			relativeRootPath,
			subDirPath,
			configOverride,
			workspaceConfig,
			workspace,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
		if err != nil {
			return nil, err
		}
		return []ModuleConfig{
			moduleConfig,
		}, nil
	}
	// The target subDirPath points to the workspace configuration,
	// so we construct a separate workspace for each of the configured
	// directories.
	var moduleConfigs []ModuleConfig
	for _, directory := range workspaceConfig.Directories {
		// TODO: We need to construct a separate workspace for each module,
		// but this is fairly duplicative in its current state. Specifically,
		// we build the same module multiple times.
		//
		// We can refactor this with a bufworkbuild.WorkspaceBuilder that
		// caches modules so that workspace modules are only ever built once.
		workspace, err := bufwork.NewWorkspace(
			ctx,
			workspaceConfig,
			readBucket,
			m.configProvider,
			m.moduleBucketBuilder,
			relativeRootPath,
			directory,
			configOverride,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
		if err != nil {
			return nil, err
		}
		moduleConfig, err := m.getSourceModuleConfig(
			ctx,
			sourceRef,
			readBucket,
			relativeRootPath,
			directory,
			configOverride,
			workspaceConfig,
			workspace,
			externalDirOrFilePaths,
			externalDirOrFilePathsAllowNotExist,
		)
		if err != nil {
			return nil, err
		}
		moduleConfigs = append(moduleConfigs, moduleConfig)
	}
	return moduleConfigs, nil
}

func (m *moduleConfigReader) getSourceModuleConfig(
	ctx context.Context,
	sourceRef buffetch.SourceRef,
	readBucket storage.ReadBucket,
	relativeRootPath string,
	subDirPath string,
	configOverride string,
	workspaceConfig *bufwork.Config,
	workspace bufmodule.Workspace,
	externalDirOrFilePaths []string,
	externalDirOrFilePathsAllowNotExist bool,
) (ModuleConfig, error) {
	mappedReadBucket := readBucket
	if subDirPath != "." {
		mappedReadBucket = storage.MapReadBucket(readBucket, storage.MapOnPrefix(subDirPath))
	}
	moduleConfig, err := bufconfig.ReadConfig(
		ctx,
		m.configProvider,
		mappedReadBucket,
		bufconfig.ReadConfigWithOverride(configOverride),
	)
	if err != nil {
		return nil, err
	}
	var buildOptions []bufmodulebuild.BuildOption
	if len(externalDirOrFilePaths) > 0 {
		// We have at least one --path filter, so we need to determine if the subDirPath is
		// contained within a workspace directory.
		if workspaceDirectoryEqualsOrContainsSubDirPath(workspaceConfig, subDirPath) {
			// We have to do this ahead of time as we are not using PathForExternalPath
			// in this if branch. This is really bad.
			for _, externalDirOrFilePath := range externalDirOrFilePaths {
				if _, err := sourceRef.PathForExternalPath(externalDirOrFilePath); err != nil {
					return nil, err
				}
			}
			// The subDirPath is contained within one of the workspace directories, so
			// we first need to reformat the externalDirOrFilePaths so that they accommodate
			// for the relativeRootPath (the path to the directory containing the buf.work.yaml).
			//
			// For example,
			//
			//  $ buf build ../../proto --path ../../proto/buf
			//
			//  // buf.work.yaml
			//  version: v1
			//  directories:
			//    - proto
			//    - enterprise/proto
			//
			// Note that we CANNOT simply use the sourceRef because we would not be able to
			// determine which workspace directory the paths apply to afterwards. To be clear,
			// if we determined the bucketRelPath from the sourceRef, the bucketRelPath would be equal
			// to ./buf/... which is ambiguous to the workspace directories ('proto' and 'enterprise/proto'
			// in this case).
			buildOptions, err = bufwork.BuildOptionsForWorkspaceDirectory(
				ctx,
				workspaceConfig,
				moduleConfig,
				relativeRootPath,
				subDirPath,
				externalDirOrFilePaths,
				externalDirOrFilePathsAllowNotExist,
			)
			if err != nil {
				return nil, err
			}
		} else {
			// The subDirPath isn't a workspace directory, so we can determine the bucketRelPaths
			// from the sourceRef on its own.
			buildOptions = []bufmodulebuild.BuildOption{
				// We can't determine the module's commit from the local file system.
				// This also may be nil.
				//
				// This is particularly useful for the GoPackage modifier used in
				// Managed Mode, which supports module-specific overrides.
				bufmodulebuild.WithModuleIdentity(moduleConfig.ModuleIdentity),
			}
			bucketRelPaths := make([]string, len(externalDirOrFilePaths))
			for i, externalDirOrFilePath := range externalDirOrFilePaths {
				bucketRelPath, err := sourceRef.PathForExternalPath(externalDirOrFilePath)
				if err != nil {
					return nil, err
				}
				bucketRelPaths[i] = bucketRelPath
			}
			if externalDirOrFilePathsAllowNotExist {
				buildOptions = append(buildOptions, bufmodulebuild.WithPathsAllowNotExist(bucketRelPaths))
			} else {
				buildOptions = append(buildOptions, bufmodulebuild.WithPaths(bucketRelPaths))
			}
		}
	}
	module, err := m.moduleBucketBuilder.BuildForBucket(
		ctx,
		mappedReadBucket,
		moduleConfig.Build,
		buildOptions...,
	)
	if err != nil {
		return nil, err
	}
	if missingReferences := detectMissingDependencies(
		moduleConfig.Build.DependencyModuleReferences,
		module.DependencyModulePins(),
	); len(missingReferences) > 0 {
		var builder strings.Builder
		_, _ = builder.WriteString(`Specified deps are not covered in your buf.lock, run "buf mod update":`)
		for _, moduleReference := range missingReferences {
			_, _ = builder.WriteString("\n\t- " + moduleReference.IdentityString())
		}
		m.logger.Warn(builder.String())
	}
	return newModuleConfig(module, moduleConfig, workspace), nil
}

func workspaceDirectoryEqualsOrContainsSubDirPath(workspaceConfig *bufwork.Config, subDirPath string) bool {
	if workspaceConfig == nil {
		return false
	}
	for _, directory := range workspaceConfig.Directories {
		if normalpath.EqualsOrContainsPath(directory, subDirPath, normalpath.Relative) {
			return true
		}
	}
	return false
}

func detectMissingDependencies(references []bufmodule.ModuleReference, pins []bufmodule.ModulePin) []bufmodule.ModuleReference {
	pinSet := make(map[string]struct{})
	for _, pin := range pins {
		pinSet[pin.IdentityString()] = struct{}{}
	}

	var missingReferences []bufmodule.ModuleReference
	for _, reference := range references {
		if _, ok := pinSet[reference.IdentityString()]; !ok {
			missingReferences = append(missingReferences, reference)
		}
	}
	return missingReferences
}
