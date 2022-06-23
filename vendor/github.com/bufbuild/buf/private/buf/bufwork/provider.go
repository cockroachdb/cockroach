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
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"github.com/bufbuild/buf/private/pkg/encoding"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type provider struct {
	logger *zap.Logger
}

func newProvider(logger *zap.Logger) *provider {
	return &provider{
		logger: logger,
	}
}

func (p *provider) GetConfig(ctx context.Context, readBucket storage.ReadBucket, relativeRootPath string) (_ *Config, retErr error) {
	ctx, span := trace.StartSpan(ctx, "get_config")
	defer span.End()

	// This will be in the order of precedence.
	var foundConfigFilePaths []string
	// Go through all valid config file paths and see which ones are present.
	// If none are present, return the default config.
	// If multiple are present, error.
	for _, configFilePath := range AllConfigFilePaths {
		exists, err := storage.Exists(ctx, readBucket, configFilePath)
		if err != nil {
			return nil, err
		}
		if exists {
			foundConfigFilePaths = append(foundConfigFilePaths, configFilePath)
		}
	}
	switch len(foundConfigFilePaths) {
	case 0:
		// Did not find anything, return the default.
		return p.newConfigV1(ExternalConfigV1{}, "default configuration")
	case 1:
		workspaceID := filepath.Join(normalpath.Unnormalize(relativeRootPath), foundConfigFilePaths[0])
		readObjectCloser, err := readBucket.Get(ctx, foundConfigFilePaths[0])
		if err != nil {
			return nil, err
		}
		defer func() {
			retErr = multierr.Append(retErr, readObjectCloser.Close())
		}()
		data, err := io.ReadAll(readObjectCloser)
		if err != nil {
			return nil, err
		}
		return p.getConfigForData(
			ctx,
			encoding.UnmarshalYAMLNonStrict,
			encoding.UnmarshalYAMLStrict,
			workspaceID,
			data,
			readObjectCloser.ExternalPath(),
		)
	default:
		return nil, fmt.Errorf("only one workspace file can exist but found multiple workspace files: %s", stringutil.SliceToString(foundConfigFilePaths))
	}
}

func (p *provider) GetConfigForData(ctx context.Context, data []byte) (*Config, error) {
	_, span := trace.StartSpan(ctx, "get_config_for_data")
	defer span.End()
	return p.getConfigForData(
		ctx,
		encoding.UnmarshalJSONOrYAMLNonStrict,
		encoding.UnmarshalJSONOrYAMLStrict,
		"configuration data",
		data,
		"Configuration data",
	)
}

func (p *provider) getConfigForData(
	ctx context.Context,
	unmarshalNonStrict func([]byte, interface{}) error,
	unmarshalStrict func([]byte, interface{}) error,
	workspaceID string,
	data []byte,
	id string,
) (*Config, error) {
	var externalConfigVersion externalConfigVersion
	if err := unmarshalNonStrict(data, &externalConfigVersion); err != nil {
		return nil, err
	}
	if err := p.validateExternalConfigVersion(externalConfigVersion, id); err != nil {
		return nil, err
	}
	var externalConfigV1 ExternalConfigV1
	if err := unmarshalStrict(data, &externalConfigV1); err != nil {
		return nil, err
	}
	return p.newConfigV1(externalConfigV1, workspaceID)
}

func (p *provider) newConfigV1(externalConfig ExternalConfigV1, workspaceID string) (*Config, error) {
	if len(externalConfig.Directories) == 0 {
		return nil, fmt.Errorf(
			`%s has no directories set. Please add "directories: [...]"`,
			workspaceID,
		)
	}
	directorySet := make(map[string]struct{}, len(externalConfig.Directories))
	for _, directory := range externalConfig.Directories {
		normalizedDirectory, err := normalpath.NormalizeAndValidate(directory)
		if err != nil {
			return nil, fmt.Errorf(`directory "%s" listed in %s is invalid: %w`, normalpath.Unnormalize(directory), workspaceID, err)
		}
		if _, ok := directorySet[normalizedDirectory]; ok {
			return nil, fmt.Errorf(
				`directory "%s" is listed more than once in %s`,
				normalpath.Unnormalize(normalizedDirectory),
				workspaceID,
			)
		}
		directorySet[normalizedDirectory] = struct{}{}
	}
	// It's very important that we sort the directories here so that the
	// constructed modules and/or images are in a deterministic order.
	directories := stringutil.MapToSlice(directorySet)
	sort.Slice(directories, func(i int, j int) bool {
		return directories[i] < directories[j]
	})
	if err := validateConfigurationOverlap(directories, workspaceID); err != nil {
		return nil, err
	}
	return &Config{
		Directories: directories,
	}, nil
}

// validateOverlap returns a non-nil error if any of the directories overlap
// with each other. The given directories are expected to be sorted.
func validateConfigurationOverlap(directories []string, workspaceID string) error {
	for i := 0; i < len(directories); i++ {
		for j := i + 1; j < len(directories); j++ {
			left := directories[i]
			right := directories[j]
			if normalpath.ContainsPath(left, right, normalpath.Relative) {
				return fmt.Errorf(
					`directory "%s" contains directory "%s" in %s`,
					normalpath.Unnormalize(left),
					normalpath.Unnormalize(right),
					workspaceID,
				)
			}
			if normalpath.ContainsPath(right, left, normalpath.Relative) {
				return fmt.Errorf(
					`directory "%s" contains directory "%s" in %s`,
					normalpath.Unnormalize(right),
					normalpath.Unnormalize(left),
					workspaceID,
				)
			}
		}
	}
	return nil
}

func (p *provider) validateExternalConfigVersion(externalConfigVersion externalConfigVersion, id string) error {
	switch externalConfigVersion.Version {
	case "":
		return fmt.Errorf(
			`%s has no version set. Please add "version: %s"`,
			id,
			V1Version,
		)
	case V1Version:
		return nil
	default:
		return fmt.Errorf(
			`%s has an invalid "version: %s" set. Please add "version: %s"`,
			id,
			externalConfigVersion.Version,
			V1Version,
		)
	}
}
