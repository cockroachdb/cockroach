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

package bufmodule

import (
	"context"

	"github.com/bufbuild/buf/private/pkg/storage"
)

var _ ModuleFileSet = &moduleFileSet{}

type moduleFileSet struct {
	Module

	allModuleReadBucket moduleReadBucket
}

func newModuleFileSet(
	module Module,
	dependencies []Module,
) *moduleFileSet {
	// TODO: We can remove the getModuleRef method on the
	// Module type if we fetch FileInfos from the Module
	// and plumb in the ModuleRef here.
	//
	// This approach assumes that all of the FileInfos returned
	// from SourceFileInfos will have their ModuleRef
	// set to the same value. That can be enforced here.
	moduleReadBuckets := []moduleReadBucket{
		newSingleModuleReadBucket(
			module.getSourceReadBucket(),
			module.getModuleIdentity(),
			module.getCommit(),
		),
	}
	for _, dependency := range dependencies {
		moduleReadBuckets = append(
			moduleReadBuckets,
			newSingleModuleReadBucket(
				dependency.getSourceReadBucket(),
				dependency.getModuleIdentity(),
				dependency.getCommit(),
			),
		)
	}
	return &moduleFileSet{
		Module:              module,
		allModuleReadBucket: newMultiModuleReadBucket(moduleReadBuckets...),
	}
}

func (m *moduleFileSet) AllFileInfos(ctx context.Context) ([]FileInfo, error) {
	var fileInfos []FileInfo
	if walkErr := m.allModuleReadBucket.WalkModuleFiles(ctx, "", func(moduleObjectInfo *moduleObjectInfo) error {
		if err := ValidateModuleFilePath(moduleObjectInfo.Path()); err != nil {
			return err
		}
		isNotImport, err := storage.Exists(ctx, m.Module.getSourceReadBucket(), moduleObjectInfo.Path())
		if err != nil {
			return err
		}
		fileInfo, err := NewFileInfo(
			moduleObjectInfo.Path(),
			moduleObjectInfo.ExternalPath(),
			!isNotImport,
			moduleObjectInfo.ModuleIdentity(),
			moduleObjectInfo.Commit(),
		)
		if err != nil {
			return err
		}
		fileInfos = append(fileInfos, fileInfo)
		return nil
	}); walkErr != nil {
		return nil, walkErr
	}
	sortFileInfos(fileInfos)
	return fileInfos, nil
}

func (m *moduleFileSet) GetModuleFile(ctx context.Context, path string) (ModuleFile, error) {
	if err := ValidateModuleFilePath(path); err != nil {
		return nil, err
	}
	readObjectCloser, err := m.allModuleReadBucket.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	isNotImport, err := storage.Exists(ctx, m.Module.getSourceReadBucket(), path)
	if err != nil {
		return nil, err
	}
	moduleObjectInfo, err := m.allModuleReadBucket.StatModuleFile(ctx, path)
	if err != nil {
		return nil, err
	}
	fileInfo, err := NewFileInfo(
		readObjectCloser.Path(),
		readObjectCloser.ExternalPath(),
		!isNotImport,
		moduleObjectInfo.ModuleIdentity(),
		moduleObjectInfo.Commit(),
	)
	if err != nil {
		return nil, err
	}
	return newModuleFile(fileInfo, readObjectCloser), nil
}

func (*moduleFileSet) isModuleFileSet() {}
