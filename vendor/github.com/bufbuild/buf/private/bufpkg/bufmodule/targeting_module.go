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
	"fmt"

	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/stringutil"
)

type targetingModule struct {
	Module
	targetPaths                    []string
	targetPathsAllowNotExistOnWalk bool
}

func newTargetingModule(
	delegate Module,
	targetPaths []string,
	targetPathsAllowNotExistOnWalk bool,
) (*targetingModule, error) {
	if err := normalpath.ValidatePathsNormalizedValidatedUnique(targetPaths); err != nil {
		return nil, err
	}
	return &targetingModule{
		Module:                         delegate,
		targetPaths:                    targetPaths,
		targetPathsAllowNotExistOnWalk: targetPathsAllowNotExistOnWalk,
	}, nil
}

func (m *targetingModule) TargetFileInfos(ctx context.Context) (fileInfos []FileInfo, retErr error) {
	defer func() {
		if retErr == nil {
			sortFileInfos(fileInfos)
		}
	}()
	sourceReadBucket := m.getSourceReadBucket()
	// potentialDirPaths are paths that we need to check if they are directories
	// these are any files that do not end in .proto, as well as files that
	// end in .proto but do not have a corresponding file in the source ReadBucket
	// if there is not an file the path ending in .proto could be a directory
	// that itself contains files, i.e. a/b.proto/c.proto is valid if not dumb
	var potentialDirPaths []string
	fileInfoPaths := make(map[string]struct{})
	for _, targetPath := range m.targetPaths {
		if normalpath.Ext(targetPath) != ".proto" {
			// not a .proto file, therefore must be a directory
			potentialDirPaths = append(potentialDirPaths, targetPath)
		} else {
			objectInfo, err := sourceReadBucket.Stat(ctx, targetPath)
			if err != nil {
				if !storage.IsNotExist(err) {
					return nil, err
				}
				// we do not have a file, so even though this path ends
				// in .proto,  this could be a directory - we need to check it
				potentialDirPaths = append(potentialDirPaths, targetPath)
			} else {
				// we have a file, therefore the targetPath was a file path
				// add to the nonImportImageFiles if does not already exist
				if _, ok := fileInfoPaths[targetPath]; !ok {
					fileInfoPaths[targetPath] = struct{}{}
					fileInfo, err := NewFileInfo(
						objectInfo.Path(),
						objectInfo.ExternalPath(),
						false,
						m.Module.getModuleIdentity(),
						m.Module.getCommit(),
					)
					if err != nil {
						return nil, err
					}
					fileInfos = append(fileInfos, fileInfo)
				}
			}
		}
	}
	if len(potentialDirPaths) == 0 {
		// we had no potential directory paths as we were able to get
		// an file for all targetPaths, so we can return the FileInfos now
		// this means we do not have to do the expensive O(sourceReadBucketSize) operation
		// to check to see if each file is within a potential directory path
		return fileInfos, nil
	}
	// we have potential directory paths, do the expensive operation
	// make a map of the directory paths
	potentialDirPathMap := stringutil.SliceToMap(potentialDirPaths)
	// the map of paths within potentialDirPath that matches a file
	// this needs to contain all paths in potentialDirPathMap at the end for us to
	// have had matches for every inputted targetPath
	matchingPotentialDirPathMap := make(map[string]struct{})
	if walkErr := sourceReadBucket.Walk(
		ctx,
		"",
		func(objectInfo storage.ObjectInfo) error {
			path := objectInfo.Path()
			// get the paths in potentialDirPathMap that match this path
			fileMatchingPathMap := normalpath.MapAllEqualOrContainingPathMap(
				potentialDirPathMap,
				path,
				normalpath.Relative,
			)
			if len(fileMatchingPathMap) > 0 {
				// we had a match, this means that some path in potentialDirPaths matched
				// the path, add all the paths in potentialDirPathMap that
				// matched to matchingPotentialDirPathMap
				for key := range fileMatchingPathMap {
					matchingPotentialDirPathMap[key] = struct{}{}
				}
				// then, add the file if it is not added
				if _, ok := fileInfoPaths[path]; !ok {
					fileInfoPaths[path] = struct{}{}
					fileInfo, err := NewFileInfo(
						objectInfo.Path(),
						objectInfo.ExternalPath(),
						false,
						m.Module.getModuleIdentity(),
						m.Module.getCommit(),
					)
					if err != nil {
						return err
					}
					fileInfos = append(fileInfos, fileInfo)
				}
			}
			return nil
		},
	); walkErr != nil {
		return nil, walkErr
	}
	// if !allowNotExist, i.e. if all targetPaths must have a matching file,
	// we check the matchingPotentialDirPathMap against the potentialDirPathMap
	// to make sure that potentialDirPathMap is covered
	if !m.targetPathsAllowNotExistOnWalk {
		for potentialDirPath := range potentialDirPathMap {
			if _, ok := matchingPotentialDirPathMap[potentialDirPath]; !ok {
				// no match, this is an error given that allowNotExist is false
				return nil, fmt.Errorf("path %q has no matching file in the module", potentialDirPath)
			}
		}
	}
	return fileInfos, nil
}
