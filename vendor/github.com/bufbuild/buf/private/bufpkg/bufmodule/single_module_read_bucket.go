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

type singleModuleReadBucket struct {
	storage.ReadBucket

	moduleIdentity ModuleIdentity
	commit         string
}

func newSingleModuleReadBucket(
	sourceReadBucket storage.ReadBucket,
	moduleIdentity ModuleIdentity,
	commit string,
) *singleModuleReadBucket {
	return &singleModuleReadBucket{
		ReadBucket:     sourceReadBucket,
		moduleIdentity: moduleIdentity,
		commit:         commit,
	}
}

func (r *singleModuleReadBucket) StatModuleFile(ctx context.Context, path string) (*moduleObjectInfo, error) {
	objectInfo, err := r.ReadBucket.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	return newModuleObjectInfo(objectInfo, r.moduleIdentity, r.commit), nil
}

func (r *singleModuleReadBucket) WalkModuleFiles(ctx context.Context, path string, f func(*moduleObjectInfo) error) error {
	return r.ReadBucket.Walk(
		ctx,
		path,
		func(objectInfo storage.ObjectInfo) error {
			return f(newModuleObjectInfo(objectInfo, r.moduleIdentity, r.commit))
		},
	)
}
