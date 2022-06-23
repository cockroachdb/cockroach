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

type multiModuleReadBucket struct {
	storage.ReadBucket

	delegates []moduleReadBucket
}

func newMultiModuleReadBucket(
	delegates ...moduleReadBucket,
) *multiModuleReadBucket {
	storageReadBuckets := make([]storage.ReadBucket, len(delegates))
	for i, delegate := range delegates {
		storageReadBuckets[i] = delegate
	}
	return &multiModuleReadBucket{
		ReadBucket: storage.MultiReadBucket(storageReadBuckets...),
		delegates:  delegates,
	}
}

func (m *multiModuleReadBucket) StatModuleFile(ctx context.Context, path string) (*moduleObjectInfo, error) {
	for _, delegate := range m.delegates {
		objectInfo, err := delegate.StatModuleFile(ctx, path)
		if err != nil {
			if storage.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		return objectInfo, nil
	}
	return nil, storage.NewErrNotExist(path)
}

func (m *multiModuleReadBucket) WalkModuleFiles(ctx context.Context, prefix string, f func(*moduleObjectInfo) error) error {
	for _, delegate := range m.delegates {
		if err := delegate.WalkModuleFiles(ctx, prefix, f); err != nil {
			return err
		}
	}
	return nil
}
