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

package storage

import (
	"context"

	"github.com/bufbuild/buf/private/pkg/storage/storageutil"
)

// NoExternalPathReadBucket disables the external paths for the ReadBucket.
//
// This results in ExternalPath just calling Path.
func NoExternalPathReadBucket(readBucket ReadBucket) ReadBucket {
	return newNoExternalPathReadBucket(readBucket)
}

type noExternalPathReadBucket struct {
	delegate ReadBucket
}

func newNoExternalPathReadBucket(
	delegate ReadBucket,
) *noExternalPathReadBucket {
	return &noExternalPathReadBucket{
		delegate: delegate,
	}
}

func (r *noExternalPathReadBucket) Get(ctx context.Context, path string) (ReadObjectCloser, error) {
	readObjectCloser, err := r.delegate.Get(ctx, path)
	// TODO: if this is a path error, we should replace the path
	if err != nil {
		return nil, err
	}
	return disableReadObjectCloserExternalPath(readObjectCloser), nil
}

func (r *noExternalPathReadBucket) Stat(ctx context.Context, path string) (ObjectInfo, error) {
	objectInfo, err := r.delegate.Stat(ctx, path)
	// TODO: if this is a path error, we should replace the path
	if err != nil {
		return nil, err
	}
	return disableObjectInfoExternalPath(objectInfo), nil
}

func (r *noExternalPathReadBucket) Walk(ctx context.Context, prefix string, f func(ObjectInfo) error) error {
	return r.delegate.Walk(
		ctx,
		prefix,
		func(objectInfo ObjectInfo) error {
			return f(disableObjectInfoExternalPath(objectInfo))
		},
	)
}

func disableObjectInfoExternalPath(objectInfo ObjectInfo) ObjectInfo {
	return storageutil.NewObjectInfo(
		objectInfo.Path(),
		objectInfo.Path(),
	)
}

func disableReadObjectCloserExternalPath(readObjectCloser ReadObjectCloser) ReadObjectCloser {
	return compositeReadObjectCloser{disableObjectInfoExternalPath(readObjectCloser), readObjectCloser}
}
