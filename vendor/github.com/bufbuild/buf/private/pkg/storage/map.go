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
	"errors"
	"fmt"
	"io"

	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage/storageutil"
)

// MapReadBucket maps the ReadBucket.
//
// If the Mappers are empty, the original ReadBucket is returned.
// If there is more than one Mapper, the Mappers are called in order
// for UnmapFullPath, with the order reversed for MapPath and MapPrefix.
//
// That is, order these assuming you are starting with a full path and
// working to a path.
func MapReadBucket(readBucket ReadBucket, mappers ...Mapper) ReadBucket {
	if len(mappers) == 0 {
		return readBucket
	}
	return newMapReadBucket(readBucket, MapChain(mappers...))
}

// MapWriteBucket maps the WriteBucket.
//
// If the Mappers are empty, the original WriteBucket is returned.
// If there is more than one Mapper, the Mappers are called in order
// for UnmapFullPath, with the order reversed for MapPath and MapPrefix.
//
// That is, order these assuming you are starting with a full path and
// working to a path.
//
// If a path that does not match is called for Put, an error is returned.
func MapWriteBucket(writeBucket WriteBucket, mappers ...Mapper) WriteBucket {
	if len(mappers) == 0 {
		return writeBucket
	}
	return newMapWriteBucket(writeBucket, MapChain(mappers...))
}

// MapReadWriteBucket maps the ReadWriteBucket.
//
// If the Mappers are empty, the original ReadWriteBucket is returned.
// If there is more than one Mapper, the Mappers are called in order
// for UnmapFullPath, with the order reversed for MapPath and MapPrefix.
//
// That is, order these assuming you are starting with a full path and
// working to a path.
func MapReadWriteBucket(readWriteBucket ReadWriteBucket, mappers ...Mapper) ReadWriteBucket {
	if len(mappers) == 0 {
		return readWriteBucket
	}
	mapper := MapChain(mappers...)
	return compositeReadWriteBucket{
		newMapReadBucket(readWriteBucket, mapper),
		newMapWriteBucket(readWriteBucket, mapper),
	}
}

type mapReadBucket struct {
	delegate ReadBucket
	mapper   Mapper
}

func newMapReadBucket(
	delegate ReadBucket,
	mapper Mapper,
) *mapReadBucket {
	return &mapReadBucket{
		delegate: delegate,
		mapper:   mapper,
	}
}

func (r *mapReadBucket) Get(ctx context.Context, path string) (ReadObjectCloser, error) {
	fullPath, err := r.getFullPath(path)
	if err != nil {
		return nil, err
	}
	readObjectCloser, err := r.delegate.Get(ctx, fullPath)
	// TODO: if this is a path error, we should replace the path
	if err != nil {
		return nil, err
	}
	return replaceReadObjectCloserPath(readObjectCloser, path), nil
}

func (r *mapReadBucket) Stat(ctx context.Context, path string) (ObjectInfo, error) {
	fullPath, err := r.getFullPath(path)
	if err != nil {
		return nil, err
	}
	objectInfo, err := r.delegate.Stat(ctx, fullPath)
	// TODO: if this is a path error, we should replace the path
	if err != nil {
		return nil, err
	}
	return replaceObjectInfoPath(objectInfo, path), nil
}

func (r *mapReadBucket) Walk(ctx context.Context, prefix string, f func(ObjectInfo) error) error {
	prefix, err := normalpath.NormalizeAndValidate(prefix)
	if err != nil {
		return err
	}
	fullPrefix, matches := r.mapper.MapPrefix(prefix)
	if !matches {
		return nil
	}
	return r.delegate.Walk(
		ctx,
		fullPrefix,
		func(objectInfo ObjectInfo) error {
			path, matches, err := r.mapper.UnmapFullPath(objectInfo.Path())
			if err != nil {
				return err
			}
			if !matches {
				return nil
			}
			return f(replaceObjectInfoPath(objectInfo, path))
		},
	)
}

func (r *mapReadBucket) getFullPath(path string) (string, error) {
	path, err := normalpath.NormalizeAndValidate(path)
	if err != nil {
		return "", err
	}
	if path == "." {
		return "", errors.New("cannot get root")
	}
	fullPath, matches := r.mapper.MapPath(path)
	if !matches {
		return "", NewErrNotExist(path)
	}
	return fullPath, nil
}

type mapWriteBucket struct {
	delegate WriteBucket
	mapper   Mapper
}

func newMapWriteBucket(
	delegate WriteBucket,
	mapper Mapper,
) *mapWriteBucket {
	return &mapWriteBucket{
		delegate: delegate,
		mapper:   mapper,
	}
}

func (w *mapWriteBucket) Put(ctx context.Context, path string) (WriteObjectCloser, error) {
	fullPath, err := w.getFullPath(path)
	if err != nil {
		return nil, err
	}
	writeObjectCloser, err := w.delegate.Put(ctx, fullPath)
	// TODO: if this is a path error, we should replace the path
	if err != nil {
		return nil, err
	}
	return replaceWriteObjectCloserExternalPathNotSupported(writeObjectCloser), nil
}

func (w *mapWriteBucket) Delete(ctx context.Context, path string) error {
	fullPath, err := w.getFullPath(path)
	if err != nil {
		return err
	}
	return w.delegate.Delete(ctx, fullPath)
}

func (w *mapWriteBucket) DeleteAll(ctx context.Context, prefix string) error {
	prefix, err := normalpath.NormalizeAndValidate(prefix)
	if err != nil {
		return err
	}
	fullPrefix, matches := w.mapper.MapPrefix(prefix)
	if !matches {
		return nil
	}
	return w.delegate.DeleteAll(ctx, fullPrefix)
}

func (*mapWriteBucket) SetExternalPathSupported() bool {
	return false
}

func (w *mapWriteBucket) getFullPath(path string) (string, error) {
	path, err := normalpath.NormalizeAndValidate(path)
	if err != nil {
		return "", err
	}
	if path == "." {
		return "", errors.New("cannot get root")
	}
	fullPath, matches := w.mapper.MapPath(path)
	if !matches {
		return "", fmt.Errorf("path does not match: %s", path)
	}
	return fullPath, nil
}

func replaceObjectInfoPath(objectInfo ObjectInfo, path string) ObjectInfo {
	if objectInfo.Path() == path {
		return objectInfo
	}
	return storageutil.NewObjectInfo(
		path,
		objectInfo.ExternalPath(),
	)
}

func replaceReadObjectCloserPath(readObjectCloser ReadObjectCloser, path string) ReadObjectCloser {
	if readObjectCloser.Path() == path {
		return readObjectCloser
	}
	return compositeReadObjectCloser{replaceObjectInfoPath(readObjectCloser, path), readObjectCloser}
}

func replaceWriteObjectCloserExternalPathNotSupported(writeObjectCloser WriteObjectCloser) WriteObjectCloser {
	return writeObjectCloserExternalPathNotSuppoted{writeObjectCloser}
}

type writeObjectCloserExternalPathNotSuppoted struct {
	io.WriteCloser
}

func (writeObjectCloserExternalPathNotSuppoted) SetExternalPath(string) error {
	return ErrSetExternalPathUnsupported
}
