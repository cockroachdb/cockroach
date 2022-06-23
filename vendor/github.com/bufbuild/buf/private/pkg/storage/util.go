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
	"io"
	"sort"

	"go.uber.org/multierr"
)

// errIsNotEmpty is used to break out of the Walk function early in IsEmpty.
//
// If errors.Is(err, errIsNotEmpty), the Walk function found a file.
var errIsNotEmpty = errors.New("__is_not_empty__")

// ReadPath is analogous to os.ReadFile.
//
// Returns an error that fufills IsNotExist if the path does not exist.
func ReadPath(ctx context.Context, readBucket ReadBucket, path string) (_ []byte, retErr error) {
	readObject, err := readBucket.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := readObject.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	return io.ReadAll(readObject)
}

// PutPath puts the data at the path.
func PutPath(ctx context.Context, writeBucket WriteBucket, path string, data []byte) (retErr error) {
	writeObject, err := writeBucket.Put(ctx, path)
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, writeObject.Close())
	}()
	_, err = writeObject.Write(data)
	return err
}

// WalkReadObjects walks the bucket and calls get on each, closing the resulting ReadObjectCloser
// when done.
func WalkReadObjects(
	ctx context.Context,
	readBucket ReadBucket,
	prefix string,
	f func(ReadObject) error,
) error {
	return readBucket.Walk(
		ctx,
		prefix,
		func(objectInfo ObjectInfo) error {
			readObjectCloser, err := readBucket.Get(ctx, objectInfo.Path())
			if err != nil {
				return err
			}
			return multierr.Append(f(readObjectCloser), readObjectCloser.Close())
		},
	)
}

// AllPaths walks the bucket and gets all the paths.
func AllPaths(ctx context.Context, readBucket ReadBucket, prefix string) ([]string, error) {
	var allPaths []string
	if err := readBucket.Walk(
		ctx,
		prefix,
		func(objectInfo ObjectInfo) error {
			allPaths = append(allPaths, objectInfo.Path())
			return nil
		},
	); err != nil {
		return nil, err
	}
	return allPaths, nil
}

// Exists returns true if the path exists, false otherwise.
//
// Returns error on system error.
func Exists(ctx context.Context, readBucket ReadBucket, path string) (bool, error) {
	_, err := readBucket.Stat(ctx, path)
	if err != nil {
		if IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsEmpty returns true if the bucket is empty under the prefix.
//
// A prefix of "" or "." will check if the entire bucket is empty.
func IsEmpty(ctx context.Context, readBucket ReadBucket, prefix string) (bool, error) {
	if err := readBucket.Walk(
		ctx,
		prefix,
		func(ObjectInfo) error {
			return errIsNotEmpty
		},
	); err != nil {
		if errors.Is(err, errIsNotEmpty) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// allObjectInfos walks the bucket and gets all the ObjectInfos.
func allObjectInfos(ctx context.Context, readBucket ReadBucket, prefix string) ([]ObjectInfo, error) {
	var allObjectInfos []ObjectInfo
	if err := readBucket.Walk(
		ctx,
		prefix,
		func(objectInfo ObjectInfo) error {
			allObjectInfos = append(allObjectInfos, objectInfo)
			return nil
		},
	); err != nil {
		return nil, err
	}
	return allObjectInfos, nil
}

func pathToObjectInfo(objectInfos []ObjectInfo) map[string]ObjectInfo {
	m := make(map[string]ObjectInfo, len(objectInfos))
	for _, objectInfo := range objectInfos {
		m[objectInfo.Path()] = objectInfo
	}
	return m
}

func sortObjectInfos(objectInfos []ObjectInfo) {
	sort.Slice(
		objectInfos,
		func(i int, j int) bool {
			return objectInfos[i].Path() < objectInfos[j].Path()
		},
	)
}

type compositeReadObjectCloser struct {
	ObjectInfo
	io.ReadCloser
}

type compositeReadWriteBucket struct {
	ReadBucket
	WriteBucket
}
