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

package internal

import (
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
)

var _ ReadWriteBucketCloser = &readWriteBucketCloser{}

type readWriteBucketCloser struct {
	storage.ReadWriteBucketCloser

	relativeRootPath string
	subDirPath       string
}

func newReadWriteBucketCloser(
	storageReadWriteBucketCloser storage.ReadWriteBucketCloser,
	relativeRootPath string,
	subDirPath string,
) (*readWriteBucketCloser, error) {
	normalizedSubDirPath, err := normalpath.NormalizeAndValidate(subDirPath)
	if err != nil {
		return nil, err
	}
	return &readWriteBucketCloser{
		ReadWriteBucketCloser: storageReadWriteBucketCloser,
		relativeRootPath:      normalpath.Normalize(relativeRootPath),
		subDirPath:            normalizedSubDirPath,
	}, nil
}

func (r *readWriteBucketCloser) RelativeRootPath() string {
	return r.relativeRootPath
}

func (r *readWriteBucketCloser) SubDirPath() string {
	return r.subDirPath
}
