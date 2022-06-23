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

// Package storagemem implements an in-memory storage Bucket.
package storagemem

import (
	"github.com/bufbuild/buf/private/pkg/storage"
)

// ReadBucketBuilder builds ReadBuckets.
type ReadBucketBuilder interface {
	storage.WriteBucket
	// ToReadBucket returns a ReadBucket for the current data in the WriteBucket.
	//
	// No further calls can be made to the ReadBucketBuilder after this call.
	// This is functionally equivalent to a Close in other contexts.
	ToReadBucket() (storage.ReadBucket, error)
}

// NewReadBucketBuilder returns a new in-memory ReadBucketBuilder.
func NewReadBucketBuilder() ReadBucketBuilder {
	return newReadBucketBuilder()
}

// NewReadBucket returns a new ReadBucket.
func NewReadBucket(pathToData map[string][]byte) (storage.ReadBucket, error) {
	return newReadBucket(pathToData, nil)
}
