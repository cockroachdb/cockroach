// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !linux
// +build !linux

package vfs

func preallocExtend(fd uintptr, offset, length int64) error {
	// It is ok for correctness to no-op file preallocation. WAL recycling is the
	// more important mechanism for WAL sync performance and it doesn't rely on
	// fallocate or posix_fallocate in order to be effective.
	return nil
}
