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

// moduleReadBucket is a ReadBucket that has associated module information.
//
// this is a helper type used in moduleFileSet.
type moduleReadBucket interface {
	storage.ReadBucket

	// StatModuleFile gets info in the object, including info
	// specific to the file's module.
	StatModuleFile(ctx context.Context, path string) (*moduleObjectInfo, error)
	// WalkModuleFiles walks the bucket with the prefix, calling f on
	// each path. If the prefix doesn't exist, this is a no-op.
	WalkModuleFiles(ctx context.Context, path string, f func(*moduleObjectInfo) error) error
}
