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

package storagemem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageutil"
)

type readBucketBuilder struct {
	pathToData         map[string][]byte
	pathToExternalPath map[string]string
	lock               sync.Mutex
}

func newReadBucketBuilder() *readBucketBuilder {
	return &readBucketBuilder{
		pathToData:         make(map[string][]byte),
		pathToExternalPath: make(map[string]string),
	}
}

func (b *readBucketBuilder) Put(ctx context.Context, path string) (storage.WriteObjectCloser, error) {
	path, err := normalpath.NormalizeAndValidate(path)
	if err != nil {
		return nil, err
	}
	if path == "." {
		return nil, errors.New("cannot put root")
	}
	return newWriteObjectCloser(b, path), nil
}

func (b *readBucketBuilder) Delete(ctx context.Context, path string) error {
	path, err := normalpath.NormalizeAndValidate(path)
	if err != nil {
		return err
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	if _, ok := b.pathToData[path]; !ok {
		return storage.NewErrNotExist(path)
	}
	delete(b.pathToData, path)
	delete(b.pathToExternalPath, path)
	return nil
}

func (b *readBucketBuilder) DeleteAll(ctx context.Context, prefix string) error {
	prefix, err := storageutil.ValidatePrefix(prefix)
	if err != nil {
		return err
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	for path := range b.pathToData {
		if normalpath.EqualsOrContainsPath(prefix, path, normalpath.Relative) {
			delete(b.pathToData, path)
			delete(b.pathToExternalPath, path)
		}
	}
	return nil
}

func (*readBucketBuilder) SetExternalPathSupported() bool {
	return true
}

func (b *readBucketBuilder) ToReadBucket() (storage.ReadBucket, error) {
	return newReadBucket(b.pathToData, b.pathToExternalPath)
}

type writeObjectCloser struct {
	readBucketBuilder    *readBucketBuilder
	path                 string
	buffer               *bytes.Buffer
	explicitExternalPath string
	closed               bool
}

func newWriteObjectCloser(
	readBucketBuilder *readBucketBuilder,
	path string,
) *writeObjectCloser {
	return &writeObjectCloser{
		readBucketBuilder: readBucketBuilder,
		path:              path,
		buffer:            bytes.NewBuffer(nil),
	}
}

func (w *writeObjectCloser) Write(p []byte) (int, error) {
	if w.closed {
		return 0, storage.ErrClosed
	}
	return w.buffer.Write(p)
}

func (w *writeObjectCloser) SetExternalPath(externalPath string) error {
	if w.explicitExternalPath != "" {
		// just to make sure
		return fmt.Errorf("external path already set: %q", w.explicitExternalPath)
	}
	w.explicitExternalPath = externalPath
	return nil
}

func (w *writeObjectCloser) Close() error {
	if w.closed {
		return storage.ErrClosed
	}
	w.closed = true
	// overwrites anything existing
	// this is the same behavior as storageos
	w.readBucketBuilder.lock.Lock()
	defer w.readBucketBuilder.lock.Unlock()
	w.readBucketBuilder.pathToData[w.path] = w.buffer.Bytes()
	if w.explicitExternalPath != "" {
		w.readBucketBuilder.pathToExternalPath[w.path] = w.explicitExternalPath
	} else {
		delete(w.readBucketBuilder.pathToExternalPath, w.path)
	}
	return nil
}
