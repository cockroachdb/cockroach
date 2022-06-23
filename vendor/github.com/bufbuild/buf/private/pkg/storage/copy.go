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
	"io"
	"sync"

	"github.com/bufbuild/buf/private/pkg/thread"
	"go.uber.org/multierr"
)

// Copy copies the bucket at from to the bucket at to.
//
// Copies done concurrently.
// Returns the number of files copied.
func Copy(
	ctx context.Context,
	from ReadBucket,
	to WriteBucket,
	options ...CopyOption,
) (int, error) {
	copyOptions := newCopyOptions()
	for _, option := range options {
		option(copyOptions)
	}
	return copyPaths(
		ctx,
		from,
		to,
		copyOptions.externalPaths,
	)
}

// CopyReadObject copies the contents of the ReadObject into the WriteBucket at the path.
func CopyReadObject(
	ctx context.Context,
	writeBucket WriteBucket,
	readObject ReadObject,
	options ...CopyOption,
) (retErr error) {
	copyOptions := newCopyOptions()
	for _, option := range options {
		option(copyOptions)
	}
	return copyReadObject(
		ctx,
		writeBucket,
		readObject,
		copyOptions.externalPaths,
	)
}

// CopyReader copies the contents of the Reader into the WriteBucket at the path.
func CopyReader(
	ctx context.Context,
	writeBucket WriteBucket,
	reader io.Reader,
	path string,
) (retErr error) {
	writeObjectCloser, err := writeBucket.Put(ctx, path)
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, writeObjectCloser.Close())
	}()
	_, err = io.Copy(writeObjectCloser, reader)
	return err
}

// CopyOption is an option for Copy.
type CopyOption func(*copyOptions)

// CopyWithExternalPaths returns a new CopyOption that says to copy external paths.
//
// The to WriteBucket must support setting external paths.
func CopyWithExternalPaths() CopyOption {
	return func(copyOptions *copyOptions) {
		copyOptions.externalPaths = true
	}
}

func copyPaths(
	ctx context.Context,
	from ReadBucket,
	to WriteBucket,
	copyExternalPaths bool,
) (int, error) {
	paths, err := AllPaths(ctx, from, "")
	if err != nil {
		return 0, err
	}
	var count int
	var lock sync.Mutex
	jobs := make([]func(context.Context) error, len(paths))
	for i, path := range paths {
		path := path
		jobs[i] = func(ctx context.Context) error {
			if err := copyPath(ctx, from, to, path, copyExternalPaths); err != nil {
				return err
			}
			lock.Lock()
			count++
			lock.Unlock()
			return nil
		}
	}
	err = thread.Parallelize(ctx, jobs)
	return count, err
}

// copyPath copies the path from the bucket at from to the bucket at to using the given paths.
//
// Paths will be normalized within this function.
func copyPath(
	ctx context.Context,
	from ReadBucket,
	to WriteBucket,
	path string,
	copyExternalPaths bool,
) (retErr error) {
	readObjectCloser, err := from.Get(ctx, path)
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(err, readObjectCloser.Close())
	}()
	return copyReadObject(ctx, to, readObjectCloser, copyExternalPaths)
}

func copyReadObject(
	ctx context.Context,
	writeBucket WriteBucket,
	readObject ReadObject,
	copyExternalPaths bool,
) (retErr error) {
	writeObjectCloser, err := writeBucket.Put(ctx, readObject.Path())
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, writeObjectCloser.Close())
	}()
	if copyExternalPaths {
		if err := writeObjectCloser.SetExternalPath(readObject.ExternalPath()); err != nil {
			return err
		}
	}
	_, err = io.Copy(writeObjectCloser, readObject)
	return err
}

type copyOptions struct {
	externalPaths bool
}

func newCopyOptions() *copyOptions {
	return &copyOptions{}
}
