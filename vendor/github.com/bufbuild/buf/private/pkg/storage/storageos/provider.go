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

package storageos

import "github.com/bufbuild/buf/private/pkg/storage"

type provider struct {
	symlinks bool
}

func newProvider(options ...ProviderOption) *provider {
	provider := &provider{}
	for _, option := range options {
		option(provider)
	}
	return provider
}

func (p *provider) NewReadWriteBucket(rootPath string, options ...ReadWriteBucketOption) (storage.ReadWriteBucket, error) {
	readWriteBucketOptions := newReadWriteBucketOptions()
	for _, option := range options {
		option(readWriteBucketOptions)
	}
	// need both options for symlinks to be enabled
	return newBucket(
		rootPath,
		p.symlinks && readWriteBucketOptions.symlinksIfSupported,
	)
}

// doing this as a separate struct so that it's clear this is resolved
// as a combination of the provider options and read write bucket options
// so there's no potential issues in newBucket
type readWriteBucketOptions struct {
	symlinksIfSupported bool
}

func newReadWriteBucketOptions() *readWriteBucketOptions {
	return &readWriteBucketOptions{}
}
