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

package buffetch

import (
	"context"
	"io"
	"net/http"

	"github.com/bufbuild/buf/private/buf/buffetch/internal"
	"github.com/bufbuild/buf/private/buf/bufwork"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/git"
	"github.com/bufbuild/buf/private/pkg/httpauth"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.uber.org/zap"
)

type reader struct {
	internalReader internal.Reader
}

func newReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	httpClient *http.Client,
	httpAuthenticator httpauth.Authenticator,
	gitCloner git.Cloner,
	moduleResolver bufmodule.ModuleResolver,
	moduleReader bufmodule.ModuleReader,
) *reader {
	return &reader{
		internalReader: internal.NewReader(
			logger,
			storageosProvider,
			internal.WithReaderHTTP(
				httpClient,
				httpAuthenticator,
			),
			internal.WithReaderGit(
				gitCloner,
			),
			internal.WithReaderLocal(),
			internal.WithReaderStdio(),
			internal.WithReaderModule(
				moduleResolver,
				moduleReader,
			),
		),
	}
}

func newImageReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	httpClient *http.Client,
	httpAuthenticator httpauth.Authenticator,
	gitCloner git.Cloner,
) *reader {
	return &reader{
		internalReader: internal.NewReader(
			logger,
			storageosProvider,
			internal.WithReaderHTTP(
				httpClient,
				httpAuthenticator,
			),
			internal.WithReaderLocal(),
			internal.WithReaderStdio(),
		),
	}
}

func newSourceReader(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	httpClient *http.Client,
	httpAuthenticator httpauth.Authenticator,
	gitCloner git.Cloner,
) *reader {
	return &reader{
		internalReader: internal.NewReader(
			logger,
			storageosProvider,
			internal.WithReaderHTTP(
				httpClient,
				httpAuthenticator,
			),
			internal.WithReaderGit(
				gitCloner,
			),
			internal.WithReaderLocal(),
			internal.WithReaderStdio(),
		),
	}
}

func newModuleFetcher(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	moduleResolver bufmodule.ModuleResolver,
	moduleReader bufmodule.ModuleReader,
) *reader {
	return &reader{
		internalReader: internal.NewReader(
			logger,
			storageosProvider,
			internal.WithReaderModule(
				moduleResolver,
				moduleReader,
			),
		),
	}
}

func (a *reader) GetImageFile(
	ctx context.Context,
	container app.EnvStdinContainer,
	imageRef ImageRef,
) (io.ReadCloser, error) {
	return a.internalReader.GetFile(ctx, container, imageRef.internalFileRef())
}

func (a *reader) GetSourceBucket(
	ctx context.Context,
	container app.EnvStdinContainer,
	sourceRef SourceRef,
	options ...GetSourceBucketOption,
) (ReadBucketCloser, error) {
	getSourceBucketOptions := &getSourceBucketOptions{}
	for _, option := range options {
		option(getSourceBucketOptions)
	}
	var getBucketOptions []internal.GetBucketOption
	if !getSourceBucketOptions.workspacesDisabled {
		getBucketOptions = append(
			getBucketOptions,
			internal.WithGetBucketTerminateFileNames(bufwork.AllConfigFilePaths...),
		)
	}
	return a.internalReader.GetBucket(
		ctx,
		container,
		sourceRef.internalBucketRef(),
		getBucketOptions...,
	)
}

func (a *reader) GetModule(
	ctx context.Context,
	container app.EnvStdinContainer,
	moduleRef ModuleRef,
) (bufmodule.Module, error) {
	return a.internalReader.GetModule(ctx, container, moduleRef.internalModuleRef())
}
