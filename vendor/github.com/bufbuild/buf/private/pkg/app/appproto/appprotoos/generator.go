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

package appprotoos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/appproto"
	"github.com/bufbuild/buf/private/pkg/app/appproto/appprotoexec"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storagearchive"
	"github.com/bufbuild/buf/private/pkg/storage/storagemem"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/pluginpb"
)

// Constants used to create .jar files
var (
	ManifestPath    = normalpath.Join("META-INF", "MANIFEST.MF")
	ManifestContent = []byte(`Manifest-Version: 1.0
Created-By: 1.6.0 (protoc)

`)
)

type generator struct {
	logger            *zap.Logger
	storageosProvider storageos.Provider
}

func newGenerator(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
) *generator {
	return &generator{
		logger:            logger,
		storageosProvider: storageosProvider,
	}
}

func (g *generator) Generate(
	ctx context.Context,
	container app.EnvStderrContainer,
	pluginName string,
	pluginOut string,
	requests []*pluginpb.CodeGeneratorRequest,
	options ...GenerateOption,
) (retErr error) {
	generateOptions := newGenerateOptions()
	for _, option := range options {
		option(generateOptions)
	}
	handler, err := appprotoexec.NewHandler(
		g.logger,
		g.storageosProvider,
		pluginName,
		appprotoexec.HandlerWithPluginPath(generateOptions.pluginPath),
	)
	if err != nil {
		return err
	}
	appprotoGenerator := appproto.NewGenerator(g.logger, handler)
	switch filepath.Ext(pluginOut) {
	case ".jar":
		return g.generateZip(
			ctx,
			container,
			appprotoGenerator,
			pluginOut,
			requests,
			true,
			generateOptions.createOutDirIfNotExists,
		)
	case ".zip":
		return g.generateZip(
			ctx,
			container,
			appprotoGenerator,
			pluginOut,
			requests,
			false,
			generateOptions.createOutDirIfNotExists,
		)
	default:
		return g.generateDirectory(
			ctx,
			container,
			appprotoGenerator,
			pluginOut,
			requests,
			generateOptions.createOutDirIfNotExists,
		)
	}
}

func (g *generator) generateZip(
	ctx context.Context,
	container app.EnvStderrContainer,
	appprotoGenerator appproto.Generator,
	outFilePath string,
	requests []*pluginpb.CodeGeneratorRequest,
	includeManifest bool,
	createOutDirIfNotExists bool,
) (retErr error) {
	outDirPath := filepath.Dir(outFilePath)
	// OK to use os.Stat instead of os.Lstat here
	fileInfo, err := os.Stat(outDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			if createOutDirIfNotExists {
				if err := os.MkdirAll(outDirPath, 0755); err != nil {
					return err
				}
			} else {
				return err
			}
		}
		return err
	} else if !fileInfo.IsDir() {
		return fmt.Errorf("not a directory: %s", outDirPath)
	}
	readBucketBuilder := storagemem.NewReadBucketBuilder()
	if err := appprotoGenerator.Generate(ctx, container, readBucketBuilder, requests); err != nil {
		return err
	}
	if includeManifest {
		if err := storage.PutPath(ctx, readBucketBuilder, ManifestPath, ManifestContent); err != nil {
			return err
		}
	}
	file, err := os.Create(outFilePath)
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, file.Close())
	}()
	readBucket, err := readBucketBuilder.ToReadBucket()
	if err != nil {
		return err
	}
	// protoc does not compress
	return storagearchive.Zip(ctx, readBucket, file, false)
}

func (g *generator) generateDirectory(
	ctx context.Context,
	container app.EnvStderrContainer,
	appprotoGenerator appproto.Generator,
	outDirPath string,
	requests []*pluginpb.CodeGeneratorRequest,
	createOutDirIfNotExists bool,
) error {
	if createOutDirIfNotExists {
		if err := os.MkdirAll(outDirPath, 0755); err != nil {
			return err
		}
	}
	// this checks that the directory exists
	readWriteBucket, err := g.storageosProvider.NewReadWriteBucket(
		outDirPath,
		storageos.ReadWriteBucketWithSymlinksIfSupported(),
	)
	if err != nil {
		return err
	}
	return appprotoGenerator.Generate(
		ctx,
		container,
		readWriteBucket,
		requests,
		appproto.GenerateWithInsertionPointReadBucket(readWriteBucket),
	)
}

type generateOptions struct {
	pluginPath              string
	createOutDirIfNotExists bool
}

func newGenerateOptions() *generateOptions {
	return &generateOptions{}
}
