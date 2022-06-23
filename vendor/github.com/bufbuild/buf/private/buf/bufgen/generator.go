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

package bufgen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufimage/bufimagemodify"
	"github.com/bufbuild/buf/private/bufpkg/bufplugin"
	"github.com/bufbuild/buf/private/gen/proto/apiclient/buf/alpha/registry/v1alpha1/registryv1alpha1apiclient"
	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/appproto"
	"github.com/bufbuild/buf/private/pkg/app/appproto/appprotoexec"
	"github.com/bufbuild/buf/private/pkg/app/appproto/appprotoos"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storagearchive"
	"github.com/bufbuild/buf/private/pkg/storage/storagemem"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/bufbuild/buf/private/pkg/thread"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/pluginpb"
)

type generator struct {
	logger            *zap.Logger
	storageosProvider storageos.Provider
	registryProvider  registryv1alpha1apiclient.Provider
}

func newGenerator(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	registryProvider registryv1alpha1apiclient.Provider,
) *generator {
	return &generator{
		logger:            logger,
		storageosProvider: storageosProvider,
		registryProvider:  registryProvider,
	}
}

func (g *generator) Generate(
	ctx context.Context,
	container app.EnvStdioContainer,
	config *Config,
	image bufimage.Image,
	options ...GenerateOption,
) error {
	generateOptions := newGenerateOptions()
	for _, option := range options {
		option(generateOptions)
	}
	return g.generate(
		ctx,
		container,
		config,
		image,
		generateOptions.baseOutDirPath,
		generateOptions.includeImports,
	)
}

func (g *generator) generate(
	ctx context.Context,
	container app.EnvStdioContainer,
	config *Config,
	image bufimage.Image,
	baseOutDirPath string,
	includeImports bool,
) error {
	if err := modifyImage(ctx, g.logger, config, image); err != nil {
		return err
	}
	pluginResponses, err := g.generateConcurrently(
		ctx,
		container,
		config,
		image,
		includeImports,
	)
	if err != nil {
		return fmt.Errorf("failed to generate plugins concurrently: %w", err)
	}
	if len(pluginResponses) != len(config.PluginConfigs) {
		return fmt.Errorf("unexpected number of responses, got %d, wanted %d", len(pluginResponses), len(config.PluginConfigs))
	}
	mergedPluginResults, err := bufplugin.MergeInsertionPoints(pluginResponses)
	if err != nil {
		return fmt.Errorf("failed to map insertion points: %w", err)
	}
	if len(mergedPluginResults) != len(config.PluginConfigs) {
		return fmt.Errorf("unexpected number of merged results, got %d, wanted %d", len(mergedPluginResults), len(config.PluginConfigs))
	}
	for i, mergedPluginResult := range mergedPluginResults {
		pluginOut := config.PluginConfigs[i].Out
		if baseOutDirPath != "" && baseOutDirPath != "." {
			pluginOut = filepath.Join(baseOutDirPath, pluginOut)
		}
		switch filepath.Ext(pluginOut) {
		case ".jar":
			if err := g.generateZip(
				ctx,
				pluginOut,
				mergedPluginResult.Files,
				true,
			); err != nil {
				return fmt.Errorf("failed to generate jar: %w", err)
			}
		case ".zip":
			if err := g.generateZip(
				ctx,
				pluginOut,
				mergedPluginResult.Files,
				false,
			); err != nil {
				return fmt.Errorf("failed to generate zip: %w", err)
			}
		default:
			if err := g.generateDirectory(
				ctx,
				pluginOut,
				mergedPluginResult.Files,
			); err != nil {
				return fmt.Errorf("failed to generate directory: %w", err)
			}
		}
	}
	return nil
}

// generateConcurrently starts a separate goroutine for each per-image-directory
// invocation of local plugins, and single grouped invocation of remote plugins,
// to run concurrently, with different and independent concurrency restrictions.
func (g *generator) generateConcurrently(
	ctx context.Context,
	container app.EnvStdioContainer,
	config *Config,
	image bufimage.Image,
	includeImports bool,
) ([]*pluginpb.CodeGeneratorResponse, error) {
	// Cache imagesByDir up-front if at least one plugin requires it
	var imagesByDir []bufimage.Image
	for _, pluginConfig := range config.PluginConfigs {
		if pluginConfig.Strategy != StrategyDirectory {
			continue
		}
		// Already guaranteed by config validation, but best to be safe.
		if pluginConfig.Remote != "" {
			return nil, fmt.Errorf("remote plugin %q cannot set strategy directory", pluginConfig.Remote)
		}
		var err error
		imagesByDir, err = bufimage.ImageByDir(image)
		if err != nil {
			return nil, err
		}
		break
	}
	remoteToPlugins := make(map[string][]*remotePlugin)
	var localPluginResponses []*localPluginResponse
	// Parallelize local and remote plugin execution with an errgroup.
	// Limit concurrent execution of local plugins using semaphore.
	localSemaphore := newSemaphore(thread.Parallelism())
	defer localSemaphore.Discard()
	// Note: this new context will be cancelled after eg.Wait() returns.
	eg, ctx := errgroup.WithContext(ctx)
	// pluginResponses contains the response for each plugin, in the same order
	// as they are specified in the plugin configs. We need to store each response
	// for processing insertion points after all plugins have finished.
	pluginResponses := make([]*pluginpb.CodeGeneratorResponse, len(config.PluginConfigs))
	for i, pluginConfig := range config.PluginConfigs {
		switch {
		case pluginConfig.Name != "": // Local plugin
			var pluginImages []bufimage.Image
			switch pluginConfig.Strategy {
			case StrategyAll:
				pluginImages = []bufimage.Image{image}
			case StrategyDirectory:
				pluginImages = imagesByDir
			default:
				return nil, fmt.Errorf("unknown strategy: %v", pluginConfig.Strategy)
			}
			var handlerOptions []appprotoexec.HandlerOption
			if pluginConfig.Path != "" {
				handlerOptions = append(handlerOptions, appprotoexec.HandlerWithPluginPath(pluginConfig.Path))
			}
			handler, err := appprotoexec.NewHandler(
				g.logger,
				g.storageosProvider,
				pluginConfig.Name,
				handlerOptions...,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create handler: %w", err)
			}
			requests := bufimage.ImagesToCodeGeneratorRequests(
				pluginImages,
				pluginConfig.Opt,
				appprotoexec.DefaultVersion,
				includeImports,
			)
			responseWriter := appproto.NewResponseWriter(container)
			for _, request := range requests {
				// prevent issues with asynchronously executed closures
				request := request
				// Queue up a parallel job for each split of the image, writing to the same response writer.
				eg.Go(func() error {
					// Limit concurrent invocations using semaphore channel, since
					// most of this work is CPU/MEM intensive.
					if err := localSemaphore.Acquire(ctx); err != nil {
						return fmt.Errorf("failed to acquire semaphore: %w", err)
					}
					defer localSemaphore.Release(ctx)
					if err := handler.Handle(ctx, container, responseWriter, request); err != nil {
						return fmt.Errorf("failed to generate: %w", err)
					}
					return nil
				})
			}
			// Responses are not valid until parallelized jobs have finished
			localPluginResponses = append(
				localPluginResponses,
				newLocalPluginResponse(responseWriter, i),
			)
		case pluginConfig.Remote != "": // Remote plugin
			remote, owner, name, version, err := bufplugin.ParsePluginVersionPath(pluginConfig.Remote)
			if err != nil {
				return nil, fmt.Errorf("invalid plugin path: %w", err)
			}
			var parameters []string
			if len(pluginConfig.Opt) > 0 {
				// Only include parameters if they're not empty
				parameters = []string{pluginConfig.Opt}
			}
			// Group remote plugins by remote to execute together.
			remoteToPlugins[remote] = append(
				remoteToPlugins[remote],
				newRemotePlugin(
					&registryv1alpha1.PluginReference{
						Owner:      owner,
						Name:       name,
						Version:    version,
						Parameters: parameters,
					},
					// So we know the order this plugins response should slot in
					i,
				),
			)
		default:
			// Already guaranteed by config validation, but best to be safe.
			return nil, fmt.Errorf("either remote or name must be specified")
		}
	}
	// Limit concurrent execution of remote plugins using separate semaphore channel.
	// This time we can use a higher concurrency limit since the work is almost
	// exclusively I/O bound.
	remoteSemaphore := newSemaphore(thread.Parallelism() * 10)
	defer remoteSemaphore.Discard()
	for remote, plugins := range remoteToPlugins {
		// prevent issues with asynchronously executed closures
		remote := remote
		plugins := plugins
		// Add a job for each remote.
		eg.Go(func() error {
			if err := remoteSemaphore.Acquire(ctx); err != nil {
				return fmt.Errorf("failed to acquire semaphore: %w", err)
			}
			defer remoteSemaphore.Release(ctx)
			generateService, err := g.registryProvider.NewGenerateService(ctx, remote)
			if err != nil {
				return fmt.Errorf("failed to create generate service for remote %q: %w", remote, err)
			}
			references := make([]*registryv1alpha1.PluginReference, 0, len(plugins))
			for _, pluginOption := range plugins {
				references = append(references, pluginOption.reference)
			}
			responses, _, err := generateService.GeneratePlugins(ctx, bufimage.ImageToProtoImage(image), references)
			if err != nil {
				return fmt.Errorf("failed to generate files for remote %q: %w", remote, err)
			}
			if len(responses) != len(references) {
				return fmt.Errorf("unexpected number of responses received, got %d, wanted %d", len(responses), len(references))
			}
			for i, response := range responses {
				// Map each plugin response back to the right place.
				// Note: does not require a lock, since each response is
				// assigned to its own index in the slice.
				pluginResponses[plugins[i].index] = response
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("failed to execute plugins concurrently: %w", err)
	}
	for _, localResponse := range localPluginResponses {
		pluginResponses[localResponse.index] = localResponse.responseWriter.ToResponse()
	}
	for i, response := range pluginResponses {
		if response == nil {
			return nil, fmt.Errorf("concurrent execution failed to populate response %d", i)
		}
	}
	return pluginResponses, nil
}

func (g *generator) generateZip(
	ctx context.Context,
	outFilePath string,
	files []*bufplugin.File,
	includeManifest bool,
) (retErr error) {
	outDirPath := filepath.Dir(outFilePath)
	// OK to use os.Stat instead of os.Lstat here
	fileInfo, err := os.Stat(outDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(outDirPath, 0755); err != nil {
				return fmt.Errorf("failed to create output directory: %w", err)
			}
		}
		return err
	} else if !fileInfo.IsDir() {
		return fmt.Errorf("not a directory: %s", outDirPath)
	}
	readBucketBuilder := storagemem.NewReadBucketBuilder()
	for _, file := range files {
		if err := storage.PutPath(ctx, readBucketBuilder, file.Name, file.Content); err != nil {
			return fmt.Errorf("failed to write generated file: %w", err)
		}
	}
	if includeManifest {
		if err := storage.PutPath(ctx, readBucketBuilder, appprotoos.ManifestPath, appprotoos.ManifestContent); err != nil {
			return fmt.Errorf("failed to write manifest file: %w", err)
		}
	}
	file, err := os.Create(outFilePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() {
		retErr = multierr.Append(retErr, file.Close())
	}()
	readBucket, err := readBucketBuilder.ToReadBucket()
	if err != nil {
		return fmt.Errorf("failed to convert in-memory bucket to read bucket: %w", err)
	}
	// protoc does not compress
	if err := storagearchive.Zip(ctx, readBucket, file, false); err != nil {
		return fmt.Errorf("failed to zip results: %w", err)
	}
	return nil
}

func (g *generator) generateDirectory(
	ctx context.Context,
	outDirPath string,
	files []*bufplugin.File,
) error {
	if err := os.MkdirAll(outDirPath, 0755); err != nil {
		return err
	}
	// this checks that the directory exists
	readWriteBucket, err := g.storageosProvider.NewReadWriteBucket(
		outDirPath,
		storageos.ReadWriteBucketWithSymlinksIfSupported(),
	)
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := storage.PutPath(ctx, readWriteBucket, file.Name, file.Content); err != nil {
			return fmt.Errorf("failed to write generated file: %w", err)
		}
	}
	return nil
}

// modifyImage modifies the image according to the given configuration (i.e. Managed Mode).
func modifyImage(
	ctx context.Context,
	logger *zap.Logger,
	config *Config,
	image bufimage.Image,
) error {
	if config.ManagedConfig == nil {
		// If the config is nil, it implies that the
		// user has not enabled managed mode.
		return nil
	}
	sweeper := bufimagemodify.NewFileOptionSweeper()
	modifier, err := newModifier(logger, config.ManagedConfig, sweeper)
	if err != nil {
		return err
	}
	modifier = bufimagemodify.Merge(modifier, bufimagemodify.ModifierFunc(sweeper.Sweep))
	return modifier.Modify(ctx, image)
}

func newModifier(
	logger *zap.Logger,
	managedConfig *ManagedConfig,
	sweeper bufimagemodify.Sweeper,
) (bufimagemodify.Modifier, error) {
	modifier := bufimagemodify.NewMultiModifier(
		bufimagemodify.JavaOuterClassname(sweeper, managedConfig.Override[bufimagemodify.JavaOuterClassNameID]),
		bufimagemodify.ObjcClassPrefix(sweeper, managedConfig.Override[bufimagemodify.ObjcClassPrefixID]),
		bufimagemodify.CsharpNamespace(sweeper, managedConfig.Override[bufimagemodify.CsharpNamespaceID]),
		bufimagemodify.PhpNamespace(sweeper, managedConfig.Override[bufimagemodify.PhpNamespaceID]),
		bufimagemodify.PhpMetadataNamespace(sweeper, managedConfig.Override[bufimagemodify.PhpMetadataNamespaceID]),
		bufimagemodify.RubyPackage(sweeper, managedConfig.Override[bufimagemodify.RubyPackageID]),
	)
	javaPackagePrefix := bufimagemodify.DefaultJavaPackagePrefix
	if managedConfig.JavaPackagePrefix != "" {
		javaPackagePrefix = managedConfig.JavaPackagePrefix
	}
	javaPackageModifier, err := bufimagemodify.JavaPackage(
		sweeper,
		javaPackagePrefix,
		managedConfig.Override[bufimagemodify.JavaPackageID],
	)
	if err != nil {
		return nil, err
	}
	modifier = bufimagemodify.Merge(modifier, javaPackageModifier)
	javaMultipleFilesValue := bufimagemodify.DefaultJavaMultipleFilesValue
	if managedConfig.JavaMultipleFiles != nil {
		javaMultipleFilesValue = *managedConfig.JavaMultipleFiles
	}
	javaMultipleFilesModifier, err := bufimagemodify.JavaMultipleFiles(
		sweeper,
		javaMultipleFilesValue,
		managedConfig.Override[bufimagemodify.JavaMultipleFilesID],
	)
	if err != nil {
		return nil, err
	}
	modifier = bufimagemodify.Merge(modifier, javaMultipleFilesModifier)
	if managedConfig.CcEnableArenas != nil {
		ccEnableArenasModifier, err := bufimagemodify.CcEnableArenas(
			sweeper,
			*managedConfig.CcEnableArenas,
			managedConfig.Override[bufimagemodify.CcEnableArenasID],
		)
		if err != nil {
			return nil, err
		}
		modifier = bufimagemodify.Merge(modifier, ccEnableArenasModifier)
	}
	if managedConfig.JavaStringCheckUtf8 != nil {
		javaStringCheckUtf8, err := bufimagemodify.JavaStringCheckUtf8(
			sweeper,
			*managedConfig.JavaStringCheckUtf8,
			managedConfig.Override[bufimagemodify.JavaStringCheckUtf8ID],
		)
		if err != nil {
			return nil, err
		}
		modifier = bufimagemodify.Merge(modifier, javaStringCheckUtf8)
	}
	if managedConfig.OptimizeFor != nil {
		optimizeFor, err := bufimagemodify.OptimizeFor(
			sweeper,
			*managedConfig.OptimizeFor,
			managedConfig.Override[bufimagemodify.OptimizeForID],
		)
		if err != nil {
			return nil, err
		}
		modifier = bufimagemodify.Merge(
			modifier,
			optimizeFor,
		)
	}
	if managedConfig.GoPackagePrefixConfig != nil {
		goPackageModifier, err := bufimagemodify.GoPackage(
			logger,
			sweeper,
			managedConfig.GoPackagePrefixConfig.Default,
			managedConfig.GoPackagePrefixConfig.Except,
			managedConfig.GoPackagePrefixConfig.Override,
			managedConfig.Override[bufimagemodify.GoPackageID],
		)
		if err != nil {
			return nil, fmt.Errorf("failed to construct go_package modifier: %w", err)
		}
		modifier = bufimagemodify.Merge(
			modifier,
			goPackageModifier,
		)
	}
	return modifier, nil
}

type generateOptions struct {
	baseOutDirPath string
	includeImports bool
}

func newGenerateOptions() *generateOptions {
	return &generateOptions{}
}

type localPluginResponse struct {
	responseWriter appproto.ResponseWriter
	index          int
}

func newLocalPluginResponse(responseWriter appproto.ResponseWriter, index int) *localPluginResponse {
	return &localPluginResponse{
		responseWriter: responseWriter,
		index:          index,
	}
}

type remotePlugin struct {
	reference *registryv1alpha1.PluginReference
	index     int
}

func newRemotePlugin(reference *registryv1alpha1.PluginReference, index int) *remotePlugin {
	return &remotePlugin{
		reference: reference,
		index:     index,
	}
}

type semaphore chan struct{}

func newSemaphore(limit int) semaphore {
	return make(semaphore, limit)
}

func (s semaphore) Acquire(ctx context.Context) error {
	select {
	case s <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s semaphore) Release(ctx context.Context) {
	select {
	case <-s:
	case <-ctx.Done():
	}
}

func (s semaphore) Discard() {
	close(s)
}
