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

package protoc

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/buffetch"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufimage/bufimagebuild"
	"github.com/bufbuild/buf/private/bufpkg/bufimage/bufimageutil"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule/bufmodulebuild"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewCommand returns a new Command.
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flagsBuilder := newFlagsBuilder()
	return &appcmd.Command{
		Use:   name + " <proto_file1> <proto_file2> ...",
		Short: "High-performance protoc replacement.",
		Long: `This replaces protoc using Buf's internal compiler.

The implementation is in progress, and while it already outperforms mainline
protoc, it has not been optimized yet. While this command is stable, it should
be considered a preview.

Additional flags:

      --(.*)_out:                   Run the named plugin.
      --(.*)_opt:                   Options for the named plugin.
      @filename:                    Parse arguments from the given filename.`,
		Run: builder.NewRunFunc(
			func(ctx context.Context, container appflag.Container) error {
				env, err := flagsBuilder.Build(app.Args(container))
				if err != nil {
					return err
				}
				return run(ctx, container, env)
			},
		),
		BindFlags:     flagsBuilder.Bind,
		NormalizeFlag: flagsBuilder.Normalize,
		Version:       "3.13.0-buf",
	}
}

func run(
	ctx context.Context,
	container appflag.Container,
	env *env,
) (retErr error) {
	if env.PrintFreeFieldNumbers && len(env.PluginNameToPluginInfo) > 0 {
		return fmt.Errorf("cannot call --%s and plugins at the same time", printFreeFieldNumbersFlagName)
	}
	if env.PrintFreeFieldNumbers && env.Output != "" {
		return fmt.Errorf("cannot call --%s and --%s at the same time", printFreeFieldNumbersFlagName, outputFlagName)
	}
	if len(env.PluginNameToPluginInfo) > 0 && env.Output != "" {
		return fmt.Errorf("cannot call --%s and plugins at the same time", outputFlagName)
	}

	if checkedEntry := container.Logger().Check(zapcore.DebugLevel, "env"); checkedEntry != nil {
		checkedEntry.Write(
			zap.Any("flags", env.flags),
			zap.Any("plugins", env.PluginNameToPluginInfo),
		)
	}

	var buildOption bufmodulebuild.BuildOption
	if len(env.FilePaths) > 0 {
		buildOption = bufmodulebuild.WithPaths(env.FilePaths)
	}
	storageosProvider := storageos.NewProvider(storageos.ProviderWithSymlinks())
	module, err := bufmodulebuild.NewModuleIncludeBuilder(container.Logger(), storageosProvider).BuildForIncludes(
		ctx,
		env.IncludeDirPaths,
		buildOption,
	)
	if err != nil {
		return err
	}
	registryProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	moduleReader, err := bufcli.NewModuleReaderAndCreateCacheDirs(container, registryProvider)
	if err != nil {
		return err
	}
	moduleFileSet, err := bufmodulebuild.NewModuleFileSetBuilder(
		zap.NewNop(),
		moduleReader,
	).Build(
		ctx,
		module,
	)
	if err != nil {
		return err
	}
	var buildOptions []bufimagebuild.BuildOption
	// we always need source code info if we are doing generation
	if len(env.PluginNameToPluginInfo) == 0 && !env.IncludeSourceInfo {
		buildOptions = append(buildOptions, bufimagebuild.WithExcludeSourceCodeInfo())
	}
	image, fileAnnotations, err := bufimagebuild.NewBuilder(container.Logger()).Build(
		ctx,
		moduleFileSet,
		buildOptions...,
	)
	if err != nil {
		return err
	}
	if len(fileAnnotations) > 0 {
		if err := bufanalysis.PrintFileAnnotations(
			container.Stderr(),
			fileAnnotations,
			env.ErrorFormat,
		); err != nil {
			return err
		}
		// we do this even though we're in protoc compatibility mode as we just need to do non-zero
		// but this also makes us consistent with the rest of buf
		return bufcli.ErrFileAnnotation
	}

	if env.PrintFreeFieldNumbers {
		fileInfos, err := module.TargetFileInfos(ctx)
		if err != nil {
			return err
		}
		var filePaths []string
		for _, fileInfo := range fileInfos {
			filePaths = append(filePaths, fileInfo.Path())
		}
		s, err := bufimageutil.FreeMessageRangeStrings(ctx, filePaths, image)
		if err != nil {
			return err
		}
		if _, err := container.Stdout().Write([]byte(strings.Join(s, "\n") + "\n")); err != nil {
			return err
		}
		return nil
	}
	if len(env.PluginNameToPluginInfo) > 0 {
		images := []bufimage.Image{image}
		if env.ByDir {
			_, span := trace.StartSpan(ctx, "image_by_dir")
			images, err = bufimage.ImageByDir(image)
			if err != nil {
				return err
			}
			span.End()
		}
		// we need to run these in the order they appear for insertion points to work
		for _, pluginName := range env.PluginNamesSortedByOutIndex {
			pluginInfo, ok := env.PluginNameToPluginInfo[pluginName]
			if !ok {
				return fmt.Errorf("no value in PluginNamesToPluginInfo for %q", pluginName)
			}
			if err := executePlugin(
				ctx,
				container.Logger(),
				storageosProvider,
				container,
				images,
				pluginName,
				pluginInfo,
			); err != nil {
				return err
			}
		}
		return nil
	}
	if env.Output == "" {
		return appcmd.NewInvalidArgumentErrorf("required flag %q not set", outputFlagName)
	}
	imageRef, err := buffetch.NewImageRefParser(container.Logger()).GetImageRef(ctx, env.Output)
	if err != nil {
		return fmt.Errorf("--%s: %v", outputFlagName, err)
	}
	return bufcli.NewWireImageWriter(container.Logger()).PutImage(ctx,
		container,
		imageRef,
		image,
		true,
		!env.IncludeImports,
	)
}
