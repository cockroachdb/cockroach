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

package convert

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/buffetch"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	asFileDescriptorSetFlagName = "as-file-descriptor-set"
	excludeImportsFlagName      = "exclude-imports"
	excludeSourceInfoFlagName   = "exclude-source-info"
	pathsFlagName               = "path"
	outputFlagName              = "output"
	outputFlagShortName         = "o"
	imageFlagName               = "image"
	imageFlagShortName          = "i"

	// deprecated
	filesFlagName = "file"
)

// NewCommand returns a new Command.
//
// This command has been replaced with build and should always be deprecated.
func NewCommand(
	name string,
	builder appflag.Builder,
	deprecated string,
	hidden bool,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:        name,
		Short:      "Convert the input Image to an output Image with the specified format and filters.",
		Deprecated: deprecated,
		Hidden:     hidden,
		Args:       cobra.NoArgs,
		Run: builder.NewRunFunc(
			func(ctx context.Context, container appflag.Container) error {
				return run(ctx, container, flags)
			},
		),
		BindFlags: flags.Bind,
	}
}

type flags struct {
	AsFileDescriptorSet bool
	ExcludeImports      bool
	ExcludeSourceInfo   bool
	Paths               []string
	Image               string
	Output              string

	// deprecated
	Files []string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	bufcli.BindAsFileDescriptorSet(flagSet, &f.AsFileDescriptorSet, asFileDescriptorSetFlagName)
	bufcli.BindExcludeImports(flagSet, &f.ExcludeImports, excludeImportsFlagName)
	bufcli.BindExcludeSourceInfo(flagSet, &f.ExcludeSourceInfo, excludeSourceInfoFlagName)
	bufcli.BindPathsAndDeprecatedFiles(flagSet, &f.Paths, pathsFlagName, &f.Files, filesFlagName)
	flagSet.StringVarP(
		&f.Image,
		imageFlagName,
		imageFlagShortName,
		"",
		fmt.Sprintf(
			`The image to convert. Must be one of format %s.`,
			buffetch.ImageFormatsString,
		),
	)
	flagSet.StringVarP(
		&f.Output,
		outputFlagName,
		outputFlagShortName,
		"",
		fmt.Sprintf(
			`Required. The location to write the image to. Must be one of format %s.`,
			buffetch.ImageFormatsString,
		),
	)
	_ = cobra.MarkFlagRequired(flagSet, outputFlagName)
}

func run(ctx context.Context, container appflag.Container, flags *flags) (retErr error) {
	paths, err := bufcli.GetStringSliceFlagOrDeprecatedFlag(
		flags.Paths,
		pathsFlagName,
		flags.Files,
		filesFlagName,
	)
	if err != nil {
		return err
	}
	imageRef, err := buffetch.NewImageRefParser(container.Logger()).GetImageRef(ctx, flags.Image)
	if err != nil {
		return fmt.Errorf("--%s: %v", imageFlagName, err)
	}
	storageosProvider := storageos.NewProvider(storageos.ProviderWithSymlinks())
	image, err := bufcli.NewWireImageReader(
		container.Logger(),
		storageosProvider,
	).GetImage(
		ctx,
		container,
		imageRef,
		paths,
		false,
		flags.ExcludeSourceInfo,
	)
	if err != nil {
		return err
	}
	imageRef, err = buffetch.NewImageRefParser(container.Logger()).GetImageRef(ctx, flags.Output)
	if err != nil {
		return fmt.Errorf("--%s: %v", outputFlagName, err)
	}
	return bufcli.NewWireImageWriter(
		container.Logger(),
	).PutImage(
		ctx,
		container,
		imageRef,
		image,
		flags.AsFileDescriptorSet,
		flags.ExcludeImports,
	)
}
