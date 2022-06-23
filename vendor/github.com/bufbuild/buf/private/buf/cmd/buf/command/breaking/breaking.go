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

package breaking

import (
	"context"
	"errors"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcheck/bufbreaking"
	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/buffetch"
	"github.com/bufbuild/buf/private/buf/bufwire"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	errorFormatFlagName       = "error-format"
	excludeImportsFlagName    = "exclude-imports"
	pathsFlagName             = "path"
	limitToInputFilesFlagName = "limit-to-input-files"
	configFlagName            = "config"
	againstFlagName           = "against"
	againstConfigFlagName     = "against-config"

	// deprecated
	inputFlagName = "input"
	// deprecated
	inputConfigFlagName = "input-config"
	// deprecated
	againstInputFlagName = "against-input"
	// deprecated
	againstInputConfigFlagName = "against-input-config"
	// deprecated
	filesFlagName = "file"
)

// NewCommand returns a new Command.
func NewCommand(
	name string,
	builder appflag.Builder,
	deprecated string,
	hidden bool,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:        name + " --against against-input <input>",
		Short:      "Check that the input location has no breaking changes compared to the against location.",
		Long:       bufcli.GetInputLong(`the source, module, or image to check for breaking changes`),
		Args:       cobra.MaximumNArgs(1),
		Deprecated: deprecated,
		Hidden:     hidden,
		Run: builder.NewRunFunc(
			func(ctx context.Context, container appflag.Container) error {
				return run(ctx, container, flags)
			},
			bufcli.NewErrorInterceptor(),
		),
		BindFlags: flags.Bind,
	}
}

type flags struct {
	ErrorFormat       string
	ExcludeImports    bool
	LimitToInputFiles bool
	Paths             []string
	Config            string
	Against           string
	AgainstConfig     string

	// deprecated
	Input string
	// deprecated
	InputConfig string
	// deprecated
	AgainstInput string
	// deprecated
	AgainstInputConfig string
	// deprecated
	Files []string
	// special
	InputHashtag string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	bufcli.BindPathsAndDeprecatedFiles(flagSet, &f.Paths, pathsFlagName, &f.Files, filesFlagName)
	bufcli.BindInputHashtag(flagSet, &f.InputHashtag)
	flagSet.StringVar(
		&f.ErrorFormat,
		errorFormatFlagName,
		"text",
		fmt.Sprintf(
			"The format for build errors or check violations, printed to stdout. Must be one of %s.",
			stringutil.SliceToString(bufanalysis.AllFormatStrings),
		),
	)
	flagSet.BoolVar(
		&f.ExcludeImports,
		excludeImportsFlagName,
		false,
		"Exclude imports from breaking change detection.",
	)
	flagSet.BoolVar(
		&f.LimitToInputFiles,
		limitToInputFilesFlagName,
		false,
		fmt.Sprintf(
			`Only run breaking checks against the files in the input.
This has the effect of filtering the against input to only contain the files in the input.
Overrides --%s.`,
			pathsFlagName,
		),
	)
	flagSet.StringVar(
		&f.Config,
		configFlagName,
		"",
		`The config file or data to use.`,
	)
	flagSet.StringVar(
		&f.Against,
		againstFlagName,
		"",
		fmt.Sprintf(
			`Required. The source, module, or image to check against. Must be one of format %s.`,
			buffetch.AllFormatsString,
		),
	)
	flagSet.StringVar(
		&f.AgainstConfig,
		againstConfigFlagName,
		"",
		`The config file or data to use for the against source, module, or image.`,
	)

	// deprecated
	flagSet.StringVar(
		&f.Input,
		inputFlagName,
		"",
		fmt.Sprintf(
			`The source or image to check for breaking changes. Must be one of format %s.`,
			buffetch.AllFormatsString,
		),
	)
	_ = flagSet.MarkDeprecated(
		inputFlagName,
		`input as the first argument instead.`+bufcli.FlagDeprecationMessageSuffix,
	)
	_ = flagSet.MarkHidden(inputFlagName)
	// deprecated
	flagSet.StringVar(
		&f.InputConfig,
		inputConfigFlagName,
		"",
		`The config file or data to use.`,
	)
	_ = flagSet.MarkDeprecated(
		inputConfigFlagName,
		fmt.Sprintf("use --%s instead.%s", configFlagName, bufcli.FlagDeprecationMessageSuffix),
	)
	_ = flagSet.MarkHidden(inputConfigFlagName)
	// deprecated
	flagSet.StringVar(
		&f.AgainstInput,
		againstInputFlagName,
		"",
		fmt.Sprintf(
			`Required. The source or image to check against. Must be one of format %s.`,
			buffetch.AllFormatsString,
		),
	)
	_ = flagSet.MarkDeprecated(
		againstInputFlagName,
		fmt.Sprintf("use --%s instead.%s", againstFlagName, bufcli.FlagDeprecationMessageSuffix),
	)
	_ = flagSet.MarkHidden(againstInputFlagName)
	// deprecated
	flagSet.StringVar(
		&f.AgainstInputConfig,
		againstInputConfigFlagName,
		"",
		`The config file or data to use for the against source or image.`,
	)
	_ = flagSet.MarkDeprecated(
		againstInputConfigFlagName,
		fmt.Sprintf("use --%s instead.%s", againstConfigFlagName, bufcli.FlagDeprecationMessageSuffix),
	)
	_ = flagSet.MarkHidden(againstInputConfigFlagName)
}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	if err := bufcli.ValidateErrorFormatFlag(flags.ErrorFormat, errorFormatFlagName); err != nil {
		return err
	}
	input, err := bufcli.GetInputValue(container, flags.InputHashtag, flags.Input, inputFlagName, ".")
	if err != nil {
		return err
	}
	inputConfig, err := bufcli.GetStringFlagOrDeprecatedFlag(
		flags.Config,
		configFlagName,
		flags.InputConfig,
		inputConfigFlagName,
	)
	if err != nil {
		return err
	}
	againstInput, err := bufcli.GetStringFlagOrDeprecatedFlag(
		flags.Against,
		againstFlagName,
		flags.AgainstInput,
		againstInputFlagName,
	)
	if err != nil {
		return err
	}
	againstInputConfig, err := bufcli.GetStringFlagOrDeprecatedFlag(
		flags.AgainstConfig,
		againstConfigFlagName,
		flags.AgainstInputConfig,
		againstInputConfigFlagName,
	)
	if err != nil {
		return err
	}
	if againstInput == "" {
		return appcmd.NewInvalidArgumentErrorf("required flag %q not set", againstFlagName)
	}
	paths, err := bufcli.GetStringSliceFlagOrDeprecatedFlag(
		flags.Paths,
		pathsFlagName,
		flags.Files,
		filesFlagName,
	)
	if err != nil {
		return err
	}
	ref, err := buffetch.NewRefParser(container.Logger()).GetRef(ctx, input)
	if err != nil {
		return err
	}
	storageosProvider := storageos.NewProvider(storageos.ProviderWithSymlinks())
	registryProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	imageConfigReader, err := bufcli.NewWireImageConfigReader(
		container,
		storageosProvider,
		registryProvider,
	)
	if err != nil {
		return err
	}
	imageConfigs, fileAnnotations, err := imageConfigReader.GetImageConfigs(
		ctx,
		container,
		ref,
		inputConfig,
		paths, // we filter checks for files
		false, // files specified must exist on the main input
		false, // we must include source info for this side of the check
	)
	if err != nil {
		return err
	}
	if len(fileAnnotations) > 0 {
		if err := bufanalysis.PrintFileAnnotations(
			container.Stdout(),
			fileAnnotations,
			flags.ErrorFormat,
		); err != nil {
			return err
		}
		return errors.New("")
	}
	// TODO: this doesn't actually work because we're using the same file paths for both sides
	// if the roots change, then we're torched
	externalPaths := paths
	if flags.LimitToInputFiles {
		externalPaths, err = getExternalPathsForImages(imageConfigs, flags.ExcludeImports)
		if err != nil {
			return err
		}
	}
	againstRef, err := buffetch.NewRefParser(container.Logger()).GetRef(ctx, againstInput)
	if err != nil {
		return err
	}
	againstImageConfigs, fileAnnotations, err := imageConfigReader.GetImageConfigs(
		ctx,
		container,
		againstRef,
		againstInputConfig,
		externalPaths, // we filter checks for files
		true,          // files are allowed to not exist on the against input
		true,          // no need to include source info for against
	)
	if err != nil {
		return err
	}
	if len(fileAnnotations) > 0 {
		if err := bufanalysis.PrintFileAnnotations(
			container.Stdout(),
			fileAnnotations,
			flags.ErrorFormat,
		); err != nil {
			return err
		}
		return bufcli.ErrFileAnnotation
	}
	if len(imageConfigs) != len(againstImageConfigs) {
		// If workspaces are being used as input, the number
		// of images MUST match. Otherwise the results will
		// be meaningless and yield false positives.
		//
		// And similar to the note above, if the roots change,
		// we're torched.
		return fmt.Errorf("input contained %d images, whereas against contained %d images", len(imageConfigs), len(againstImageConfigs))
	}
	var allFileAnnotations []bufanalysis.FileAnnotation
	for i, imageConfig := range imageConfigs {
		fileAnnotations, err := breakingForImage(
			ctx,
			container,
			imageConfig,
			againstImageConfigs[i],
			flags.ExcludeImports,
			flags.ErrorFormat,
		)
		if err != nil {
			return err
		}
		allFileAnnotations = append(allFileAnnotations, fileAnnotations...)
	}
	if len(allFileAnnotations) > 0 {
		if err := bufanalysis.PrintFileAnnotations(
			container.Stdout(),
			bufanalysis.DeduplicateAndSortFileAnnotations(allFileAnnotations),
			flags.ErrorFormat,
		); err != nil {
			return err
		}
		return bufcli.ErrFileAnnotation
	}
	return nil
}

func breakingForImage(
	ctx context.Context,
	container appflag.Container,
	imageConfig bufwire.ImageConfig,
	againstImageConfig bufwire.ImageConfig,
	excludeImports bool,
	errorFormat string,
) ([]bufanalysis.FileAnnotation, error) {
	image := imageConfig.Image()
	if excludeImports {
		image = bufimage.ImageWithoutImports(image)
	}
	againstImage := againstImageConfig.Image()
	if excludeImports {
		againstImage = bufimage.ImageWithoutImports(againstImage)
	}
	return bufbreaking.NewHandler(container.Logger()).Check(
		ctx,
		imageConfig.Config().Breaking,
		againstImage,
		image,
	)
}

func getExternalPathsForImages(imageConfigs []bufwire.ImageConfig, excludeImports bool) ([]string, error) {
	externalPaths := make(map[string]struct{})
	for _, imageConfig := range imageConfigs {
		image := imageConfig.Image()
		if excludeImports {
			image = bufimage.ImageWithoutImports(image)
		}
		for _, imageFile := range image.Files() {
			externalPaths[imageFile.ExternalPath()] = struct{}{}
		}
	}
	return stringutil.MapToSlice(externalPaths), nil
}
