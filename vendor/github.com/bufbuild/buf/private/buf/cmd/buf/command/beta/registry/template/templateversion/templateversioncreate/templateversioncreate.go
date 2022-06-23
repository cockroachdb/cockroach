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

package templateversioncreate

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/bufprint"
	"github.com/bufbuild/buf/private/bufpkg/bufplugin"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	configFlagName = "config"
	nameFlagName   = "name"
	formatFlagName = "format"
)

// NewCommand returns a new Command
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name + " <buf.build/owner/" + bufplugin.TemplatesPathName + "/template>",
		Short: "Create a new template version.",
		Args:  cobra.ExactArgs(1),
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
	Config string
	Name   string
	Format string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&f.Config,
		configFlagName,
		"",
		"The template config file or data to use. Must be in either YAML or JSON format.",
	)
	_ = cobra.MarkFlagRequired(flagSet, configFlagName)
	flagSet.StringVar(
		&f.Name,
		nameFlagName,
		"",
		"The name of the new template version",
	)
	_ = cobra.MarkFlagRequired(flagSet, nameFlagName)
	flagSet.StringVar(
		&f.Format,
		formatFlagName,
		bufprint.FormatText.String(),
		fmt.Sprintf("The output format to use. Must be one of %s", bufprint.AllFormatsString),
	)
}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	templatePath := container.Arg(0)
	format, err := bufprint.ParseFormat(flags.Format)
	if err != nil {
		return appcmd.NewInvalidArgumentError(err.Error())
	}
	templateVersionConfig, err := bufplugin.ParseTemplateVersionConfig(flags.Config)
	if err != nil {
		return err
	}
	registryProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	remote, templateOwner, templateName, err := bufplugin.ParseTemplatePath(templatePath)
	if err != nil {
		return err
	}
	pluginService, err := registryProvider.NewPluginService(ctx, remote)
	if err != nil {
		return err
	}
	templateVersion, err := pluginService.CreateTemplateVersion(
		ctx,
		flags.Name,
		templateOwner,
		templateName,
		bufplugin.TemplateVersionConfigToProtoPluginVersionMappings(templateVersionConfig),
	)
	if err != nil {
		return err
	}
	return bufprint.NewTemplateVersionPrinter(container.Stdout()).PrintTemplateVersion(ctx, format, templateVersion)
}
