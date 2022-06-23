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

package repositorycreate

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/bufprint"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	formatFlagName     = "format"
	visibilityFlagName = "visibility"

	publicVisibility  = "public"
	privateVisibility = "private"
)

var allVisibiltyStrings = []string{
	publicVisibility,
	privateVisibility,
}

// NewCommand returns a new Command
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name + " <buf.build/owner/repository>",
		Short: "Create a new repository.",
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
	Format     string
	Visibility string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&f.Format,
		formatFlagName,
		bufprint.FormatText.String(),
		fmt.Sprintf(`The output format to use. Must be one of %s.`, bufprint.AllFormatsString),
	)
	flagSet.StringVar(
		&f.Visibility,
		visibilityFlagName,
		"",
		fmt.Sprintf(`The repository's visibility setting. Must be one of %s.`, stringutil.SliceToString(allVisibiltyStrings)),
	)
	_ = cobra.MarkFlagRequired(flagSet, visibilityFlagName)
}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	moduleIdentity, err := bufmodule.ModuleIdentityForString(container.Arg(0))
	if err != nil {
		return appcmd.NewInvalidArgumentError(err.Error())
	}
	visibility, err := visibilityFlagToVisibility(flags.Visibility)
	if err != nil {
		return appcmd.NewInvalidArgumentError(err.Error())
	}
	format, err := bufprint.ParseFormat(flags.Format)
	if err != nil {
		return appcmd.NewInvalidArgumentError(err.Error())
	}

	apiProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	service, err := apiProvider.NewRepositoryService(ctx, moduleIdentity.Remote())
	if err != nil {
		return err
	}
	repository, err := service.CreateRepositoryByFullName(
		ctx,
		moduleIdentity.Owner()+"/"+moduleIdentity.Repository(),
		visibility,
	)
	if err != nil {
		if rpc.GetErrorCode(err) == rpc.ErrorCodeAlreadyExists {
			return bufcli.NewRepositoryNameAlreadyExistsError(container.Arg(0))
		}
		return err
	}
	return bufprint.NewRepositoryPrinter(
		apiProvider,
		moduleIdentity.Remote(),
		container.Stdout(),
	).PrintRepository(ctx, format, repository)
}

// visibilityFlagToVisibility parses the given string as a registryv1alpha1.Visibility.
func visibilityFlagToVisibility(visibility string) (registryv1alpha1.Visibility, error) {
	switch visibility {
	case publicVisibility:
		return registryv1alpha1.Visibility_VISIBILITY_PUBLIC, nil
	case privateVisibility:
		return registryv1alpha1.Visibility_VISIBILITY_PRIVATE, nil
	default:
		return 0, fmt.Errorf("invalid visibility: %s, expected one of %s", visibility, stringutil.SliceToString(allVisibiltyStrings))
	}
}
