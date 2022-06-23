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

package tagcreate

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/bufprint"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const formatFlagName = "format"

// NewCommand returns a new Command
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name + " <buf.build/owner/repository:commit> <tag>",
		Short: "Create a tag for the specified commit.",
		Args:  cobra.ExactArgs(2),
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
	Format string
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
}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	moduleReference, err := bufmodule.ModuleReferenceForString(
		container.Arg(0),
	)
	if err != nil {
		return appcmd.NewInvalidArgumentError(err.Error())
	}
	if !bufmodule.IsCommitModuleReference(moduleReference) {
		return fmt.Errorf("commit is required, but a tag was given: %q", container.Arg(0))
	}
	format, err := bufprint.ParseFormat(flags.Format)
	if err != nil {
		return appcmd.NewInvalidArgumentError(err.Error())
	}

	apiProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	repositoryService, err := apiProvider.NewRepositoryService(ctx, moduleReference.Remote())
	if err != nil {
		return err
	}
	repositoryTagService, err := apiProvider.NewRepositoryTagService(ctx, moduleReference.Remote())
	if err != nil {
		return err
	}
	repository, err := repositoryService.GetRepositoryByFullName(ctx, moduleReference.Owner()+"/"+moduleReference.Repository())
	if err != nil {
		if rpc.GetErrorCode(err) == rpc.ErrorCodeNotFound {
			return bufcli.NewRepositoryNotFoundError(moduleReference.Remote() + "/" + moduleReference.Owner() + "/" + moduleReference.Repository())
		}
		return err
	}
	tag := container.Arg(1)
	commit := moduleReference.Reference()
	repositoryTag, err := repositoryTagService.CreateRepositoryTag(
		ctx,
		repository.Id,
		tag,
		commit,
	)
	if err != nil {
		if rpc.GetErrorCode(err) == rpc.ErrorCodeAlreadyExists {
			return bufcli.NewBranchOrTagNameAlreadyExistsError(tag)
		}
		if rpc.GetErrorCode(err) == rpc.ErrorCodeNotFound {
			return bufcli.NewModuleReferenceNotFoundError(moduleReference)
		}
		return err
	}
	return bufprint.NewRepositoryTagPrinter(container.Stdout()).PrintRepositoryTag(ctx, format, repositoryTag)
}
