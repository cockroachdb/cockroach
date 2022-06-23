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

package tokendelete

import (
	"context"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	forceFlagName   = "force"
	tokenIDFlagName = "token-id"
)

// NewCommand returns a new Command
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name + " <buf.build>",
		Short: "Delete a token by ID.",
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
	Force   bool
	TokenID string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.BoolVar(
		&f.Force,
		forceFlagName,
		false,
		"Force deletion without confirming. Use with caution.",
	)
	flagSet.StringVar(
		&f.TokenID,
		tokenIDFlagName,
		"",
		"The ID of the token to delete.",
	)
	_ = cobra.MarkFlagRequired(flagSet, tokenIDFlagName)
}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	remote := container.Arg(0)
	if remote == "" {
		return appcmd.NewInvalidArgumentError("a module remote must be specified")
	}
	registryProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	service, err := registryProvider.NewTokenService(ctx, remote)
	if err != nil {
		return err
	}
	if !flags.Force {
		if err := bufcli.PromptUserForDelete(container, "token", flags.TokenID); err != nil {
			return err
		}
	}
	if err := service.DeleteToken(ctx, flags.TokenID); err != nil {
		if rpc.GetErrorCode(err) == rpc.ErrorCodeNotFound {
			return bufcli.NewTokenNotFoundError(flags.TokenID)
		}
		return err
	}
	return nil
}
