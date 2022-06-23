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

package tokencreate

import (
	"context"
	"time"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/prototime"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	noteFlagName       = "note"
	timeToLiveFlagName = "ttl"
)

// NewCommand returns a new Command
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name + " <buf.build>",
		Short: "Create a new token for a user.",
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
	Note       string
	TimeToLive time.Duration
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&f.Note,
		noteFlagName,
		"",
		`A human-readable note that identifies the token.`,
	)
	_ = cobra.MarkFlagRequired(flagSet, noteFlagName)
	flagSet.DurationVar(
		&f.TimeToLive,
		timeToLiveFlagName,
		24*30*time.Hour,
		`How long the token should live for. Set to 0 for no expiry.`,
	)
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
	var expireTime *timestamppb.Timestamp
	var err error
	if flags.TimeToLive != 0 {
		expireTime, err = prototime.NewTimestamp(time.Now().Add(flags.TimeToLive))
		if err != nil {
			return err
		}
	}
	registryProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	tokenService, err := registryProvider.NewTokenService(ctx, remote)
	if err != nil {
		return err
	}
	token, err := tokenService.CreateToken(ctx, flags.Note, expireTime)
	if err != nil {
		return err
	}
	_, err = container.Stdout().Write([]byte(token))
	return err
}
