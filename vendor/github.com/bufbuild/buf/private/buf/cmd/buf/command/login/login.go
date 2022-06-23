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

package login

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/bufpkg/bufrpc"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/netrc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	usernameFlagName   = "username"
	tokenStdinFlagName = "token-stdin"
)

// NewCommand returns a new Command.
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		// Not documenting the first arg (remote) as this is just for testing for now.
		// TODO: Update when we have self-hosted.
		Use:   name,
		Short: `Login to the Buf Schema Registry.`,
		Long:  fmt.Sprintf(`This will prompt for your BSR username and a BSR token, and update your %s file with these credentials.`, netrc.Filename),
		Args:  cobra.MaximumNArgs(1),
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
	Username   string
	TokenStdin bool
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&f.Username,
		usernameFlagName,
		"",
		"The username to use. By default, this command will prompt for a username.",
	)
	flagSet.BoolVar(
		&f.TokenStdin,
		tokenStdinFlagName,
		false,
		"Read the token from stdin. By default, this command will prompt for a token.",
	)
}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	// Do not print unless we are prompting
	if flags.Username == "" && !flags.TokenStdin {
		if _, err := container.Stdout().Write(
			[]byte("Login with your Buf Schema Registry username. If you don't have a username, head over to https://buf.build to create one.\n\n"),
		); err != nil {
			return err
		}
	}
	username := flags.Username
	if username == "" {
		var err error
		username, err = bufcli.PromptUser(container, "Username: ")
		if err != nil {
			if errors.Is(err, bufcli.ErrNotATTY) {
				return errors.New("cannot perform an interactive login from a non-TTY device")
			}
			return err
		}
	}
	var token string
	if flags.TokenStdin {
		data, err := io.ReadAll(container.Stdin())
		if err != nil {
			return err
		}
		token = string(data)
	} else {
		var err error
		token, err = bufcli.PromptUserForPassword(container, "Token: ")
		if err != nil {
			if errors.Is(err, bufcli.ErrNotATTY) {
				return errors.New("cannot perform an interactive login from a non-TTY device")
			}
			return err
		}
	}
	remote := bufrpc.DefaultRemote
	if container.NumArgs() == 1 {
		remote = container.Arg(0)
	}
	if err := netrc.PutMachines(
		container,
		netrc.NewMachine(
			remote,
			username,
			token,
			"",
		),
		netrc.NewMachine(
			"go."+remote,
			username,
			token,
			"",
		),
	); err != nil {
		return err
	}
	netrcFilePath, err := netrc.GetFilePath(container)
	if err != nil {
		return err
	}
	loggedInMessage := fmt.Sprintf("Credentials saved to %s.\n", netrcFilePath)
	// Unless we did not prompt at all, print a newline first
	if flags.Username == "" || !flags.TokenStdin {
		loggedInMessage = "\n" + loggedInMessage
	}
	if _, err := container.Stdout().Write([]byte(loggedInMessage)); err != nil {
		return err
	}
	return nil
}
