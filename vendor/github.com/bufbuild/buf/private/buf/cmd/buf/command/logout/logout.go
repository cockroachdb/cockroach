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

package logout

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/bufpkg/bufrpc"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/netrc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
		Short: `Logout of the Buf Schema Registry.`,
		Long:  fmt.Sprintf(`This will remove any BSR credentials from your %s file.`, netrc.Filename),
		Args:  cobra.MaximumNArgs(1),
		Run: builder.NewRunFunc(
			func(ctx context.Context, container appflag.Container) error {
				return run(ctx, container, flags)
			},
			bufcli.NewErrorInterceptor(),
		),
	}
}

type flags struct {
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	remote := bufrpc.DefaultRemote
	if container.NumArgs() == 1 {
		remote = container.Arg(0)
	}
	modified1, err := netrc.DeleteMachineForName(container, remote)
	if err != nil {
		return err
	}
	modified2, err := netrc.DeleteMachineForName(container, "go."+remote)
	if err != nil {
		return err
	}
	netrcFilePath, err := netrc.GetFilePath(container)
	if err != nil {
		return err
	}
	loggedOutMessage := fmt.Sprintf("All existing BSR credentials removed from %s.\n", netrcFilePath)
	if !modified1 && !modified2 {
		loggedOutMessage = fmt.Sprintf("No BSR credentials found in %s, you are already logged out.\n", netrcFilePath)
	}
	if _, err := container.Stdout().Write([]byte(loggedOutMessage)); err != nil {
		return err
	}
	return nil
}
