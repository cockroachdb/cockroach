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

package modclearcache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// NewCommand returns a new Command.
func NewCommand(
	name string,
	builder appflag.Builder,
	deprecated string,
	hidden bool,
	aliases ...string,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:        name,
		Aliases:    aliases,
		Short:      "Clear the module cache.",
		Args:       cobra.NoArgs,
		Deprecated: deprecated,
		Hidden:     hidden,
		Run: builder.NewRunFunc(
			func(ctx context.Context, container appflag.Container) error {
				return run(ctx, container, flags)
			},
		),
		BindFlags: flags.Bind,
	}
}

type flags struct{}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {}

func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	for _, cacheModuleRelDirPath := range bufcli.AllCacheModuleRelDirPaths {
		dirPath := filepath.Join(container.CacheDirPath(), normalpath.Unnormalize(cacheModuleRelDirPath))
		fileInfo, err := os.Stat(dirPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if !fileInfo.IsDir() {
			return fmt.Errorf("expected %q to be a directory", dirPath)
		}
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("could not remove %q: %w", dirPath, err)
		}
		if _, err := container.Stderr().Write([]byte("deleted " + dirPath + "\n")); err != nil {
			return err
		}
	}
	return nil
}
