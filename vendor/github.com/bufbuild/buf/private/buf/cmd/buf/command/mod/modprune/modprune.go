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

package modprune

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/bufconfig"
	"github.com/bufbuild/buf/private/bufpkg/buflock"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/bufpkg/bufrpc"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	dirFlagName = "dir"
)

// NewCommand returns a new prune Command.
func NewCommand(
	name string,
	builder appflag.Builder,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name,
		Short: "Prunes unused dependencies from the " + buflock.ExternalConfigFilePath + " file.",
		Args:  cobra.NoArgs,
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
	// for testing only
	Dir string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&f.Dir,
		dirFlagName,
		".",
		"The directory to operate in. For testing only.",
	)
	_ = flagSet.MarkHidden(dirFlagName)
}

// run tidy to trim the buf.lock file for a specific module.
func run(
	ctx context.Context,
	container appflag.Container,
	flags *flags,
) error {
	storageosProvider := storageos.NewProvider(storageos.ProviderWithSymlinks())
	readWriteBucket, err := storageosProvider.NewReadWriteBucket(
		flags.Dir,
		storageos.ReadWriteBucketWithSymlinksIfSupported(),
	)
	if err != nil {
		return err
	}
	existingConfigFilePath, err := bufconfig.ExistingConfigFilePath(ctx, readWriteBucket)
	if err != nil {
		return err
	}
	if existingConfigFilePath == "" {
		return bufcli.ErrNoConfigFile
	}
	config, err := bufconfig.NewProvider(container.Logger()).GetConfig(ctx, readWriteBucket)
	if err != nil {
		return err
	}
	remote := bufrpc.DefaultRemote
	if config.ModuleIdentity != nil && config.ModuleIdentity.Remote() != "" {
		remote = config.ModuleIdentity.Remote()
	}
	apiProvider, err := bufcli.NewRegistryProvider(ctx, container)
	if err != nil {
		return err
	}
	service, err := apiProvider.NewResolveService(ctx, remote)
	if err != nil {
		return err
	}

	module, err := bufmodule.NewModuleForBucket(ctx, readWriteBucket)
	if err != nil {
		return fmt.Errorf("couldn't read current dependencies: %w", err)
	}

	requestReferences, err := referencesPinnedByLock(config.Build.DependencyModuleReferences, module.DependencyModulePins())
	if err != nil {
		return err
	}
	var dependencyModulePins []bufmodule.ModulePin
	if len(requestReferences) > 0 {
		protoDependencyModulePins, err := service.GetModulePins(ctx, bufmodule.NewProtoModuleReferencesForModuleReferences(requestReferences...), nil)
		if err != nil {
			if rpc.GetErrorCode(err) == rpc.ErrorCodeUnimplemented && remote != bufrpc.DefaultRemote {
				return bufcli.NewUnimplementedRemoteError(err, remote, config.ModuleIdentity.IdentityString())
			}
			return err
		}
		dependencyModulePins, err = bufmodule.NewModulePinsForProtos(protoDependencyModulePins...)
		if err != nil {
			return bufcli.NewInternalError(err)
		}
	}
	if err := bufmodule.PutDependencyModulePinsToBucket(ctx, readWriteBucket, dependencyModulePins); err != nil {
		return err
	}
	return nil
}

// referencesPinnedByLock takes moduleReferences and a list of pins, then
// returns a new list of moduleReferences with the same identity, but their
// reference set to the commit of the pin with the corresponding identity.
func referencesPinnedByLock(moduleReferences []bufmodule.ModuleReference, modulePins []bufmodule.ModulePin) ([]bufmodule.ModuleReference, error) {
	pinsByIdentity := make(map[string]bufmodule.ModulePin, len(modulePins))
	for _, modulePin := range modulePins {
		pinsByIdentity[modulePin.IdentityString()] = modulePin
	}

	var pinnedModuleReferences []bufmodule.ModuleReference
	for _, moduleReference := range moduleReferences {
		pin, ok := pinsByIdentity[moduleReference.IdentityString()]
		if !ok {
			return nil, fmt.Errorf("cannot tidy with dependency %q: no corresponding entry found in buf.lock; use `mod update` first if this is a new dependency", moduleReference.IdentityString())
		}
		newModuleReference, err := bufmodule.NewModuleReference(
			moduleReference.Remote(),
			moduleReference.Owner(),
			moduleReference.Repository(),
			pin.Commit(),
		)
		if err != nil {
			return nil, err
		}
		pinnedModuleReferences = append(pinnedModuleReferences, newModuleReference)
	}
	return pinnedModuleReferences, nil
}
