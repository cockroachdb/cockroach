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

package modupdate

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/bufconfig"
	"github.com/bufbuild/buf/private/bufpkg/buflock"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/bufpkg/bufrpc"
	modulev1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/module/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	dirFlagName  = "dir"
	onlyFlagName = "only"
)

// NewCommand returns a new update Command.
func NewCommand(
	name string,
	builder appflag.Builder,
	deprecated string,
	hidden bool,
) *appcmd.Command {
	flags := newFlags()
	return &appcmd.Command{
		Use:   name,
		Short: "Update the modules dependencies. Updates the " + buflock.ExternalConfigFilePath + " file.",
		Long: "Gets the latest digests for the specified references in the config file, " +
			"and writes them and their transitive dependencies to the " +
			buflock.ExternalConfigFilePath +
			" file.",
		Args:       cobra.NoArgs,
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
	// for testing only
	Dir string

	Only []string
}

func newFlags() *flags {
	return &flags{}
}

func (f *flags) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringSliceVar(
		&f.Only,
		onlyFlagName,
		nil,
		"The name of a dependency to update. When used, only this dependency (and possibly its dependencies) will be updated. May be passed multiple times.",
	)
	flagSet.StringVar(
		&f.Dir,
		dirFlagName,
		".",
		"The directory to operate in. For testing only.",
	)
	_ = flagSet.MarkHidden(dirFlagName)
}

// run update the buf.lock file for a specific module.
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
		return bufcli.NewInternalError(err)
	}
	existingConfigFilePath, err := bufconfig.ExistingConfigFilePath(ctx, readWriteBucket)
	if err != nil {
		return bufcli.NewInternalError(err)
	}
	if existingConfigFilePath == "" {
		return bufcli.ErrNoConfigFile
	}
	moduleConfig, err := bufconfig.NewProvider(container.Logger()).GetConfig(ctx, readWriteBucket)
	if err != nil {
		return err
	}

	remote := bufrpc.DefaultRemote
	if moduleConfig.ModuleIdentity != nil && moduleConfig.ModuleIdentity.Remote() != "" {
		remote = moduleConfig.ModuleIdentity.Remote()
	}
	var dependencyModulePins []bufmodule.ModulePin
	if len(moduleConfig.Build.DependencyModuleReferences) != 0 {
		apiProvider, err := bufcli.NewRegistryProvider(ctx, container)
		if err != nil {
			return err
		}
		service, err := apiProvider.NewResolveService(ctx, remote)
		if err != nil {
			return err
		}
		var protoDependencyModuleReferences []*modulev1alpha1.ModuleReference
		var currentProtoModulePins []*modulev1alpha1.ModulePin
		if len(flags.Only) > 0 {
			referencesByIdentity := map[string]bufmodule.ModuleReference{}
			for _, reference := range moduleConfig.Build.DependencyModuleReferences {
				referencesByIdentity[reference.IdentityString()] = reference
			}
			for _, only := range flags.Only {
				moduleReference, ok := referencesByIdentity[only]
				if !ok {
					return fmt.Errorf("%q is not a valid --only input: no such dependency in current module deps", only)
				}
				protoDependencyModuleReferences = append(protoDependencyModuleReferences, bufmodule.NewProtoModuleReferenceForModuleReference(moduleReference))
			}
			currentModulePins, err := bufmodule.DependencyModulePinsForBucket(ctx, readWriteBucket)
			if err != nil {
				return fmt.Errorf("couldn't read current dependencies: %w", err)
			}
			currentProtoModulePins = bufmodule.NewProtoModulePinsForModulePins(currentModulePins...)
		} else {
			protoDependencyModuleReferences = bufmodule.NewProtoModuleReferencesForModuleReferences(
				moduleConfig.Build.DependencyModuleReferences...,
			)
		}
		protoDependencyModulePins, err := service.GetModulePins(
			ctx,
			protoDependencyModuleReferences,
			currentProtoModulePins,
		)
		if err != nil {
			if rpc.GetErrorCode(err) == rpc.ErrorCodeUnimplemented && remote != bufrpc.DefaultRemote {
				return bufcli.NewUnimplementedRemoteError(err, remote, moduleConfig.ModuleIdentity.IdentityString())
			}
			return err
		}
		dependencyModulePins, err = bufmodule.NewModulePinsForProtos(protoDependencyModulePins...)
		if err != nil {
			return bufcli.NewInternalError(err)
		}
	}
	if err := bufmodule.PutDependencyModulePinsToBucket(ctx, readWriteBucket, dependencyModulePins); err != nil {
		return bufcli.NewInternalError(err)
	}
	return nil
}
