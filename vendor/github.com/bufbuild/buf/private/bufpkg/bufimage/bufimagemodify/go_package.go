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

package bufimagemodify

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// GoPackageID is the ID of the go_package modifier.
const GoPackageID = "GO_PACKAGE"

// goPackagePath is the SourceCodeInfo path for the go_package option.
// https://github.com/protocolbuffers/protobuf/blob/ee04809540c098718121e092107fbc0abc231725/src/google/protobuf/descriptor.proto#L392
var goPackagePath = []int32{8, 11}

func goPackage(
	logger *zap.Logger,
	sweeper Sweeper,
	defaultImportPathPrefix string,
	except []bufmodule.ModuleIdentity,
	moduleOverrides map[bufmodule.ModuleIdentity]string,
	overrides map[string]string,
) (Modifier, error) {
	if defaultImportPathPrefix == "" {
		return nil, fmt.Errorf("a non-empty import path prefix is required")
	}
	// Convert the bufmodule.ModuleIdentity types into
	// strings so that they're comparable.
	exceptModuleIdentityStrings := make(map[string]struct{}, len(except))
	for _, moduleIdentity := range except {
		exceptModuleIdentityStrings[moduleIdentity.IdentityString()] = struct{}{}
	}
	overrideModuleIdentityStrings := make(map[string]string, len(moduleOverrides))
	for moduleIdentity, goPackagePrefix := range moduleOverrides {
		overrideModuleIdentityStrings[moduleIdentity.IdentityString()] = goPackagePrefix
	}
	seenModuleIdentityStrings := make(map[string]struct{}, len(overrideModuleIdentityStrings))
	return ModifierFunc(
		func(ctx context.Context, image bufimage.Image) error {
			for _, imageFile := range image.Files() {
				importPathPrefix := defaultImportPathPrefix
				if moduleIdentity := imageFile.ModuleIdentity(); moduleIdentity != nil {
					moduleIdentityString := moduleIdentity.IdentityString()
					if modulePrefixOverride, ok := overrideModuleIdentityStrings[moduleIdentityString]; ok {
						importPathPrefix = modulePrefixOverride
						seenModuleIdentityStrings[moduleIdentityString] = struct{}{}
					}
				}
				goPackageValue := GoPackageImportPathForFile(imageFile, importPathPrefix)
				if overrideValue, ok := overrides[imageFile.Path()]; ok {
					goPackageValue = overrideValue
				}
				if err := goPackageForFile(
					ctx,
					sweeper,
					imageFile,
					goPackageValue,
					exceptModuleIdentityStrings,
				); err != nil {
					return err
				}
			}
			for moduleIdentityString := range overrideModuleIdentityStrings {
				if _, ok := seenModuleIdentityStrings[moduleIdentityString]; !ok {
					logger.Sugar().Warnf("go_package_prefix override for %q was unused", moduleIdentityString)
				}
			}
			return nil
		},
	), nil
}

func goPackageForFile(
	ctx context.Context,
	sweeper Sweeper,
	imageFile bufimage.ImageFile,
	goPackageValue string,
	exceptModuleIdentityStrings map[string]struct{},
) error {
	if shouldSkipGoPackageForFile(ctx, imageFile, exceptModuleIdentityStrings) {
		return nil
	}
	descriptor := imageFile.Proto()
	if descriptor.Options == nil {
		descriptor.Options = &descriptorpb.FileOptions{}
	}
	descriptor.Options.GoPackage = proto.String(goPackageValue)
	if sweeper != nil {
		sweeper.mark(imageFile.Path(), goPackagePath)
	}
	return nil
}

func shouldSkipGoPackageForFile(
	ctx context.Context,
	imageFile bufimage.ImageFile,
	exceptModuleIdentityStrings map[string]struct{},
) bool {
	if isWellKnownType(ctx, imageFile) && imageFile.Proto().GetOptions().GetGoPackage() != "" {
		// The well-known type defines the go_package option, so this is a no-op.
		// If a well-known type ever omits the go_package option, we make sure
		// to include it.
		return true
	}
	if moduleIdentity := imageFile.ModuleIdentity(); moduleIdentity != nil {
		if _, ok := exceptModuleIdentityStrings[moduleIdentity.IdentityString()]; ok {
			return true
		}
	}
	return false
}
