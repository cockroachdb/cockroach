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
	"path"
	"strconv"
	"strings"

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/gen/data/datawkt"
	"github.com/bufbuild/buf/private/pkg/protoversion"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Modifier modifies Images.
type Modifier interface {
	// Modify modifies the Image.
	Modify(context.Context, bufimage.Image) error
}

// NewMultiModifier returns a new Modifier for the given Modifiers.
func NewMultiModifier(modifiers ...Modifier) Modifier {
	switch len(modifiers) {
	case 0:
		return nil
	case 1:
		return modifiers[0]
	default:
		return newMultiModifier(modifiers)
	}
}

// ModifierFunc is a convenience type that implements the Modifier interface.
type ModifierFunc func(context.Context, bufimage.Image) error

// Modify invokes the ModifierFunc with the given context and image.
func (m ModifierFunc) Modify(ctx context.Context, image bufimage.Image) error {
	return m(ctx, image)
}

// Sweeper is used to mark-and-sweep SourceCodeInfo_Locations from images.
type Sweeper interface {
	// Sweep implements the ModifierFunc signature so that the Sweeper
	// can be used as a Modifier.
	Sweep(context.Context, bufimage.Image) error

	// mark is un-exported so that the Sweeper cannot be implemented
	// outside of this package.
	mark(string, []int32)
}

// NewFileOptionSweeper constructs a new file option Sweeper that removes
// the SourceCodeInfo_Locations associated with the marks.
func NewFileOptionSweeper() Sweeper {
	return newFileOptionSweeper()
}

// Merge merges the given modifiers together so that they are run in the order
// they are provided. This is particularly useful for constructing a modifier
// from its initial 'nil' value.
//
//  var modifier Modifier
//  if config.JavaMultipleFiles {
//    modifier = Merge(modifier, JavaMultipleFiles)
//  }
func Merge(left Modifier, right Modifier) Modifier {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return NewMultiModifier(left, right)
}

// CcEnableArenas returns a Modifier that sets the cc_enable_arenas
// file option to the given value in all of the files contained in
// the Image.
func CcEnableArenas(sweeper Sweeper, value bool, overrides map[string]string) (Modifier, error) {
	validatedOverrides, err := stringOverridesToBoolOverrides(overrides)
	if err != nil {
		return nil, fmt.Errorf("invalid override for %s: %w", CcEnableArenasID, err)
	}
	return ccEnableArenas(sweeper, value, validatedOverrides), nil
}

// GoPackage returns a Modifier that sets the go_package file option
// according to the given defaultImportPathPrefix, exceptions, and
// overrides.
func GoPackage(
	logger *zap.Logger,
	sweeper Sweeper,
	defaultImportPathPrefix string,
	except []bufmodule.ModuleIdentity,
	moduleOverrides map[bufmodule.ModuleIdentity]string,
	overrides map[string]string,
) (Modifier, error) {
	return goPackage(
		logger,
		sweeper,
		defaultImportPathPrefix,
		except,
		moduleOverrides,
		overrides,
	)
}

// JavaMultipleFiles returns a Modifier that sets the java_multiple_files
// file option to the given value in all of the files contained in
// the Image.
func JavaMultipleFiles(sweeper Sweeper, value bool, overrides map[string]string) (Modifier, error) {
	validatedOverrides, err := stringOverridesToBoolOverrides(overrides)
	if err != nil {
		return nil, fmt.Errorf("invalid override for %s: %w", JavaMultipleFilesID, err)
	}
	return javaMultipleFiles(sweeper, value, validatedOverrides), nil
}

// JavaOuterClassname returns a Modifier that sets the java_outer_classname file option
// in all of the files contained in the Image based on the PascalCase of their filename.
func JavaOuterClassname(sweeper Sweeper, overrides map[string]string) Modifier {
	return javaOuterClassname(sweeper, overrides)
}

// JavaPackage returns a Modifier that sets the java_package file option
// according to the given packagePrefix.
func JavaPackage(sweeper Sweeper, packagePrefix string, overrides map[string]string) (Modifier, error) {
	return javaPackage(sweeper, packagePrefix, overrides)
}

// JavaStringCheckUtf8 returns a Modifier that sets the java_string_check_utf8 file option according
// to the given value.
func JavaStringCheckUtf8(sweeper Sweeper, value bool, overrides map[string]string) (Modifier, error) {
	validatedOverrides, err := stringOverridesToBoolOverrides(overrides)
	if err != nil {
		return nil, fmt.Errorf("invalid override for %s: %w", JavaStringCheckUtf8ID, err)
	}
	return javaStringCheckUtf8(sweeper, value, validatedOverrides), nil
}

// OptimizeFor returns a Modifier that sets the optimize_for file
// option to the given value in all of the files contained in
// the Image.
func OptimizeFor(
	sweeper Sweeper,
	value descriptorpb.FileOptions_OptimizeMode,
	overrides map[string]string,
) (Modifier, error) {
	validatedOverrides, err := stringOverridesToOptimizeModeOverrides(overrides)
	if err != nil {
		return nil, fmt.Errorf("invalid override for %s: %w", OptimizeForID, err)
	}
	return optimizeFor(sweeper, value, validatedOverrides), nil
}

// GoPackageImportPathForFile returns the go_package import path for the given
// ImageFile. If the package contains a version suffix, and if there are more
// than two components, concatenate the final two components. Otherwise, we
// exclude the ';' separator and adopt the default behavior from the import path.
//
// For example, an ImageFile with `package acme.weather.v1;` will include `;weatherv1`
// in the `go_package` declaration so that the generated package is named as such.
func GoPackageImportPathForFile(imageFile bufimage.ImageFile, importPathPrefix string) string {
	goPackageImportPath := path.Join(importPathPrefix, path.Dir(imageFile.Path()))
	packageName := imageFile.FileDescriptor().GetPackage()
	if _, ok := protoversion.NewPackageVersionForPackage(packageName); ok {
		parts := strings.Split(packageName, ".")
		if len(parts) >= 2 {
			goPackageImportPath += ";" + parts[len(parts)-2] + parts[len(parts)-1]
		}
	}
	return goPackageImportPath
}

// ObjcClassPrefix returns a Modifier that sets the objc_class_prefix file option
// according to the package name. It is set to the uppercase first letter of each package sub-name,
// not including the package version, with the following rules:
//  * If the resulting abbreviation is 2 characters, add "X".
//  * If the resulting abbreviation is 1 character, add "XX".
//  * If the resulting abbreviation is "GPB", change it to "GPX".
//    "GPB" is reserved by Google for the Protocol Buffers implementation.
func ObjcClassPrefix(sweeper Sweeper, overrides map[string]string) Modifier {
	return objcClassPrefix(sweeper, overrides)
}

// CsharpNamespace returns a Modifier that sets the csharp_namespace file option
// according to the package name. It is set to the package name with each package sub-name capitalized.
func CsharpNamespace(sweeper Sweeper, overrides map[string]string) Modifier {
	return csharpNamespace(sweeper, overrides)
}

// PhpNamespace returns a Modifier that sets the php_namespace file option
// according to the package name. It is set to the package name with each package sub-name capitalized
// and each "." replaced with "\\".
func PhpNamespace(sweeper Sweeper, overrides map[string]string) Modifier {
	return phpNamespace(sweeper, overrides)
}

// PhpMetadataNamespace returns a Modifier that sets the php_metadata_namespace file option
// according to the package name. It appends "\\GPBMetadata" to the heuristic used by PhpNamespace.
func PhpMetadataNamespace(sweeper Sweeper, overrides map[string]string) Modifier {
	return phpMetadataNamespace(sweeper, overrides)
}

// RubyPackage returns a Modifier that sets the ruby_package file option
// according to the given packagePrefix. It is set to the package name with each package sub-name capitalized
// and each "." replaced with "::".
func RubyPackage(sweeper Sweeper, overrides map[string]string) Modifier {
	return rubyPackage(sweeper, overrides)
}

// isWellKnownType returns true if the given path is one of the well-known types.
func isWellKnownType(ctx context.Context, imageFile bufimage.ImageFile) bool {
	if _, err := datawkt.ReadBucket.Stat(ctx, imageFile.Path()); err == nil {
		return true
	}
	return false
}

// int32SliceIsEqual returns true if x and y contain the same elements.
func int32SliceIsEqual(x []int32, y []int32) bool {
	if len(x) != len(y) {
		return false
	}
	for i, elem := range x {
		if elem != y[i] {
			return false
		}
	}
	return true
}

func stringOverridesToBoolOverrides(stringOverrides map[string]string) (map[string]bool, error) {
	validatedOverrides := make(map[string]bool, len(stringOverrides))
	for fileImportPath, overrideString := range stringOverrides {
		overrideBool, err := strconv.ParseBool(overrideString)
		if err != nil {
			return nil, fmt.Errorf("non-boolean override %s set for file %s", overrideString, fileImportPath)
		}
		validatedOverrides[fileImportPath] = overrideBool
	}
	return validatedOverrides, nil
}

func stringOverridesToOptimizeModeOverrides(stringOverrides map[string]string) (map[string]descriptorpb.FileOptions_OptimizeMode, error) {
	validatedOverrides := make(map[string]descriptorpb.FileOptions_OptimizeMode, len(stringOverrides))
	for fileImportPath, stringOverride := range stringOverrides {
		optimizeMode, ok := descriptorpb.FileOptions_OptimizeMode_value[stringOverride]
		if !ok {
			return nil, fmt.Errorf("invalid optimize mode %s set for file %s", stringOverride, fileImportPath)
		}
		validatedOverrides[fileImportPath] = descriptorpb.FileOptions_OptimizeMode(optimizeMode)
	}
	return validatedOverrides, nil
}
