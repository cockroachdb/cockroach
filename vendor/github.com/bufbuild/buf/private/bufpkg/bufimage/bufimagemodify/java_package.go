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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	// DefaultJavaPackagePrefix is the default java_package prefix used in the java_package modifier.
	DefaultJavaPackagePrefix = "com"

	// JavaPackageID is the ID of the java_package modifier.
	JavaPackageID = "JAVA_PACKAGE"
)

// javaPackagePath is the SourceCodeInfo path for the java_package option.
// https://github.com/protocolbuffers/protobuf/blob/61689226c0e3ec88287eaed66164614d9c4f2bf7/src/google/protobuf/descriptor.proto#L348
var javaPackagePath = []int32{8, 1}

func javaPackage(
	sweeper Sweeper,
	packagePrefix string,
	overrides map[string]string,
) (Modifier, error) {
	if packagePrefix == "" {
		return nil, fmt.Errorf("a non-empty package prefix is required")
	}
	return ModifierFunc(
		func(ctx context.Context, image bufimage.Image) error {
			for _, imageFile := range image.Files() {
				javaPackageValue := javaPackageValue(imageFile, packagePrefix)
				if overridePackagePrefix, ok := overrides[imageFile.Path()]; ok {
					javaPackageValue = overridePackagePrefix
				}
				if err := javaPackageForFile(ctx, sweeper, imageFile, javaPackageValue); err != nil {
					return err
				}
			}
			return nil
		},
	), nil
}

func javaPackageForFile(
	ctx context.Context,
	sweeper Sweeper,
	imageFile bufimage.ImageFile,
	javaPackageValue string,
) error {
	descriptor := imageFile.Proto()
	if isWellKnownType(ctx, imageFile) || javaPackageValue == "" {
		// This is a well-known type or we could not resolve a non-empty java_package
		// value, so this is a no-op.
		return nil
	}
	if descriptor.Options == nil {
		descriptor.Options = &descriptorpb.FileOptions{}
	}
	descriptor.Options.JavaPackage = proto.String(javaPackageValue)
	if sweeper != nil {
		sweeper.mark(imageFile.Path(), javaPackagePath)
	}
	return nil
}

// javaPackageValue returns the java_package for the given ImageFile based on its
// package declaration. If the image file doesn't have a package declaration, an
// empty string is returned.
func javaPackageValue(imageFile bufimage.ImageFile, packagePrefix string) string {
	if pkg := imageFile.Proto().GetPackage(); pkg != "" {
		return packagePrefix + "." + pkg
	}
	return ""
}
