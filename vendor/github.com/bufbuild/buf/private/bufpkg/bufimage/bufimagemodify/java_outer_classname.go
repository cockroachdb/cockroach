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

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// JavaOuterClassNameID is the ID for the java_outer_classname modifier.
const JavaOuterClassNameID = "JAVA_OUTER_CLASSNAME"

// javaOuterClassnamePath is the SourceCodeInfo path for the java_outer_classname option.
// https://github.com/protocolbuffers/protobuf/blob/87d140f851131fb8a6e8a80449cf08e73e568259/src/google/protobuf/descriptor.proto#L356
var javaOuterClassnamePath = []int32{8, 8}

func javaOuterClassname(
	sweeper Sweeper,
	overrides map[string]string,
) Modifier {
	return ModifierFunc(
		func(ctx context.Context, image bufimage.Image) error {
			for _, imageFile := range image.Files() {
				javaOuterClassnameValue := javaOuterClassnameValue(imageFile)
				if overrideValue, ok := overrides[imageFile.Path()]; ok {
					javaOuterClassnameValue = overrideValue
				}
				if err := javaOuterClassnameForFile(ctx, sweeper, imageFile, javaOuterClassnameValue); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func javaOuterClassnameForFile(
	ctx context.Context,
	sweeper Sweeper,
	imageFile bufimage.ImageFile,
	javaOuterClassnameValue string,
) error {
	descriptor := imageFile.Proto()
	if options := descriptor.GetOptions(); isWellKnownType(ctx, imageFile) || (options != nil && options.GetJavaOuterClassname() == javaOuterClassnameValue) {
		// The file is a well-known type or already defines the java_outer_classname
		// option with the given value, so this is a no-op.
		return nil
	}
	if descriptor.Options == nil {
		descriptor.Options = &descriptorpb.FileOptions{}
	}
	descriptor.Options.JavaOuterClassname = proto.String(javaOuterClassnameValue)
	if sweeper != nil {
		sweeper.mark(imageFile.Path(), javaOuterClassnamePath)
	}
	return nil
}

func javaOuterClassnameValue(imageFile bufimage.ImageFile) string {
	return stringutil.ToPascalCase(normalpath.Base(imageFile.Path()))
}
