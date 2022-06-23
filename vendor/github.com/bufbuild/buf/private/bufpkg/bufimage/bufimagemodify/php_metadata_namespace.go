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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// PhpMetadataNamespaceID is the ID of the php_metadata_namespace modifier.
const PhpMetadataNamespaceID = "PHP_METADATA_NAMESPACE"

var (
	// phpMetadataNamespacePath is the SourceCodeInfo path for the php_metadata_namespace option.
	// Ref: https://github.com/protocolbuffers/protobuf/blob/61689226c0e3ec88287eaed66164614d9c4f2bf7/src/google/protobuf/descriptor.proto#L448
	phpMetadataNamespacePath = []int32{8, 41}
)

func phpMetadataNamespace(sweeper Sweeper, overrides map[string]string) Modifier {
	return ModifierFunc(
		func(ctx context.Context, image bufimage.Image) error {
			for _, imageFile := range image.Files() {
				phpMetadataNamespaceValue := phpMetadataNamespaceValue(imageFile)
				if overrideValue, ok := overrides[imageFile.Path()]; ok {
					phpMetadataNamespaceValue = overrideValue
				}
				if err := phpMetadataNamespaceForFile(ctx, sweeper, imageFile, phpMetadataNamespaceValue); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func phpMetadataNamespaceForFile(
	ctx context.Context,
	sweeper Sweeper,
	imageFile bufimage.ImageFile,
	phpMetadataNamespaceValue string,
) error {
	descriptor := imageFile.Proto()
	if isWellKnownType(ctx, imageFile) || phpMetadataNamespaceValue == "" {
		// This is a well-known type or we could not resolve a non-empty php_metadata_namespace
		// value, so this is a no-op.
		return nil
	}
	if descriptor.Options == nil {
		descriptor.Options = &descriptorpb.FileOptions{}
	}
	descriptor.Options.PhpMetadataNamespace = proto.String(phpMetadataNamespaceValue)
	if sweeper != nil {
		sweeper.mark(imageFile.Path(), phpMetadataNamespacePath)
	}
	return nil
}

// phpMetadataNamespaceValue returns the php_metadata_namespace for the given ImageFile based on its
// package declaration. If the image file doesn't have a package declaration, an
// empty string is returned.
func phpMetadataNamespaceValue(imageFile bufimage.ImageFile) string {
	phpNamespace := phpNamespaceValue(imageFile)
	if phpNamespace == "" {
		return ""
	}
	return phpNamespace + `\GPBMetadata`
}
