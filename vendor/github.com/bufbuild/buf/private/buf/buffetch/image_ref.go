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

package buffetch

import (
	"github.com/bufbuild/buf/private/buf/buffetch/internal"
	"github.com/bufbuild/buf/private/pkg/normalpath"
)

var _ ImageRef = &imageRef{}

type imageRef struct {
	fileRef       internal.FileRef
	imageEncoding ImageEncoding
}

func newImageRef(
	fileRef internal.FileRef,
	imageEncoding ImageEncoding,
) *imageRef {
	return &imageRef{
		fileRef:       fileRef,
		imageEncoding: imageEncoding,
	}
}

func (r *imageRef) PathForExternalPath(externalPath string) (string, error) {
	return normalpath.NormalizeAndValidate(externalPath)
}

func (r *imageRef) ImageEncoding() ImageEncoding {
	return r.imageEncoding
}

func (r *imageRef) IsNull() bool {
	return r.fileRef.FileScheme() == internal.FileSchemeNull
}

func (r *imageRef) internalRef() internal.Ref {
	return r.fileRef
}

func (r *imageRef) internalFileRef() internal.FileRef {
	return r.fileRef
}
