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

package bufimage

import (
	"fmt"
	"sort"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	imagev1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/image/v1"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/protodescriptor"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// ImageFile is a Protobuf file within an image.
type ImageFile interface {
	bufmodule.FileInfo
	// Proto is the backing *descriptorpb.FileDescriptorProto for this File.
	//
	// FileDescriptor should be preferred to Proto. We keep this method around
	// because we have code that does modification to the ImageFile via this.
	//
	// This will never be nil.
	// The value Path() is equal to Proto.GetName() .
	Proto() *descriptorpb.FileDescriptorProto
	// FileDescriptor is the backing FileDescriptor for this File.
	//
	// This will never be nil.
	// The value Path() is equal to FileDescriptor.GetName() .
	FileDescriptor() protodescriptor.FileDescriptor
	// IsSyntaxUnspecified will be true if the syntax was not explicitly specified.
	IsSyntaxUnspecified() bool
	// UnusedDependencyIndexes returns the indexes of the unused dependencies within
	// FileDescriptor.GetDependency().
	//
	// All indexes will be valid.
	// Will return nil if empty.
	UnusedDependencyIndexes() []int32

	withIsImport(isImport bool) ImageFile
	isImageFile()
}

// NewImageFile returns a new ImageFile.
//
// If externalPath is empty, path is used.
//
// TODO: moduleIdentity and commit should be options since they are optional.
func NewImageFile(
	fileDescriptor protodescriptor.FileDescriptor,
	moduleIdentity bufmodule.ModuleIdentity,
	commit string,
	externalPath string,
	isImport bool,
	isSyntaxUnspecified bool,
	unusedDependencyIndexes []int32,
) (ImageFile, error) {
	return newImageFile(
		fileDescriptor,
		moduleIdentity,
		commit,
		externalPath,
		isImport,
		isSyntaxUnspecified,
		unusedDependencyIndexes,
	)
}

// Image is a buf image.
type Image interface {
	// Files are the files that comprise the image.
	//
	// This contains all files, including imports if available.
	// The returned files are in correct DAG order.
	Files() []ImageFile
	// GetFile gets the file for the root relative file path.
	//
	// If the file does not exist, nil is returned.
	// The path is expected to be normalized and validated.
	// Note that all values of GetDependency() can be used here.
	GetFile(path string) ImageFile
	isImage()
}

// NewImage returns a new Image for the given ImageFiles.
//
// The input ImageFiles are expected to be in correct DAG order!
// TODO: Consider checking the above, and if not, reordering the Files.
// If imageFiles is empty, returns error
func NewImage(imageFiles []ImageFile) (Image, error) {
	return newImage(imageFiles, false)
}

// NewMultiImage returns a new Image for the given Images.
//
// Reorders the ImageFiles to be in DAG order.
// Duplicates cannot exist across the Images.
func NewMultiImage(images ...Image) (Image, error) {
	switch len(images) {
	case 0:
		return nil, nil
	case 1:
		return images[0], nil
	default:
		var imageFiles []ImageFile
		for _, image := range images {
			imageFiles = append(imageFiles, image.Files()...)
		}
		return newImage(imageFiles, true)
	}
}

// MergeImages returns a new Image for the given Images. ImageFiles
// treated as non-imports in at least one of the given Images will
// be treated as non-imports in the returned Image. The first non-import
// version of a file will be used in the result.
//
// Reorders the ImageFiles to be in DAG order.
// Duplicates can exist across the Images, but only if duplicates are non-imports.
func MergeImages(images ...Image) (Image, error) {
	switch len(images) {
	case 0:
		return nil, nil
	case 1:
		return images[0], nil
	default:
		imageFileSet := make(map[string]ImageFile)
		for _, image := range images {
			for _, currentImageFile := range image.Files() {
				storedImageFile, ok := imageFileSet[currentImageFile.Path()]
				if !ok {
					imageFileSet[currentImageFile.Path()] = currentImageFile
					continue
				}
				if !storedImageFile.IsImport() && !currentImageFile.IsImport() {
					return nil, fmt.Errorf("%s is a non-import in multiple images", currentImageFile.Path())
				}
				if storedImageFile.IsImport() && !currentImageFile.IsImport() {
					imageFileSet[currentImageFile.Path()] = currentImageFile
				}
			}
		}
		imageFiles := make([]ImageFile, 0, len(imageFileSet))
		for _, imageFile := range imageFileSet {
			imageFiles = append(imageFiles, imageFile)
		}
		return newImage(imageFiles, true)
	}
}

// NewImageForProto returns a new Image for the given proto Image.
//
// The input Files are expected to be in correct DAG order!
// TODO: Consider checking the above, and if not, reordering the Files.
//
// TODO: do we want to add the ability to do external path resolution here?
func NewImageForProto(protoImage *imagev1.Image) (Image, error) {
	if err := validateProtoImage(protoImage); err != nil {
		return nil, err
	}
	imageFiles := make([]ImageFile, len(protoImage.File))
	for i, protoImageFile := range protoImage.File {
		var isImport bool
		var isSyntaxUnspecified bool
		var unusedDependencyIndexes []int32
		var moduleIdentity bufmodule.ModuleIdentity
		var commit string
		var err error
		if protoImageFileExtension := protoImageFile.GetBufExtension(); protoImageFileExtension != nil {
			isImport = protoImageFileExtension.GetIsImport()
			isSyntaxUnspecified = protoImageFileExtension.GetIsSyntaxUnspecified()
			unusedDependencyIndexes = protoImageFileExtension.GetUnusedDependency()
			if protoModuleInfo := protoImageFileExtension.GetModuleInfo(); protoModuleInfo != nil {
				if protoModuleName := protoModuleInfo.GetName(); protoModuleName != nil {
					moduleIdentity, err = bufmodule.NewModuleIdentity(
						protoModuleName.GetRemote(),
						protoModuleName.GetOwner(),
						protoModuleName.GetRepository(),
					)
					if err != nil {
						return nil, err
					}
					// we only want to set this if there is a module name
					commit = protoModuleInfo.GetCommit()
				}
			}
		}
		imageFile, err := NewImageFile(
			protoImageFile,
			moduleIdentity,
			commit,
			protoImageFile.GetName(),
			isImport,
			isSyntaxUnspecified,
			unusedDependencyIndexes,
		)
		if err != nil {
			return nil, err
		}
		imageFiles[i] = imageFile
	}
	return NewImage(imageFiles)
}

// NewImageForCodeGeneratorRequest returns a new Image from a given CodeGeneratorRequest.
//
// The input Files are expected to be in correct DAG order!
// TODO: Consider checking the above, and if not, reordering the Files.
func NewImageForCodeGeneratorRequest(request *pluginpb.CodeGeneratorRequest) (Image, error) {
	if err := protodescriptor.ValidateCodeGeneratorRequestExceptFileDescriptorProtos(request); err != nil {
		return nil, err
	}
	protoImageFiles := make([]*imagev1.ImageFile, len(request.GetProtoFile()))
	for i, fileDescriptorProto := range request.GetProtoFile() {
		// we filter whether something is an import or not in ImageWithOnlyPaths
		// we cannot determine if the syntax was unset
		protoImageFiles[i] = fileDescriptorProtoToProtoImageFile(fileDescriptorProto, false, false, nil, nil, "")
	}
	image, err := NewImageForProto(
		&imagev1.Image{
			File: protoImageFiles,
		},
	)
	if err != nil {
		return nil, err
	}
	return ImageWithOnlyPaths(
		image,
		request.GetFileToGenerate(),
	)
}

// ImageWithoutImports returns a copy of the Image without imports.
//
// The backing Files are not copied.
func ImageWithoutImports(image Image) Image {
	imageFiles := image.Files()
	newImageFiles := make([]ImageFile, 0, len(imageFiles))
	for _, imageFile := range imageFiles {
		if !imageFile.IsImport() {
			newImageFiles = append(newImageFiles, imageFile)
		}
	}
	return newImageNoValidate(newImageFiles)
}

// ImageWithOnlyPaths returns a copy of the Image that only includes the files
// with the given root relative file paths or directories.
//
// Note that paths can be either files or directories - whether or not a path
// is included is a result of normalpath.EqualsOrContainsPath.
//
// If a root relative file path does not exist, this errors.
func ImageWithOnlyPaths(
	image Image,
	paths []string,
) (Image, error) {
	return imageWithOnlyPaths(image, paths, false)
}

// ImageWithOnlyPathsAllowNotExist returns a copy of the Image that only includes the files
// with the given root relative file paths.
//
// Note that paths can be either files or directories - whether or not a path
// is included is a result of normalpath.EqualsOrContainsPath.
//
// If a root relative file path does not exist, this skips this path.
func ImageWithOnlyPathsAllowNotExist(
	image Image,
	paths []string,
) (Image, error) {
	return imageWithOnlyPaths(image, paths, true)
}

// ImageByDir returns multiple images that have non-imports split
// by directory.
//
// That is, each Image will only contain a single directoy's files
// as it's non-imports, along with all required imports for the
// files in that directory.
func ImageByDir(image Image) ([]Image, error) {
	imageFiles := image.Files()
	paths := make([]string, 0, len(imageFiles))
	for _, imageFile := range imageFiles {
		if !imageFile.IsImport() {
			paths = append(paths, imageFile.Path())
		}
	}
	dirToPaths := normalpath.ByDir(paths...)
	// we need this to produce a deterministic order of the returned Images
	dirs := make([]string, 0, len(dirToPaths))
	for dir := range dirToPaths {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	newImages := make([]Image, 0, len(dirToPaths))
	for _, dir := range dirs {
		paths, ok := dirToPaths[dir]
		if !ok {
			// this should never happen
			return nil, fmt.Errorf("no dir for %q in dirToPaths", dir)
		}
		newImage, err := ImageWithOnlyPaths(image, paths)
		if err != nil {
			return nil, err
		}
		newImages = append(newImages, newImage)
	}
	return newImages, nil
}

// ImageToProtoImage returns a new ProtoImage for the Image.
func ImageToProtoImage(image Image) *imagev1.Image {
	imageFiles := image.Files()
	protoImage := &imagev1.Image{
		File: make([]*imagev1.ImageFile, len(imageFiles)),
	}
	for i, imageFile := range imageFiles {
		protoImage.File[i] = imageFileToProtoImageFile(imageFile)
	}
	return protoImage
}

// ImageToFileDescriptorSet returns a new FileDescriptorSet for the Image.
func ImageToFileDescriptorSet(image Image) *descriptorpb.FileDescriptorSet {
	return protodescriptor.FileDescriptorSetForFileDescriptors(ImageToFileDescriptors(image)...)
}

// ImageToFileDescriptors returns the FileDescriptors for the Image.
func ImageToFileDescriptors(image Image) []protodescriptor.FileDescriptor {
	return imageFilesToFileDescriptors(image.Files())
}

// ImageToFileDescriptorProtos returns the FileDescriptorProtos for the Image.
func ImageToFileDescriptorProtos(image Image) []*descriptorpb.FileDescriptorProto {
	return imageFilesToFileDescriptorProtos(image.Files())
}

// ImageToCodeGeneratorRequest returns a new CodeGeneratorRequest for the Image.
//
// All non-imports are added as files to generate.
// If includeImports is set, all non-well-known-type imports are also added as files to generate.
func ImageToCodeGeneratorRequest(
	image Image,
	parameter string,
	compilerVersion *pluginpb.Version,
	includeImports bool,
) *pluginpb.CodeGeneratorRequest {
	return imageToCodeGeneratorRequest(
		image,
		parameter,
		compilerVersion,
		includeImports,
		nil,
		nil,
	)
}

// ImagesToCodeGeneratorRequests converts the Images to CodeGeneratorRequests.
//
// All non-imports are added as files to generate.
// If includeImports is set, all non-well-known-type imports are also added as files to generate.
// If includeImports is set, only one CodeGeneratorRequest will contain any given file as a FileToGenerate.
func ImagesToCodeGeneratorRequests(
	images []Image,
	parameter string,
	compilerVersion *pluginpb.Version,
	includeImports bool,
) []*pluginpb.CodeGeneratorRequest {
	requests := make([]*pluginpb.CodeGeneratorRequest, len(images))
	// we don't need to track these if includeImports as false, so don't waste the time
	var alreadyUsedPaths map[string]struct{}
	var nonImportPaths map[string]struct{}
	if includeImports {
		alreadyUsedPaths = make(map[string]struct{})
		nonImportPaths = make(map[string]struct{})
		for _, image := range images {
			for _, imageFile := range image.Files() {
				if !imageFile.IsImport() {
					nonImportPaths[imageFile.Path()] = struct{}{}
				}
			}
		}
	}
	for i, image := range images {
		requests[i] = imageToCodeGeneratorRequest(
			image,
			parameter,
			compilerVersion,
			includeImports,
			alreadyUsedPaths,
			nonImportPaths,
		)
	}
	return requests
}

// ProtoImageToFileDescriptors returns the FileDescriptors for the proto Image.
func ProtoImageToFileDescriptors(protoImage *imagev1.Image) []protodescriptor.FileDescriptor {
	return protoImageFilesToFileDescriptors(protoImage.File)
}
