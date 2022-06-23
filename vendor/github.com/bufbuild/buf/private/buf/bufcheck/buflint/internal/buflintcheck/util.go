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

package buflintcheck

import (
	"strings"

	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/pkg/protosource"
	"github.com/bufbuild/buf/private/pkg/stringutil"
)

// addFunc adds a FileAnnotation.
//
// Both the Descriptor and Locations can be nil.
type addFunc func(protosource.Descriptor, protosource.Location, []protosource.Location, string, ...interface{})

func fieldToLowerSnakeCase(s string) string {
	// Try running this on googleapis and watch
	// We allow both effectively by not passing the option
	//return stringutil.ToLowerSnakeCase(s, stringutil.SnakeCaseWithNewWordOnDigits())
	return stringutil.ToLowerSnakeCase(s)
}

func fieldToUpperSnakeCase(s string) string {
	// Try running this on googleapis and watch
	// We allow both effectively by not passing the option
	//return stringutil.ToUpperSnakeCase(s, stringutil.SnakeCaseWithNewWordOnDigits())
	return stringutil.ToUpperSnakeCase(s)
}

// validLeadingComment returns true if comment has at least one line that isn't empty
// and doesn't start with CommentIgnorePrefix.
func validLeadingComment(comment string) bool {
	for _, line := range strings.Split(comment, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, CommentIgnorePrefix) {
			return true
		}
	}
	return false
}

func newFilesCheckFunc(
	f func(addFunc, []protosource.File) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return func(id string, ignoreFunc internal.IgnoreFunc, files []protosource.File) ([]bufanalysis.FileAnnotation, error) {
		helper := internal.NewHelper(id, ignoreFunc)
		if err := f(helper.AddFileAnnotationWithExtraIgnoreLocationsf, files); err != nil {
			return nil, err
		}
		return helper.FileAnnotations(), nil
	}
}

func newPackageToFilesCheckFunc(
	f func(add addFunc, pkg string, files []protosource.File) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, files []protosource.File) error {
			packageToFiles, err := protosource.PackageToFiles(files...)
			if err != nil {
				return err
			}
			for pkg, files := range packageToFiles {
				if err := f(add, pkg, files); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newDirToFilesCheckFunc(
	f func(add addFunc, dirPath string, files []protosource.File) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, files []protosource.File) error {
			dirPathToFiles, err := protosource.DirPathToFiles(files...)
			if err != nil {
				return err
			}
			for dirPath, files := range dirPathToFiles {
				if err := f(add, dirPath, files); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newFileCheckFunc(
	f func(addFunc, protosource.File) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, files []protosource.File) error {
			for _, file := range files {
				if err := f(add, file); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newFileImportCheckFunc(
	f func(addFunc, protosource.FileImport) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFileCheckFunc(
		func(add addFunc, file protosource.File) error {
			for _, fileImport := range file.FileImports() {
				if err := f(add, fileImport); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newEnumCheckFunc(
	f func(addFunc, protosource.Enum) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFileCheckFunc(
		func(add addFunc, file protosource.File) error {
			return protosource.ForEachEnum(
				func(enum protosource.Enum) error {
					return f(add, enum)
				},
				file,
			)
		},
	)
}

func newEnumValueCheckFunc(
	f func(addFunc, protosource.EnumValue) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newEnumCheckFunc(
		func(add addFunc, enum protosource.Enum) error {
			for _, enumValue := range enum.Values() {
				if err := f(add, enumValue); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newMessageCheckFunc(
	f func(addFunc, protosource.Message) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFileCheckFunc(
		func(add addFunc, file protosource.File) error {
			return protosource.ForEachMessage(
				func(message protosource.Message) error {
					return f(add, message)
				},
				file,
			)
		},
	)
}

func newFieldCheckFunc(
	f func(addFunc, protosource.Field) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newMessageCheckFunc(
		func(add addFunc, message protosource.Message) error {
			for _, field := range message.Fields() {
				if err := f(add, field); err != nil {
					return err
				}
			}
			// TODO: is this right?
			for _, field := range message.Extensions() {
				if err := f(add, field); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newOneofCheckFunc(
	f func(addFunc, protosource.Oneof) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newMessageCheckFunc(
		func(add addFunc, message protosource.Message) error {
			for _, oneof := range message.Oneofs() {
				if err := f(add, oneof); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newServiceCheckFunc(
	f func(addFunc, protosource.Service) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFileCheckFunc(
		func(add addFunc, file protosource.File) error {
			for _, service := range file.Services() {
				if err := f(add, service); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func newMethodCheckFunc(
	f func(addFunc, protosource.Method) error,
) func(string, internal.IgnoreFunc, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newServiceCheckFunc(
		func(add addFunc, service protosource.Service) error {
			for _, method := range service.Methods() {
				if err := f(add, method); err != nil {
					return err
				}
			}
			return nil
		},
	)
}
