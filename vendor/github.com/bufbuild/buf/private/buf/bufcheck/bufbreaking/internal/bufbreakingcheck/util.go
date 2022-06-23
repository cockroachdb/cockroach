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

package bufbreakingcheck

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/pkg/protosource"
)

var (
	// https://developers.google.com/protocol-buffers/docs/proto3#updating
	fieldDescriptorProtoTypeToWireCompatiblityGroup = map[protosource.FieldDescriptorProtoType]int{
		protosource.FieldDescriptorProtoTypeInt32:  1,
		protosource.FieldDescriptorProtoTypeInt64:  1,
		protosource.FieldDescriptorProtoTypeUint32: 1,
		protosource.FieldDescriptorProtoTypeUint64: 1,
		protosource.FieldDescriptorProtoTypeBool:   1,
		protosource.FieldDescriptorProtoTypeSint32: 2,
		protosource.FieldDescriptorProtoTypeSint64: 2,
		// While string and bytes are compatible if the bytes are valid UTF-8, we cannot
		// determine if a field will actually be valid UTF-8, as we are concerned with the
		// definitions and not individual messages, so we have these in different
		// compatibility groups. We allow string to evolve to bytes, but not bytes to
		// string, but we need them to be in different compatibility groups so that
		// we have to manually detect this.
		protosource.FieldDescriptorProtoTypeString:   3,
		protosource.FieldDescriptorProtoTypeBytes:    4,
		protosource.FieldDescriptorProtoTypeFixed32:  5,
		protosource.FieldDescriptorProtoTypeSfixed32: 5,
		protosource.FieldDescriptorProtoTypeFixed64:  6,
		protosource.FieldDescriptorProtoTypeSfixed64: 6,
		protosource.FieldDescriptorProtoTypeDouble:   7,
		protosource.FieldDescriptorProtoTypeFloat:    8,
		protosource.FieldDescriptorProtoTypeGroup:    9,
		// Embedded messages are compatible with bytes if the bytes are serialized versions
		// of the message, but we have no way of verifying this.
		protosource.FieldDescriptorProtoTypeMessage: 10,
		// Enum is compatible with int32, uint32, int64, uint64 if the values match
		// an enum value, but we have no way of verifying this.
		protosource.FieldDescriptorProtoTypeEnum: 11,
	}

	// https://developers.google.com/protocol-buffers/docs/proto3#json
	// this is not just JSON-compatible, but also wire-compatible, i.e. the intersection
	fieldDescriptorProtoTypeToWireJSONCompatiblityGroup = map[protosource.FieldDescriptorProtoType]int{
		// fixed32 not compatible for wire so not included
		protosource.FieldDescriptorProtoTypeInt32:  1,
		protosource.FieldDescriptorProtoTypeUint32: 1,
		// fixed64 not compatible for wire so not included
		protosource.FieldDescriptorProtoTypeInt64:    2,
		protosource.FieldDescriptorProtoTypeUint64:   2,
		protosource.FieldDescriptorProtoTypeFixed32:  3,
		protosource.FieldDescriptorProtoTypeSfixed32: 3,
		protosource.FieldDescriptorProtoTypeFixed64:  4,
		protosource.FieldDescriptorProtoTypeSfixed64: 4,
		protosource.FieldDescriptorProtoTypeBool:     5,
		protosource.FieldDescriptorProtoTypeSint32:   6,
		protosource.FieldDescriptorProtoTypeSint64:   7,
		protosource.FieldDescriptorProtoTypeString:   8,
		protosource.FieldDescriptorProtoTypeBytes:    9,
		protosource.FieldDescriptorProtoTypeDouble:   10,
		protosource.FieldDescriptorProtoTypeFloat:    11,
		protosource.FieldDescriptorProtoTypeGroup:    12,
		protosource.FieldDescriptorProtoTypeMessage:  14,
		protosource.FieldDescriptorProtoTypeEnum:     15,
	}
)

// addFunc adds a FileAnnotation.
//
// Both the Descriptor and Location can be nil.
type addFunc func(protosource.Descriptor, []protosource.Descriptor, protosource.Location, string, ...interface{})

// corpus is a store of the previous files and files given to a check function.
//
// this is passed down so that pair functions have access to the original inputs.
type corpus struct {
	previousFiles []protosource.File
	files         []protosource.File
}

func newCorpus(
	previousFiles []protosource.File,
	files []protosource.File,
) *corpus {
	return &corpus{
		previousFiles: previousFiles,
		files:         files,
	}
}

func newFilesCheckFunc(
	f func(addFunc, *corpus) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return func(id string, ignoreFunc internal.IgnoreFunc, previousFiles []protosource.File, files []protosource.File) ([]bufanalysis.FileAnnotation, error) {
		helper := internal.NewHelper(id, ignoreFunc)
		if err := f(helper.AddFileAnnotationWithExtraIgnoreDescriptorsf, newCorpus(previousFiles, files)); err != nil {
			return nil, err
		}
		return helper.FileAnnotations(), nil
	}
}

func newFilePairCheckFunc(
	f func(addFunc, *corpus, protosource.File, protosource.File) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, corpus *corpus) error {
			previousFilePathToFile, err := protosource.FilePathToFile(corpus.previousFiles...)
			if err != nil {
				return err
			}
			filePathToFile, err := protosource.FilePathToFile(corpus.files...)
			if err != nil {
				return err
			}
			for previousFilePath, previousFile := range previousFilePathToFile {
				if file, ok := filePathToFile[previousFilePath]; ok {
					if err := f(add, corpus, previousFile, file); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func newEnumPairCheckFunc(
	f func(addFunc, *corpus, protosource.Enum, protosource.Enum) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, corpus *corpus) error {
			previousFullNameToEnum, err := protosource.FullNameToEnum(corpus.previousFiles...)
			if err != nil {
				return err
			}
			fullNameToEnum, err := protosource.FullNameToEnum(corpus.files...)
			if err != nil {
				return err
			}
			for previousFullName, previousEnum := range previousFullNameToEnum {
				if enum, ok := fullNameToEnum[previousFullName]; ok {
					if err := f(add, corpus, previousEnum, enum); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

// compares all the enums that are of the same number
// map is from name to EnumValue for the given number
func newEnumValuePairCheckFunc(
	f func(addFunc, *corpus, map[string]protosource.EnumValue, map[string]protosource.EnumValue) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newEnumPairCheckFunc(
		func(add addFunc, corpus *corpus, previousEnum protosource.Enum, enum protosource.Enum) error {
			previousNumberToNameToEnumValue, err := protosource.NumberToNameToEnumValue(previousEnum)
			if err != nil {
				return err
			}
			numberToNameToEnumValue, err := protosource.NumberToNameToEnumValue(enum)
			if err != nil {
				return err
			}
			for previousNumber, previousNameToEnumValue := range previousNumberToNameToEnumValue {
				if nameToEnumValue, ok := numberToNameToEnumValue[previousNumber]; ok {
					if err := f(add, corpus, previousNameToEnumValue, nameToEnumValue); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func newMessagePairCheckFunc(
	f func(addFunc, *corpus, protosource.Message, protosource.Message) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, corpus *corpus) error {
			previousFullNameToMessage, err := protosource.FullNameToMessage(corpus.previousFiles...)
			if err != nil {
				return err
			}
			fullNameToMessage, err := protosource.FullNameToMessage(corpus.files...)
			if err != nil {
				return err
			}
			for previousFullName, previousMessage := range previousFullNameToMessage {
				if message, ok := fullNameToMessage[previousFullName]; ok {
					if err := f(add, corpus, previousMessage, message); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func newFieldPairCheckFunc(
	f func(addFunc, *corpus, protosource.Field, protosource.Field) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newMessagePairCheckFunc(
		func(add addFunc, corpus *corpus, previousMessage protosource.Message, message protosource.Message) error {
			previousNumberToField, err := protosource.NumberToMessageField(previousMessage)
			if err != nil {
				return err
			}
			numberToField, err := protosource.NumberToMessageField(message)
			if err != nil {
				return err
			}
			for previousNumber, previousField := range previousNumberToField {
				if field, ok := numberToField[previousNumber]; ok {
					if err := f(add, corpus, previousField, field); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func newServicePairCheckFunc(
	f func(addFunc, *corpus, protosource.Service, protosource.Service) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, corpus *corpus) error {
			previousFullNameToService, err := protosource.FullNameToService(corpus.previousFiles...)
			if err != nil {
				return err
			}
			fullNameToService, err := protosource.FullNameToService(corpus.files...)
			if err != nil {
				return err
			}
			for previousFullName, previousService := range previousFullNameToService {
				if service, ok := fullNameToService[previousFullName]; ok {
					if err := f(add, corpus, previousService, service); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func newMethodPairCheckFunc(
	f func(addFunc, *corpus, protosource.Method, protosource.Method) error,
) func(string, internal.IgnoreFunc, []protosource.File, []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return newServicePairCheckFunc(
		func(add addFunc, corpus *corpus, previousService protosource.Service, service protosource.Service) error {
			previousNameToMethod, err := protosource.NameToMethod(previousService)
			if err != nil {
				return err
			}
			nameToMethod, err := protosource.NameToMethod(service)
			if err != nil {
				return err
			}
			for previousName, previousMethod := range previousNameToMethod {
				if method, ok := nameToMethod[previousName]; ok {
					if err := f(add, corpus, previousMethod, method); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func getDescriptorAndLocationForDeletedEnum(file protosource.File, previousNestedName string) (protosource.Descriptor, protosource.Location, error) {
	if strings.Contains(previousNestedName, ".") {
		nestedNameToMessage, err := protosource.NestedNameToMessage(file)
		if err != nil {
			return nil, nil, err
		}
		split := strings.Split(previousNestedName, ".")
		for i := len(split) - 1; i > 0; i-- {
			if message, ok := nestedNameToMessage[strings.Join(split[0:i], ".")]; ok {
				return message, message.Location(), nil
			}
		}
	}
	return file, nil, nil
}

func getDescriptorAndLocationForDeletedMessage(file protosource.File, nestedNameToMessage map[string]protosource.Message, previousNestedName string) (protosource.Descriptor, protosource.Location) {
	if strings.Contains(previousNestedName, ".") {
		split := strings.Split(previousNestedName, ".")
		for i := len(split) - 1; i > 0; i-- {
			if message, ok := nestedNameToMessage[strings.Join(split[0:i], ".")]; ok {
				return message, message.Location()
			}
		}
	}
	return file, nil
}

func getSortedEnumValueNames(nameToEnumValue map[string]protosource.EnumValue) []string {
	names := make([]string, 0, len(nameToEnumValue))
	for name := range nameToEnumValue {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func getEnumByFullName(files []protosource.File, enumFullName string) (protosource.Enum, error) {
	fullNameToEnum, err := protosource.FullNameToEnum(files...)
	if err != nil {
		return nil, err
	}
	enum, ok := fullNameToEnum[enumFullName]
	if !ok {
		return nil, fmt.Errorf("expected enum %q to exist but was not found", enumFullName)
	}
	return enum, nil
}

func withBackupLocation(primary protosource.Location, secondary protosource.Location) protosource.Location {
	if primary != nil {
		return primary
	}
	return secondary
}
