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

// Package buflintcheck impelements the check functions.
//
// These are used by buflintbuild to create RuleBuilders.
package buflintcheck

import (
	"errors"
	"strconv"
	"strings"

	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/protosource"
	"github.com/bufbuild/buf/private/pkg/protoversion"
	"github.com/bufbuild/buf/private/pkg/stringutil"
)

const (
	// CommentIgnorePrefix is the comment ignore prefix.
	//
	// Comments with this prefix do not count towards valid comments in the comment checkers.
	// This is also used in buflint when constructing a new Runner, and is passed to the
	// RunnerWithIgnorePrefix option.
	CommentIgnorePrefix = "buf:lint:ignore"
)

var (
	// CheckCommentEnum is a check function.
	CheckCommentEnum = newEnumCheckFunc(checkCommentEnum)
	// CheckCommentEnumValue is a check function.
	CheckCommentEnumValue = newEnumValueCheckFunc(checkCommentEnumValue)
	// CheckCommentField is a check function.
	CheckCommentField = newFieldCheckFunc(checkCommentField)
	// CheckCommentMessage is a check function.
	CheckCommentMessage = newMessageCheckFunc(checkCommentMessage)
	// CheckCommentOneof is a check function.
	CheckCommentOneof = newOneofCheckFunc(checkCommentOneof)
	// CheckCommentService is a check function.
	CheckCommentService = newServiceCheckFunc(checkCommentService)
	// CheckCommentRPC is a check function.
	CheckCommentRPC = newMethodCheckFunc(checkCommentRPC)
)

func checkCommentEnum(add addFunc, value protosource.Enum) error {
	return checkCommentNamedDescriptor(add, value, "Enum")
}

func checkCommentEnumValue(add addFunc, value protosource.EnumValue) error {
	return checkCommentNamedDescriptor(add, value, "Enum value")
}

func checkCommentField(add addFunc, value protosource.Field) error {
	return checkCommentNamedDescriptor(add, value, "Field")
}

func checkCommentMessage(add addFunc, value protosource.Message) error {
	return checkCommentNamedDescriptor(add, value, "Message")
}

func checkCommentOneof(add addFunc, value protosource.Oneof) error {
	return checkCommentNamedDescriptor(add, value, "Oneof")
}

func checkCommentRPC(add addFunc, value protosource.Method) error {
	return checkCommentNamedDescriptor(add, value, "RPC")
}

func checkCommentService(add addFunc, value protosource.Service) error {
	return checkCommentNamedDescriptor(add, value, "Service")
}

func checkCommentNamedDescriptor(
	add addFunc,
	namedDescriptor protosource.NamedDescriptor,
	typeName string,
) error {
	location := namedDescriptor.Location()
	if location == nil {
		// this will magically skip map entry fields as well as a side-effect, although originally unintended
		return nil
	}
	if !validLeadingComment(location.LeadingComments()) {
		add(namedDescriptor, location, nil, "%s %q should have a non-empty comment for documentation.", typeName, namedDescriptor.Name())
	}
	return nil
}

// CheckDirectorySamePackage is a check function.
var CheckDirectorySamePackage = newDirToFilesCheckFunc(checkDirectorySamePackage)

func checkDirectorySamePackage(add addFunc, dirPath string, files []protosource.File) error {
	pkgMap := make(map[string]struct{})
	for _, file := range files {
		// works for no package set as this will result in "" which is a valid map key
		pkgMap[file.Package()] = struct{}{}
	}
	if len(pkgMap) > 1 {
		pkgs := stringutil.MapToSortedSlice(pkgMap)
		for _, file := range files {
			add(file, file.PackageLocation(), nil, "Multiple packages %q detected within directory %q.", strings.Join(pkgs, ","), dirPath)
		}
	}
	return nil
}

// CheckEnumNoAllowAlias is a check function.
var CheckEnumNoAllowAlias = newEnumCheckFunc(checkEnumNoAllowAlias)

func checkEnumNoAllowAlias(add addFunc, enum protosource.Enum) error {
	if enum.AllowAlias() {
		add(enum, enum.AllowAliasLocation(), nil, `Enum option "allow_alias" on enum %q must be false.`, enum.Name())
	}
	return nil
}

// CheckEnumPascalCase is a check function.
var CheckEnumPascalCase = newEnumCheckFunc(checkEnumPascalCase)

func checkEnumPascalCase(add addFunc, enum protosource.Enum) error {
	name := enum.Name()
	expectedName := stringutil.ToPascalCase(name)
	if name != expectedName {
		add(enum, enum.NameLocation(), nil, "Enum name %q should be PascalCase, such as %q.", name, expectedName)
	}
	return nil
}

// CheckEnumFirstValueZero is a check function.
var CheckEnumFirstValueZero = newEnumCheckFunc(checkEnumFirstValueZero)

func checkEnumFirstValueZero(add addFunc, enum protosource.Enum) error {
	if values := enum.Values(); len(values) > 0 {
		if firstEnumValue := values[0]; firstEnumValue.Number() != 0 {
			// proto3 compilation references the number
			add(firstEnumValue, firstEnumValue.NumberLocation(), nil, "First enum value %q should have a numeric value of 0", firstEnumValue.Name())
		}
	}
	return nil
}

// CheckEnumValuePrefix is a check function.
var CheckEnumValuePrefix = newEnumValueCheckFunc(checkEnumValuePrefix)

func checkEnumValuePrefix(add addFunc, enumValue protosource.EnumValue) error {
	name := enumValue.Name()
	expectedPrefix := fieldToUpperSnakeCase(enumValue.Enum().Name()) + "_"
	if !strings.HasPrefix(name, expectedPrefix) {
		add(
			enumValue,
			enumValue.NameLocation(),
			// also check the enum for this comment ignore
			// this allows users to set this "globally" for an enum
			// this came up in https://github.com/bufbuild/buf/issues/161
			[]protosource.Location{
				enumValue.Enum().Location(),
			},
			"Enum value name %q should be prefixed with %q.",
			name,
			expectedPrefix,
		)
	}
	return nil
}

// CheckEnumValueUpperSnakeCase is a check function.
var CheckEnumValueUpperSnakeCase = newEnumValueCheckFunc(checkEnumValueUpperSnakeCase)

func checkEnumValueUpperSnakeCase(add addFunc, enumValue protosource.EnumValue) error {
	name := enumValue.Name()
	expectedName := fieldToUpperSnakeCase(name)
	if name != expectedName {
		add(
			enumValue,
			enumValue.NameLocation(),
			// also check the enum for this comment ignore
			// this allows users to set this "globally" for an enum
			[]protosource.Location{
				enumValue.Enum().Location(),
			},
			"Enum value name %q should be UPPER_SNAKE_CASE, such as %q.",
			name,
			expectedName,
		)
	}
	return nil
}

// CheckEnumZeroValueSuffix is a check function.
var CheckEnumZeroValueSuffix = func(
	id string,
	ignoreFunc internal.IgnoreFunc,
	files []protosource.File,
	suffix string,
) ([]bufanalysis.FileAnnotation, error) {
	return newEnumValueCheckFunc(
		func(add addFunc, enumValue protosource.EnumValue) error {
			return checkEnumZeroValueSuffix(add, enumValue, suffix)
		},
	)(id, ignoreFunc, files)
}

func checkEnumZeroValueSuffix(add addFunc, enumValue protosource.EnumValue, suffix string) error {
	if enumValue.Number() != 0 {
		return nil
	}
	name := enumValue.Name()
	if !strings.HasSuffix(name, suffix) {
		add(
			enumValue,
			enumValue.NameLocation(),
			// also check the enum for this comment ignore
			// this allows users to set this "globally" for an enum
			[]protosource.Location{
				enumValue.Enum().Location(),
			},
			"Enum zero value name %q should be suffixed with %q.",
			name,
			suffix,
		)
	}
	return nil
}

// CheckFieldLowerSnakeCase is a check function.
var CheckFieldLowerSnakeCase = newFieldCheckFunc(checkFieldLowerSnakeCase)

func checkFieldLowerSnakeCase(add addFunc, field protosource.Field) error {
	message := field.Message()
	if message == nil {
		// just a sanity check
		return errors.New("field.Message() was nil")
	}
	if message.IsMapEntry() {
		// this check should always pass anyways but just in case
		return nil
	}
	name := field.Name()
	expectedName := fieldToLowerSnakeCase(name)
	if name != expectedName {
		add(
			field,
			field.NameLocation(),
			// also check the message for this comment ignore
			// this allows users to set this "globally" for a message
			[]protosource.Location{
				field.Message().Location(),
			},
			"Field name %q should be lower_snake_case, such as %q.",
			name,
			expectedName,
		)
	}
	return nil
}

// CheckFieldNoDescriptor is a check function.
var CheckFieldNoDescriptor = newFieldCheckFunc(checkFieldNoDescriptor)

func checkFieldNoDescriptor(add addFunc, field protosource.Field) error {
	name := field.Name()
	if strings.ToLower(strings.Trim(name, "_")) == "descriptor" {
		add(
			field,
			field.NameLocation(),
			// also check the message for this comment ignore
			// this allows users to set this "globally" for a message
			[]protosource.Location{
				field.Message().Location(),
			},
			`Field name %q cannot be any capitalization of "descriptor" with any number of prefix or suffix underscores.`,
			name,
		)
	}
	return nil
}

// CheckFileLowerSnakeCase is a check function.
var CheckFileLowerSnakeCase = newFileCheckFunc(checkFileLowerSnakeCase)

func checkFileLowerSnakeCase(add addFunc, file protosource.File) error {
	filename := file.Path()
	base := normalpath.Base(filename)
	ext := normalpath.Ext(filename)
	baseWithoutExt := strings.TrimSuffix(base, ext)
	expectedBaseWithoutExt := stringutil.ToLowerSnakeCase(baseWithoutExt)
	if baseWithoutExt != expectedBaseWithoutExt {
		add(file, nil, nil, `Filename %q should be lower_snake_case%s, such as "%s%s".`, base, ext, expectedBaseWithoutExt, ext)
	}
	return nil
}

var (
	// CheckImportNoPublic is a check function.
	CheckImportNoPublic = newFileImportCheckFunc(checkImportNoPublic)
	// CheckImportNoWeak is a check function.
	CheckImportNoWeak = newFileImportCheckFunc(checkImportNoWeak)
	// CheckImportUsed is a check function.
	CheckImportUsed = newFileImportCheckFunc(checkImportUsed)
)

func checkImportNoPublic(add addFunc, fileImport protosource.FileImport) error {
	return checkImportNoPublicWeak(add, fileImport, fileImport.IsPublic(), "public")
}

func checkImportNoWeak(add addFunc, fileImport protosource.FileImport) error {
	return checkImportNoPublicWeak(add, fileImport, fileImport.IsWeak(), "weak")
}

func checkImportNoPublicWeak(add addFunc, fileImport protosource.FileImport, value bool, name string) error {
	if value {
		add(fileImport, fileImport.Location(), nil, `Import %q must not be %s.`, fileImport.Import(), name)
	}
	return nil
}

func checkImportUsed(add addFunc, fileImport protosource.FileImport) error {
	if fileImport.IsUnused() {
		add(fileImport, fileImport.Location(), nil, `Import %q is unused.`, fileImport.Import())
	}
	return nil
}

// CheckMessagePascalCase is a check function.
var CheckMessagePascalCase = newMessageCheckFunc(checkMessagePascalCase)

func checkMessagePascalCase(add addFunc, message protosource.Message) error {
	if message.IsMapEntry() {
		// map entries should always be pascal case but we don't want to check them anyways
		return nil
	}
	name := message.Name()
	expectedName := stringutil.ToPascalCase(name)
	if name != expectedName {
		add(message, message.NameLocation(), nil, "Message name %q should be PascalCase, such as %q.", name, expectedName)
	}
	return nil
}

// CheckOneofLowerSnakeCase is a check function.
var CheckOneofLowerSnakeCase = newOneofCheckFunc(checkOneofLowerSnakeCase)

func checkOneofLowerSnakeCase(add addFunc, oneof protosource.Oneof) error {
	name := oneof.Name()
	expectedName := fieldToLowerSnakeCase(name)
	if name != expectedName {
		// if this is an implicit oneof for a proto3 optional field, do not error
		// https://github.com/protocolbuffers/protobuf/blob/master/docs/implementing_proto3_presence.md
		if fields := oneof.Fields(); len(fields) == 1 {
			if fields[0].Proto3Optional() {
				return nil
			}
		}
		add(
			oneof,
			oneof.NameLocation(),
			// also check the message for this comment ignore
			// this allows users to set this "globally" for a message
			[]protosource.Location{
				oneof.Message().Location(),
			},
			"Oneof name %q should be lower_snake_case, such as %q.",
			name,
			expectedName,
		)
	}
	return nil
}

// CheckPackageDefined is a check function.
var CheckPackageDefined = newFileCheckFunc(checkPackageDefined)

func checkPackageDefined(add addFunc, file protosource.File) error {
	if file.Package() == "" {
		add(file, nil, nil, "Files must have a package defined.")
	}
	return nil
}

// CheckPackageDirectoryMatch is a check function.
var CheckPackageDirectoryMatch = newFileCheckFunc(checkPackageDirectoryMatch)

func checkPackageDirectoryMatch(add addFunc, file protosource.File) error {
	pkg := file.Package()
	if pkg == "" {
		return nil
	}
	expectedDirPath := strings.ReplaceAll(pkg, ".", "/")
	dirPath := normalpath.Dir(file.Path())
	// need to check case where in root relative directory and no package defined
	// this should be valid although if SENSIBLE is turned on this will be invalid
	if dirPath != expectedDirPath {
		add(file, file.PackageLocation(), nil, `Files with package %q must be within a directory "%s" relative to root but were in directory "%s".`, pkg, normalpath.Unnormalize(expectedDirPath), normalpath.Unnormalize(dirPath))
	}
	return nil
}

// CheckPackageLowerSnakeCase is a check function.
var CheckPackageLowerSnakeCase = newFileCheckFunc(checkPackageLowerSnakeCase)

func checkPackageLowerSnakeCase(add addFunc, file protosource.File) error {
	pkg := file.Package()
	if pkg == "" {
		return nil
	}
	split := strings.Split(pkg, ".")
	for i, elem := range split {
		split[i] = stringutil.ToLowerSnakeCase(elem)
	}
	expectedPkg := strings.Join(split, ".")
	if pkg != expectedPkg {
		add(file, file.PackageLocation(), nil, "Package name %q should be lower_snake.case, such as %q.", pkg, expectedPkg)
	}
	return nil
}

// CheckPackageSameDirectory is a check function.
var CheckPackageSameDirectory = newPackageToFilesCheckFunc(checkPackageSameDirectory)

func checkPackageSameDirectory(add addFunc, pkg string, files []protosource.File) error {
	dirMap := make(map[string]struct{})
	for _, file := range files {
		dirMap[normalpath.Dir(file.Path())] = struct{}{}
	}
	if len(dirMap) > 1 {
		dirs := stringutil.MapToSortedSlice(dirMap)
		for _, file := range files {
			add(file, file.PackageLocation(), nil, "Multiple directories %q contain files with package %q.", strings.Join(dirs, ","), pkg)
		}
	}
	return nil
}

var (
	// CheckPackageSameCsharpNamespace is a check function.
	CheckPackageSameCsharpNamespace = newPackageToFilesCheckFunc(checkPackageSameCsharpNamespace)
	// CheckPackageSameGoPackage is a check function.
	CheckPackageSameGoPackage = newPackageToFilesCheckFunc(checkPackageSameGoPackage)
	// CheckPackageSameJavaMultipleFiles is a check function.
	CheckPackageSameJavaMultipleFiles = newPackageToFilesCheckFunc(checkPackageSameJavaMultipleFiles)
	// CheckPackageSameJavaPackage is a check function.
	CheckPackageSameJavaPackage = newPackageToFilesCheckFunc(checkPackageSameJavaPackage)
	// CheckPackageSamePhpNamespace is a check function.
	CheckPackageSamePhpNamespace = newPackageToFilesCheckFunc(checkPackageSamePhpNamespace)
	// CheckPackageSameRubyPackage is a check function.
	CheckPackageSameRubyPackage = newPackageToFilesCheckFunc(checkPackageSameRubyPackage)
	// CheckPackageSameSwiftPrefix is a check function.
	CheckPackageSameSwiftPrefix = newPackageToFilesCheckFunc(checkPackageSameSwiftPrefix)
)

func checkPackageSameCsharpNamespace(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(add, pkg, files, protosource.File.CsharpNamespace, protosource.File.CsharpNamespaceLocation, "csharp_namespace")
}

func checkPackageSameGoPackage(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(add, pkg, files, protosource.File.GoPackage, protosource.File.GoPackageLocation, "go_package")
}

func checkPackageSameJavaMultipleFiles(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(
		add,
		pkg,
		files,
		func(file protosource.File) string {
			return strconv.FormatBool(file.JavaMultipleFiles())
		},
		protosource.File.JavaMultipleFilesLocation,
		"java_multiple_files",
	)
}

func checkPackageSameJavaPackage(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(add, pkg, files, protosource.File.JavaPackage, protosource.File.JavaPackageLocation, "java_package")
}

func checkPackageSamePhpNamespace(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(add, pkg, files, protosource.File.PhpNamespace, protosource.File.PhpNamespaceLocation, "php_namespace")
}

func checkPackageSameRubyPackage(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(add, pkg, files, protosource.File.RubyPackage, protosource.File.RubyPackageLocation, "ruby_package")
}

func checkPackageSameSwiftPrefix(add addFunc, pkg string, files []protosource.File) error {
	return checkPackageSameOptionValue(add, pkg, files, protosource.File.SwiftPrefix, protosource.File.SwiftPrefixLocation, "swift_prefix")
}

func checkPackageSameOptionValue(
	add addFunc,
	pkg string,
	files []protosource.File,
	getOptionValue func(protosource.File) string,
	getOptionLocation func(protosource.File) protosource.Location,
	name string,
) error {
	optionValueMap := make(map[string]struct{})
	for _, file := range files {
		optionValueMap[getOptionValue(file)] = struct{}{}
	}
	if len(optionValueMap) > 1 {
		_, noOptionValue := optionValueMap[""]
		delete(optionValueMap, "")
		optionValues := stringutil.MapToSortedSlice(optionValueMap)
		for _, file := range files {
			if noOptionValue {
				add(file, getOptionLocation(file), nil, "Files in package %q have both values %q and no value for option %q and all values must be equal.", pkg, strings.Join(optionValues, ","), name)
			} else {
				add(file, getOptionLocation(file), nil, "Files in package %q have multiple values %q for option %q and all values must be equal.", pkg, strings.Join(optionValues, ","), name)
			}
		}
	}
	return nil
}

// CheckPackageVersionSuffix is a check function.
var CheckPackageVersionSuffix = newFileCheckFunc(checkPackageVersionSuffix)

func checkPackageVersionSuffix(add addFunc, file protosource.File) error {
	pkg := file.Package()
	if pkg == "" {
		return nil
	}
	if _, ok := protoversion.NewPackageVersionForPackage(pkg); !ok {
		add(file, file.PackageLocation(), nil, `Package name %q should be suffixed with a correctly formed version, such as %q.`, pkg, pkg+".v1")
	}
	return nil
}

// CheckRPCNoClientStreaming is a check function.
var CheckRPCNoClientStreaming = newMethodCheckFunc(checkRPCNoClientStreaming)

func checkRPCNoClientStreaming(add addFunc, method protosource.Method) error {
	if method.ClientStreaming() {
		add(
			method,
			method.Location(),
			// also check the service for this comment ignore
			// this allows users to set this "globally" for a service
			[]protosource.Location{
				method.Service().Location(),
			},
			"RPC %q is client streaming.",
			method.Name(),
		)
	}
	return nil
}

// CheckRPCNoServerStreaming is a check function.
var CheckRPCNoServerStreaming = newMethodCheckFunc(checkRPCNoServerStreaming)

func checkRPCNoServerStreaming(add addFunc, method protosource.Method) error {
	if method.ServerStreaming() {
		add(
			method,
			method.Location(),
			// also check the service for this comment ignore
			// this allows users to set this "globally" for a service
			[]protosource.Location{
				method.Service().Location(),
			},
			"RPC %q is server streaming.",
			method.Name(),
		)
	}
	return nil
}

// CheckRPCPascalCase is a check function.
var CheckRPCPascalCase = newMethodCheckFunc(checkRPCPascalCase)

func checkRPCPascalCase(add addFunc, method protosource.Method) error {
	name := method.Name()
	expectedName := stringutil.ToPascalCase(name)
	if name != expectedName {
		add(
			method,
			method.NameLocation(),
			// also check the service for this comment ignore
			// this allows users to set this "globally" for a service
			[]protosource.Location{
				method.Service().Location(),
			},
			"RPC name %q should be PascalCase, such as %q.",
			name,
			expectedName,
		)
	}
	return nil
}

// CheckRPCRequestResponseUnique is a check function.
var CheckRPCRequestResponseUnique = func(
	id string,
	ignoreFunc internal.IgnoreFunc,
	files []protosource.File,
	allowSameRequestResponse bool,
	allowGoogleProtobufEmptyRequests bool,
	allowGoogleProtobufEmptyResponses bool,
) ([]bufanalysis.FileAnnotation, error) {
	return newFilesCheckFunc(
		func(add addFunc, files []protosource.File) error {
			return checkRPCRequestResponseUnique(
				add,
				files,
				allowSameRequestResponse,
				allowGoogleProtobufEmptyRequests,
				allowGoogleProtobufEmptyResponses,
			)
		},
	)(id, ignoreFunc, files)
}

func checkRPCRequestResponseUnique(
	add addFunc,
	files []protosource.File,
	allowSameRequestResponse bool,
	allowGoogleProtobufEmptyRequests bool,
	allowGoogleProtobufEmptyResponses bool,
) error {
	allFullNameToMethod, err := protosource.FullNameToMethod(files...)
	if err != nil {
		return err
	}
	// first check if any requests or responses are the same
	// if not, we can treat requests and responses equally for checking if more than
	// one method uses a type
	if !allowSameRequestResponse {
		for _, method := range allFullNameToMethod {
			if method.InputTypeName() == method.OutputTypeName() {
				// if we allow both empty requests and responses, we do not want to add a FileAnnotation
				if !(method.InputTypeName() == ".google.protobuf.Empty" && allowGoogleProtobufEmptyRequests && allowGoogleProtobufEmptyResponses) {
					add(
						method,
						method.Location(),
						// also check the service for this comment ignore
						// this allows users to set this "globally" for a service
						[]protosource.Location{
							method.Service().Location(),
						},
						"RPC %q has the same type %q for the request and response.",
						method.Name(),
						method.InputTypeName(),
					)
				}
			}
		}
	}
	// we have now added errors for the same request and response type if applicable
	// we can now check methods for unique usage of a given type
	requestResponseTypeToFullNameToMethod := make(map[string]map[string]protosource.Method)
	for fullName, method := range allFullNameToMethod {
		for _, requestResponseType := range []string{method.InputTypeName(), method.OutputTypeName()} {
			fullNameToMethod, ok := requestResponseTypeToFullNameToMethod[requestResponseType]
			if !ok {
				fullNameToMethod = make(map[string]protosource.Method)
				requestResponseTypeToFullNameToMethod[requestResponseType] = fullNameToMethod
			}
			fullNameToMethod[fullName] = method
		}
	}
	for requestResponseType, fullNameToMethod := range requestResponseTypeToFullNameToMethod {
		// only this method uses this request or response type, no issue
		if len(fullNameToMethod) == 1 {
			continue
		}
		// if the request or response type is google.protobuf.Empty and we allow this for requests or responses,
		// we have to do a harder check
		if requestResponseType == ".google.protobuf.Empty" && (allowGoogleProtobufEmptyRequests || allowGoogleProtobufEmptyResponses) {
			// if both requests and responses can be google.protobuf.Empty, then do not add any error
			// else, we check
			if !(allowGoogleProtobufEmptyRequests && allowGoogleProtobufEmptyResponses) {
				// inside this if statement, one of allowGoogleProtobufEmptyRequests or allowGoogleProtobufEmptyResponses is true
				var requestMethods []protosource.Method
				var responseMethods []protosource.Method
				for _, method := range fullNameToMethod {
					if method.InputTypeName() == ".google.protobuf.Empty" {
						requestMethods = append(requestMethods, method)
					}
					if method.OutputTypeName() == ".google.protobuf.Empty" {
						responseMethods = append(responseMethods, method)
					}
				}
				if !allowGoogleProtobufEmptyRequests && len(requestMethods) > 1 {
					for _, method := range requestMethods {
						add(
							method,
							method.Location(),
							// also check the service for this comment ignore
							// this allows users to set this "globally" for a service
							[]protosource.Location{
								method.Service().Location(),
							},
							"%q is used as the request for multiple RPCs.",
							requestResponseType,
						)
					}
				}
				if !allowGoogleProtobufEmptyResponses && len(responseMethods) > 1 {
					for _, method := range responseMethods {
						add(
							method,
							method.Location(),
							// also check the service for this comment ignore
							// this allows users to set this "globally" for a service
							[]protosource.Location{
								method.Service().Location(),
							},
							"%q is used as the response for multiple RPCs.",
							requestResponseType,
						)
					}
				}
			}
		} else {
			// else, we have a duplicate usage of requestResponseType, add an FileAnnotation to each method
			for _, method := range fullNameToMethod {
				add(
					method,
					method.Location(),
					// also check the service for this comment ignore
					// this allows users to set this "globally" for a service
					[]protosource.Location{
						method.Service().Location(),
					},
					"%q is used as the request or response type for multiple RPCs.",
					requestResponseType,
				)
			}
		}
	}
	return nil
}

// CheckRPCRequestStandardName is a check function.
var CheckRPCRequestStandardName = func(
	id string,
	ignoreFunc internal.IgnoreFunc,
	files []protosource.File,
	allowGoogleProtobufEmptyRequests bool,
) ([]bufanalysis.FileAnnotation, error) {
	return newMethodCheckFunc(
		func(add addFunc, method protosource.Method) error {
			return checkRPCRequestStandardName(add, method, allowGoogleProtobufEmptyRequests)
		},
	)(id, ignoreFunc, files)
}

func checkRPCRequestStandardName(add addFunc, method protosource.Method, allowGoogleProtobufEmptyRequests bool) error {
	service := method.Service()
	if service == nil {
		return errors.New("method.Service() is nil")
	}
	name := method.InputTypeName()
	if allowGoogleProtobufEmptyRequests && name == ".google.protobuf.Empty" {
		return nil
	}
	if strings.Contains(name, ".") {
		split := strings.Split(name, ".")
		name = split[len(split)-1]
	}
	expectedName1 := stringutil.ToPascalCase(method.Name()) + "Request"
	expectedName2 := stringutil.ToPascalCase(service.Name()) + expectedName1
	if name != expectedName1 && name != expectedName2 {
		add(
			method,
			method.InputTypeLocation(),
			// also check the method and service for this comment ignore
			// this came up in https://github.com/bufbuild/buf/issues/242
			[]protosource.Location{
				method.Location(),
				method.Service().Location(),
			},
			"RPC request type %q should be named %q or %q.",
			name,
			expectedName1,
			expectedName2,
		)
	}
	return nil
}

// CheckRPCResponseStandardName is a check function.
var CheckRPCResponseStandardName = func(
	id string,
	ignoreFunc internal.IgnoreFunc,
	files []protosource.File,
	allowGoogleProtobufEmptyResponses bool,
) ([]bufanalysis.FileAnnotation, error) {
	return newMethodCheckFunc(
		func(add addFunc, method protosource.Method) error {
			return checkRPCResponseStandardName(add, method, allowGoogleProtobufEmptyResponses)
		},
	)(id, ignoreFunc, files)
}

func checkRPCResponseStandardName(add addFunc, method protosource.Method, allowGoogleProtobufEmptyResponses bool) error {
	service := method.Service()
	if service == nil {
		return errors.New("method.Service() is nil")
	}
	name := method.OutputTypeName()
	if allowGoogleProtobufEmptyResponses && name == ".google.protobuf.Empty" {
		return nil
	}
	if strings.Contains(name, ".") {
		split := strings.Split(name, ".")
		name = split[len(split)-1]
	}
	expectedName1 := stringutil.ToPascalCase(method.Name()) + "Response"
	expectedName2 := stringutil.ToPascalCase(service.Name()) + expectedName1
	if name != expectedName1 && name != expectedName2 {
		add(
			method,
			method.OutputTypeLocation(),
			// also check the method and service for this comment ignore
			// this came up in https://github.com/bufbuild/buf/issues/242
			[]protosource.Location{
				method.Location(),
				method.Service().Location(),
			},
			"RPC response type %q should be named %q or %q.",
			name,
			expectedName1,
			expectedName2,
		)
	}
	return nil
}

// CheckServicePascalCase is a check function.
var CheckServicePascalCase = newServiceCheckFunc(checkServicePascalCase)

func checkServicePascalCase(add addFunc, service protosource.Service) error {
	name := service.Name()
	expectedName := stringutil.ToPascalCase(name)
	if name != expectedName {
		add(service, service.NameLocation(), nil, "Service name %q should be PascalCase, such as %q.", name, expectedName)
	}
	return nil
}

// CheckServiceSuffix is a check function.
var CheckServiceSuffix = func(
	id string,
	ignoreFunc internal.IgnoreFunc,
	files []protosource.File,
	suffix string,
) ([]bufanalysis.FileAnnotation, error) {
	return newServiceCheckFunc(
		func(add addFunc, service protosource.Service) error {
			return checkServiceSuffix(add, service, suffix)
		},
	)(id, ignoreFunc, files)
}

func checkServiceSuffix(add addFunc, service protosource.Service, suffix string) error {
	name := service.Name()
	if !strings.HasSuffix(name, suffix) {
		add(service, service.NameLocation(), nil, "Service name %q should be suffixed with %q.", name, suffix)
	}
	return nil
}

// CheckSyntaxSpecified is a check function.
var CheckSyntaxSpecified = newFileCheckFunc(checkSyntaxSpecified)

func checkSyntaxSpecified(add addFunc, file protosource.File) error {
	if file.Syntax() == protosource.SyntaxUnspecified {
		add(file, file.SyntaxLocation(), nil, `Files must have a syntax explicitly specified. If no syntax is specified, the file defaults to "proto2".`)
	}
	return nil
}
