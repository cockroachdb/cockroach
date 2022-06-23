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

package buflintv1

import (
	"github.com/bufbuild/buf/private/buf/bufcheck/buflint/internal/buflintbuild"
	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
)

var (
	// v1RuleBuilders are the rule builders.
	v1RuleBuilders = []*internal.RuleBuilder{
		buflintbuild.CommentEnumRuleBuilder,
		buflintbuild.CommentEnumValueRuleBuilder,
		buflintbuild.CommentFieldRuleBuilder,
		buflintbuild.CommentMessageRuleBuilder,
		buflintbuild.CommentOneofRuleBuilder,
		buflintbuild.CommentRPCRuleBuilder,
		buflintbuild.CommentServiceRuleBuilder,
		buflintbuild.DirectorySamePackageRuleBuilder,
		buflintbuild.EnumFirstValueZeroRuleBuilder,
		buflintbuild.EnumNoAllowAliasRuleBuilder,
		buflintbuild.EnumPascalCaseRuleBuilder,
		buflintbuild.EnumValuePrefixRuleBuilder,
		buflintbuild.EnumValueUpperSnakeCaseRuleBuilder,
		buflintbuild.EnumZeroValueSuffixRuleBuilder,
		buflintbuild.FieldLowerSnakeCaseRuleBuilder,
		buflintbuild.FileLowerSnakeCaseRuleBuilder,
		buflintbuild.ImportNoPublicRuleBuilder,
		buflintbuild.ImportNoWeakRuleBuilder,
		buflintbuild.ImportUsedRuleBuilder,
		buflintbuild.MessagePascalCaseRuleBuilder,
		buflintbuild.OneofLowerSnakeCaseRuleBuilder,
		buflintbuild.PackageDefinedRuleBuilder,
		buflintbuild.PackageDirectoryMatchRuleBuilder,
		buflintbuild.PackageLowerSnakeCaseRuleBuilder,
		buflintbuild.PackageSameCsharpNamespaceRuleBuilder,
		buflintbuild.PackageSameDirectoryRuleBuilder,
		buflintbuild.PackageSameGoPackageRuleBuilder,
		buflintbuild.PackageSameJavaMultipleFilesRuleBuilder,
		buflintbuild.PackageSameJavaPackageRuleBuilder,
		buflintbuild.PackageSamePhpNamespaceRuleBuilder,
		buflintbuild.PackageSameRubyPackageRuleBuilder,
		buflintbuild.PackageSameSwiftPrefixRuleBuilder,
		buflintbuild.PackageVersionSuffixRuleBuilder,
		buflintbuild.RPCNoClientStreamingRuleBuilder,
		buflintbuild.RPCNoServerStreamingRuleBuilder,
		buflintbuild.RPCPascalCaseRuleBuilder,
		buflintbuild.RPCRequestResponseUniqueRuleBuilder,
		buflintbuild.RPCRequestStandardNameRuleBuilder,
		buflintbuild.RPCResponseStandardNameRuleBuilder,
		buflintbuild.ServicePascalCaseRuleBuilder,
		buflintbuild.ServiceSuffixRuleBuilder,
		buflintbuild.SyntaxSpecifiedRuleBuilder,
	}

	// v1DefaultCategories are the default categories.
	v1DefaultCategories = []string{
		"DEFAULT",
	}
	// v1AllCategories are all categories.
	v1AllCategories = []string{
		"MINIMAL",
		"BASIC",
		"DEFAULT",
		"COMMENTS",
		"UNARY_RPC",
	}
	// v1IDToCategories associates IDs to categories.
	v1IDToCategories = map[string][]string{
		"COMMENT_ENUM": {
			"COMMENTS",
		},
		"COMMENT_ENUM_VALUE": {
			"COMMENTS",
		},
		"COMMENT_FIELD": {
			"COMMENTS",
		},
		"COMMENT_MESSAGE": {
			"COMMENTS",
		},
		"COMMENT_ONEOF": {
			"COMMENTS",
		},
		"COMMENT_RPC": {
			"COMMENTS",
		},
		"COMMENT_SERVICE": {
			"COMMENTS",
		},
		"DIRECTORY_SAME_PACKAGE": {
			"MINIMAL",
			"BASIC",
			"DEFAULT",
		},
		"ENUM_FIRST_VALUE_ZERO": {
			"BASIC",
			"DEFAULT",
		},
		"ENUM_NO_ALLOW_ALIAS": {
			"BASIC",
			"DEFAULT",
		},
		"ENUM_PASCAL_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"ENUM_VALUE_PREFIX": {
			"DEFAULT",
		},
		"ENUM_VALUE_UPPER_SNAKE_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"ENUM_ZERO_VALUE_SUFFIX": {
			"DEFAULT",
		},
		"FIELD_LOWER_SNAKE_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"FILE_LOWER_SNAKE_CASE": {
			"DEFAULT",
		},
		"IMPORT_NO_PUBLIC": {
			"BASIC",
			"DEFAULT",
		},
		"IMPORT_NO_WEAK": {
			"BASIC",
			"DEFAULT",
		},
		"IMPORT_USED": {
			"BASIC",
			"DEFAULT",
		},
		"MESSAGE_PASCAL_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"ONEOF_LOWER_SNAKE_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_DEFINED": {
			"MINIMAL",
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_DIRECTORY_MATCH": {
			"MINIMAL",
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_LOWER_SNAKE_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_CSHARP_NAMESPACE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_DIRECTORY": {
			"MINIMAL",
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_GO_PACKAGE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_JAVA_MULTIPLE_FILES": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_JAVA_PACKAGE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_PHP_NAMESPACE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_RUBY_PACKAGE": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_SAME_SWIFT_PREFIX": {
			"BASIC",
			"DEFAULT",
		},
		"PACKAGE_VERSION_SUFFIX": {
			"DEFAULT",
		},
		"RPC_NO_CLIENT_STREAMING": {
			"UNARY_RPC",
		},
		"RPC_NO_SERVER_STREAMING": {
			"UNARY_RPC",
		},
		"RPC_PASCAL_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"RPC_REQUEST_RESPONSE_UNIQUE": {
			"DEFAULT",
		},
		"RPC_REQUEST_STANDARD_NAME": {
			"DEFAULT",
		},
		"RPC_RESPONSE_STANDARD_NAME": {
			"DEFAULT",
		},
		"SERVICE_PASCAL_CASE": {
			"BASIC",
			"DEFAULT",
		},
		"SERVICE_SUFFIX": {
			"DEFAULT",
		},
		"SYNTAX_SPECIFIED": {
			"BASIC",
			"DEFAULT",
		},
	}
)
