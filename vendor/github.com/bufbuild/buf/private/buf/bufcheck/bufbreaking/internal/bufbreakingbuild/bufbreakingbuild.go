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

// Package bufbreakingbuild contains the RuleBuilders used by bufbreakingv*.
//
// In the future, we can have multiple versions of a RuleBuilder here, and then
// include them separately in the bufbreakingv* packages. For example, FieldSameTypeRuleBuilder
// could be split into FieldSameTypeRuleBuilder/FieldSameTypeRuleBuilderV2 which handle
// primitives differently, and we could use the former in v1beta1, and the latter in v1.
package bufbreakingbuild

import (
	"github.com/bufbuild/buf/private/buf/bufcheck/bufbreaking/internal/bufbreakingcheck"
	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
)

var (
	// EnumNoDeleteRuleBuilder is a rule builder.
	EnumNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"ENUM_NO_DELETE",
		"enums are not deleted from a given file",
		bufbreakingcheck.CheckEnumNoDelete,
	)
	// EnumValueNoDeleteRuleBuilder is a rule builder.
	EnumValueNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"ENUM_VALUE_NO_DELETE",
		"enum values are not deleted from a given enum",
		bufbreakingcheck.CheckEnumValueNoDelete,
	)
	// EnumValueNoDeleteUnlessNameReservedRuleBuilder is a rule builder.
	EnumValueNoDeleteUnlessNameReservedRuleBuilder = internal.NewNopRuleBuilder(
		"ENUM_VALUE_NO_DELETE_UNLESS_NAME_RESERVED",
		"enum values are not deleted from a given enum unless the name is reserved",
		bufbreakingcheck.CheckEnumValueNoDeleteUnlessNameReserved,
	)
	// EnumValueNoDeleteUnlessNumberReservedRuleBuilder is a rule builder.
	EnumValueNoDeleteUnlessNumberReservedRuleBuilder = internal.NewNopRuleBuilder(
		"ENUM_VALUE_NO_DELETE_UNLESS_NUMBER_RESERVED",
		"enum values are not deleted from a given enum unless the number is reserved",
		bufbreakingcheck.CheckEnumValueNoDeleteUnlessNumberReserved,
	)
	// EnumValueSameNameRuleBuilder is a rule builder.
	EnumValueSameNameRuleBuilder = internal.NewNopRuleBuilder(
		"ENUM_VALUE_SAME_NAME",
		"enum values have the same name",
		bufbreakingcheck.CheckEnumValueSameName,
	)
	// ExtensionMessageNoDeleteRuleBuilder is a rule builder.
	ExtensionMessageNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"EXTENSION_MESSAGE_NO_DELETE",
		"extension ranges are not deleted from a given message",
		bufbreakingcheck.CheckExtensionMessageNoDelete,
	)
	// FieldNoDeleteRuleBuilder is a rule builder.
	FieldNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_NO_DELETE",
		"fields are not deleted from a given message",
		bufbreakingcheck.CheckFieldNoDelete,
	)
	// FieldNoDeleteUnlessNameReservedRuleBuilder is a rule builder.
	FieldNoDeleteUnlessNameReservedRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_NO_DELETE_UNLESS_NAME_RESERVED",
		"fields are not deleted from a given message unless the name is reserved",
		bufbreakingcheck.CheckFieldNoDeleteUnlessNameReserved,
	)
	// FieldNoDeleteUnlessNumberReservedRuleBuilder is a rule builder.
	FieldNoDeleteUnlessNumberReservedRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED",
		"fields are not deleted from a given message unless the number is reserved",
		bufbreakingcheck.CheckFieldNoDeleteUnlessNumberReserved,
	)
	// FieldSameCTypeRuleBuilder is a rule builder.
	FieldSameCTypeRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_CTYPE",
		"fields have the same value for the ctype option",
		bufbreakingcheck.CheckFieldSameCType,
	)
	// FieldSameJSONNameRuleBuilder is a rule builder.
	FieldSameJSONNameRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_JSON_NAME",
		"fields have the same value for the json_name option",
		bufbreakingcheck.CheckFieldSameJSONName,
	)
	// FieldSameJSTypeRuleBuilder is a rule builder.
	FieldSameJSTypeRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_JSTYPE",
		"fields have the same value for the jstype option",
		bufbreakingcheck.CheckFieldSameJSType,
	)
	// FieldSameLabelRuleBuilder is a rule builder.
	FieldSameLabelRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_LABEL",
		"fields have the same labels in a given message",
		bufbreakingcheck.CheckFieldSameLabel,
	)
	// FieldSameNameRuleBuilder is a rule builder.
	FieldSameNameRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_NAME",
		"fields have the same names in a given message",
		bufbreakingcheck.CheckFieldSameName,
	)
	// FieldSameOneofRuleBuilder is a rule builder.
	FieldSameOneofRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_ONEOF",
		"fields have the same oneofs in a given message",
		bufbreakingcheck.CheckFieldSameOneof,
	)
	// FieldSameTypeRuleBuilder is a rule builder.
	FieldSameTypeRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_SAME_TYPE",
		"fields have the same types in a given message",
		bufbreakingcheck.CheckFieldSameType,
	)
	// FieldWireCompatibleTypeRuleBuilder is a rule builder.
	FieldWireCompatibleTypeRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_WIRE_COMPATIBLE_TYPE",
		"fields have wire-compatible types in a given message",
		bufbreakingcheck.CheckFieldWireCompatibleType,
	)
	// FieldWireJSONCompatibleTypeRuleBuilder is a rule builder.
	FieldWireJSONCompatibleTypeRuleBuilder = internal.NewNopRuleBuilder(
		"FIELD_WIRE_JSON_COMPATIBLE_TYPE",
		"fields have wire and JSON compatible types in a given message",
		bufbreakingcheck.CheckFieldWireJSONCompatibleType,
	)
	// FileNoDeleteRuleBuilder is a rule builder.
	FileNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_NO_DELETE",
		"files are not deleted",
		bufbreakingcheck.CheckFileNoDelete,
	)
	// FileSameCsharpNamespaceRuleBuilder is a rule builder.
	FileSameCsharpNamespaceRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_CSHARP_NAMESPACE",
		"files have the same value for the csharp_namespace option",
		bufbreakingcheck.CheckFileSameCsharpNamespace,
	)
	// FileSameGoPackageRuleBuilder is a rule builder.
	FileSameGoPackageRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_GO_PACKAGE",
		"files have the same value for the go_package option",
		bufbreakingcheck.CheckFileSameGoPackage,
	)
	// FileSameJavaMultipleFilesRuleBuilder is a rule builder.
	FileSameJavaMultipleFilesRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_JAVA_MULTIPLE_FILES",
		"files have the same value for the java_multiple_files option",
		bufbreakingcheck.CheckFileSameJavaMultipleFiles,
	)
	// FileSameJavaOuterClassnameRuleBuilder is a rule builder.
	FileSameJavaOuterClassnameRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_JAVA_OUTER_CLASSNAME",
		"files have the same value for the java_outer_classname option",
		bufbreakingcheck.CheckFileSameJavaOuterClassname,
	)
	// FileSameJavaPackageRuleBuilder is a rule builder.
	FileSameJavaPackageRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_JAVA_PACKAGE",
		"files have the same value for the java_package option",
		bufbreakingcheck.CheckFileSameJavaPackage,
	)
	// FileSameJavaStringCheckUtf8RuleBuilder is a rule builder.
	FileSameJavaStringCheckUtf8RuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_JAVA_STRING_CHECK_UTF8",
		"files have the same value for the java_string_check_utf8 option",
		bufbreakingcheck.CheckFileSameJavaStringCheckUtf8,
	)
	// FileSameObjcClassPrefixRuleBuilder is a rule builder.
	FileSameObjcClassPrefixRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_OBJC_CLASS_PREFIX",
		"files have the same value for the objc_class_prefix option",
		bufbreakingcheck.CheckFileSameObjcClassPrefix,
	)
	// FileSamePackageRuleBuilder is a rule builder.
	FileSamePackageRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_PACKAGE",
		"files have the same package",
		bufbreakingcheck.CheckFileSamePackage,
	)
	// FileSamePhpClassPrefixRuleBuilder is a rule builder.
	FileSamePhpClassPrefixRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_PHP_CLASS_PREFIX",
		"files have the same value for the php_class_prefix option",
		bufbreakingcheck.CheckFileSamePhpClassPrefix,
	)
	// FileSamePhpMetadataNamespaceRuleBuilder is a rule builder.
	FileSamePhpMetadataNamespaceRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_PHP_METADATA_NAMESPACE",
		"files have the same value for the php_metadata_namespace option",
		bufbreakingcheck.CheckFileSamePhpMetadataNamespace,
	)
	// FileSamePhpNamespaceRuleBuilder is a rule builder.
	FileSamePhpNamespaceRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_PHP_NAMESPACE",
		"files have the same value for the php_namespace option",
		bufbreakingcheck.CheckFileSamePhpNamespace,
	)
	// FileSameRubyPackageRuleBuilder is a rule builder.
	FileSameRubyPackageRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_RUBY_PACKAGE",
		"files have the same value for the ruby_package option",
		bufbreakingcheck.CheckFileSameRubyPackage,
	)
	// FileSameSwiftPrefixRuleBuilder is a rule builder.
	FileSameSwiftPrefixRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_SWIFT_PREFIX",
		"files have the same value for the swift_prefix option",
		bufbreakingcheck.CheckFileSameSwiftPrefix,
	)
	// FileSameOptimizeForRuleBuilder is a rule builder.
	FileSameOptimizeForRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_OPTIMIZE_FOR",
		"files have the same value for the optimize_for option",
		bufbreakingcheck.CheckFileSameOptimizeFor,
	)
	// FileSameCcGenericServicesRuleBuilder is a rule builder.
	FileSameCcGenericServicesRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_CC_GENERIC_SERVICES",
		"files have the same value for the cc_generic_services option",
		bufbreakingcheck.CheckFileSameCcGenericServices,
	)
	// FileSameJavaGenericServicesRuleBuilder is a rule builder.
	FileSameJavaGenericServicesRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_JAVA_GENERIC_SERVICES",
		"files have the same value for the java_generic_services option",
		bufbreakingcheck.CheckFileSameJavaGenericServices,
	)
	// FileSamePyGenericServicesRuleBuilder is a rule builder.
	FileSamePyGenericServicesRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_PY_GENERIC_SERVICES",
		"files have the same value for the py_generic_services option",
		bufbreakingcheck.CheckFileSamePyGenericServices,
	)
	// FileSamePhpGenericServicesRuleBuilder is a rule builder.
	FileSamePhpGenericServicesRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_PHP_GENERIC_SERVICES",
		"files have the same value for the php_generic_services option",
		bufbreakingcheck.CheckFileSamePhpGenericServices,
	)
	// FileSameCcEnableArenasRuleBuilder is a rule builder.
	FileSameCcEnableArenasRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_CC_ENABLE_ARENAS",
		"files have the same value for the cc_enable_arenas option",
		bufbreakingcheck.CheckFileSameCcEnableArenas,
	)
	// FileSameSyntaxRuleBuilder is a rule builder.
	FileSameSyntaxRuleBuilder = internal.NewNopRuleBuilder(
		"FILE_SAME_SYNTAX",
		"files have the same syntax",
		bufbreakingcheck.CheckFileSameSyntax,
	)
	// MessageNoDeleteRuleBuilder is a rule builder.
	MessageNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"MESSAGE_NO_DELETE",
		"messages are not deleted from a given file",
		bufbreakingcheck.CheckMessageNoDelete,
	)
	// MessageNoRemoveStandardDescriptorAccessorRuleBuilder is a rule builder.
	MessageNoRemoveStandardDescriptorAccessorRuleBuilder = internal.NewNopRuleBuilder(
		"MESSAGE_NO_REMOVE_STANDARD_DESCRIPTOR_ACCESSOR",
		"messages do not change the no_standard_descriptor_accessor option from false or unset to true",
		bufbreakingcheck.CheckMessageNoRemoveStandardDescriptorAccessor,
	)
	// MessageSameMessageSetWireFormatRuleBuilder is a rule builder.
	MessageSameMessageSetWireFormatRuleBuilder = internal.NewNopRuleBuilder(
		"MESSAGE_SAME_MESSAGE_SET_WIRE_FORMAT",
		"messages have the same value for the message_set_wire_format option",
		bufbreakingcheck.CheckMessageSameMessageSetWireFormat,
	)
	// MessageSameRequiredFieldsRuleBuilder is a rule builder.
	MessageSameRequiredFieldsRuleBuilder = internal.NewNopRuleBuilder(
		"MESSAGE_SAME_REQUIRED_FIELDS",
		"messages have no added or deleted required fields",
		bufbreakingcheck.CheckMessageSameRequiredFields,
	)
	// OneofNoDeleteRuleBuilder is a rule builder.
	OneofNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"ONEOF_NO_DELETE",
		"oneofs are not deleted from a given message",
		bufbreakingcheck.CheckOneofNoDelete,
	)
	// PackageEnumNoDeleteRuleBuilder is a rule builder.
	PackageEnumNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"PACKAGE_ENUM_NO_DELETE",
		"enums are not deleted from a given package",
		bufbreakingcheck.CheckPackageEnumNoDelete,
	)
	// PackageMessageNoDeleteRuleBuilder is a rule builder.
	PackageMessageNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"PACKAGE_MESSAGE_NO_DELETE",
		"messages are not deleted from a given package",
		bufbreakingcheck.CheckPackageMessageNoDelete,
	)
	// PackageNoDeleteRuleBuilder is a rule builder.
	PackageNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"PACKAGE_NO_DELETE",
		"packages are not deleted",
		bufbreakingcheck.CheckPackageNoDelete,
	)
	// PackageServiceNoDeleteRuleBuilder is a rule builder.
	PackageServiceNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"PACKAGE_SERVICE_NO_DELETE",
		"services are not deleted from a given package",
		bufbreakingcheck.CheckPackageServiceNoDelete,
	)
	// ReservedEnumNoDeleteRuleBuilder is a rule builder.
	ReservedEnumNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"RESERVED_ENUM_NO_DELETE",
		"reserved ranges and names are not deleted from a given enum",
		bufbreakingcheck.CheckReservedEnumNoDelete,
	)
	// ReservedMessageNoDeleteRuleBuilder is a rule builder.
	ReservedMessageNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"RESERVED_MESSAGE_NO_DELETE",
		"reserved ranges and names are not deleted from a given message",
		bufbreakingcheck.CheckReservedMessageNoDelete,
	)
	// RPCNoDeleteRuleBuilder is a rule builder.
	RPCNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"RPC_NO_DELETE",
		"rpcs are not deleted from a given service",
		bufbreakingcheck.CheckRPCNoDelete,
	)
	// RPCSameClientStreamingRuleBuilder is a rule builder.
	RPCSameClientStreamingRuleBuilder = internal.NewNopRuleBuilder(
		"RPC_SAME_CLIENT_STREAMING",
		"rpcs have the same client streaming value",
		bufbreakingcheck.CheckRPCSameClientStreaming,
	)
	// RPCSameIdempotencyLevelRuleBuilder is a rule builder.
	RPCSameIdempotencyLevelRuleBuilder = internal.NewNopRuleBuilder(
		"RPC_SAME_IDEMPOTENCY_LEVEL",
		"rpcs have the same value for the idempotency_level option",
		bufbreakingcheck.CheckRPCSameIdempotencyLevel,
	)
	// RPCSameRequestTypeRuleBuilder is a rule builder.
	RPCSameRequestTypeRuleBuilder = internal.NewNopRuleBuilder(
		"RPC_SAME_REQUEST_TYPE",
		"rpcs are have the same request type",
		bufbreakingcheck.CheckRPCSameRequestType,
	)
	// RPCSameResponseTypeRuleBuilder is a rule builder.
	RPCSameResponseTypeRuleBuilder = internal.NewNopRuleBuilder(
		"RPC_SAME_RESPONSE_TYPE",
		"rpcs are have the same response type",
		bufbreakingcheck.CheckRPCSameResponseType,
	)
	// RPCSameServerStreamingRuleBuilder is a rule builder.
	RPCSameServerStreamingRuleBuilder = internal.NewNopRuleBuilder(
		"RPC_SAME_SERVER_STREAMING",
		"rpcs have the same server streaming value",
		bufbreakingcheck.CheckRPCSameServerStreaming,
	)
	// ServiceNoDeleteRuleBuilder is a rule builder.
	ServiceNoDeleteRuleBuilder = internal.NewNopRuleBuilder(
		"SERVICE_NO_DELETE",
		"services are not deleted from a given file",
		bufbreakingcheck.CheckServiceNoDelete,
	)
)
