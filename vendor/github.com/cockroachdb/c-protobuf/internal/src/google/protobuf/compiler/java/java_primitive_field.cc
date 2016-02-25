// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Author: kenton@google.com (Kenton Varda)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.

#include <map>
#include <string>

#include <google/protobuf/stubs/logging.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/compiler/java/java_context.h>
#include <google/protobuf/compiler/java/java_doc_comment.h>
#include <google/protobuf/compiler/java/java_helpers.h>
#include <google/protobuf/compiler/java/java_name_resolver.h>
#include <google/protobuf/compiler/java/java_primitive_field.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/wire_format.h>
#include <google/protobuf/stubs/strutil.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace java {

using internal::WireFormat;
using internal::WireFormatLite;

namespace {

void SetPrimitiveVariables(const FieldDescriptor* descriptor,
                           int messageBitIndex,
                           int builderBitIndex,
                           const FieldGeneratorInfo* info,
                           ClassNameResolver* name_resolver,
                           map<string, string>* variables) {
  SetCommonFieldVariables(descriptor, info, variables);

  (*variables)["type"] = PrimitiveTypeName(GetJavaType(descriptor));
  (*variables)["boxed_type"] = BoxedPrimitiveTypeName(GetJavaType(descriptor));
  (*variables)["field_type"] = (*variables)["type"];
  (*variables)["field_list_type"] = "java.util.List<" +
      (*variables)["boxed_type"] + ">";
  (*variables)["empty_list"] = "java.util.Collections.emptyList()";
  (*variables)["default"] = ImmutableDefaultValue(descriptor, name_resolver);
  (*variables)["default_init"] = IsDefaultValueJavaDefault(descriptor) ?
      "" : ("= " + ImmutableDefaultValue(descriptor, name_resolver));
  (*variables)["capitalized_type"] =
      GetCapitalizedType(descriptor, /* immutable = */ true);
  if (descriptor->is_packed()) {
    (*variables)["tag"] = SimpleItoa(WireFormatLite::MakeTag(
        descriptor->number(), WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
  } else {
    (*variables)["tag"] = SimpleItoa(WireFormat::MakeTag(descriptor));
  }
  (*variables)["tag_size"] = SimpleItoa(
      WireFormat::TagSize(descriptor->number(), GetType(descriptor)));
  if (IsReferenceType(GetJavaType(descriptor))) {
    (*variables)["null_check"] =
        "  if (value == null) {\n"
        "    throw new NullPointerException();\n"
        "  }\n";
  } else {
    (*variables)["null_check"] = "";
  }
  // TODO(birdo): Add @deprecated javadoc when generating javadoc is supported
  // by the proto compiler
  (*variables)["deprecation"] = descriptor->options().deprecated()
      ? "@java.lang.Deprecated " : "";
  int fixed_size = FixedSize(GetType(descriptor));
  if (fixed_size != -1) {
    (*variables)["fixed_size"] = SimpleItoa(fixed_size);
  }
  (*variables)["on_changed"] =
      HasDescriptorMethods(descriptor->containing_type()) ? "onChanged();" : "";

  if (SupportFieldPresence(descriptor->file())) {
    // For singular messages and builders, one bit is used for the hasField bit.
    (*variables)["get_has_field_bit_message"] = GenerateGetBit(messageBitIndex);
    (*variables)["get_has_field_bit_builder"] = GenerateGetBit(builderBitIndex);

    // Note that these have a trailing ";".
    (*variables)["set_has_field_bit_message"] =
        GenerateSetBit(messageBitIndex) + ";";
    (*variables)["set_has_field_bit_builder"] =
        GenerateSetBit(builderBitIndex) + ";";
    (*variables)["clear_has_field_bit_builder"] =
        GenerateClearBit(builderBitIndex) + ";";

    (*variables)["is_field_present_message"] = GenerateGetBit(messageBitIndex);
  } else {
    (*variables)["set_has_field_bit_message"] = "";
    (*variables)["set_has_field_bit_builder"] = "";
    (*variables)["clear_has_field_bit_builder"] = "";

    if (descriptor->type() == FieldDescriptor::TYPE_BYTES) {
      (*variables)["is_field_present_message"] =
          "!" + (*variables)["name"] + "_.isEmpty()";
    } else {
      (*variables)["is_field_present_message"] =
          (*variables)["name"] + "_ != " + (*variables)["default"];
    }
  }

  // For repated builders, one bit is used for whether the array is immutable.
  (*variables)["get_mutable_bit_builder"] = GenerateGetBit(builderBitIndex);
  (*variables)["set_mutable_bit_builder"] = GenerateSetBit(builderBitIndex);
  (*variables)["clear_mutable_bit_builder"] = GenerateClearBit(builderBitIndex);

  // For repeated fields, one bit is used for whether the array is immutable
  // in the parsing constructor.
  (*variables)["get_mutable_bit_parser"] =
      GenerateGetBitMutableLocal(builderBitIndex);
  (*variables)["set_mutable_bit_parser"] =
      GenerateSetBitMutableLocal(builderBitIndex);

  (*variables)["get_has_field_bit_from_local"] =
      GenerateGetBitFromLocal(builderBitIndex);
  (*variables)["set_has_field_bit_to_local"] =
      GenerateSetBitToLocal(messageBitIndex);
}

}  // namespace

// ===================================================================

ImmutablePrimitiveFieldGenerator::
ImmutablePrimitiveFieldGenerator(const FieldDescriptor* descriptor,
                                 int messageBitIndex,
                                 int builderBitIndex,
                                 Context* context)
  : descriptor_(descriptor), messageBitIndex_(messageBitIndex),
    builderBitIndex_(builderBitIndex), context_(context),
    name_resolver_(context->GetNameResolver()) {
  SetPrimitiveVariables(descriptor, messageBitIndex, builderBitIndex,
                        context->GetFieldGeneratorInfo(descriptor),
                        name_resolver_, &variables_);
}

ImmutablePrimitiveFieldGenerator::~ImmutablePrimitiveFieldGenerator() {}

int ImmutablePrimitiveFieldGenerator::GetNumBitsForMessage() const {
  return 1;
}

int ImmutablePrimitiveFieldGenerator::GetNumBitsForBuilder() const {
  return 1;
}

void ImmutablePrimitiveFieldGenerator::
GenerateInterfaceMembers(io::Printer* printer) const {
  if (SupportFieldPresence(descriptor_->file())) {
    WriteFieldDocComment(printer, descriptor_);
    printer->Print(variables_,
      "$deprecation$boolean has$capitalized_name$();\n");
  }
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$$type$ get$capitalized_name$();\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateMembers(io::Printer* printer) const {
  printer->Print(variables_,
    "private $field_type$ $name$_;\n");
  PrintExtraFieldInfo(variables_, printer);
  if (SupportFieldPresence(descriptor_->file())) {
    WriteFieldDocComment(printer, descriptor_);
    printer->Print(variables_,
      "$deprecation$public boolean has$capitalized_name$() {\n"
      "  return $get_has_field_bit_message$;\n"
      "}\n");
  }

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public $type$ get$capitalized_name$() {\n"
    "  return $name$_;\n"
    "}\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateBuilderMembers(io::Printer* printer) const {
  printer->Print(variables_,
    "private $field_type$ $name$_ $default_init$;\n");

  if (SupportFieldPresence(descriptor_->file())) {
    WriteFieldDocComment(printer, descriptor_);
    printer->Print(variables_,
      "$deprecation$public boolean has$capitalized_name$() {\n"
      "  return $get_has_field_bit_builder$;\n"
      "}\n");
  }

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public $type$ get$capitalized_name$() {\n"
    "  return $name$_;\n"
    "}\n");

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder set$capitalized_name$($type$ value) {\n"
    "$null_check$"
    "  $set_has_field_bit_builder$\n"
    "  $name$_ = value;\n"
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder clear$capitalized_name$() {\n"
    "  $clear_has_field_bit_builder$\n");
  JavaType type = GetJavaType(descriptor_);
  if (type == JAVATYPE_STRING || type == JAVATYPE_BYTES) {
    // The default value is not a simple literal so we want to avoid executing
    // it multiple times.  Instead, get the default out of the default instance.
    printer->Print(variables_,
      "  $name$_ = getDefaultInstance().get$capitalized_name$();\n");
  } else {
    printer->Print(variables_,
      "  $name$_ = $default$;\n");
  }
  printer->Print(variables_,
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateFieldBuilderInitializationCode(io::Printer* printer)  const {
  // noop for primitives
}

void ImmutablePrimitiveFieldGenerator::
GenerateInitializationCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_ = $default$;\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateBuilderClearCode(io::Printer* printer) const {
  printer->Print(variables_,
    "$name$_ = $default$;\n"
    "$clear_has_field_bit_builder$\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateMergingCode(io::Printer* printer) const {
  if (SupportFieldPresence(descriptor_->file())) {
    printer->Print(variables_,
      "if (other.has$capitalized_name$()) {\n"
      "  set$capitalized_name$(other.get$capitalized_name$());\n"
      "}\n");
  } else {
    printer->Print(variables_,
      "if (other.get$capitalized_name$() != $default$) {\n"
      "  set$capitalized_name$(other.get$capitalized_name$());\n"
      "}\n");
  }
}

void ImmutablePrimitiveFieldGenerator::
GenerateBuildingCode(io::Printer* printer) const {
  if (SupportFieldPresence(descriptor_->file())) {
    printer->Print(variables_,
      "if ($get_has_field_bit_from_local$) {\n"
      "  $set_has_field_bit_to_local$;\n"
      "}\n");
  }
  printer->Print(variables_,
    "result.$name$_ = $name$_;\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateParsingCode(io::Printer* printer) const {
  printer->Print(variables_,
    "$set_has_field_bit_message$\n"
    "$name$_ = input.read$capitalized_type$();\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateParsingDoneCode(io::Printer* printer) const {
  // noop for primitives.
}

void ImmutablePrimitiveFieldGenerator::
GenerateSerializationCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if ($is_field_present_message$) {\n"
    "  output.write$capitalized_type$($number$, $name$_);\n"
    "}\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateSerializedSizeCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if ($is_field_present_message$) {\n"
    "  size += com.google.protobuf.CodedOutputStream\n"
    "    .compute$capitalized_type$Size($number$, $name$_);\n"
    "}\n");
}

void ImmutablePrimitiveFieldGenerator::
GenerateEqualsCode(io::Printer* printer) const {
  switch (GetJavaType(descriptor_)) {
    case JAVATYPE_INT:
    case JAVATYPE_LONG:
    case JAVATYPE_BOOLEAN:
      printer->Print(variables_,
        "result = result && (get$capitalized_name$()\n"
        "    == other.get$capitalized_name$());\n");
      break;

    case JAVATYPE_FLOAT:
      printer->Print(variables_,
        "result = result && (\n"
        "    java.lang.Float.floatToIntBits(get$capitalized_name$())\n"
        "    == java.lang.Float.floatToIntBits(\n"
        "        other.get$capitalized_name$()));\n");
      break;

    case JAVATYPE_DOUBLE:
      printer->Print(variables_,
        "result = result && (\n"
        "    java.lang.Double.doubleToLongBits(get$capitalized_name$())\n"
        "    == java.lang.Double.doubleToLongBits(\n"
        "        other.get$capitalized_name$()));\n");
      break;

    case JAVATYPE_STRING:
    case JAVATYPE_BYTES:
      printer->Print(variables_,
        "result = result && get$capitalized_name$()\n"
        "    .equals(other.get$capitalized_name$());\n");
      break;

    case JAVATYPE_ENUM:
    case JAVATYPE_MESSAGE:
    default:
      GOOGLE_LOG(FATAL) << "Can't get here.";
      break;
  }
}

void ImmutablePrimitiveFieldGenerator::
GenerateHashCode(io::Printer* printer) const {
  printer->Print(variables_,
    "hash = (37 * hash) + $constant_name$;\n");
  switch (GetJavaType(descriptor_)) {
    case JAVATYPE_INT:
      printer->Print(variables_,
        "hash = (53 * hash) + get$capitalized_name$();\n");
      break;

    case JAVATYPE_LONG:
      printer->Print(variables_,
        "hash = (53 * hash) + com.google.protobuf.Internal.hashLong(\n"
        "    get$capitalized_name$());\n");
      break;

    case JAVATYPE_BOOLEAN:
      printer->Print(variables_,
        "hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(\n"
        "    get$capitalized_name$());\n");
      break;

    case JAVATYPE_FLOAT:
      printer->Print(variables_,
        "hash = (53 * hash) + java.lang.Float.floatToIntBits(\n"
        "    get$capitalized_name$());\n");
      break;

    case JAVATYPE_DOUBLE:
      printer->Print(variables_,
        "hash = (53 * hash) + com.google.protobuf.Internal.hashLong(\n"
        "    java.lang.Double.doubleToLongBits(get$capitalized_name$()));\n");
      break;

    case JAVATYPE_STRING:
    case JAVATYPE_BYTES:
      printer->Print(variables_,
        "hash = (53 * hash) + get$capitalized_name$().hashCode();\n");
      break;

    case JAVATYPE_ENUM:
    case JAVATYPE_MESSAGE:
    default:
      GOOGLE_LOG(FATAL) << "Can't get here.";
      break;
  }
}

string ImmutablePrimitiveFieldGenerator::GetBoxedType() const {
  return BoxedPrimitiveTypeName(GetJavaType(descriptor_));
}

// ===================================================================

ImmutablePrimitiveOneofFieldGenerator::
ImmutablePrimitiveOneofFieldGenerator(const FieldDescriptor* descriptor,
                                 int messageBitIndex,
                                 int builderBitIndex,
                                 Context* context)
    : ImmutablePrimitiveFieldGenerator(
          descriptor, messageBitIndex, builderBitIndex, context) {
  const OneofGeneratorInfo* info =
      context->GetOneofGeneratorInfo(descriptor->containing_oneof());
  SetCommonOneofVariables(descriptor, info, &variables_);
}

ImmutablePrimitiveOneofFieldGenerator::
~ImmutablePrimitiveOneofFieldGenerator() {}

void ImmutablePrimitiveOneofFieldGenerator::
GenerateMembers(io::Printer* printer) const {
  PrintExtraFieldInfo(variables_, printer);
  if (SupportFieldPresence(descriptor_->file())) {
    WriteFieldDocComment(printer, descriptor_);
    printer->Print(variables_,
      "$deprecation$public boolean has$capitalized_name$() {\n"
      "  return $has_oneof_case_message$;\n"
      "}\n");
  }

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public $type$ get$capitalized_name$() {\n"
    "  if ($has_oneof_case_message$) {\n"
    "    return ($boxed_type$) $oneof_name$_;\n"
    "  }\n"
    "  return $default$;\n"
    "}\n");
}


void ImmutablePrimitiveOneofFieldGenerator::
GenerateBuilderMembers(io::Printer* printer) const {
  if (SupportFieldPresence(descriptor_->file())) {
    WriteFieldDocComment(printer, descriptor_);
    printer->Print(variables_,
      "$deprecation$public boolean has$capitalized_name$() {\n"
      "  return $has_oneof_case_message$;\n"
      "}\n");
  }

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public $type$ get$capitalized_name$() {\n"
    "  if ($has_oneof_case_message$) {\n"
    "    return ($boxed_type$) $oneof_name$_;\n"
    "  }\n"
    "  return $default$;\n"
    "}\n");

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder set$capitalized_name$($type$ value) {\n"
    "$null_check$"
    "  $set_oneof_case_message$;\n"
    "  $oneof_name$_ = value;\n"
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");

  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder clear$capitalized_name$() {\n"
    "  if ($has_oneof_case_message$) {\n"
    "    $clear_oneof_case_message$;\n"
    "    $oneof_name$_ = null;\n"
    "    $on_changed$\n"
    "  }\n"
    "  return this;\n"
    "}\n");
}

void ImmutablePrimitiveOneofFieldGenerator::
GenerateBuildingCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if ($has_oneof_case_message$) {\n"
    "  result.$oneof_name$_ = $oneof_name$_;\n"
    "}\n");
}

void ImmutablePrimitiveOneofFieldGenerator::
GenerateMergingCode(io::Printer* printer) const {
  printer->Print(variables_,
    "set$capitalized_name$(other.get$capitalized_name$());\n");
}

void ImmutablePrimitiveOneofFieldGenerator::
GenerateParsingCode(io::Printer* printer) const {
  printer->Print(variables_,
    "$set_oneof_case_message$;\n"
    "$oneof_name$_ = input.read$capitalized_type$();\n");
}

void ImmutablePrimitiveOneofFieldGenerator::
GenerateSerializationCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if ($has_oneof_case_message$) {\n"
    "  output.write$capitalized_type$(\n"
    "      $number$, ($type$)(($boxed_type$) $oneof_name$_));\n"
    "}\n");
}

void ImmutablePrimitiveOneofFieldGenerator::
GenerateSerializedSizeCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if ($has_oneof_case_message$) {\n"
    "  size += com.google.protobuf.CodedOutputStream\n"
    "    .compute$capitalized_type$Size(\n"
    "        $number$, ($type$)(($boxed_type$) $oneof_name$_));\n"
    "}\n");
}

// ===================================================================

RepeatedImmutablePrimitiveFieldGenerator::
RepeatedImmutablePrimitiveFieldGenerator(const FieldDescriptor* descriptor,
                                         int messageBitIndex,
                                         int builderBitIndex,
                                         Context* context)
  : descriptor_(descriptor), messageBitIndex_(messageBitIndex),
    builderBitIndex_(builderBitIndex), context_(context),
    name_resolver_(context->GetNameResolver()) {
  SetPrimitiveVariables(descriptor, messageBitIndex, builderBitIndex,
                        context->GetFieldGeneratorInfo(descriptor),
                        name_resolver_, &variables_);
}

RepeatedImmutablePrimitiveFieldGenerator::
~RepeatedImmutablePrimitiveFieldGenerator() {}

int RepeatedImmutablePrimitiveFieldGenerator::GetNumBitsForMessage() const {
  return 0;
}

int RepeatedImmutablePrimitiveFieldGenerator::GetNumBitsForBuilder() const {
  return 1;
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateInterfaceMembers(io::Printer* printer) const {
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$java.util.List<$boxed_type$> get$capitalized_name$List();\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$int get$capitalized_name$Count();\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$$type$ get$capitalized_name$(int index);\n");
}


void RepeatedImmutablePrimitiveFieldGenerator::
GenerateMembers(io::Printer* printer) const {
  printer->Print(variables_,
    "private $field_list_type$ $name$_;\n");
  PrintExtraFieldInfo(variables_, printer);
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public java.util.List<$boxed_type$>\n"
    "    get$capitalized_name$List() {\n"
    "  return $name$_;\n"   // note:  unmodifiable list
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public int get$capitalized_name$Count() {\n"
    "  return $name$_.size();\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public $type$ get$capitalized_name$(int index) {\n"
    "  return $name$_.get(index);\n"
    "}\n");

  if (descriptor_->is_packed() &&
      HasGeneratedMethods(descriptor_->containing_type())) {
    printer->Print(variables_,
      "private int $name$MemoizedSerializedSize = -1;\n");
  }
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateBuilderMembers(io::Printer* printer) const {
  // One field is the list and the bit field keeps track of whether the
  // list is immutable. If it's immutable, the invariant is that it must
  // either an instance of Collections.emptyList() or it's an ArrayList
  // wrapped in a Collections.unmodifiableList() wrapper and nobody else has
  // a refererence to the underlying ArrayList. This invariant allows us to
  // share instances of lists between protocol buffers avoiding expensive
  // memory allocations. Note, immutable is a strong guarantee here -- not
  // just that the list cannot be modified via the reference but that the
  // list can never be modified.
  printer->Print(variables_,
    "private $field_list_type$ $name$_ = $empty_list$;\n");

  printer->Print(variables_,
    "private void ensure$capitalized_name$IsMutable() {\n"
    "  if (!$get_mutable_bit_builder$) {\n"
    "    $name$_ = new java.util.ArrayList<$boxed_type$>($name$_);\n"
    "    $set_mutable_bit_builder$;\n"
    "   }\n"
    "}\n");

    // Note:  We return an unmodifiable list because otherwise the caller
    //   could hold on to the returned list and modify it after the message
    //   has been built, thus mutating the message which is supposed to be
    //   immutable.
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public java.util.List<$boxed_type$>\n"
    "    get$capitalized_name$List() {\n"
    "  return java.util.Collections.unmodifiableList($name$_);\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public int get$capitalized_name$Count() {\n"
    "  return $name$_.size();\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public $type$ get$capitalized_name$(int index) {\n"
    "  return $name$_.get(index);\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder set$capitalized_name$(\n"
    "    int index, $type$ value) {\n"
    "$null_check$"
    "  ensure$capitalized_name$IsMutable();\n"
    "  $name$_.set(index, value);\n"
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder add$capitalized_name$($type$ value) {\n"
    "$null_check$"
    "  ensure$capitalized_name$IsMutable();\n"
    "  $name$_.add(value);\n"
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder addAll$capitalized_name$(\n"
    "    java.lang.Iterable<? extends $boxed_type$> values) {\n"
    "  ensure$capitalized_name$IsMutable();\n"
    "  com.google.protobuf.AbstractMessageLite.Builder.addAll(\n"
    "      values, $name$_);\n"
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");
  WriteFieldDocComment(printer, descriptor_);
  printer->Print(variables_,
    "$deprecation$public Builder clear$capitalized_name$() {\n"
    "  $name$_ = $empty_list$;\n"
    "  $clear_mutable_bit_builder$;\n"
    "  $on_changed$\n"
    "  return this;\n"
    "}\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateFieldBuilderInitializationCode(io::Printer* printer)  const {
  // noop for primitives
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateInitializationCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_ = $empty_list$;\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateBuilderClearCode(io::Printer* printer) const {
  printer->Print(variables_,
    "$name$_ = $empty_list$;\n"
    "$clear_mutable_bit_builder$;\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateMergingCode(io::Printer* printer) const {
  // The code below does two optimizations:
  //   1. If the other list is empty, there's nothing to do. This ensures we
  //      don't allocate a new array if we already have an immutable one.
  //   2. If the other list is non-empty and our current list is empty, we can
  //      reuse the other list which is guaranteed to be immutable.
  printer->Print(variables_,
    "if (!other.$name$_.isEmpty()) {\n"
    "  if ($name$_.isEmpty()) {\n"
    "    $name$_ = other.$name$_;\n"
    "    $clear_mutable_bit_builder$;\n"
    "  } else {\n"
    "    ensure$capitalized_name$IsMutable();\n"
    "    $name$_.addAll(other.$name$_);\n"
    "  }\n"
    "  $on_changed$\n"
    "}\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateBuildingCode(io::Printer* printer) const {
  // The code below ensures that the result has an immutable list. If our
  // list is immutable, we can just reuse it. If not, we make it immutable.
  printer->Print(variables_,
    "if ($get_mutable_bit_builder$) {\n"
    "  $name$_ = java.util.Collections.unmodifiableList($name$_);\n"
    "  $clear_mutable_bit_builder$;\n"
    "}\n"
    "result.$name$_ = $name$_;\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateParsingCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if (!$get_mutable_bit_parser$) {\n"
    "  $name$_ = new java.util.ArrayList<$boxed_type$>();\n"
    "  $set_mutable_bit_parser$;\n"
    "}\n"
    "$name$_.add(input.read$capitalized_type$());\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateParsingCodeFromPacked(io::Printer* printer) const {
  printer->Print(variables_,
    "int length = input.readRawVarint32();\n"
    "int limit = input.pushLimit(length);\n"
    "if (!$get_mutable_bit_parser$ && input.getBytesUntilLimit() > 0) {\n"
    "  $name$_ = new java.util.ArrayList<$boxed_type$>();\n"
    "  $set_mutable_bit_parser$;\n"
    "}\n"
    "while (input.getBytesUntilLimit() > 0) {\n"
    "  $name$_.add(input.read$capitalized_type$());\n"
    "}\n"
    "input.popLimit(limit);\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateParsingDoneCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if ($get_mutable_bit_parser$) {\n"
    "  $name$_ = java.util.Collections.unmodifiableList($name$_);\n"
    "}\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateSerializationCode(io::Printer* printer) const {
  if (descriptor_->is_packed()) {
    // We invoke getSerializedSize in writeTo for messages that have packed
    // fields in ImmutableMessageGenerator::GenerateMessageSerializationMethods.
    // That makes it safe to rely on the memoized size here.
    printer->Print(variables_,
      "if (get$capitalized_name$List().size() > 0) {\n"
      "  output.writeRawVarint32($tag$);\n"
      "  output.writeRawVarint32($name$MemoizedSerializedSize);\n"
      "}\n"
      "for (int i = 0; i < $name$_.size(); i++) {\n"
      "  output.write$capitalized_type$NoTag($name$_.get(i));\n"
      "}\n");
  } else {
    printer->Print(variables_,
      "for (int i = 0; i < $name$_.size(); i++) {\n"
      "  output.write$capitalized_type$($number$, $name$_.get(i));\n"
      "}\n");
  }
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateSerializedSizeCode(io::Printer* printer) const {
  printer->Print(variables_,
    "{\n"
    "  int dataSize = 0;\n");
  printer->Indent();

  if (FixedSize(GetType(descriptor_)) == -1) {
    printer->Print(variables_,
      "for (int i = 0; i < $name$_.size(); i++) {\n"
      "  dataSize += com.google.protobuf.CodedOutputStream\n"
      "    .compute$capitalized_type$SizeNoTag($name$_.get(i));\n"
      "}\n");
  } else {
    printer->Print(variables_,
      "dataSize = $fixed_size$ * get$capitalized_name$List().size();\n");
  }

  printer->Print(
      "size += dataSize;\n");

  if (descriptor_->is_packed()) {
    printer->Print(variables_,
      "if (!get$capitalized_name$List().isEmpty()) {\n"
      "  size += $tag_size$;\n"
      "  size += com.google.protobuf.CodedOutputStream\n"
      "      .computeInt32SizeNoTag(dataSize);\n"
      "}\n");
  } else {
    printer->Print(variables_,
      "size += $tag_size$ * get$capitalized_name$List().size();\n");
  }

  // cache the data size for packed fields.
  if (descriptor_->is_packed()) {
    printer->Print(variables_,
      "$name$MemoizedSerializedSize = dataSize;\n");
  }

  printer->Outdent();
  printer->Print("}\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateEqualsCode(io::Printer* printer) const {
  printer->Print(variables_,
    "result = result && get$capitalized_name$List()\n"
    "    .equals(other.get$capitalized_name$List());\n");
}

void RepeatedImmutablePrimitiveFieldGenerator::
GenerateHashCode(io::Printer* printer) const {
  printer->Print(variables_,
    "if (get$capitalized_name$Count() > 0) {\n"
    "  hash = (37 * hash) + $constant_name$;\n"
    "  hash = (53 * hash) + get$capitalized_name$List().hashCode();\n"
    "}\n");
}

string RepeatedImmutablePrimitiveFieldGenerator::GetBoxedType() const {
  return BoxedPrimitiveTypeName(GetJavaType(descriptor_));
}

}  // namespace java
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
