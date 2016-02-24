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

#include <map>
#include <string>

#include <google/protobuf/compiler/objectivec/objectivec_primitive_field.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/compiler/objectivec/objectivec_helpers.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/wire_format.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/stubs/strutil.h>
#include <google/protobuf/stubs/substitute.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace objectivec {

using internal::WireFormat;
using internal::WireFormatLite;

namespace {

const char* PrimitiveTypeName(const FieldDescriptor* descriptor) {
  ObjectiveCType type = GetObjectiveCType(descriptor);
  switch (type) {
    case OBJECTIVECTYPE_INT32:
      return "int32_t";
    case OBJECTIVECTYPE_UINT32:
      return "uint32_t";
    case OBJECTIVECTYPE_INT64:
      return "int64_t";
    case OBJECTIVECTYPE_UINT64:
      return "uint64_t";
    case OBJECTIVECTYPE_FLOAT:
      return "float";
    case OBJECTIVECTYPE_DOUBLE:
      return "double";
    case OBJECTIVECTYPE_BOOLEAN:
      return "BOOL";
    case OBJECTIVECTYPE_STRING:
      return "NSString";
    case OBJECTIVECTYPE_DATA:
      return "NSData";
    case OBJECTIVECTYPE_ENUM:
      return "int32_t";
    case OBJECTIVECTYPE_MESSAGE:
      return NULL;
  }

  // Some compilers report reaching end of function even though all cases of
  // the enum are handed in the switch.
  GOOGLE_LOG(FATAL) << "Can't get here.";
  return NULL;
}

const char* PrimitiveArrayTypeName(const FieldDescriptor* descriptor) {
  ObjectiveCType type = GetObjectiveCType(descriptor);
  switch (type) {
    case OBJECTIVECTYPE_INT32:
      return "Int32";
    case OBJECTIVECTYPE_UINT32:
      return "UInt32";
    case OBJECTIVECTYPE_INT64:
      return "Int64";
    case OBJECTIVECTYPE_UINT64:
      return "UInt64";
    case OBJECTIVECTYPE_FLOAT:
      return "Float";
    case OBJECTIVECTYPE_DOUBLE:
      return "Double";
    case OBJECTIVECTYPE_BOOLEAN:
      return "Bool";
    case OBJECTIVECTYPE_STRING:
      return "";  // Want NSArray
    case OBJECTIVECTYPE_DATA:
      return "";  // Want NSArray
    case OBJECTIVECTYPE_ENUM:
      return "Enum";
    case OBJECTIVECTYPE_MESSAGE:
      return "";  // Want NSArray
  }

  // Some compilers report reaching end of function even though all cases of
  // the enum are handed in the switch.
  GOOGLE_LOG(FATAL) << "Can't get here.";
  return NULL;
}

void SetPrimitiveVariables(const FieldDescriptor* descriptor,
                           map<string, string>* variables) {
  std::string primitive_name = PrimitiveTypeName(descriptor);
  (*variables)["type"] = primitive_name;
  (*variables)["storage_type"] = primitive_name;
}

}  // namespace

PrimitiveFieldGenerator::PrimitiveFieldGenerator(
    const FieldDescriptor* descriptor)
    : SingleFieldGenerator(descriptor) {
  SetPrimitiveVariables(descriptor, &variables_);
}

PrimitiveFieldGenerator::~PrimitiveFieldGenerator() {}

PrimitiveObjFieldGenerator::PrimitiveObjFieldGenerator(
    const FieldDescriptor* descriptor)
    : ObjCObjFieldGenerator(descriptor) {
  SetPrimitiveVariables(descriptor, &variables_);
  variables_["property_storage_attribute"] = "copy";
}

PrimitiveObjFieldGenerator::~PrimitiveObjFieldGenerator() {}

RepeatedPrimitiveFieldGenerator::RepeatedPrimitiveFieldGenerator(
    const FieldDescriptor* descriptor)
    : RepeatedFieldGenerator(descriptor) {
  SetPrimitiveVariables(descriptor, &variables_);

  string base_name = PrimitiveArrayTypeName(descriptor);
  if (base_name.length()) {
    variables_["array_storage_type"] = "GPB" + base_name + "Array";
  } else {
    variables_["array_storage_type"] = "NSMutableArray";
  }
}

RepeatedPrimitiveFieldGenerator::~RepeatedPrimitiveFieldGenerator() {}

void RepeatedPrimitiveFieldGenerator::FinishInitialization(void) {
  RepeatedFieldGenerator::FinishInitialization();
  if (IsPrimitiveType(descriptor_)) {
    // No comment needed for primitive types.
    variables_["array_comment"] = "";
  }
}

}  // namespace objectivec
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
