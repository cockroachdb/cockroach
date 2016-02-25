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

#include <google/protobuf/compiler/objectivec/objectivec_message_field.h>
#include <google/protobuf/compiler/objectivec/objectivec_helpers.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/wire_format.h>
#include <google/protobuf/stubs/strutil.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace objectivec {

namespace {

void SetMessageVariables(const FieldDescriptor* descriptor,
                         map<string, string>* variables) {
  const string& message_type = ClassName(descriptor->message_type());
  (*variables)["type"] = message_type;
  (*variables)["containing_class"] = ClassName(descriptor->containing_type());
  (*variables)["storage_type"] = message_type;
  (*variables)["group_or_message"] =
      (descriptor->type() == FieldDescriptor::TYPE_GROUP) ? "Group" : "Message";

  (*variables)["dataTypeSpecific_value"] = "GPBStringifySymbol(" + message_type + ")";
}

}  // namespace

MessageFieldGenerator::MessageFieldGenerator(const FieldDescriptor* descriptor)
    : ObjCObjFieldGenerator(descriptor) {
  SetMessageVariables(descriptor, &variables_);
}

MessageFieldGenerator::~MessageFieldGenerator() {}

void MessageFieldGenerator::DetermineForwardDeclarations(
    set<string>* fwd_decls) const {
  // Class name is already in "storage_type".
  fwd_decls->insert("@class " + variable("storage_type"));
}

bool MessageFieldGenerator::WantsHasProperty(void) const {
  if (descriptor_->containing_oneof() != NULL) {
    // If in a oneof, it uses the oneofcase instead of a has bit.
    return false;
  }
  // In both proto2 & proto3, message fields have a has* property to tell
  // when it is a non default value.
  return true;
}

RepeatedMessageFieldGenerator::RepeatedMessageFieldGenerator(
    const FieldDescriptor* descriptor)
    : RepeatedFieldGenerator(descriptor) {
  SetMessageVariables(descriptor, &variables_);
  variables_["array_storage_type"] = "NSMutableArray";
}

RepeatedMessageFieldGenerator::~RepeatedMessageFieldGenerator() {}

}  // namespace objectivec
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
