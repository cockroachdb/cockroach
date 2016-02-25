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

#include <google/protobuf/compiler/cpp/cpp_enum_field.h>
#include <google/protobuf/compiler/cpp/cpp_helpers.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/stubs/strutil.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace cpp {

namespace {

void SetEnumVariables(const FieldDescriptor* descriptor,
                      map<string, string>* variables,
                      const Options& options) {
  SetCommonFieldVariables(descriptor, variables, options);
  const EnumValueDescriptor* default_value = descriptor->default_value_enum();
  (*variables)["type"] = ClassName(descriptor->enum_type(), true);
  (*variables)["default"] = Int32ToString(default_value->number());
  (*variables)["full_name"] = descriptor->full_name();
}

}  // namespace

// ===================================================================

EnumFieldGenerator::
EnumFieldGenerator(const FieldDescriptor* descriptor,
                   const Options& options)
  : descriptor_(descriptor) {
  SetEnumVariables(descriptor, &variables_, options);
}

EnumFieldGenerator::~EnumFieldGenerator() {}

void EnumFieldGenerator::
GeneratePrivateMembers(io::Printer* printer) const {
  printer->Print(variables_, "int $name$_;\n");
}

void EnumFieldGenerator::
GenerateAccessorDeclarations(io::Printer* printer) const {
  printer->Print(variables_,
    "$type$ $name$() const$deprecation$;\n"
    "void set_$name$($type$ value)$deprecation$;\n");
}

void EnumFieldGenerator::
GenerateInlineAccessorDefinitions(io::Printer* printer,
                                  bool is_inline) const {
  map<string, string> variables(variables_);
  variables["inline"] = is_inline ? "inline" : "";
  printer->Print(variables,
    "$inline$ $type$ $classname$::$name$() const {\n"
    "  // @@protoc_insertion_point(field_get:$full_name$)\n"
    "  return static_cast< $type$ >($name$_);\n"
    "}\n"
    "$inline$ void $classname$::set_$name$($type$ value) {\n");
  if (!HasPreservingUnknownEnumSemantics(descriptor_->file())) {
    printer->Print(variables,
    "  assert($type$_IsValid(value));\n");
  }
  printer->Print(variables,
    "  $set_hasbit$\n"
    "  $name$_ = value;\n"
    "  // @@protoc_insertion_point(field_set:$full_name$)\n"
    "}\n");
}

void EnumFieldGenerator::
GenerateClearingCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_ = $default$;\n");
}

void EnumFieldGenerator::
GenerateMergingCode(io::Printer* printer) const {
  printer->Print(variables_, "set_$name$(from.$name$());\n");
}

void EnumFieldGenerator::
GenerateSwappingCode(io::Printer* printer) const {
  printer->Print(variables_, "std::swap($name$_, other->$name$_);\n");
}

void EnumFieldGenerator::
GenerateConstructorCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_ = $default$;\n");
}

void EnumFieldGenerator::
GenerateMergeFromCodedStream(io::Printer* printer) const {
  printer->Print(variables_,
    "int value;\n"
    "DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<\n"
    "         int, ::google::protobuf::internal::WireFormatLite::TYPE_ENUM>(\n"
    "       input, &value)));\n");
  if (HasPreservingUnknownEnumSemantics(descriptor_->file())) {
    printer->Print(variables_,
      "set_$name$(static_cast< $type$ >(value));\n");
  } else {
    printer->Print(variables_,
      "if ($type$_IsValid(value)) {\n"
      "  set_$name$(static_cast< $type$ >(value));\n");
    if (UseUnknownFieldSet(descriptor_->file())) {
      printer->Print(variables_,
        "} else {\n"
        "  mutable_unknown_fields()->AddVarint($number$, value);\n");
    } else {
      printer->Print(
        "} else {\n"
        "  unknown_fields_stream.WriteVarint32(tag);\n"
        "  unknown_fields_stream.WriteVarint32(value);\n");
    }
    printer->Print(variables_,
      "}\n");
  }
}

void EnumFieldGenerator::
GenerateSerializeWithCachedSizes(io::Printer* printer) const {
  printer->Print(variables_,
    "::google::protobuf::internal::WireFormatLite::WriteEnum(\n"
    "  $number$, this->$name$(), output);\n");
}

void EnumFieldGenerator::
GenerateSerializeWithCachedSizesToArray(io::Printer* printer) const {
  printer->Print(variables_,
    "target = ::google::protobuf::internal::WireFormatLite::WriteEnumToArray(\n"
    "  $number$, this->$name$(), target);\n");
}

void EnumFieldGenerator::
GenerateByteSize(io::Printer* printer) const {
  printer->Print(variables_,
    "total_size += $tag_size$ +\n"
    "  ::google::protobuf::internal::WireFormatLite::EnumSize(this->$name$());\n");
}

// ===================================================================

EnumOneofFieldGenerator::
EnumOneofFieldGenerator(const FieldDescriptor* descriptor,
                        const Options& options)
  : EnumFieldGenerator(descriptor, options) {
  SetCommonOneofFieldVariables(descriptor, &variables_);
}

EnumOneofFieldGenerator::~EnumOneofFieldGenerator() {}

void EnumOneofFieldGenerator::
GenerateInlineAccessorDefinitions(io::Printer* printer,
                                  bool is_inline) const {
  map<string, string> variables(variables_);
  variables["inline"] = is_inline ? "inline" : "";
  printer->Print(variables,
    "$inline$ $type$ $classname$::$name$() const {\n"
    "  // @@protoc_insertion_point(field_get:$full_name$)\n"
    "  if (has_$name$()) {\n"
    "    return static_cast< $type$ >($oneof_prefix$$name$_);\n"
    "  }\n"
    "  return static_cast< $type$ >($default$);\n"
    "}\n"
    "$inline$ void $classname$::set_$name$($type$ value) {\n");
  if (!HasPreservingUnknownEnumSemantics(descriptor_->file())) {
    printer->Print(variables,
    "  assert($type$_IsValid(value));\n");
  }
  printer->Print(variables,
    "  if (!has_$name$()) {\n"
    "    clear_$oneof_name$();\n"
    "    set_has_$name$();\n"
    "  }\n"
    "  $oneof_prefix$$name$_ = value;\n"
    "  // @@protoc_insertion_point(field_set:$full_name$)\n"
    "}\n");
}

void EnumOneofFieldGenerator::
GenerateClearingCode(io::Printer* printer) const {
  printer->Print(variables_, "$oneof_prefix$$name$_ = $default$;\n");
}

void EnumOneofFieldGenerator::
GenerateSwappingCode(io::Printer* printer) const {
  // Don't print any swapping code. Swapping the union will swap this field.
}

void EnumOneofFieldGenerator::
GenerateConstructorCode(io::Printer* printer) const {
  printer->Print(variables_,
    "  $classname$_default_oneof_instance_->$name$_ = $default$;\n");
}

// ===================================================================

RepeatedEnumFieldGenerator::
RepeatedEnumFieldGenerator(const FieldDescriptor* descriptor,
                           const Options& options)
  : descriptor_(descriptor) {
  SetEnumVariables(descriptor, &variables_, options);
}

RepeatedEnumFieldGenerator::~RepeatedEnumFieldGenerator() {}

void RepeatedEnumFieldGenerator::
GeneratePrivateMembers(io::Printer* printer) const {
  printer->Print(variables_,
    "::google::protobuf::RepeatedField<int> $name$_;\n");
  if (descriptor_->is_packed()
      && HasGeneratedMethods(descriptor_->file())) {
    printer->Print(variables_,
      "mutable int _$name$_cached_byte_size_;\n");
  }
}

void RepeatedEnumFieldGenerator::
GenerateAccessorDeclarations(io::Printer* printer) const {
  printer->Print(variables_,
    "$type$ $name$(int index) const$deprecation$;\n"
    "void set_$name$(int index, $type$ value)$deprecation$;\n"
    "void add_$name$($type$ value)$deprecation$;\n");
  printer->Print(variables_,
    "const ::google::protobuf::RepeatedField<int>& $name$() const$deprecation$;\n"
    "::google::protobuf::RepeatedField<int>* mutable_$name$()$deprecation$;\n");
}

void RepeatedEnumFieldGenerator::
GenerateInlineAccessorDefinitions(io::Printer* printer,
                                  bool is_inline) const {
  map<string, string> variables(variables_);
  variables["inline"] = is_inline ? "inline" : "";
  printer->Print(variables,
    "$inline$ $type$ $classname$::$name$(int index) const {\n"
    "  // @@protoc_insertion_point(field_get:$full_name$)\n"
    "  return static_cast< $type$ >($name$_.Get(index));\n"
    "}\n"
    "$inline$ void $classname$::set_$name$(int index, $type$ value) {\n");
  if (!HasPreservingUnknownEnumSemantics(descriptor_->file())) {
    printer->Print(variables,
    "  assert($type$_IsValid(value));\n");
  }
  printer->Print(variables,
    "  $name$_.Set(index, value);\n"
    "  // @@protoc_insertion_point(field_set:$full_name$)\n"
    "}\n"
    "$inline$ void $classname$::add_$name$($type$ value) {\n");
  if (!HasPreservingUnknownEnumSemantics(descriptor_->file())) {
    printer->Print(variables,
    "  assert($type$_IsValid(value));\n");
  }
  printer->Print(variables,
    "  $name$_.Add(value);\n"
    "  // @@protoc_insertion_point(field_add:$full_name$)\n"
    "}\n");
  printer->Print(variables,
    "$inline$ const ::google::protobuf::RepeatedField<int>&\n"
    "$classname$::$name$() const {\n"
    "  // @@protoc_insertion_point(field_list:$full_name$)\n"
    "  return $name$_;\n"
    "}\n"
    "$inline$ ::google::protobuf::RepeatedField<int>*\n"
    "$classname$::mutable_$name$() {\n"
    "  // @@protoc_insertion_point(field_mutable_list:$full_name$)\n"
    "  return &$name$_;\n"
    "}\n");
}

void RepeatedEnumFieldGenerator::
GenerateClearingCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_.Clear();\n");
}

void RepeatedEnumFieldGenerator::
GenerateMergingCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_.MergeFrom(from.$name$_);\n");
}

void RepeatedEnumFieldGenerator::
GenerateSwappingCode(io::Printer* printer) const {
  printer->Print(variables_, "$name$_.UnsafeArenaSwap(&other->$name$_);\n");
}

void RepeatedEnumFieldGenerator::
GenerateConstructorCode(io::Printer* printer) const {
  // Not needed for repeated fields.
}

void RepeatedEnumFieldGenerator::
GenerateMergeFromCodedStream(io::Printer* printer) const {
  // Don't use ReadRepeatedPrimitive here so that the enum can be validated.
  printer->Print(variables_,
    "int value;\n"
    "DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<\n"
    "         int, ::google::protobuf::internal::WireFormatLite::TYPE_ENUM>(\n"
    "       input, &value)));\n");
  if (HasPreservingUnknownEnumSemantics(descriptor_->file())) {
    printer->Print(variables_,
      "add_$name$(static_cast< $type$ >(value));\n");
  } else {
    printer->Print(variables_,
      "if ($type$_IsValid(value)) {\n"
      "  add_$name$(static_cast< $type$ >(value));\n");
    if (UseUnknownFieldSet(descriptor_->file())) {
      printer->Print(variables_,
        "} else {\n"
        "  mutable_unknown_fields()->AddVarint($number$, value);\n");
    } else {
      printer->Print(
        "} else {\n"
        "  unknown_fields_stream.WriteVarint32(tag);\n"
        "  unknown_fields_stream.WriteVarint32(value);\n");
    }
    printer->Print("}\n");
  }
}

void RepeatedEnumFieldGenerator::
GenerateMergeFromCodedStreamWithPacking(io::Printer* printer) const {
  if (!descriptor_->is_packed()) {
      // This path is rarely executed, so we use a non-inlined implementation.
    if (HasPreservingUnknownEnumSemantics(descriptor_->file())) {
      printer->Print(variables_,
        "DO_((::google::protobuf::internal::"
                    "WireFormatLite::ReadPackedEnumPreserveUnknowns(\n"
        "       input,\n"
        "       $number$,\n"
        "       NULL,\n"
        "       NULL,\n"
        "       this->mutable_$name$())));\n");
    } else if (UseUnknownFieldSet(descriptor_->file())) {
      printer->Print(variables_,
        "DO_((::google::protobuf::internal::WireFormat::ReadPackedEnumPreserveUnknowns(\n"
        "       input,\n"
        "       $number$,\n"
        "       $type$_IsValid,\n"
        "       mutable_unknown_fields(),\n"
        "       this->mutable_$name$())));\n");
    } else {
      printer->Print(variables_,
        "DO_((::google::protobuf::internal::"
                     "WireFormatLite::ReadPackedEnumPreserveUnknowns(\n"
        "       input,\n"
        "       $number$,\n"
        "       $type$_IsValid,\n"
        "       &unknown_fields_stream,\n"
        "       this->mutable_$name$())));\n");
    }
  } else {
    printer->Print(variables_,
      "::google::protobuf::uint32 length;\n"
      "DO_(input->ReadVarint32(&length));\n"
      "::google::protobuf::io::CodedInputStream::Limit limit = "
          "input->PushLimit(length);\n"
      "while (input->BytesUntilLimit() > 0) {\n"
      "  int value;\n"
      "  DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<\n"
      "         int, ::google::protobuf::internal::WireFormatLite::TYPE_ENUM>(\n"
      "       input, &value)));\n");
    if (HasPreservingUnknownEnumSemantics(descriptor_->file())) {
      printer->Print(variables_,
      "  add_$name$(static_cast< $type$ >(value));\n");
    } else {
      printer->Print(variables_,
      "  if ($type$_IsValid(value)) {\n"
      "    add_$name$(static_cast< $type$ >(value));\n"
      "  } else {\n");
      if (UseUnknownFieldSet(descriptor_->file())) {
        printer->Print(variables_,
        "    mutable_unknown_fields()->AddVarint($number$, value);\n");
      } else {
        printer->Print(variables_,
        "    unknown_fields_stream.WriteVarint32(tag);\n"
        "    unknown_fields_stream.WriteVarint32(value);\n");
      }
      printer->Print(
      "  }\n");
    }
    printer->Print(variables_,
      "}\n"
      "input->PopLimit(limit);\n");
  }
}

void RepeatedEnumFieldGenerator::
GenerateSerializeWithCachedSizes(io::Printer* printer) const {
  if (descriptor_->is_packed()) {
    // Write the tag and the size.
    printer->Print(variables_,
      "if (this->$name$_size() > 0) {\n"
      "  ::google::protobuf::internal::WireFormatLite::WriteTag(\n"
      "    $number$,\n"
      "    ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED,\n"
      "    output);\n"
      "  output->WriteVarint32(_$name$_cached_byte_size_);\n"
      "}\n");
  }
  printer->Print(variables_,
      "for (int i = 0; i < this->$name$_size(); i++) {\n");
  if (descriptor_->is_packed()) {
    printer->Print(variables_,
      "  ::google::protobuf::internal::WireFormatLite::WriteEnumNoTag(\n"
      "    this->$name$(i), output);\n");
  } else {
    printer->Print(variables_,
      "  ::google::protobuf::internal::WireFormatLite::WriteEnum(\n"
      "    $number$, this->$name$(i), output);\n");
  }
  printer->Print("}\n");
}

void RepeatedEnumFieldGenerator::
GenerateSerializeWithCachedSizesToArray(io::Printer* printer) const {
  if (descriptor_->is_packed()) {
    // Write the tag and the size.
    printer->Print(variables_,
      "if (this->$name$_size() > 0) {\n"
      "  target = ::google::protobuf::internal::WireFormatLite::WriteTagToArray(\n"
      "    $number$,\n"
      "    ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED,\n"
      "    target);\n"
      "  target = ::google::protobuf::io::CodedOutputStream::WriteVarint32ToArray("
      "    _$name$_cached_byte_size_, target);\n"
      "}\n");
  }
  printer->Print(variables_,
      "for (int i = 0; i < this->$name$_size(); i++) {\n");
  if (descriptor_->is_packed()) {
    printer->Print(variables_,
      "  target = ::google::protobuf::internal::WireFormatLite::WriteEnumNoTagToArray(\n"
      "    this->$name$(i), target);\n");
  } else {
    printer->Print(variables_,
      "  target = ::google::protobuf::internal::WireFormatLite::WriteEnumToArray(\n"
      "    $number$, this->$name$(i), target);\n");
  }
  printer->Print("}\n");
}

void RepeatedEnumFieldGenerator::
GenerateByteSize(io::Printer* printer) const {
  printer->Print(variables_,
    "{\n"
    "  int data_size = 0;\n");
  printer->Indent();
  printer->Print(variables_,
      "for (int i = 0; i < this->$name$_size(); i++) {\n"
      "  data_size += ::google::protobuf::internal::WireFormatLite::EnumSize(\n"
      "    this->$name$(i));\n"
      "}\n");

  if (descriptor_->is_packed()) {
    printer->Print(variables_,
      "if (data_size > 0) {\n"
      "  total_size += $tag_size$ +\n"
      "    ::google::protobuf::internal::WireFormatLite::Int32Size(data_size);\n"
      "}\n"
      "GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();\n"
      "_$name$_cached_byte_size_ = data_size;\n"
      "GOOGLE_SAFE_CONCURRENT_WRITES_END();\n"
      "total_size += data_size;\n");
  } else {
    printer->Print(variables_,
      "total_size += $tag_size$ * this->$name$_size() + data_size;\n");
  }
  printer->Outdent();
  printer->Print("}\n");
}

}  // namespace cpp
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
