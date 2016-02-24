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

#include <algorithm>
#include <google/protobuf/stubs/hash.h>
#include <limits>
#include <vector>

#include <google/protobuf/compiler/java/java_helpers.h>
#include <google/protobuf/compiler/java/java_name_resolver.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/wire_format.h>
#include <google/protobuf/stubs/strutil.h>
#include <google/protobuf/stubs/substitute.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace java {

using internal::WireFormat;
using internal::WireFormatLite;

const char kThickSeparator[] =
  "// ===================================================================\n";
const char kThinSeparator[] =
  "// -------------------------------------------------------------------\n";

namespace {

const char* kDefaultPackage = "";

// Names that should be avoided as field names.
// Using them will cause the compiler to generate accessors whose names are
// colliding with methods defined in base classes.
const char* kForbiddenWordList[] = {
  // message base class:
  "cached_size", "serialized_size",
  // java.lang.Object:
  "class",
};

bool IsForbidden(const string& field_name) {
  for (int i = 0; i < GOOGLE_ARRAYSIZE(kForbiddenWordList); ++i) {
    if (field_name == kForbiddenWordList[i]) {
      return true;
    }
  }
  return false;
}

string FieldName(const FieldDescriptor* field) {
  string field_name;
  // Groups are hacky:  The name of the field is just the lower-cased name
  // of the group type.  In Java, though, we would like to retain the original
  // capitalization of the type name.
  if (GetType(field) == FieldDescriptor::TYPE_GROUP) {
    field_name = field->message_type()->name();
  } else {
    field_name = field->name();
  }
  if (IsForbidden(field_name)) {
    // Append a trailing "#" to indicate that the name should be decorated to
    // avoid collision with other names.
    field_name += "#";
  }
  return field_name;
}


}  // namespace

string UnderscoresToCamelCase(const string& input, bool cap_next_letter) {
  string result;
  // Note:  I distrust ctype.h due to locales.
  for (int i = 0; i < input.size(); i++) {
    if ('a' <= input[i] && input[i] <= 'z') {
      if (cap_next_letter) {
        result += input[i] + ('A' - 'a');
      } else {
        result += input[i];
      }
      cap_next_letter = false;
    } else if ('A' <= input[i] && input[i] <= 'Z') {
      if (i == 0 && !cap_next_letter) {
        // Force first letter to lower-case unless explicitly told to
        // capitalize it.
        result += input[i] + ('a' - 'A');
      } else {
        // Capital letters after the first are left as-is.
        result += input[i];
      }
      cap_next_letter = false;
    } else if ('0' <= input[i] && input[i] <= '9') {
      result += input[i];
      cap_next_letter = true;
    } else {
      cap_next_letter = true;
    }
  }
  // Add a trailing "_" if the name should be altered.
  if (input[input.size() - 1] == '#') {
    result += '_';
  }
  return result;
}

string UnderscoresToCamelCase(const FieldDescriptor* field) {
  return UnderscoresToCamelCase(FieldName(field), false);
}

string UnderscoresToCapitalizedCamelCase(const FieldDescriptor* field) {
  return UnderscoresToCamelCase(FieldName(field), true);
}

string UnderscoresToCamelCase(const MethodDescriptor* method) {
  return UnderscoresToCamelCase(method->name(), false);
}

string UniqueFileScopeIdentifier(const Descriptor* descriptor) {
  return "static_" + StringReplace(descriptor->full_name(), ".", "_", true);
}

string StripProto(const string& filename) {
  if (HasSuffixString(filename, ".protodevel")) {
    return StripSuffixString(filename, ".protodevel");
  } else {
    return StripSuffixString(filename, ".proto");
  }
}

string FileClassName(const FileDescriptor* file, bool immutable) {
  ClassNameResolver name_resolver;
  return name_resolver.GetFileClassName(file, immutable);
}

string FileJavaPackage(const FileDescriptor* file, bool immutable) {
  string result;

  if (file->options().has_java_package()) {
    result = file->options().java_package();
  } else {
    result = kDefaultPackage;
    if (!file->package().empty()) {
      if (!result.empty()) result += '.';
      result += file->package();
    }
  }

  return result;
}

string JavaPackageToDir(string package_name) {
  string package_dir =
    StringReplace(package_name, ".", "/", true);
  if (!package_dir.empty()) package_dir += "/";
  return package_dir;
}

// TODO(xiaofeng): This function is only kept for it's publicly referenced.
// It should be removed after mutable API up-integration.
string ToJavaName(const string& full_name,
                  const FileDescriptor* file) {
  string result;
  if (file->options().java_multiple_files()) {
    result = FileJavaPackage(file);
  } else {
    result = ClassName(file);
  }
  if (!result.empty()) {
    result += '.';
  }
  if (file->package().empty()) {
    result += full_name;
  } else {
    // Strip the proto package from full_name since we've replaced it with
    // the Java package.
    result += full_name.substr(file->package().size() + 1);
  }
  return result;
}

string ClassName(const Descriptor* descriptor) {
  ClassNameResolver name_resolver;
  return name_resolver.GetClassName(descriptor, true);
}

string ClassName(const EnumDescriptor* descriptor) {
  ClassNameResolver name_resolver;
  return name_resolver.GetClassName(descriptor, true);
}

string ClassName(const ServiceDescriptor* descriptor) {
  ClassNameResolver name_resolver;
  return name_resolver.GetClassName(descriptor, true);
}

string ClassName(const FileDescriptor* descriptor) {
  ClassNameResolver name_resolver;
  return name_resolver.GetClassName(descriptor, true);
}

string ExtraMessageInterfaces(const Descriptor* descriptor) {
  string interfaces = "// @@protoc_insertion_point(message_implements:"
      + descriptor->full_name() + ")";
  return interfaces;
}


string ExtraBuilderInterfaces(const Descriptor* descriptor) {
  string interfaces = "// @@protoc_insertion_point(builder_implements:"
      + descriptor->full_name() + ")";
  return interfaces;
}

string ExtraMessageOrBuilderInterfaces(const Descriptor* descriptor) {
  string interfaces = "// @@protoc_insertion_point(interface_extends:"
      + descriptor->full_name() + ")";
  return interfaces;
}

string FieldConstantName(const FieldDescriptor *field) {
  string name = field->name() + "_FIELD_NUMBER";
  UpperString(&name);
  return name;
}

FieldDescriptor::Type GetType(const FieldDescriptor* field) {
  return field->type();
}

JavaType GetJavaType(const FieldDescriptor* field) {
  switch (GetType(field)) {
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_SFIXED32:
      return JAVATYPE_INT;

    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_FIXED64:
    case FieldDescriptor::TYPE_SFIXED64:
      return JAVATYPE_LONG;

    case FieldDescriptor::TYPE_FLOAT:
      return JAVATYPE_FLOAT;

    case FieldDescriptor::TYPE_DOUBLE:
      return JAVATYPE_DOUBLE;

    case FieldDescriptor::TYPE_BOOL:
      return JAVATYPE_BOOLEAN;

    case FieldDescriptor::TYPE_STRING:
      return JAVATYPE_STRING;

    case FieldDescriptor::TYPE_BYTES:
      return JAVATYPE_BYTES;

    case FieldDescriptor::TYPE_ENUM:
      return JAVATYPE_ENUM;

    case FieldDescriptor::TYPE_GROUP:
    case FieldDescriptor::TYPE_MESSAGE:
      return JAVATYPE_MESSAGE;

    // No default because we want the compiler to complain if any new
    // types are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return JAVATYPE_INT;
}

const char* PrimitiveTypeName(JavaType type) {
  switch (type) {
    case JAVATYPE_INT    : return "int";
    case JAVATYPE_LONG   : return "long";
    case JAVATYPE_FLOAT  : return "float";
    case JAVATYPE_DOUBLE : return "double";
    case JAVATYPE_BOOLEAN: return "boolean";
    case JAVATYPE_STRING : return "java.lang.String";
    case JAVATYPE_BYTES  : return "com.google.protobuf.ByteString";
    case JAVATYPE_ENUM   : return NULL;
    case JAVATYPE_MESSAGE: return NULL;

    // No default because we want the compiler to complain if any new
    // JavaTypes are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return NULL;
}

const char* BoxedPrimitiveTypeName(JavaType type) {
  switch (type) {
    case JAVATYPE_INT    : return "java.lang.Integer";
    case JAVATYPE_LONG   : return "java.lang.Long";
    case JAVATYPE_FLOAT  : return "java.lang.Float";
    case JAVATYPE_DOUBLE : return "java.lang.Double";
    case JAVATYPE_BOOLEAN: return "java.lang.Boolean";
    case JAVATYPE_STRING : return "java.lang.String";
    case JAVATYPE_BYTES  : return "com.google.protobuf.ByteString";
    case JAVATYPE_ENUM   : return NULL;
    case JAVATYPE_MESSAGE: return NULL;

    // No default because we want the compiler to complain if any new
    // JavaTypes are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return NULL;
}

const char* FieldTypeName(FieldDescriptor::Type field_type) {
  switch (field_type) {
    case FieldDescriptor::TYPE_INT32   : return "INT32";
    case FieldDescriptor::TYPE_UINT32  : return "UINT32";
    case FieldDescriptor::TYPE_SINT32  : return "SINT32";
    case FieldDescriptor::TYPE_FIXED32 : return "FIXED32";
    case FieldDescriptor::TYPE_SFIXED32: return "SFIXED32";
    case FieldDescriptor::TYPE_INT64   : return "INT64";
    case FieldDescriptor::TYPE_UINT64  : return "UINT64";
    case FieldDescriptor::TYPE_SINT64  : return "SINT64";
    case FieldDescriptor::TYPE_FIXED64 : return "FIXED64";
    case FieldDescriptor::TYPE_SFIXED64: return "SFIXED64";
    case FieldDescriptor::TYPE_FLOAT   : return "FLOAT";
    case FieldDescriptor::TYPE_DOUBLE  : return "DOUBLE";
    case FieldDescriptor::TYPE_BOOL    : return "BOOL";
    case FieldDescriptor::TYPE_STRING  : return "STRING";
    case FieldDescriptor::TYPE_BYTES   : return "BYTES";
    case FieldDescriptor::TYPE_ENUM    : return "ENUM";
    case FieldDescriptor::TYPE_GROUP   : return "GROUP";
    case FieldDescriptor::TYPE_MESSAGE : return "MESSAGE";

    // No default because we want the compiler to complain if any new
    // types are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return NULL;
}

bool AllAscii(const string& text) {
  for (int i = 0; i < text.size(); i++) {
    if ((text[i] & 0x80) != 0) {
      return false;
    }
  }
  return true;
}

string DefaultValue(const FieldDescriptor* field, bool immutable,
                    ClassNameResolver* name_resolver) {
  // Switch on CppType since we need to know which default_value_* method
  // of FieldDescriptor to call.
  switch (field->cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
      return SimpleItoa(field->default_value_int32());
    case FieldDescriptor::CPPTYPE_UINT32:
      // Need to print as a signed int since Java has no unsigned.
      return SimpleItoa(static_cast<int32>(field->default_value_uint32()));
    case FieldDescriptor::CPPTYPE_INT64:
      return SimpleItoa(field->default_value_int64()) + "L";
    case FieldDescriptor::CPPTYPE_UINT64:
      return SimpleItoa(static_cast<int64>(field->default_value_uint64())) +
             "L";
    case FieldDescriptor::CPPTYPE_DOUBLE: {
      double value = field->default_value_double();
      if (value == numeric_limits<double>::infinity()) {
        return "Double.POSITIVE_INFINITY";
      } else if (value == -numeric_limits<double>::infinity()) {
        return "Double.NEGATIVE_INFINITY";
      } else if (value != value) {
        return "Double.NaN";
      } else {
        return SimpleDtoa(value) + "D";
      }
    }
    case FieldDescriptor::CPPTYPE_FLOAT: {
      float value = field->default_value_float();
      if (value == numeric_limits<float>::infinity()) {
        return "Float.POSITIVE_INFINITY";
      } else if (value == -numeric_limits<float>::infinity()) {
        return "Float.NEGATIVE_INFINITY";
      } else if (value != value) {
        return "Float.NaN";
      } else {
        return SimpleFtoa(value) + "F";
      }
    }
    case FieldDescriptor::CPPTYPE_BOOL:
      return field->default_value_bool() ? "true" : "false";
    case FieldDescriptor::CPPTYPE_STRING:
      if (GetType(field) == FieldDescriptor::TYPE_BYTES) {
        if (field->has_default_value()) {
          // See comments in Internal.java for gory details.
          return strings::Substitute(
            "com.google.protobuf.Internal.bytesDefaultValue(\"$0\")",
            CEscape(field->default_value_string()));
        } else {
          return "com.google.protobuf.ByteString.EMPTY";
        }
      } else {
        if (AllAscii(field->default_value_string())) {
          // All chars are ASCII.  In this case CEscape() works fine.
          return "\"" + CEscape(field->default_value_string()) + "\"";
        } else {
          // See comments in Internal.java for gory details.
          return strings::Substitute(
              "com.google.protobuf.Internal.stringDefaultValue(\"$0\")",
              CEscape(field->default_value_string()));
        }
      }

    case FieldDescriptor::CPPTYPE_ENUM:
      return name_resolver->GetClassName(field->enum_type(), immutable) + "." +
          field->default_value_enum()->name();

    case FieldDescriptor::CPPTYPE_MESSAGE:
      return name_resolver->GetClassName(field->message_type(), immutable) +
          ".getDefaultInstance()";

    // No default because we want the compiler to complain if any new
    // types are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return "";
}

bool IsDefaultValueJavaDefault(const FieldDescriptor* field) {
  // Switch on CppType since we need to know which default_value_* method
  // of FieldDescriptor to call.
  switch (field->cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
      return field->default_value_int32() == 0;
    case FieldDescriptor::CPPTYPE_UINT32:
      return field->default_value_uint32() == 0;
    case FieldDescriptor::CPPTYPE_INT64:
      return field->default_value_int64() == 0L;
    case FieldDescriptor::CPPTYPE_UINT64:
      return field->default_value_uint64() == 0L;
    case FieldDescriptor::CPPTYPE_DOUBLE:
      return field->default_value_double() == 0.0;
    case FieldDescriptor::CPPTYPE_FLOAT:
      return field->default_value_float() == 0.0;
    case FieldDescriptor::CPPTYPE_BOOL:
      return field->default_value_bool() == false;

    case FieldDescriptor::CPPTYPE_STRING:
    case FieldDescriptor::CPPTYPE_ENUM:
    case FieldDescriptor::CPPTYPE_MESSAGE:
      return false;

    // No default because we want the compiler to complain if any new
    // types are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return false;
}

const char* bit_masks[] = {
  "0x00000001",
  "0x00000002",
  "0x00000004",
  "0x00000008",
  "0x00000010",
  "0x00000020",
  "0x00000040",
  "0x00000080",

  "0x00000100",
  "0x00000200",
  "0x00000400",
  "0x00000800",
  "0x00001000",
  "0x00002000",
  "0x00004000",
  "0x00008000",

  "0x00010000",
  "0x00020000",
  "0x00040000",
  "0x00080000",
  "0x00100000",
  "0x00200000",
  "0x00400000",
  "0x00800000",

  "0x01000000",
  "0x02000000",
  "0x04000000",
  "0x08000000",
  "0x10000000",
  "0x20000000",
  "0x40000000",
  "0x80000000",
};

string GetBitFieldName(int index) {
  string varName = "bitField";
  varName += SimpleItoa(index);
  varName += "_";
  return varName;
}

string GetBitFieldNameForBit(int bitIndex) {
  return GetBitFieldName(bitIndex / 32);
}

namespace {

string GenerateGetBitInternal(const string& prefix, int bitIndex) {
  string varName = prefix + GetBitFieldNameForBit(bitIndex);
  int bitInVarIndex = bitIndex % 32;

  string mask = bit_masks[bitInVarIndex];
  string result = "((" + varName + " & " + mask + ") == " + mask + ")";
  return result;
}

string GenerateSetBitInternal(const string& prefix, int bitIndex) {
  string varName = prefix + GetBitFieldNameForBit(bitIndex);
  int bitInVarIndex = bitIndex % 32;

  string mask = bit_masks[bitInVarIndex];
  string result = varName + " |= " + mask;
  return result;
}

}  // namespace

string GenerateGetBit(int bitIndex) {
  return GenerateGetBitInternal("", bitIndex);
}

string GenerateSetBit(int bitIndex) {
  return GenerateSetBitInternal("", bitIndex);
}

string GenerateClearBit(int bitIndex) {
  string varName = GetBitFieldNameForBit(bitIndex);
  int bitInVarIndex = bitIndex % 32;

  string mask = bit_masks[bitInVarIndex];
  string result = varName + " = (" + varName + " & ~" + mask + ")";
  return result;
}

string GenerateGetBitFromLocal(int bitIndex) {
  return GenerateGetBitInternal("from_", bitIndex);
}

string GenerateSetBitToLocal(int bitIndex) {
  return GenerateSetBitInternal("to_", bitIndex);
}

string GenerateGetBitMutableLocal(int bitIndex) {
  return GenerateGetBitInternal("mutable_", bitIndex);
}

string GenerateSetBitMutableLocal(int bitIndex) {
  return GenerateSetBitInternal("mutable_", bitIndex);
}

bool IsReferenceType(JavaType type) {
  switch (type) {
    case JAVATYPE_INT    : return false;
    case JAVATYPE_LONG   : return false;
    case JAVATYPE_FLOAT  : return false;
    case JAVATYPE_DOUBLE : return false;
    case JAVATYPE_BOOLEAN: return false;
    case JAVATYPE_STRING : return true;
    case JAVATYPE_BYTES  : return true;
    case JAVATYPE_ENUM   : return true;
    case JAVATYPE_MESSAGE: return true;

    // No default because we want the compiler to complain if any new
    // JavaTypes are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return false;
}

const char* GetCapitalizedType(const FieldDescriptor* field, bool immutable) {
  switch (GetType(field)) {
    case FieldDescriptor::TYPE_INT32   : return "Int32";
    case FieldDescriptor::TYPE_UINT32  : return "UInt32";
    case FieldDescriptor::TYPE_SINT32  : return "SInt32";
    case FieldDescriptor::TYPE_FIXED32 : return "Fixed32";
    case FieldDescriptor::TYPE_SFIXED32: return "SFixed32";
    case FieldDescriptor::TYPE_INT64   : return "Int64";
    case FieldDescriptor::TYPE_UINT64  : return "UInt64";
    case FieldDescriptor::TYPE_SINT64  : return "SInt64";
    case FieldDescriptor::TYPE_FIXED64 : return "Fixed64";
    case FieldDescriptor::TYPE_SFIXED64: return "SFixed64";
    case FieldDescriptor::TYPE_FLOAT   : return "Float";
    case FieldDescriptor::TYPE_DOUBLE  : return "Double";
    case FieldDescriptor::TYPE_BOOL    : return "Bool";
    case FieldDescriptor::TYPE_STRING  : return "String";
    case FieldDescriptor::TYPE_BYTES   : {
      return "Bytes";
    }
    case FieldDescriptor::TYPE_ENUM    : return "Enum";
    case FieldDescriptor::TYPE_GROUP   : return "Group";
    case FieldDescriptor::TYPE_MESSAGE : return "Message";

    // No default because we want the compiler to complain if any new
    // types are added.
  }

  GOOGLE_LOG(FATAL) << "Can't get here.";
  return NULL;
}

// For encodings with fixed sizes, returns that size in bytes.  Otherwise
// returns -1.
int FixedSize(FieldDescriptor::Type type) {
  switch (type) {
    case FieldDescriptor::TYPE_INT32   : return -1;
    case FieldDescriptor::TYPE_INT64   : return -1;
    case FieldDescriptor::TYPE_UINT32  : return -1;
    case FieldDescriptor::TYPE_UINT64  : return -1;
    case FieldDescriptor::TYPE_SINT32  : return -1;
    case FieldDescriptor::TYPE_SINT64  : return -1;
    case FieldDescriptor::TYPE_FIXED32 : return WireFormatLite::kFixed32Size;
    case FieldDescriptor::TYPE_FIXED64 : return WireFormatLite::kFixed64Size;
    case FieldDescriptor::TYPE_SFIXED32: return WireFormatLite::kSFixed32Size;
    case FieldDescriptor::TYPE_SFIXED64: return WireFormatLite::kSFixed64Size;
    case FieldDescriptor::TYPE_FLOAT   : return WireFormatLite::kFloatSize;
    case FieldDescriptor::TYPE_DOUBLE  : return WireFormatLite::kDoubleSize;

    case FieldDescriptor::TYPE_BOOL    : return WireFormatLite::kBoolSize;
    case FieldDescriptor::TYPE_ENUM    : return -1;

    case FieldDescriptor::TYPE_STRING  : return -1;
    case FieldDescriptor::TYPE_BYTES   : return -1;
    case FieldDescriptor::TYPE_GROUP   : return -1;
    case FieldDescriptor::TYPE_MESSAGE : return -1;

    // No default because we want the compiler to complain if any new
    // types are added.
  }
  GOOGLE_LOG(FATAL) << "Can't get here.";
  return -1;
}

// Sort the fields of the given Descriptor by number into a new[]'d array
// and return it. The caller should delete the returned array.
const FieldDescriptor** SortFieldsByNumber(const Descriptor* descriptor) {
  const FieldDescriptor** fields =
    new const FieldDescriptor*[descriptor->field_count()];
  for (int i = 0; i < descriptor->field_count(); i++) {
    fields[i] = descriptor->field(i);
  }
  std::sort(fields, fields + descriptor->field_count(),
            FieldOrderingByNumber());
  return fields;
}

// Returns true if the message type has any required fields.  If it doesn't,
// we can optimize out calls to its isInitialized() method.
//
// already_seen is used to avoid checking the same type multiple times
// (and also to protect against recursion).
bool HasRequiredFields(
    const Descriptor* type,
    hash_set<const Descriptor*>* already_seen) {
  if (already_seen->count(type) > 0) {
    // The type is already in cache.  This means that either:
    // a. The type has no required fields.
    // b. We are in the midst of checking if the type has required fields,
    //    somewhere up the stack.  In this case, we know that if the type
    //    has any required fields, they'll be found when we return to it,
    //    and the whole call to HasRequiredFields() will return true.
    //    Therefore, we don't have to check if this type has required fields
    //    here.
    return false;
  }
  already_seen->insert(type);

  // If the type has extensions, an extension with message type could contain
  // required fields, so we have to be conservative and assume such an
  // extension exists.
  if (type->extension_range_count() > 0) return true;

  for (int i = 0; i < type->field_count(); i++) {
    const FieldDescriptor* field = type->field(i);
    if (field->is_required()) {
      return true;
    }
    if (GetJavaType(field) == JAVATYPE_MESSAGE) {
      if (HasRequiredFields(field->message_type(), already_seen)) {
        return true;
      }
    }
  }

  return false;
}

bool HasRequiredFields(const Descriptor* type) {
  hash_set<const Descriptor*> already_seen;
  return HasRequiredFields(type, &already_seen);
}

bool HasRepeatedFields(const Descriptor* descriptor) {
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const FieldDescriptor* field = descriptor->field(i);
    if (field->is_repeated()) {
      return true;
    }
  }
  return false;
}

}  // namespace java
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
