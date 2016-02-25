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

// Author: jschorr@google.com (Joseph Schorr)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.

#include <algorithm>
#include <float.h>
#include <math.h>
#include <stdio.h>
#include <stack>
#include <limits>
#include <vector>

#include <google/protobuf/text_format.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/io/strtod.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/unknown_field_set.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/any.h>
#include <google/protobuf/stubs/stringprintf.h>
#include <google/protobuf/stubs/strutil.h>
#include <google/protobuf/stubs/map_util.h>
#include <google/protobuf/stubs/stl_util.h>

namespace google {
namespace protobuf {

namespace {

inline bool IsHexNumber(const string& str) {
  return (str.length() >= 2 && str[0] == '0' &&
          (str[1] == 'x' || str[1] == 'X'));
}

inline bool IsOctNumber(const string& str) {
  return (str.length() >= 2 && str[0] == '0' &&
          (str[1] >= '0' && str[1] < '8'));
}

inline bool GetAnyFieldDescriptors(const Message& message,
                                   const FieldDescriptor** type_url_field,
                                   const FieldDescriptor** value_field) {
    const Descriptor* descriptor = message.GetDescriptor();
    *type_url_field = descriptor->FindFieldByNumber(1);
    *value_field = descriptor->FindFieldByNumber(2);
    return (*type_url_field != NULL &&
            (*type_url_field)->type() == FieldDescriptor::TYPE_STRING &&
            *value_field != NULL &&
            (*value_field)->type() == FieldDescriptor::TYPE_BYTES);
}

}  // namespace

string Message::DebugString() const {
  string debug_string;

  TextFormat::PrintToString(*this, &debug_string);

  return debug_string;
}

string Message::ShortDebugString() const {
  string debug_string;

  TextFormat::Printer printer;
  printer.SetSingleLineMode(true);

  printer.PrintToString(*this, &debug_string);
  // Single line mode currently might have an extra space at the end.
  if (debug_string.size() > 0 &&
      debug_string[debug_string.size() - 1] == ' ') {
    debug_string.resize(debug_string.size() - 1);
  }

  return debug_string;
}

string Message::Utf8DebugString() const {
  string debug_string;

  TextFormat::Printer printer;
  printer.SetUseUtf8StringEscaping(true);

  printer.PrintToString(*this, &debug_string);

  return debug_string;
}

void Message::PrintDebugString() const {
  printf("%s", DebugString().c_str());
}


// ===========================================================================
// Implementation of the parse information tree class.
TextFormat::ParseInfoTree::ParseInfoTree() { }

TextFormat::ParseInfoTree::~ParseInfoTree() {
  // Remove any nested information trees, as they are owned by this tree.
  for (NestedMap::iterator it = nested_.begin(); it != nested_.end(); ++it) {
    STLDeleteElements(&(it->second));
  }
}

void TextFormat::ParseInfoTree::RecordLocation(
    const FieldDescriptor* field,
    TextFormat::ParseLocation location) {
  locations_[field].push_back(location);
}

TextFormat::ParseInfoTree* TextFormat::ParseInfoTree::CreateNested(
    const FieldDescriptor* field) {
  // Owned by us in the map.
  TextFormat::ParseInfoTree* instance = new TextFormat::ParseInfoTree();
  vector<TextFormat::ParseInfoTree*>* trees = &nested_[field];
  GOOGLE_CHECK(trees);
  trees->push_back(instance);
  return instance;
}

void CheckFieldIndex(const FieldDescriptor* field, int index) {
  if (field == NULL) { return; }

  if (field->is_repeated() && index == -1) {
    GOOGLE_LOG(DFATAL) << "Index must be in range of repeated field values. "
                << "Field: " << field->name();
  } else if (!field->is_repeated() && index != -1) {
    GOOGLE_LOG(DFATAL) << "Index must be -1 for singular fields."
                << "Field: " << field->name();
  }
}

TextFormat::ParseLocation TextFormat::ParseInfoTree::GetLocation(
    const FieldDescriptor* field, int index) const {
  CheckFieldIndex(field, index);
  if (index == -1) { index = 0; }

  const vector<TextFormat::ParseLocation>* locations =
      FindOrNull(locations_, field);
  if (locations == NULL || index >= locations->size()) {
    return TextFormat::ParseLocation();
  }

  return (*locations)[index];
}

TextFormat::ParseInfoTree* TextFormat::ParseInfoTree::GetTreeForNested(
    const FieldDescriptor* field, int index) const {
  CheckFieldIndex(field, index);
  if (index == -1) { index = 0; }

  const vector<TextFormat::ParseInfoTree*>* trees = FindOrNull(nested_, field);
  if (trees == NULL || index >= trees->size()) {
    return NULL;
  }

  return (*trees)[index];
}


// ===========================================================================
// Internal class for parsing an ASCII representation of a Protocol Message.
// This class makes use of the Protocol Message compiler's tokenizer found
// in //google/protobuf/io/tokenizer.h. Note that class's Parse
// method is *not* thread-safe and should only be used in a single thread at
// a time.

// Makes code slightly more readable.  The meaning of "DO(foo)" is
// "Execute foo and fail if it fails.", where failure is indicated by
// returning false. Borrowed from parser.cc (Thanks Kenton!).
#define DO(STATEMENT) if (STATEMENT) {} else return false

class TextFormat::Parser::ParserImpl {
 public:

  // Determines if repeated values for non-repeated fields and
  // oneofs are permitted, e.g., the string "foo: 1 foo: 2" for a
  // required/optional field named "foo", or "baz: 1 qux: 2"
  // where "baz" and "qux" are members of the same oneof.
  enum SingularOverwritePolicy {
    ALLOW_SINGULAR_OVERWRITES = 0,   // the last value is retained
    FORBID_SINGULAR_OVERWRITES = 1,  // an error is issued
  };

  ParserImpl(const Descriptor* root_message_type,
             io::ZeroCopyInputStream* input_stream,
             io::ErrorCollector* error_collector,
             TextFormat::Finder* finder,
             ParseInfoTree* parse_info_tree,
             SingularOverwritePolicy singular_overwrite_policy,
             bool allow_case_insensitive_field,
             bool allow_unknown_field,
             bool allow_unknown_enum,
             bool allow_field_number,
             bool allow_relaxed_whitespace)
    : error_collector_(error_collector),
      finder_(finder),
      parse_info_tree_(parse_info_tree),
      tokenizer_error_collector_(this),
      tokenizer_(input_stream, &tokenizer_error_collector_),
      root_message_type_(root_message_type),
      singular_overwrite_policy_(singular_overwrite_policy),
      allow_case_insensitive_field_(allow_case_insensitive_field),
      allow_unknown_field_(allow_unknown_field),
      allow_unknown_enum_(allow_unknown_enum),
      allow_field_number_(allow_field_number),
      had_errors_(false) {
    // For backwards-compatibility with proto1, we need to allow the 'f' suffix
    // for floats.
    tokenizer_.set_allow_f_after_float(true);

    // '#' starts a comment.
    tokenizer_.set_comment_style(io::Tokenizer::SH_COMMENT_STYLE);

    if (allow_relaxed_whitespace) {
      tokenizer_.set_require_space_after_number(false);
      tokenizer_.set_allow_multiline_strings(true);
    }

    // Consume the starting token.
    tokenizer_.Next();
  }
  ~ParserImpl() { }

  // Parses the ASCII representation specified in input and saves the
  // information into the output pointer (a Message). Returns
  // false if an error occurs (an error will also be logged to
  // GOOGLE_LOG(ERROR)).
  bool Parse(Message* output) {
    // Consume fields until we cannot do so anymore.
    while (true) {
      if (LookingAtType(io::Tokenizer::TYPE_END)) {
        return !had_errors_;
      }

      DO(ConsumeField(output));
    }
  }

  bool ParseField(const FieldDescriptor* field, Message* output) {
    bool suc;
    if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
      suc = ConsumeFieldMessage(output, output->GetReflection(), field);
    } else {
      suc = ConsumeFieldValue(output, output->GetReflection(), field);
    }
    return suc && LookingAtType(io::Tokenizer::TYPE_END);
  }

  void ReportError(int line, int col, const string& message) {
    had_errors_ = true;
    if (error_collector_ == NULL) {
      if (line >= 0) {
        GOOGLE_LOG(ERROR) << "Error parsing text-format "
                   << root_message_type_->full_name()
                   << ": " << (line + 1) << ":"
                   << (col + 1) << ": " << message;
      } else {
        GOOGLE_LOG(ERROR) << "Error parsing text-format "
                   << root_message_type_->full_name()
                   << ": " << message;
      }
    } else {
      error_collector_->AddError(line, col, message);
    }
  }

  void ReportWarning(int line, int col, const string& message) {
    if (error_collector_ == NULL) {
      if (line >= 0) {
        GOOGLE_LOG(WARNING) << "Warning parsing text-format "
                     << root_message_type_->full_name()
                     << ": " << (line + 1) << ":"
                     << (col + 1) << ": " << message;
      } else {
        GOOGLE_LOG(WARNING) << "Warning parsing text-format "
                     << root_message_type_->full_name()
                     << ": " << message;
      }
    } else {
      error_collector_->AddWarning(line, col, message);
    }
  }

 private:
  GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(ParserImpl);

  // Reports an error with the given message with information indicating
  // the position (as derived from the current token).
  void ReportError(const string& message) {
    ReportError(tokenizer_.current().line, tokenizer_.current().column,
                message);
  }

  // Reports a warning with the given message with information indicating
  // the position (as derived from the current token).
  void ReportWarning(const string& message) {
    ReportWarning(tokenizer_.current().line, tokenizer_.current().column,
                  message);
  }

  // Consumes the specified message with the given starting delimiter.
  // This method checks to see that the end delimiter at the conclusion of
  // the consumption matches the starting delimiter passed in here.
  bool ConsumeMessage(Message* message, const string delimiter) {
    while (!LookingAt(">") &&  !LookingAt("}")) {
      DO(ConsumeField(message));
    }

    // Confirm that we have a valid ending delimiter.
    DO(Consume(delimiter));
    return true;
  }

  // Consume either "<" or "{".
  bool ConsumeMessageDelimiter(string* delimiter) {
    if (TryConsume("<")) {
      *delimiter = ">";
    } else {
      DO(Consume("{"));
      *delimiter = "}";
    }
    return true;
  }


  // Consumes the current field (as returned by the tokenizer) on the
  // passed in message.
  bool ConsumeField(Message* message) {
    const Reflection* reflection = message->GetReflection();
    const Descriptor* descriptor = message->GetDescriptor();

    string field_name;

    const FieldDescriptor* field = NULL;
    int start_line = tokenizer_.current().line;
    int start_column = tokenizer_.current().column;

    const FieldDescriptor* any_type_url_field;
    const FieldDescriptor* any_value_field;
    if (internal::GetAnyFieldDescriptors(*message, &any_type_url_field,
                                         &any_value_field) &&
        TryConsume("[")) {
      string full_type_name, prefix;
      DO(ConsumeAnyTypeUrl(&full_type_name, &prefix));
      DO(Consume("]"));
      TryConsume(":");  // ':' is optional between message labels and values.
      string serialized_value;
      DO(ConsumeAnyValue(full_type_name,
                         message->GetDescriptor()->file()->pool(),
                         &serialized_value));
      reflection->SetString(
          message, any_type_url_field,
          string(prefix + full_type_name));
      reflection->SetString(message, any_value_field, serialized_value);
      return true;
    }
    if (TryConsume("[")) {
      // Extension.
      DO(ConsumeFullTypeName(&field_name));
      DO(Consume("]"));

      field = (finder_ != NULL
               ? finder_->FindExtension(message, field_name)
               : reflection->FindKnownExtensionByName(field_name));

      if (field == NULL) {
        if (!allow_unknown_field_) {
          ReportError("Extension \"" + field_name + "\" is not defined or "
                      "is not an extension of \"" +
                      descriptor->full_name() + "\".");
          return false;
        } else {
          ReportWarning("Extension \"" + field_name + "\" is not defined or "
                        "is not an extension of \"" +
                        descriptor->full_name() + "\".");
        }
      }
    } else {
      DO(ConsumeIdentifier(&field_name));

      int32 field_number;
      if (allow_field_number_ && safe_strto32(field_name, &field_number)) {
        if (descriptor->IsExtensionNumber(field_number)) {
          field = reflection->FindKnownExtensionByNumber(field_number);
        } else {
          field = descriptor->FindFieldByNumber(field_number);
        }
      } else {
        field = descriptor->FindFieldByName(field_name);
        // Group names are expected to be capitalized as they appear in the
        // .proto file, which actually matches their type names, not their
        // field names.
        if (field == NULL) {
          string lower_field_name = field_name;
          LowerString(&lower_field_name);
          field = descriptor->FindFieldByName(lower_field_name);
          // If the case-insensitive match worked but the field is NOT a group,
          if (field != NULL && field->type() != FieldDescriptor::TYPE_GROUP) {
            field = NULL;
          }
        }
        // Again, special-case group names as described above.
        if (field != NULL && field->type() == FieldDescriptor::TYPE_GROUP
            && field->message_type()->name() != field_name) {
          field = NULL;
        }

        if (field == NULL && allow_case_insensitive_field_) {
          string lower_field_name = field_name;
          LowerString(&lower_field_name);
          field = descriptor->FindFieldByLowercaseName(lower_field_name);
        }
      }

      if (field == NULL) {
        if (!allow_unknown_field_) {
          ReportError("Message type \"" + descriptor->full_name() +
                      "\" has no field named \"" + field_name + "\".");
          return false;
        } else {
          ReportWarning("Message type \"" + descriptor->full_name() +
                        "\" has no field named \"" + field_name + "\".");
        }
      }
    }

    // Skips unknown field.
    if (field == NULL) {
      GOOGLE_CHECK(allow_unknown_field_);
      // Try to guess the type of this field.
      // If this field is not a message, there should be a ":" between the
      // field name and the field value and also the field value should not
      // start with "{" or "<" which indicates the beginning of a message body.
      // If there is no ":" or there is a "{" or "<" after ":", this field has
      // to be a message or the input is ill-formed.
      if (TryConsume(":") && !LookingAt("{") && !LookingAt("<")) {
        return SkipFieldValue();
      } else {
        return SkipFieldMessage();
      }
    }

    if (singular_overwrite_policy_ == FORBID_SINGULAR_OVERWRITES) {
      // Fail if the field is not repeated and it has already been specified.
      if (!field->is_repeated() && reflection->HasField(*message, field)) {
        ReportError("Non-repeated field \"" + field_name +
                    "\" is specified multiple times.");
        return false;
      }
      // Fail if the field is a member of a oneof and another member has already
      // been specified.
      const OneofDescriptor* oneof = field->containing_oneof();
      if (oneof != NULL && reflection->HasOneof(*message, oneof)) {
        const FieldDescriptor* other_field =
            reflection->GetOneofFieldDescriptor(*message, oneof);
        ReportError("Field \"" + field_name + "\" is specified along with "
                    "field \"" + other_field->name() + "\", another member "
                    "of oneof \"" + oneof->name() + "\".");
        return false;
      }
    }

    // Perform special handling for embedded message types.
    if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
      // ':' is optional here.
      TryConsume(":");
    } else {
      // ':' is required here.
      DO(Consume(":"));
    }

    if (field->is_repeated() && TryConsume("[")) {
      // Short repeated format, e.g.  "foo: [1, 2, 3]"
      while (true) {
        if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
          // Perform special handling for embedded message types.
          DO(ConsumeFieldMessage(message, reflection, field));
        } else {
          DO(ConsumeFieldValue(message, reflection, field));
        }
        if (TryConsume("]")) {
          break;
        }
        DO(Consume(","));
      }
    } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
      DO(ConsumeFieldMessage(message, reflection, field));
    } else {
      DO(ConsumeFieldValue(message, reflection, field));
    }

    // For historical reasons, fields may optionally be separated by commas or
    // semicolons.
    TryConsume(";") || TryConsume(",");

    if (field->options().deprecated()) {
      ReportWarning("text format contains deprecated field \""
                    + field_name + "\"");
    }

    // If a parse info tree exists, add the location for the parsed
    // field.
    if (parse_info_tree_ != NULL) {
      RecordLocation(parse_info_tree_, field,
                     ParseLocation(start_line, start_column));
    }

    return true;
  }

  // Skips the next field including the field's name and value.
  bool SkipField() {
    string field_name;
    if (TryConsume("[")) {
      // Extension name.
      DO(ConsumeFullTypeName(&field_name));
      DO(Consume("]"));
    } else {
      DO(ConsumeIdentifier(&field_name));
    }

    // Try to guess the type of this field.
    // If this field is not a message, there should be a ":" between the
    // field name and the field value and also the field value should not
    // start with "{" or "<" which indicates the beginning of a message body.
    // If there is no ":" or there is a "{" or "<" after ":", this field has
    // to be a message or the input is ill-formed.
    if (TryConsume(":") && !LookingAt("{") && !LookingAt("<")) {
      DO(SkipFieldValue());
    } else {
      DO(SkipFieldMessage());
    }
    // For historical reasons, fields may optionally be separated by commas or
    // semicolons.
    TryConsume(";") || TryConsume(",");
    return true;
  }

  bool ConsumeFieldMessage(Message* message,
                           const Reflection* reflection,
                           const FieldDescriptor* field) {

    // If the parse information tree is not NULL, create a nested one
    // for the nested message.
    ParseInfoTree* parent = parse_info_tree_;
    if (parent != NULL) {
      parse_info_tree_ = CreateNested(parent, field);
    }

    string delimiter;
    DO(ConsumeMessageDelimiter(&delimiter));
    if (field->is_repeated()) {
      DO(ConsumeMessage(reflection->AddMessage(message, field), delimiter));
    } else {
      DO(ConsumeMessage(reflection->MutableMessage(message, field),
                        delimiter));
    }

    // Reset the parse information tree.
    parse_info_tree_ = parent;
    return true;
  }

  // Skips the whole body of a message including the beginning delimiter and
  // the ending delimiter.
  bool SkipFieldMessage() {
    string delimiter;
    DO(ConsumeMessageDelimiter(&delimiter));
    while (!LookingAt(">") &&  !LookingAt("}")) {
      DO(SkipField());
    }
    DO(Consume(delimiter));
    return true;
  }

  bool ConsumeFieldValue(Message* message,
                         const Reflection* reflection,
                         const FieldDescriptor* field) {

// Define an easy to use macro for setting fields. This macro checks
// to see if the field is repeated (in which case we need to use the Add
// methods or not (in which case we need to use the Set methods).
#define SET_FIELD(CPPTYPE, VALUE)                                  \
        if (field->is_repeated()) {                                \
          reflection->Add##CPPTYPE(message, field, VALUE);         \
        } else {                                                   \
          reflection->Set##CPPTYPE(message, field, VALUE);         \
        }                                                          \

    switch(field->cpp_type()) {
      case FieldDescriptor::CPPTYPE_INT32: {
        int64 value;
        DO(ConsumeSignedInteger(&value, kint32max));
        SET_FIELD(Int32, static_cast<int32>(value));
        break;
      }

      case FieldDescriptor::CPPTYPE_UINT32: {
        uint64 value;
        DO(ConsumeUnsignedInteger(&value, kuint32max));
        SET_FIELD(UInt32, static_cast<uint32>(value));
        break;
      }

      case FieldDescriptor::CPPTYPE_INT64: {
        int64 value;
        DO(ConsumeSignedInteger(&value, kint64max));
        SET_FIELD(Int64, value);
        break;
      }

      case FieldDescriptor::CPPTYPE_UINT64: {
        uint64 value;
        DO(ConsumeUnsignedInteger(&value, kuint64max));
        SET_FIELD(UInt64, value);
        break;
      }

      case FieldDescriptor::CPPTYPE_FLOAT: {
        double value;
        DO(ConsumeDouble(&value));
        SET_FIELD(Float, io::SafeDoubleToFloat(value));
        break;
      }

      case FieldDescriptor::CPPTYPE_DOUBLE: {
        double value;
        DO(ConsumeDouble(&value));
        SET_FIELD(Double, value);
        break;
      }

      case FieldDescriptor::CPPTYPE_STRING: {
        string value;
        DO(ConsumeString(&value));
        SET_FIELD(String, value);
        break;
      }

      case FieldDescriptor::CPPTYPE_BOOL: {
        if (LookingAtType(io::Tokenizer::TYPE_INTEGER)) {
          uint64 value;
          DO(ConsumeUnsignedInteger(&value, 1));
          SET_FIELD(Bool, value);
        } else {
          string value;
          DO(ConsumeIdentifier(&value));
          if (value == "true" || value == "True" || value == "t") {
            SET_FIELD(Bool, true);
          } else if (value == "false" || value == "False" || value == "f") {
            SET_FIELD(Bool, false);
          } else {
            ReportError("Invalid value for boolean field \"" + field->name()
                        + "\". Value: \"" + value  + "\".");
            return false;
          }
        }
        break;
      }

      case FieldDescriptor::CPPTYPE_ENUM: {
        string value;
        const EnumDescriptor* enum_type = field->enum_type();
        const EnumValueDescriptor* enum_value = NULL;

        if (LookingAtType(io::Tokenizer::TYPE_IDENTIFIER)) {
          DO(ConsumeIdentifier(&value));
          // Find the enumeration value.
          enum_value = enum_type->FindValueByName(value);

        } else if (LookingAt("-") ||
                   LookingAtType(io::Tokenizer::TYPE_INTEGER)) {
          int64 int_value;
          DO(ConsumeSignedInteger(&int_value, kint32max));
          value = SimpleItoa(int_value);        // for error reporting
          enum_value = enum_type->FindValueByNumber(int_value);
        } else {
          ReportError("Expected integer or identifier.");
          return false;
        }

        if (enum_value == NULL) {
          if (!allow_unknown_enum_) {
            ReportError("Unknown enumeration value of \"" + value  + "\" for "
                        "field \"" + field->name() + "\".");
            return false;
          } else {
            ReportWarning("Unknown enumeration value of \"" + value  + "\" for "
                          "field \"" + field->name() + "\".");
            return true;
          }
        }

        SET_FIELD(Enum, enum_value);
        break;
      }

      case FieldDescriptor::CPPTYPE_MESSAGE: {
        // We should never get here. Put here instead of a default
        // so that if new types are added, we get a nice compiler warning.
        GOOGLE_LOG(FATAL) << "Reached an unintended state: CPPTYPE_MESSAGE";
        break;
      }
    }
#undef SET_FIELD
    return true;
  }

  bool SkipFieldValue() {
    if (LookingAtType(io::Tokenizer::TYPE_STRING)) {
      while (LookingAtType(io::Tokenizer::TYPE_STRING)) {
        tokenizer_.Next();
      }
      return true;
    }
    // Possible field values other than string:
    //   12345        => TYPE_INTEGER
    //   -12345       => TYPE_SYMBOL + TYPE_INTEGER
    //   1.2345       => TYPE_FLOAT
    //   -1.2345      => TYPE_SYMBOL + TYPE_FLOAT
    //   inf          => TYPE_IDENTIFIER
    //   -inf         => TYPE_SYMBOL + TYPE_IDENTIFIER
    //   TYPE_INTEGER => TYPE_IDENTIFIER
    // Divides them into two group, one with TYPE_SYMBOL
    // and the other without:
    //   Group one:
    //     12345        => TYPE_INTEGER
    //     1.2345       => TYPE_FLOAT
    //     inf          => TYPE_IDENTIFIER
    //     TYPE_INTEGER => TYPE_IDENTIFIER
    //   Group two:
    //     -12345       => TYPE_SYMBOL + TYPE_INTEGER
    //     -1.2345      => TYPE_SYMBOL + TYPE_FLOAT
    //     -inf         => TYPE_SYMBOL + TYPE_IDENTIFIER
    // As we can see, the field value consists of an optional '-' and one of
    // TYPE_INTEGER, TYPE_FLOAT and TYPE_IDENTIFIER.
    bool has_minus = TryConsume("-");
    if (!LookingAtType(io::Tokenizer::TYPE_INTEGER) &&
        !LookingAtType(io::Tokenizer::TYPE_FLOAT) &&
        !LookingAtType(io::Tokenizer::TYPE_IDENTIFIER)) {
      return false;
    }
    // Combination of '-' and TYPE_IDENTIFIER may result in an invalid field
    // value while other combinations all generate valid values.
    // We check if the value of this combination is valid here.
    // TYPE_IDENTIFIER after a '-' should be one of the float values listed
    // below:
    //   inf, inff, infinity, nan
    if (has_minus && LookingAtType(io::Tokenizer::TYPE_IDENTIFIER)) {
      string text = tokenizer_.current().text;
      LowerString(&text);
      if (text != "inf" &&
          text != "infinity" &&
          text != "nan") {
        ReportError("Invalid float number: " + text);
        return false;
      }
    }
    tokenizer_.Next();
    return true;
  }

  // Returns true if the current token's text is equal to that specified.
  bool LookingAt(const string& text) {
    return tokenizer_.current().text == text;
  }

  // Returns true if the current token's type is equal to that specified.
  bool LookingAtType(io::Tokenizer::TokenType token_type) {
    return tokenizer_.current().type == token_type;
  }

  // Consumes an identifier and saves its value in the identifier parameter.
  // Returns false if the token is not of type IDENTFIER.
  bool ConsumeIdentifier(string* identifier) {
    if (LookingAtType(io::Tokenizer::TYPE_IDENTIFIER)) {
      *identifier = tokenizer_.current().text;
      tokenizer_.Next();
      return true;
    }

    // If allow_field_numer_ or allow_unknown_field_ is true, we should able
    // to parse integer identifiers.
    if ((allow_field_number_ || allow_unknown_field_)
        && LookingAtType(io::Tokenizer::TYPE_INTEGER)) {
      *identifier = tokenizer_.current().text;
      tokenizer_.Next();
      return true;
    }

    ReportError("Expected identifier.");
    return false;
  }

  // Consume a string of form "<id1>.<id2>....<idN>".
  bool ConsumeFullTypeName(string* name) {
    DO(ConsumeIdentifier(name));
    while (TryConsume(".")) {
      string part;
      DO(ConsumeIdentifier(&part));
      *name += ".";
      *name += part;
    }
    return true;
  }

  // Consumes a string and saves its value in the text parameter.
  // Returns false if the token is not of type STRING.
  bool ConsumeString(string* text) {
    if (!LookingAtType(io::Tokenizer::TYPE_STRING)) {
      ReportError("Expected string.");
      return false;
    }

    text->clear();
    while (LookingAtType(io::Tokenizer::TYPE_STRING)) {
      io::Tokenizer::ParseStringAppend(tokenizer_.current().text, text);

      tokenizer_.Next();
    }

    return true;
  }

  // Consumes a uint64 and saves its value in the value parameter.
  // Returns false if the token is not of type INTEGER.
  bool ConsumeUnsignedInteger(uint64* value, uint64 max_value) {
    if (!LookingAtType(io::Tokenizer::TYPE_INTEGER)) {
      ReportError("Expected integer.");
      return false;
    }

    if (!io::Tokenizer::ParseInteger(tokenizer_.current().text,
                                     max_value, value)) {
      ReportError("Integer out of range.");
      return false;
    }

    tokenizer_.Next();
    return true;
  }

  // Consumes an int64 and saves its value in the value parameter.
  // Note that since the tokenizer does not support negative numbers,
  // we actually may consume an additional token (for the minus sign) in this
  // method. Returns false if the token is not an integer
  // (signed or otherwise).
  bool ConsumeSignedInteger(int64* value, uint64 max_value) {
    bool negative = false;

    if (TryConsume("-")) {
      negative = true;
      // Two's complement always allows one more negative integer than
      // positive.
      ++max_value;
    }

    uint64 unsigned_value;

    DO(ConsumeUnsignedInteger(&unsigned_value, max_value));

    *value = static_cast<int64>(unsigned_value);

    if (negative) {
      *value = -*value;
    }

    return true;
  }

  // Consumes a uint64 and saves its value in the value parameter.
  // Accepts decimal numbers only, rejects hex or oct numbers.
  bool ConsumeUnsignedDecimalInteger(uint64* value, uint64 max_value) {
    if (!LookingAtType(io::Tokenizer::TYPE_INTEGER)) {
      ReportError("Expected integer.");
      return false;
    }

    const string& text = tokenizer_.current().text;
    if (IsHexNumber(text) || IsOctNumber(text)) {
      ReportError("Expect a decimal number.");
      return false;
    }

    if (!io::Tokenizer::ParseInteger(text, max_value, value)) {
      ReportError("Integer out of range.");
      return false;
    }

    tokenizer_.Next();
    return true;
  }

  // Consumes a double and saves its value in the value parameter.
  // Note that since the tokenizer does not support negative numbers,
  // we actually may consume an additional token (for the minus sign) in this
  // method. Returns false if the token is not a double
  // (signed or otherwise).
  bool ConsumeDouble(double* value) {
    bool negative = false;

    if (TryConsume("-")) {
      negative = true;
    }

    // A double can actually be an integer, according to the tokenizer.
    // Therefore, we must check both cases here.
    if (LookingAtType(io::Tokenizer::TYPE_INTEGER)) {
      // We have found an integer value for the double.
      uint64 integer_value;
      DO(ConsumeUnsignedDecimalInteger(&integer_value, kuint64max));

      *value = static_cast<double>(integer_value);
    } else if (LookingAtType(io::Tokenizer::TYPE_FLOAT)) {
      // We have found a float value for the double.
      *value = io::Tokenizer::ParseFloat(tokenizer_.current().text);

      // Mark the current token as consumed.
      tokenizer_.Next();
    } else if (LookingAtType(io::Tokenizer::TYPE_IDENTIFIER)) {
      string text = tokenizer_.current().text;
      LowerString(&text);
      if (text == "inf" ||
          text == "infinity") {
        *value = std::numeric_limits<double>::infinity();
        tokenizer_.Next();
      } else if (text == "nan") {
        *value = std::numeric_limits<double>::quiet_NaN();
        tokenizer_.Next();
      } else {
        ReportError("Expected double.");
        return false;
      }
    } else {
      ReportError("Expected double.");
      return false;
    }

    if (negative) {
      *value = -*value;
    }

    return true;
  }

  // Consumes Any::type_url value, of form "type.googleapis.com/full.type.Name"
  // or "type.googleprod.com/full.type.Name"
  bool ConsumeAnyTypeUrl(string* full_type_name, string* prefix) {
    // TODO(saito) Extend Consume() to consume multiple tokens at once, so that
    // this code can be written as just DO(Consume(kGoogleApisTypePrefix)).
    string url1, url2, url3;
    DO(ConsumeIdentifier(&url1));  // type
    DO(Consume("."));
    DO(ConsumeIdentifier(&url2));  // googleapis
    DO(Consume("."));
    DO(ConsumeIdentifier(&url3));  // com
    DO(Consume("/"));
    DO(ConsumeFullTypeName(full_type_name));

    *prefix = url1 + "." + url2 + "." + url3 + "/";
    if (*prefix != internal::kTypeGoogleApisComPrefix &&
        *prefix != internal::kTypeGoogleProdComPrefix) {
      ReportError("TextFormat::Parser for Any supports only "
                  "type.googleapis.com and type.googleprod.com, "
                  "but found \"" + *prefix + "\"");
      return false;
    }
    return true;
  }

  // A helper function for reconstructing Any::value. Consumes a text of
  // full_type_name, then serializes it into serialized_value. "pool" is used to
  // look up and create a temporary object with full_type_name.
  bool ConsumeAnyValue(const string& full_type_name, const DescriptorPool* pool,
                       string* serialized_value) {
    const Descriptor* value_descriptor =
        pool->FindMessageTypeByName(full_type_name);
    if (value_descriptor == NULL) {
      ReportError("Could not find type \"" + full_type_name +
                  "\" stored in google.protobuf.Any.");
      return false;
    }
    DynamicMessageFactory factory;
    const Message* value_prototype = factory.GetPrototype(value_descriptor);
    if (value_prototype == NULL) {
      return false;
    }
    google::protobuf::scoped_ptr<Message> value(value_prototype->New());
    string sub_delimiter;
    DO(ConsumeMessageDelimiter(&sub_delimiter));
    DO(ConsumeMessage(value.get(), sub_delimiter));

    value->AppendToString(serialized_value);
    return true;
  }

  // Consumes a token and confirms that it matches that specified in the
  // value parameter. Returns false if the token found does not match that
  // which was specified.
  bool Consume(const string& value) {
    const string& current_value = tokenizer_.current().text;

    if (current_value != value) {
      ReportError("Expected \"" + value + "\", found \"" + current_value
                  + "\".");
      return false;
    }

    tokenizer_.Next();

    return true;
  }

  // Attempts to consume the supplied value. Returns false if a the
  // token found does not match the value specified.
  bool TryConsume(const string& value) {
    if (tokenizer_.current().text == value) {
      tokenizer_.Next();
      return true;
    } else {
      return false;
    }
  }

  // An internal instance of the Tokenizer's error collector, used to
  // collect any base-level parse errors and feed them to the ParserImpl.
  class ParserErrorCollector : public io::ErrorCollector {
   public:
    explicit ParserErrorCollector(TextFormat::Parser::ParserImpl* parser) :
        parser_(parser) { }

    virtual ~ParserErrorCollector() { }

    virtual void AddError(int line, int column, const string& message) {
      parser_->ReportError(line, column, message);
    }

    virtual void AddWarning(int line, int column, const string& message) {
      parser_->ReportWarning(line, column, message);
    }

   private:
    GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(ParserErrorCollector);
    TextFormat::Parser::ParserImpl* parser_;
  };

  io::ErrorCollector* error_collector_;
  TextFormat::Finder* finder_;
  ParseInfoTree* parse_info_tree_;
  ParserErrorCollector tokenizer_error_collector_;
  io::Tokenizer tokenizer_;
  const Descriptor* root_message_type_;
  SingularOverwritePolicy singular_overwrite_policy_;
  const bool allow_case_insensitive_field_;
  const bool allow_unknown_field_;
  const bool allow_unknown_enum_;
  const bool allow_field_number_;
  bool had_errors_;
};

#undef DO

// ===========================================================================
// Internal class for writing text to the io::ZeroCopyOutputStream. Adapted
// from the Printer found in //google/protobuf/io/printer.h
class TextFormat::Printer::TextGenerator {
 public:
  explicit TextGenerator(io::ZeroCopyOutputStream* output,
                         int initial_indent_level)
    : output_(output),
      buffer_(NULL),
      buffer_size_(0),
      at_start_of_line_(true),
      failed_(false),
      indent_(""),
      initial_indent_level_(initial_indent_level) {
    indent_.resize(initial_indent_level_ * 2, ' ');
  }

  ~TextGenerator() {
    // Only BackUp() if we're sure we've successfully called Next() at least
    // once.
    if (!failed_ && buffer_size_ > 0) {
      output_->BackUp(buffer_size_);
    }
  }

  // Indent text by two spaces.  After calling Indent(), two spaces will be
  // inserted at the beginning of each line of text.  Indent() may be called
  // multiple times to produce deeper indents.
  void Indent() {
    indent_ += "  ";
  }

  // Reduces the current indent level by two spaces, or crashes if the indent
  // level is zero.
  void Outdent() {
    if (indent_.empty() ||
        indent_.size() < initial_indent_level_ * 2) {
      GOOGLE_LOG(DFATAL) << " Outdent() without matching Indent().";
      return;
    }

    indent_.resize(indent_.size() - 2);
  }

  // Print text to the output stream.
  void Print(const string& str) {
    Print(str.data(), str.size());
  }

  // Print text to the output stream.
  void Print(const char* text) {
    Print(text, strlen(text));
  }

  // Print text to the output stream.
  void Print(const char* text, int size) {
    int pos = 0;  // The number of bytes we've written so far.

    for (int i = 0; i < size; i++) {
      if (text[i] == '\n') {
        // Saw newline.  If there is more text, we may need to insert an indent
        // here.  So, write what we have so far, including the '\n'.
        Write(text + pos, i - pos + 1);
        pos = i + 1;

        // Setting this true will cause the next Write() to insert an indent
        // first.
        at_start_of_line_ = true;
      }
    }

    // Write the rest.
    Write(text + pos, size - pos);
  }

  // True if any write to the underlying stream failed.  (We don't just
  // crash in this case because this is an I/O failure, not a programming
  // error.)
  bool failed() const { return failed_; }

 private:
  GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(TextGenerator);

  void Write(const char* data, int size) {
    if (failed_) return;
    if (size == 0) return;

    if (at_start_of_line_) {
      // Insert an indent.
      at_start_of_line_ = false;
      Write(indent_.data(), indent_.size());
      if (failed_) return;
    }

    while (size > buffer_size_) {
      // Data exceeds space in the buffer.  Copy what we can and request a
      // new buffer.
      memcpy(buffer_, data, buffer_size_);
      data += buffer_size_;
      size -= buffer_size_;
      void* void_buffer;
      failed_ = !output_->Next(&void_buffer, &buffer_size_);
      if (failed_) return;
      buffer_ = reinterpret_cast<char*>(void_buffer);
    }

    // Buffer is big enough to receive the data; copy it.
    memcpy(buffer_, data, size);
    buffer_ += size;
    buffer_size_ -= size;
  }

  io::ZeroCopyOutputStream* const output_;
  char* buffer_;
  int buffer_size_;
  bool at_start_of_line_;
  bool failed_;

  string indent_;
  int initial_indent_level_;
};

// ===========================================================================

TextFormat::Finder::~Finder() {
}

TextFormat::Parser::Parser()
  : error_collector_(NULL),
    finder_(NULL),
    parse_info_tree_(NULL),
    allow_partial_(false),
    allow_case_insensitive_field_(false),
    allow_unknown_field_(false),
    allow_unknown_enum_(false),
    allow_field_number_(false),
    allow_relaxed_whitespace_(false),
    allow_singular_overwrites_(false) {
}

TextFormat::Parser::~Parser() {}

bool TextFormat::Parser::Parse(io::ZeroCopyInputStream* input,
                               Message* output) {
  output->Clear();

  ParserImpl::SingularOverwritePolicy overwrites_policy =
      allow_singular_overwrites_
      ? ParserImpl::ALLOW_SINGULAR_OVERWRITES
      : ParserImpl::FORBID_SINGULAR_OVERWRITES;

  ParserImpl parser(output->GetDescriptor(), input, error_collector_,
                    finder_, parse_info_tree_,
                    overwrites_policy,
                    allow_case_insensitive_field_, allow_unknown_field_,
                    allow_unknown_enum_, allow_field_number_,
                    allow_relaxed_whitespace_);
  return MergeUsingImpl(input, output, &parser);
}

bool TextFormat::Parser::ParseFromString(const string& input,
                                         Message* output) {
  io::ArrayInputStream input_stream(input.data(), input.size());
  return Parse(&input_stream, output);
}

bool TextFormat::Parser::Merge(io::ZeroCopyInputStream* input,
                               Message* output) {
  ParserImpl parser(output->GetDescriptor(), input, error_collector_,
                    finder_, parse_info_tree_,
                    ParserImpl::ALLOW_SINGULAR_OVERWRITES,
                    allow_case_insensitive_field_, allow_unknown_field_,
                    allow_unknown_enum_, allow_field_number_,
                    allow_relaxed_whitespace_);
  return MergeUsingImpl(input, output, &parser);
}

bool TextFormat::Parser::MergeFromString(const string& input,
                                         Message* output) {
  io::ArrayInputStream input_stream(input.data(), input.size());
  return Merge(&input_stream, output);
}

bool TextFormat::Parser::MergeUsingImpl(io::ZeroCopyInputStream* /* input */,
                                        Message* output,
                                        ParserImpl* parser_impl) {
  if (!parser_impl->Parse(output)) return false;
  if (!allow_partial_ && !output->IsInitialized()) {
    vector<string> missing_fields;
    output->FindInitializationErrors(&missing_fields);
    parser_impl->ReportError(-1, 0, "Message missing required fields: " +
                                        Join(missing_fields, ", "));
    return false;
  }
  return true;
}

bool TextFormat::Parser::ParseFieldValueFromString(
    const string& input,
    const FieldDescriptor* field,
    Message* output) {
  io::ArrayInputStream input_stream(input.data(), input.size());
  ParserImpl parser(output->GetDescriptor(), &input_stream, error_collector_,
                    finder_, parse_info_tree_,
                    ParserImpl::ALLOW_SINGULAR_OVERWRITES,
                    allow_case_insensitive_field_, allow_unknown_field_,
                    allow_unknown_enum_, allow_field_number_,
                    allow_relaxed_whitespace_);
  return parser.ParseField(field, output);
}

/* static */ bool TextFormat::Parse(io::ZeroCopyInputStream* input,
                                    Message* output) {
  return Parser().Parse(input, output);
}

/* static */ bool TextFormat::Merge(io::ZeroCopyInputStream* input,
                                    Message* output) {
  return Parser().Merge(input, output);
}

/* static */ bool TextFormat::ParseFromString(const string& input,
                                              Message* output) {
  return Parser().ParseFromString(input, output);
}

/* static */ bool TextFormat::MergeFromString(const string& input,
                                              Message* output) {
  return Parser().MergeFromString(input, output);
}

// ===========================================================================

// The default implementation for FieldValuePrinter. The base class just
// does simple formatting. That way, deriving classes could decide to fallback
// to that behavior.
TextFormat::FieldValuePrinter::FieldValuePrinter() {}
TextFormat::FieldValuePrinter::~FieldValuePrinter() {}
string TextFormat::FieldValuePrinter::PrintBool(bool val) const {
  return val ? "true" : "false";
}
string TextFormat::FieldValuePrinter::PrintInt32(int32 val) const {
  return SimpleItoa(val);
}
string TextFormat::FieldValuePrinter::PrintUInt32(uint32 val) const {
  return SimpleItoa(val);
}
string TextFormat::FieldValuePrinter::PrintInt64(int64 val) const {
  return SimpleItoa(val);
}
string TextFormat::FieldValuePrinter::PrintUInt64(uint64 val) const {
  return SimpleItoa(val);
}
string TextFormat::FieldValuePrinter::PrintFloat(float val) const {
  return SimpleFtoa(val);
}
string TextFormat::FieldValuePrinter::PrintDouble(double val) const {
  return SimpleDtoa(val);
}
string TextFormat::FieldValuePrinter::PrintString(const string& val) const {
  string printed("\"");
  CEscapeAndAppend(val, &printed);
  printed.push_back('\"');
  return printed;
}
string TextFormat::FieldValuePrinter::PrintBytes(const string& val) const {
  return PrintString(val);
}
string TextFormat::FieldValuePrinter::PrintEnum(int32 val,
                                                const string& name) const {
  return name;
}
string TextFormat::FieldValuePrinter::PrintFieldName(
    const Message& message,
    const Reflection* reflection,
    const FieldDescriptor* field) const {
  if (field->is_extension()) {
    // We special-case MessageSet elements for compatibility with proto1.
    if (field->containing_type()->options().message_set_wire_format()
        && field->type() == FieldDescriptor::TYPE_MESSAGE
        && field->is_optional()
        && field->extension_scope() == field->message_type()) {
      return StrCat("[", field->message_type()->full_name(), "]");
    } else {
      return StrCat("[", field->full_name(), "]");
    }
  } else if (field->type() == FieldDescriptor::TYPE_GROUP) {
    // Groups must be serialized with their original capitalization.
    return field->message_type()->name();
  } else {
    return field->name();
  }
}
string TextFormat::FieldValuePrinter::PrintMessageStart(
    const Message& message,
    int field_index,
    int field_count,
    bool single_line_mode) const {
  return single_line_mode ? " { " : " {\n";
}
string TextFormat::FieldValuePrinter::PrintMessageEnd(
    const Message& message,
    int field_index,
    int field_count,
    bool single_line_mode) const {
  return single_line_mode ? "} " : "}\n";
}

namespace {
// Our own specialization: for UTF8 escaped strings.
class FieldValuePrinterUtf8Escaping : public TextFormat::FieldValuePrinter {
 public:
  virtual string PrintString(const string& val) const {
    return StrCat("\"", strings::Utf8SafeCEscape(val), "\"");
  }
  virtual string PrintBytes(const string& val) const {
    return TextFormat::FieldValuePrinter::PrintString(val);
  }
};

}  // namespace

TextFormat::Printer::Printer()
  : initial_indent_level_(0),
    single_line_mode_(false),
    use_field_number_(false),
    use_short_repeated_primitives_(false),
    hide_unknown_fields_(false),
    print_message_fields_in_index_order_(false),
    expand_any_(false),
    truncate_string_field_longer_than_(0LL) {
  SetUseUtf8StringEscaping(false);
}

TextFormat::Printer::~Printer() {
  STLDeleteValues(&custom_printers_);
}

void TextFormat::Printer::SetUseUtf8StringEscaping(bool as_utf8) {
  SetDefaultFieldValuePrinter(as_utf8
                              ? new FieldValuePrinterUtf8Escaping()
                              : new FieldValuePrinter());
}

void TextFormat::Printer::SetDefaultFieldValuePrinter(
    const FieldValuePrinter* printer) {
  default_field_value_printer_.reset(printer);
}

bool TextFormat::Printer::RegisterFieldValuePrinter(
    const FieldDescriptor* field,
    const FieldValuePrinter* printer) {
  return field != NULL && printer != NULL &&
         custom_printers_.insert(std::make_pair(field, printer)).second;
}

bool TextFormat::Printer::PrintToString(const Message& message,
                                        string* output) const {
  GOOGLE_DCHECK(output) << "output specified is NULL";

  output->clear();
  io::StringOutputStream output_stream(output);

  return Print(message, &output_stream);
}

bool TextFormat::Printer::PrintUnknownFieldsToString(
    const UnknownFieldSet& unknown_fields,
    string* output) const {
  GOOGLE_DCHECK(output) << "output specified is NULL";

  output->clear();
  io::StringOutputStream output_stream(output);
  return PrintUnknownFields(unknown_fields, &output_stream);
}

bool TextFormat::Printer::Print(const Message& message,
                                io::ZeroCopyOutputStream* output) const {
  TextGenerator generator(output, initial_indent_level_);

  Print(message, generator);

  // Output false if the generator failed internally.
  return !generator.failed();
}

bool TextFormat::Printer::PrintUnknownFields(
    const UnknownFieldSet& unknown_fields,
    io::ZeroCopyOutputStream* output) const {
  TextGenerator generator(output, initial_indent_level_);

  PrintUnknownFields(unknown_fields, generator);

  // Output false if the generator failed internally.
  return !generator.failed();
}

namespace {
// Comparison functor for sorting FieldDescriptors by field index.
struct FieldIndexSorter {
  bool operator()(const FieldDescriptor* left,
                  const FieldDescriptor* right) const {
    return left->index() < right->index();
  }
};

}  // namespace

bool TextFormat::Printer::PrintAny(const Message& message,
                                   TextGenerator& generator) const {
  const FieldDescriptor* type_url_field;
  const FieldDescriptor* value_field;
  if (!internal::GetAnyFieldDescriptors(message, &type_url_field,
                                        &value_field)) {
    return false;
  }

  const Reflection* reflection = message.GetReflection();

  // Extract the full type name from the type_url field.
  const string& type_url = reflection->GetString(message, type_url_field);
  string full_type_name;
  if (!internal::ParseAnyTypeUrl(type_url, &full_type_name)) {
    return false;
  }

  // Print the "value" in text.
  const google::protobuf::Descriptor* value_descriptor =
      message.GetDescriptor()->file()->pool()->FindMessageTypeByName(
          full_type_name);
  if (value_descriptor == NULL) {
    GOOGLE_LOG(WARNING) << "Proto type " << type_url << " not found";
    return false;
  }
  DynamicMessageFactory factory;
  google::protobuf::scoped_ptr<google::protobuf::Message> value_message(
      factory.GetPrototype(value_descriptor)->New());
  string serialized_value = reflection->GetString(message, value_field);
  if (!value_message->ParseFromString(serialized_value)) {
    GOOGLE_LOG(WARNING) << type_url << ": failed to parse contents";
    return false;
  }
  generator.Print(StrCat("[", type_url, "]"));
  const FieldValuePrinter* printer = FindWithDefault(
      custom_printers_, value_field, default_field_value_printer_.get());
  generator.Print(
      printer->PrintMessageStart(message, -1, 0, single_line_mode_));
  generator.Indent();
  Print(*value_message, generator);
  generator.Outdent();
  generator.Print(printer->PrintMessageEnd(message, -1, 0, single_line_mode_));
  return true;
}

void TextFormat::Printer::Print(const Message& message,
                                TextGenerator& generator) const {
  const Descriptor* descriptor = message.GetDescriptor();
  const Reflection* reflection = message.GetReflection();
  if (descriptor->full_name() == internal::kAnyFullTypeName && expand_any_ &&
      PrintAny(message, generator)) {
    return;
  }
  vector<const FieldDescriptor*> fields;
  reflection->ListFields(message, &fields);
  if (print_message_fields_in_index_order_) {
    std::sort(fields.begin(), fields.end(), FieldIndexSorter());
  }
  for (int i = 0; i < fields.size(); i++) {
    PrintField(message, reflection, fields[i], generator);
  }
  if (!hide_unknown_fields_) {
    PrintUnknownFields(reflection->GetUnknownFields(message), generator);
  }
}

void TextFormat::Printer::PrintFieldValueToString(
    const Message& message,
    const FieldDescriptor* field,
    int index,
    string* output) const {

  GOOGLE_DCHECK(output) << "output specified is NULL";

  output->clear();
  io::StringOutputStream output_stream(output);
  TextGenerator generator(&output_stream, initial_indent_level_);

  PrintFieldValue(message, message.GetReflection(), field, index, generator);
}

class MapEntryMessageComparator {
 public:
  explicit MapEntryMessageComparator(const Descriptor* descriptor)
      : field_(descriptor->field(0)) {}

  bool operator()(const Message* a, const Message* b) {
    const Reflection* reflection = a->GetReflection();
    switch (field_->cpp_type()) {
      case FieldDescriptor::CPPTYPE_BOOL: {
          bool first = reflection->GetBool(*a, field_);
          bool second = reflection->GetBool(*b, field_);
          return first < second;
      }
      case FieldDescriptor::CPPTYPE_INT32: {
          int32 first = reflection->GetInt32(*a, field_);
          int32 second = reflection->GetInt32(*b, field_);
          return first < second;
      }
      case FieldDescriptor::CPPTYPE_INT64: {
          int64 first = reflection->GetInt64(*a, field_);
          int64 second = reflection->GetInt64(*b, field_);
          return first < second;
      }
      case FieldDescriptor::CPPTYPE_UINT32: {
          uint32 first = reflection->GetUInt32(*a, field_);
          uint32 second = reflection->GetUInt32(*b, field_);
          return first < second;
      }
      case FieldDescriptor::CPPTYPE_UINT64: {
          uint64 first = reflection->GetUInt64(*a, field_);
          uint64 second = reflection->GetUInt64(*b, field_);
          return first < second;
      }
      case FieldDescriptor::CPPTYPE_STRING: {
          string first = reflection->GetString(*a, field_);
          string second = reflection->GetString(*b, field_);
          return first < second;
      }
      default:
        GOOGLE_LOG(DFATAL) << "Invalid key for map field.";
        return true;
    }
  }

 private:
  const FieldDescriptor* field_;
};

void TextFormat::Printer::PrintField(const Message& message,
                                     const Reflection* reflection,
                                     const FieldDescriptor* field,
                                     TextGenerator& generator) const {
  if (use_short_repeated_primitives_ &&
      field->is_repeated() &&
      field->cpp_type() != FieldDescriptor::CPPTYPE_STRING &&
      field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
    PrintShortRepeatedField(message, reflection, field, generator);
    return;
  }

  int count = 0;

  if (field->is_repeated()) {
    count = reflection->FieldSize(message, field);
  } else if (reflection->HasField(message, field)) {
    count = 1;
  }

  std::vector<const Message*> sorted_map_field;
  if (field->is_map()) {
    const RepeatedPtrField<Message>& map_field =
        reflection->GetRepeatedPtrField<Message>(message, field);
    for (RepeatedPtrField<Message>::const_pointer_iterator it =
             map_field.pointer_begin();
         it != map_field.pointer_end(); ++it) {
      sorted_map_field.push_back(*it);
    }

    MapEntryMessageComparator comparator(field->message_type());
    std::stable_sort(sorted_map_field.begin(), sorted_map_field.end(),
                     comparator);
  }

  for (int j = 0; j < count; ++j) {
    const int field_index = field->is_repeated() ? j : -1;

    PrintFieldName(message, reflection, field, generator);

    if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
      const FieldValuePrinter* printer = FindWithDefault(
          custom_printers_, field, default_field_value_printer_.get());
      const Message& sub_message =
          field->is_repeated()
              ? (field->is_map()
                     ? *sorted_map_field[j]
                     : reflection->GetRepeatedMessage(message, field, j))
              : reflection->GetMessage(message, field);
      generator.Print(
          printer->PrintMessageStart(
              sub_message, field_index, count, single_line_mode_));
      generator.Indent();
      Print(sub_message, generator);
      generator.Outdent();
      generator.Print(
          printer->PrintMessageEnd(
              sub_message, field_index, count, single_line_mode_));
    } else {
      generator.Print(": ");
      // Write the field value.
      PrintFieldValue(message, reflection, field, field_index, generator);
      if (single_line_mode_) {
        generator.Print(" ");
      } else {
        generator.Print("\n");
      }
    }
  }
}

void TextFormat::Printer::PrintShortRepeatedField(
    const Message& message,
    const Reflection* reflection,
    const FieldDescriptor* field,
    TextGenerator& generator) const {
  // Print primitive repeated field in short form.
  PrintFieldName(message, reflection, field, generator);

  int size = reflection->FieldSize(message, field);
  generator.Print(": [");
  for (int i = 0; i < size; i++) {
    if (i > 0) generator.Print(", ");
    PrintFieldValue(message, reflection, field, i, generator);
  }
  if (single_line_mode_) {
    generator.Print("] ");
  } else {
    generator.Print("]\n");
  }
}

void TextFormat::Printer::PrintFieldName(const Message& message,
                                         const Reflection* reflection,
                                         const FieldDescriptor* field,
                                         TextGenerator& generator) const {
  // if use_field_number_ is true, prints field number instead
  // of field name.
  if (use_field_number_) {
    generator.Print(SimpleItoa(field->number()));
    return;
  }

  const FieldValuePrinter* printer = FindWithDefault(
      custom_printers_, field, default_field_value_printer_.get());
  generator.Print(printer->PrintFieldName(message, reflection, field));
}

void TextFormat::Printer::PrintFieldValue(
    const Message& message,
    const Reflection* reflection,
    const FieldDescriptor* field,
    int index,
    TextGenerator& generator) const {
  GOOGLE_DCHECK(field->is_repeated() || (index == -1))
      << "Index must be -1 for non-repeated fields";

  const FieldValuePrinter* printer
      = FindWithDefault(custom_printers_, field,
                        default_field_value_printer_.get());

  switch (field->cpp_type()) {
#define OUTPUT_FIELD(CPPTYPE, METHOD)                                   \
    case FieldDescriptor::CPPTYPE_##CPPTYPE:                            \
      generator.Print(printer->Print##METHOD(field->is_repeated()       \
               ? reflection->GetRepeated##METHOD(message, field, index) \
               : reflection->Get##METHOD(message, field)));             \
        break

    OUTPUT_FIELD( INT32,  Int32);
    OUTPUT_FIELD( INT64,  Int64);
    OUTPUT_FIELD(UINT32, UInt32);
    OUTPUT_FIELD(UINT64, UInt64);
    OUTPUT_FIELD( FLOAT,  Float);
    OUTPUT_FIELD(DOUBLE, Double);
    OUTPUT_FIELD(  BOOL,   Bool);
#undef OUTPUT_FIELD

    case FieldDescriptor::CPPTYPE_STRING: {
      string scratch;
      const string& value = field->is_repeated()
          ? reflection->GetRepeatedStringReference(
              message, field, index, &scratch)
          : reflection->GetStringReference(message, field, &scratch);
      int64 size = value.size();
      if (truncate_string_field_longer_than_ > 0) {
        size = std::min(truncate_string_field_longer_than_,
                        static_cast<int64>(value.size()));
      }
      string truncated_value(value.substr(0, size) + "...<truncated>...");
      const string* value_to_print = &value;
      if (size < value.size()) {
        value_to_print = &truncated_value;
      }
      if (field->type() == FieldDescriptor::TYPE_STRING) {
        generator.Print(printer->PrintString(*value_to_print));
      } else {
        GOOGLE_DCHECK_EQ(field->type(), FieldDescriptor::TYPE_BYTES);
        generator.Print(printer->PrintBytes(*value_to_print));
      }
      break;
    }

    case FieldDescriptor::CPPTYPE_ENUM: {
      int enum_value = field->is_repeated()
          ? reflection->GetRepeatedEnumValue(message, field, index)
          : reflection->GetEnumValue(message, field);
      const EnumValueDescriptor* enum_desc =
          field->enum_type()->FindValueByNumber(enum_value);
      if (enum_desc != NULL) {
        generator.Print(printer->PrintEnum(enum_value, enum_desc->name()));
      } else {
        // Ordinarily, enum_desc should not be null, because proto2 has the
        // invariant that set enum field values must be in-range, but with the
        // new integer-based API for enums (or the RepeatedField<int> loophole),
        // it is possible for the user to force an unknown integer value.  So we
        // simply use the integer value itself as the enum value name in this
        // case.
        generator.Print(printer->PrintEnum(enum_value,
                                           StringPrintf("%d", enum_value)));
      }
      break;
    }

    case FieldDescriptor::CPPTYPE_MESSAGE:
      Print(field->is_repeated()
            ? reflection->GetRepeatedMessage(message, field, index)
            : reflection->GetMessage(message, field),
            generator);
      break;
  }
}

/* static */ bool TextFormat::Print(const Message& message,
                                    io::ZeroCopyOutputStream* output) {
  return Printer().Print(message, output);
}

/* static */ bool TextFormat::PrintUnknownFields(
    const UnknownFieldSet& unknown_fields,
    io::ZeroCopyOutputStream* output) {
  return Printer().PrintUnknownFields(unknown_fields, output);
}

/* static */ bool TextFormat::PrintToString(
    const Message& message, string* output) {
  return Printer().PrintToString(message, output);
}

/* static */ bool TextFormat::PrintUnknownFieldsToString(
    const UnknownFieldSet& unknown_fields, string* output) {
  return Printer().PrintUnknownFieldsToString(unknown_fields, output);
}

/* static */ void TextFormat::PrintFieldValueToString(
    const Message& message,
    const FieldDescriptor* field,
    int index,
    string* output) {
  return Printer().PrintFieldValueToString(message, field, index, output);
}

/* static */ bool TextFormat::ParseFieldValueFromString(
    const string& input,
    const FieldDescriptor* field,
    Message* message) {
  return Parser().ParseFieldValueFromString(input, field, message);
}

// Prints an integer as hex with a fixed number of digits dependent on the
// integer type.
template<typename IntType>
static string PaddedHex(IntType value) {
  string result;
  result.reserve(sizeof(value) * 2);
  for (int i = sizeof(value) * 2 - 1; i >= 0; i--) {
    result.push_back(int_to_hex_digit(value >> (i*4) & 0x0F));
  }
  return result;
}

void TextFormat::Printer::PrintUnknownFields(
    const UnknownFieldSet& unknown_fields, TextGenerator& generator) const {
  for (int i = 0; i < unknown_fields.field_count(); i++) {
    const UnknownField& field = unknown_fields.field(i);
    string field_number = SimpleItoa(field.number());

    switch (field.type()) {
      case UnknownField::TYPE_VARINT:
        generator.Print(field_number);
        generator.Print(": ");
        generator.Print(SimpleItoa(field.varint()));
        if (single_line_mode_) {
          generator.Print(" ");
        } else {
          generator.Print("\n");
        }
        break;
      case UnknownField::TYPE_FIXED32: {
        generator.Print(field_number);
        generator.Print(": 0x");
        generator.Print(
            StrCat(strings::Hex(field.fixed32(), strings::ZERO_PAD_8)));
        if (single_line_mode_) {
          generator.Print(" ");
        } else {
          generator.Print("\n");
        }
        break;
      }
      case UnknownField::TYPE_FIXED64: {
        generator.Print(field_number);
        generator.Print(": 0x");
        generator.Print(
            StrCat(strings::Hex(field.fixed64(), strings::ZERO_PAD_16)));
        if (single_line_mode_) {
          generator.Print(" ");
        } else {
          generator.Print("\n");
        }
        break;
      }
      case UnknownField::TYPE_LENGTH_DELIMITED: {
        generator.Print(field_number);
        const string& value = field.length_delimited();
        UnknownFieldSet embedded_unknown_fields;
        if (!value.empty() && embedded_unknown_fields.ParseFromString(value)) {
          // This field is parseable as a Message.
          // So it is probably an embedded message.
          if (single_line_mode_) {
            generator.Print(" { ");
          } else {
            generator.Print(" {\n");
            generator.Indent();
          }
          PrintUnknownFields(embedded_unknown_fields, generator);
          if (single_line_mode_) {
            generator.Print("} ");
          } else {
            generator.Outdent();
            generator.Print("}\n");
          }
        } else {
          // This field is not parseable as a Message.
          // So it is probably just a plain string.
          string printed(": \"");
          CEscapeAndAppend(value, &printed);
          printed.append(single_line_mode_ ? "\" " : "\"\n");
          generator.Print(printed);
        }
        break;
      }
      case UnknownField::TYPE_GROUP:
        generator.Print(field_number);
        if (single_line_mode_) {
          generator.Print(" { ");
        } else {
          generator.Print(" {\n");
          generator.Indent();
        }
        PrintUnknownFields(field.group(), generator);
        if (single_line_mode_) {
          generator.Print("} ");
        } else {
          generator.Outdent();
          generator.Print("}\n");
        }
        break;
    }
  }
}

}  // namespace protobuf
}  // namespace google
