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

#include <google/protobuf/util/internal/protostream_objectwriter.h>

#include <functional>
#include <stack>

#include <google/protobuf/stubs/once.h>
#include <google/protobuf/stubs/time.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/util/internal/field_mask_utility.h>
#include <google/protobuf/util/internal/object_location_tracker.h>
#include <google/protobuf/util/internal/constants.h>
#include <google/protobuf/util/internal/utility.h>
#include <google/protobuf/stubs/strutil.h>
#include <google/protobuf/stubs/map_util.h>
#include <google/protobuf/stubs/statusor.h>


namespace google {
namespace protobuf {
namespace util {
namespace converter {

using google::protobuf::internal::WireFormatLite;
using util::error::INVALID_ARGUMENT;
using util::Status;
using util::StatusOr;


ProtoStreamObjectWriter::ProtoStreamObjectWriter(
    TypeResolver* type_resolver, const google::protobuf::Type& type,
    strings::ByteSink* output, ErrorListener* listener)
    : ProtoWriter(type_resolver, type, output, listener),
      master_type_(type),
      current_(NULL) {}

ProtoStreamObjectWriter::ProtoStreamObjectWriter(
    const TypeInfo* typeinfo, const google::protobuf::Type& type,
    strings::ByteSink* output, ErrorListener* listener)
    : ProtoWriter(typeinfo, type, output, listener),
      master_type_(type),
      current_(NULL) {}

ProtoStreamObjectWriter::~ProtoStreamObjectWriter() {
  if (current_ == NULL) return;
  // Cleanup explicitly in order to avoid destructor stack overflow when input
  // is deeply nested.
  // Cast to BaseElement to avoid doing additional checks (like missing fields)
  // during pop().
  google::protobuf::scoped_ptr<BaseElement> element(
      static_cast<BaseElement*>(current_.get())->pop<BaseElement>());
  while (element != NULL) {
    element.reset(element->pop<BaseElement>());
  }
}

namespace {
// Utility method to split a string representation of Timestamp or Duration and
// return the parts.
void SplitSecondsAndNanos(StringPiece input, StringPiece* seconds,
                          StringPiece* nanos) {
  size_t idx = input.rfind('.');
  if (idx != string::npos) {
    *seconds = input.substr(0, idx);
    *nanos = input.substr(idx + 1);
  } else {
    *seconds = input;
    *nanos = StringPiece();
  }
}

Status GetNanosFromStringPiece(StringPiece s_nanos,
                               const char* parse_failure_message,
                               const char* exceeded_limit_message,
                               int32* nanos) {
  *nanos = 0;

  // Count the number of leading 0s and consume them.
  int num_leading_zeros = 0;
  while (s_nanos.Consume("0")) {
    num_leading_zeros++;
  }
  int32 i_nanos = 0;
  // 's_nanos' contains fractional seconds -- i.e. 'nanos' is equal to
  // "0." + s_nanos.ToString() seconds. An int32 is used for the
  // conversion to 'nanos', rather than a double, so that there is no
  // loss of precision.
  if (!s_nanos.empty() && !safe_strto32(s_nanos.ToString(), &i_nanos)) {
    return Status(INVALID_ARGUMENT, parse_failure_message);
  }
  if (i_nanos > kNanosPerSecond || i_nanos < 0) {
    return Status(INVALID_ARGUMENT, exceeded_limit_message);
  }
  // s_nanos should only have digits. No whitespace.
  if (s_nanos.find_first_not_of("0123456789") != StringPiece::npos) {
    return Status(INVALID_ARGUMENT, parse_failure_message);
  }

  if (i_nanos > 0) {
    // 'scale' is the number of digits to the right of the decimal
    // point in "0." + s_nanos.ToString()
    int32 scale = num_leading_zeros + s_nanos.size();
    // 'conversion' converts i_nanos into nanoseconds.
    // conversion = kNanosPerSecond / static_cast<int32>(std::pow(10, scale))
    // For efficiency, we precompute the conversion factor.
    int32 conversion = 0;
    switch (scale) {
      case 1:
        conversion = 100000000;
        break;
      case 2:
        conversion = 10000000;
        break;
      case 3:
        conversion = 1000000;
        break;
      case 4:
        conversion = 100000;
        break;
      case 5:
        conversion = 10000;
        break;
      case 6:
        conversion = 1000;
        break;
      case 7:
        conversion = 100;
        break;
      case 8:
        conversion = 10;
        break;
      case 9:
        conversion = 1;
        break;
      default:
        return Status(INVALID_ARGUMENT, exceeded_limit_message);
    }
    *nanos = i_nanos * conversion;
  }

  return Status::OK;
}

}  // namespace

ProtoStreamObjectWriter::AnyWriter::AnyWriter(ProtoStreamObjectWriter* parent)
    : parent_(parent),
      ow_(),
      invalid_(false),
      data_(),
      output_(&data_),
      depth_(0),
      has_injected_value_message_(false) {}

ProtoStreamObjectWriter::AnyWriter::~AnyWriter() {}

void ProtoStreamObjectWriter::AnyWriter::StartObject(StringPiece name) {
  ++depth_;
  // If an object writer is absent, that means we have not called StartAny()
  // before reaching here. This is an invalid state. StartAny() gets called
  // whenever we see an "@type" being rendered (see AnyWriter::RenderDataPiece).
  if (ow_ == NULL) {
    // Make sure we are not already in an invalid state. This avoids making
    // multiple unnecessary InvalidValue calls.
    if (!invalid_) {
      parent_->InvalidValue("Any",
                            StrCat("Missing or invalid @type for any field in ",
                                   parent_->master_type_.name()));
      invalid_ = true;
    }
  } else if (!has_injected_value_message_ || depth_ != 1 || name != "value") {
    // We don't propagate to ow_ StartObject("value") calls for nested Anys or
    // Struct at depth 1 as they are nested one level deep with an injected
    // "value" field.
    ow_->StartObject(name);
  }
}

bool ProtoStreamObjectWriter::AnyWriter::EndObject() {
  --depth_;
  // As long as depth_ >= 0, we know we haven't reached the end of Any.
  // Propagate these EndObject() calls to the contained ow_.  If we are in a
  // nested Any or Struct type, ignore the second to last EndObject call (depth_
  // == -1)
  if (ow_ != NULL && (!has_injected_value_message_ || depth_ >= 0)) {
    ow_->EndObject();
  }
  // A negative depth_ implies that we have reached the end of Any
  // object. Now we write out its contents.
  if (depth_ < 0) {
    WriteAny();
    return false;
  }
  return true;
}

void ProtoStreamObjectWriter::AnyWriter::StartList(StringPiece name) {
  ++depth_;
  // We expect ow_ to be present as this call only makes sense inside an Any.
  if (ow_ == NULL) {
    if (!invalid_) {
      parent_->InvalidValue("Any",
                            StrCat("Missing or invalid @type for any field in ",
                                   parent_->master_type_.name()));
      invalid_ = true;
    }
  } else {
    ow_->StartList(name);
  }
}

void ProtoStreamObjectWriter::AnyWriter::EndList() {
  --depth_;
  if (depth_ < 0) {
    GOOGLE_LOG(DFATAL) << "Mismatched EndList found, should not be possible";
    depth_ = 0;
  }
  // We don't write an error on the close, only on the open
  if (ow_ != NULL) {
    ow_->EndList();
  }
}

void ProtoStreamObjectWriter::AnyWriter::RenderDataPiece(
    StringPiece name, const DataPiece& value) {
  // Start an Any only at depth_ 0. Other RenderDataPiece calls with "@type"
  // should go to the contained ow_ as they indicate nested Anys.
  if (depth_ == 0 && ow_ == NULL && name == "@type") {
    StartAny(value);
  } else if (ow_ == NULL) {
    if (!invalid_) {
      parent_->InvalidValue("Any",
                            StrCat("Missing or invalid @type for any field in ",
                                   parent_->master_type_.name()));
      invalid_ = true;
    }
  } else {
    // Check to see if the data needs to be rendered with well-known-type
    // renderer.
    const TypeRenderer* type_renderer =
        FindTypeRenderer(GetFullTypeWithUrl(ow_->master_type_.name()));
    if (type_renderer) {
      Status status = (*type_renderer)(ow_.get(), value);
      if (!status.ok()) ow_->InvalidValue("Any", status.error_message());
    } else {
      ow_->RenderDataPiece(name, value);
    }
  }
}

void ProtoStreamObjectWriter::AnyWriter::StartAny(const DataPiece& value) {
  // Figure out the type url. This is a copy-paste from WriteString but we also
  // need the value, so we can't just call through to that.
  if (value.type() == DataPiece::TYPE_STRING) {
    type_url_ = value.str().ToString();
  } else {
    StatusOr<string> s = value.ToString();
    if (!s.ok()) {
      parent_->InvalidValue("String", s.status().error_message());
      invalid_ = true;
      return;
    }
    type_url_ = s.ValueOrDie();
  }
  // Resolve the type url, and report an error if we failed to resolve it.
  StatusOr<const google::protobuf::Type*> resolved_type =
      parent_->typeinfo()->ResolveTypeUrl(type_url_);
  if (!resolved_type.ok()) {
    parent_->InvalidValue("Any", resolved_type.status().error_message());
    invalid_ = true;
    return;
  }
  // At this point, type is never null.
  const google::protobuf::Type* type = resolved_type.ValueOrDie();

  // If this is the case of an Any in an Any or Struct in an Any, we need to
  // expect a StartObject call with "value" while we're at depth_ 0, which we
  // should ignore (not propagate to our nested object writer). We also need to
  // ignore the second-to-last EndObject call, and not propagate that either.
  if (type->name() == kAnyType || type->name() == kStructType) {
    has_injected_value_message_ = true;
  }

  // Create our object writer and initialize it with the first StartObject
  // call.
  ow_.reset(new ProtoStreamObjectWriter(parent_->typeinfo(), *type, &output_,
                                        parent_->listener()));
  ow_->StartObject("");
}

void ProtoStreamObjectWriter::AnyWriter::WriteAny() {
  if (ow_ == NULL) {
    // If we had no object writer, we never got any content, so just return
    // immediately, which is equivalent to writing an empty Any.
    return;
  }
  // Render the type_url and value fields directly to the stream.
  // type_url has tag 1 and value has tag 2.
  WireFormatLite::WriteString(1, type_url_, parent_->stream());
  if (!data_.empty()) {
    WireFormatLite::WriteBytes(2, data_, parent_->stream());
  }
}

ProtoStreamObjectWriter::Item::Item(ProtoStreamObjectWriter* enclosing,
                                    ItemType item_type, bool is_placeholder,
                                    bool is_list)
    : BaseElement(NULL),
      ow_(enclosing),
      any_(),
      item_type_(item_type),
      is_placeholder_(is_placeholder),
      is_list_(is_list) {
  if (item_type_ == ANY) {
    any_.reset(new AnyWriter(ow_));
  }
}

ProtoStreamObjectWriter::Item::Item(ProtoStreamObjectWriter::Item* parent,
                                    ItemType item_type, bool is_placeholder,
                                    bool is_list)
    : BaseElement(parent),
      ow_(this->parent()->ow_),
      any_(),
      item_type_(item_type),
      is_placeholder_(is_placeholder),
      is_list_(is_list) {
  if (item_type == ANY) {
    any_.reset(new AnyWriter(ow_));
  }
}

bool ProtoStreamObjectWriter::Item::InsertMapKeyIfNotPresent(
    StringPiece map_key) {
  return InsertIfNotPresent(&map_keys_, map_key.ToString());
}

ProtoStreamObjectWriter* ProtoStreamObjectWriter::StartObject(
    StringPiece name) {
  if (invalid_depth() > 0) {
    IncrementInvalidDepth();
    return this;
  }

  // Starting the root message. Create the root Item and return.
  // ANY message type does not need special handling, just set the ItemType
  // to ANY.
  if (current_ == NULL) {
    ProtoWriter::StartObject(name);
    current_.reset(new Item(
        this, master_type_.name() == kAnyType ? Item::ANY : Item::MESSAGE,
        false, false));

    // If master type is a special type that needs extra values to be written to
    // stream, we write those values.
    if (master_type_.name() == kStructType) {
      // Struct has a map<string, Value> field called "fields".
      // https://github.com/google/protobuf/blob/master/src/google/protobuf/struct.proto
      // "fields": [
      Push("fields", Item::MAP, true, true);
      return this;
    }

    if (master_type_.name() == kStructValueType) {
      // We got a StartObject call with google.protobuf.Value field. The only
      // object within that type is a struct type. So start a struct.
      //
      // The struct field in Value type is named "struct_value"
      // https://github.com/google/protobuf/blob/master/src/google/protobuf/struct.proto
      // Also start the map field "fields" within the struct.
      // "struct_value": {
      //   "fields": [
      Push("struct_value", Item::MESSAGE, true, false);
      Push("fields", Item::MAP, true, true);
      return this;
    }

    if (master_type_.name() == kStructListValueType) {
      InvalidValue(kStructListValueType,
                   "Cannot start root message with ListValue.");
    }

    return this;
  }

  // Send all ANY events to AnyWriter.
  if (current_->IsAny()) {
    current_->any()->StartObject(name);
    return this;
  }

  // If we are within a map, we render name as keys and send StartObject to the
  // value field.
  if (current_->IsMap()) {
    if (!ValidMapKey(name)) {
      IncrementInvalidDepth();
      return this;
    }

    // Map is a repeated field of message type with a "key" and a "value" field.
    // https://developers.google.com/protocol-buffers/docs/proto3?hl=en#maps
    // message MapFieldEntry {
    //   key_type key = 1;
    //   value_type value = 2;
    // }
    //
    // repeated MapFieldEntry map_field = N;
    //
    // That means, we render the following element within a list (hence no
    // name):
    // { "key": "<name>", "value": {
    Push("", Item::MESSAGE, false, false);
    ProtoWriter::RenderDataPiece("key", DataPiece(name));
    Push("value", Item::MESSAGE, true, false);

    // Make sure we are valid so far after starting map fields.
    if (invalid_depth() > 0) return this;

    // If top of stack is g.p.Struct type, start the struct the map field within
    // it.
    if (element() != NULL && IsStruct(*element()->parent_field())) {
      // Render "fields": [
      Push("fields", Item::MAP, true, true);
      return this;
    }

    // If top of stack is g.p.Value type, start the Struct within it.
    if (element() != NULL && IsStructValue(*element()->parent_field())) {
      // Render
      // "struct_value": {
      //   "fields": [
      Push("struct_value", Item::MESSAGE, true, false);
      Push("fields", Item::MAP, true, true);
    }
    return this;
  }

  const google::protobuf::Field* field = BeginNamed(name, false);
  if (field == NULL) return this;

  if (IsStruct(*field)) {
    // Start a struct object.
    // Render
    // "<name>": {
    //   "fields": {
    Push(name, Item::MESSAGE, false, false);
    Push("fields", Item::MAP, true, true);
    return this;
  }

  if (IsStructValue(*field)) {
    // We got a StartObject call with google.protobuf.Value field.  The only
    // object within that type is a struct type. So start a struct.
    // Render
    // "<name>": {
    //   "struct_value": {
    //     "fields": {
    Push(name, Item::MESSAGE, false, false);
    Push("struct_value", Item::MESSAGE, true, false);
    Push("fields", Item::MAP, true, true);
    return this;
  }

  if (IsMap(*field)) {
    // Begin a map. A map is triggered by a StartObject() call if the current
    // field has a map type.
    // A map type is always repeated, hence set is_list to true.
    // Render
    // "<name>": [
    Push(name, Item::MAP, false, true);
    return this;
  }

  // A regular message type. Pass it directly to ProtoWriter.
  // Render
  // "<name>": {
  Push(name, IsAny(*field) ? Item::ANY : Item::MESSAGE, false, false);
  return this;
}

ProtoStreamObjectWriter* ProtoStreamObjectWriter::EndObject() {
  if (invalid_depth() > 0) {
    DecrementInvalidDepth();
    return this;
  }

  if (current_ == NULL) return this;

  if (current_->IsAny()) {
    if (current_->any()->EndObject()) return this;
  }

  Pop();

  return this;
}

ProtoStreamObjectWriter* ProtoStreamObjectWriter::StartList(StringPiece name) {
  if (invalid_depth() > 0) {
    IncrementInvalidDepth();
    return this;
  }

  // Since we cannot have a top-level repeated item in protobuf, the only way
  // this is valid is if we start a special type google.protobuf.ListValue or
  // google.protobuf.Value.
  if (current_ == NULL) {
    if (!name.empty()) {
      InvalidName(name, "Root element should not be named.");
      IncrementInvalidDepth();
      return this;
    }

    // If master type is a special type that needs extra values to be written to
    // stream, we write those values.
    if (master_type_.name() == kStructValueType) {
      // We got a StartList with google.protobuf.Value master type. This means
      // we have to start the "list_value" within google.protobuf.Value.
      //
      // See
      // https://github.com/google/protobuf/blob/master/src/google/protobuf/struct.proto
      //
      // Render
      // "<name>": {
      //   "list_value": {
      //     "values": [  // Start this list.
      ProtoWriter::StartObject(name);
      current_.reset(new Item(this, Item::MESSAGE, false, false));
      Push("list_value", Item::MESSAGE, true, false);
      Push("values", Item::MESSAGE, true, true);
      return this;
    }

    if (master_type_.name() == kStructListValueType) {
      // We got a StartList with google.protobuf.ListValue master type. This
      // means we have to start the "values" within google.protobuf.ListValue.
      //
      // Render
      // "<name>": {
      //   "values": [  // Start this list.
      ProtoWriter::StartObject(name);
      current_.reset(new Item(this, Item::MESSAGE, false, false));
      Push("values", Item::MESSAGE, true, true);
      return this;
    }

    // Send the event to ProtoWriter so proper errors can be reported.
    //
    // Render a regular list:
    // "<name>": [
    ProtoWriter::StartList(name);
    current_.reset(new Item(this, Item::MESSAGE, false, true));
    return this;
  }

  if (current_->IsAny()) {
    current_->any()->StartList(name);
    return this;
  }

  // If the top of stack is a map, we are starting a list value within a map.
  // Since map does not allow repeated values, this can only happen when the map
  // value is of a special type that renders a list in JSON.  These can be one
  // of 3 cases:
  // i. We are rendering a list value within google.protobuf.Struct
  // ii. We are rendering a list value within google.protobuf.Value
  // iii. We are rendering a list value with type google.protobuf.ListValue.
  if (current_->IsMap()) {
    if (!ValidMapKey(name)) {
      IncrementInvalidDepth();
      return this;
    }

    // Start the repeated map entry object.
    // Render
    // { "key": "<name>", "value": {
    Push("", Item::MESSAGE, false, false);
    ProtoWriter::RenderDataPiece("key", DataPiece(name));
    Push("value", Item::MESSAGE, true, false);

    // Make sure we are valid after pushing all above items.
    if (invalid_depth() > 0) return this;

    // case i and ii above. Start "list_value" field within g.p.Value
    if (element() != NULL && element()->parent_field() != NULL) {
      // Render
      // "list_value": {
      //   "values": [  // Start this list
      if (IsStructValue(*element()->parent_field())) {
        Push("list_value", Item::MESSAGE, true, false);
        Push("values", Item::MESSAGE, true, true);
        return this;
      }

      // Render
      // "values": [
      if (IsStructListValue(*element()->parent_field())) {
        // case iii above. Bind directly to g.p.ListValue
        Push("values", Item::MESSAGE, true, true);
        return this;
      }
    }

    // Report an error.
    InvalidValue("Map", StrCat("Cannot have repeated items ('", name,
                               "') within a map."));
    return this;
  }

  // When name is empty and stack is not empty, we are rendering an item within
  // a list.
  if (name.empty()) {
    if (element() != NULL && element()->parent_field() != NULL) {
      if (IsStructValue(*element()->parent_field())) {
        // Since it is g.p.Value, we bind directly to the list_value.
        // Render
        // {  // g.p.Value item within the list
        //   "list_value": {
        //     "values": [
        Push("", Item::MESSAGE, false, false);
        Push("list_value", Item::MESSAGE, true, false);
        Push("values", Item::MESSAGE, true, true);
        return this;
      }

      if (IsStructListValue(*element()->parent_field())) {
        // Since it is g.p.ListValue, we bind to it directly.
        // Render
        // {  // g.p.ListValue item within the list
        //   "values": [
        Push("", Item::MESSAGE, false, false);
        Push("values", Item::MESSAGE, true, true);
        return this;
      }
    }

    // Pass the event to underlying ProtoWriter.
    Push(name, Item::MESSAGE, false, true);
    return this;
  }

  // name is not empty
  const google::protobuf::Field* field = Lookup(name);
  if (field == NULL) {
    IncrementInvalidDepth();
    return this;
  }

  if (IsStructValue(*field)) {
    // If g.p.Value is repeated, start that list. Otherwise, start the
    // "list_value" within it.
    if (IsRepeated(*field)) {
      // Render it just like a regular repeated field.
      // "<name>": [
      Push(name, Item::MESSAGE, false, true);
      return this;
    }

    // Start the "list_value" field.
    // Render
    // "<name>": {
    //   "list_value": {
    //     "values": [
    Push(name, Item::MESSAGE, false, false);
    Push("list_value", Item::MESSAGE, true, false);
    Push("values", Item::MESSAGE, true, true);
    return this;
  }

  if (IsStructListValue(*field)) {
    // If g.p.ListValue is repeated, start that list. Otherwise, start the
    // "values" within it.
    if (IsRepeated(*field)) {
      // Render it just like a regular repeated field.
      // "<name>": [
      Push(name, Item::MESSAGE, false, true);
      return this;
    }

    // Start the "values" field within g.p.ListValue.
    // Render
    // "<name>": {
    //   "values": [
    Push(name, Item::MESSAGE, false, false);
    Push("values", Item::MESSAGE, true, true);
    return this;
  }

  // If we are here, the field should be repeated. Report an error otherwise.
  if (!IsRepeated(*field)) {
    IncrementInvalidDepth();
    InvalidName(name, "Proto field is not repeating, cannot start list.");
    return this;
  }

  if (IsMap(*field)) {
    InvalidValue("Map",
                 StrCat("Cannot bind a list to map for field '", name, "'."));
    IncrementInvalidDepth();
    return this;
  }

  // Pass the event to ProtoWriter.
  // Render
  // "<name>": [
  Push(name, Item::MESSAGE, false, true);
  return this;
}

ProtoStreamObjectWriter* ProtoStreamObjectWriter::EndList() {
  if (invalid_depth() > 0) {
    DecrementInvalidDepth();
    return this;
  }

  if (current_ == NULL) return this;

  if (current_->IsAny()) {
    current_->any()->EndList();
    return this;
  }

  Pop();
  return this;
}

Status ProtoStreamObjectWriter::RenderStructValue(ProtoStreamObjectWriter* ow,
                                                  const DataPiece& data) {
  string struct_field_name;
  switch (data.type()) {
    // Our JSON parser parses numbers as either int64, uint64, or double.
    case DataPiece::TYPE_INT64:
    case DataPiece::TYPE_UINT64:
    case DataPiece::TYPE_DOUBLE: {
      struct_field_name = "number_value";
      break;
    }
    case DataPiece::TYPE_STRING: {
      struct_field_name = "string_value";
      break;
    }
    case DataPiece::TYPE_BOOL: {
      struct_field_name = "bool_value";
      break;
    }
    case DataPiece::TYPE_NULL: {
      struct_field_name = "null_value";
      break;
    }
    default: {
      return Status(INVALID_ARGUMENT,
                    "Invalid struct data type. Only number, string, boolean or "
                    "null values are supported.");
    }
  }
  ow->ProtoWriter::RenderDataPiece(struct_field_name, data);
  return Status::OK;
}

Status ProtoStreamObjectWriter::RenderTimestamp(ProtoStreamObjectWriter* ow,
                                                const DataPiece& data) {
  if (data.type() != DataPiece::TYPE_STRING) {
    return Status(INVALID_ARGUMENT,
                  StrCat("Invalid data type for timestamp, value is ",
                         data.ValueAsStringOrDefault("")));
  }

  StringPiece value(data.str());

  int64 seconds;
  int32 nanos;
  if (!::google::protobuf::internal::ParseTime(value.ToString(), &seconds,
                                               &nanos)) {
    return Status(INVALID_ARGUMENT, StrCat("Invalid time format: ", value));
  }


  ow->ProtoWriter::RenderDataPiece("seconds", DataPiece(seconds));
  ow->ProtoWriter::RenderDataPiece("nanos", DataPiece(nanos));
  return Status::OK;
}

static inline util::Status RenderOneFieldPath(ProtoStreamObjectWriter* ow,
                                                StringPiece path) {
  ow->ProtoWriter::RenderDataPiece(
      "paths", DataPiece(ConvertFieldMaskPath(path, &ToSnakeCase)));
  return Status::OK;
}

Status ProtoStreamObjectWriter::RenderFieldMask(ProtoStreamObjectWriter* ow,
                                                const DataPiece& data) {
  if (data.type() != DataPiece::TYPE_STRING) {
    return Status(INVALID_ARGUMENT,
                  StrCat("Invalid data type for field mask, value is ",
                         data.ValueAsStringOrDefault("")));
  }

// TODO(tsun): figure out how to do proto descriptor based snake case
// conversions as much as possible. Because ToSnakeCase sometimes returns the
// wrong value.
  google::protobuf::scoped_ptr<ResultCallback1<util::Status, StringPiece> > callback(
      google::protobuf::internal::NewPermanentCallback(&RenderOneFieldPath, ow));
  return DecodeCompactFieldMaskPaths(data.str(), callback.get());
}

Status ProtoStreamObjectWriter::RenderDuration(ProtoStreamObjectWriter* ow,
                                               const DataPiece& data) {
  if (data.type() != DataPiece::TYPE_STRING) {
    return Status(INVALID_ARGUMENT,
                  StrCat("Invalid data type for duration, value is ",
                         data.ValueAsStringOrDefault("")));
  }

  StringPiece value(data.str());

  if (!value.ends_with("s")) {
    return Status(INVALID_ARGUMENT,
                  "Illegal duration format; duration must end with 's'");
  }
  value = value.substr(0, value.size() - 1);
  int sign = 1;
  if (value.starts_with("-")) {
    sign = -1;
    value = value.substr(1);
  }

  StringPiece s_secs, s_nanos;
  SplitSecondsAndNanos(value, &s_secs, &s_nanos);
  uint64 unsigned_seconds;
  if (!safe_strtou64(s_secs, &unsigned_seconds)) {
    return Status(INVALID_ARGUMENT,
                  "Invalid duration format, failed to parse seconds");
  }

  int32 nanos = 0;
  Status nanos_status = GetNanosFromStringPiece(
      s_nanos, "Invalid duration format, failed to parse nano seconds",
      "Duration value exceeds limits", &nanos);
  if (!nanos_status.ok()) {
    return nanos_status;
  }
  nanos = sign * nanos;

  int64 seconds = sign * unsigned_seconds;
  if (seconds > kMaxSeconds || seconds < kMinSeconds ||
      nanos <= -kNanosPerSecond || nanos >= kNanosPerSecond) {
    return Status(INVALID_ARGUMENT, "Duration value exceeds limits");
  }

  ow->ProtoWriter::RenderDataPiece("seconds", DataPiece(seconds));
  ow->ProtoWriter::RenderDataPiece("nanos", DataPiece(nanos));
  return Status::OK;
}

Status ProtoStreamObjectWriter::RenderWrapperType(ProtoStreamObjectWriter* ow,
                                                  const DataPiece& data) {
  ow->ProtoWriter::RenderDataPiece("value", data);
  return Status::OK;
}

ProtoStreamObjectWriter* ProtoStreamObjectWriter::RenderDataPiece(
    StringPiece name, const DataPiece& data) {
  Status status;
  if (invalid_depth() > 0) return this;

  if (current_ == NULL) {
    const TypeRenderer* type_renderer =
        FindTypeRenderer(GetFullTypeWithUrl(master_type_.name()));
    if (type_renderer == NULL) {
      InvalidName(name, "Root element must be a message.");
      return this;
    }
    // Render the special type.
    // "<name>": {
    //   ... Render special type ...
    // }
    ProtoWriter::StartObject(name);
    status = (*type_renderer)(this, data);
    if (!status.ok()) {
      InvalidValue(master_type_.name(),
                   StrCat("Field '", name, "', ", status.error_message()));
    }
    ProtoWriter::EndObject();
    return this;
  }

  if (current_->IsAny()) {
    current_->any()->RenderDataPiece(name, data);
    return this;
  }

  const google::protobuf::Field* field = NULL;
  if (current_->IsMap()) {
    if (!ValidMapKey(name)) return this;

    // Render an item in repeated map list.
    // { "key": "<name>", "value":
    Push("", Item::MESSAGE, false, false);
    ProtoWriter::RenderDataPiece("key", DataPiece(name));
    field = Lookup("value");
    if (field == NULL) {
      GOOGLE_LOG(DFATAL) << "Map does not have a value field.";
      return this;
    }

    const TypeRenderer* type_renderer = FindTypeRenderer(field->type_url());
    if (type_renderer != NULL) {
      // Map's value type is a special type. Render it like a message:
      // "value": {
      //   ... Render special type ...
      // }
      Push("value", Item::MESSAGE, true, false);
      status = (*type_renderer)(this, data);
      if (!status.ok()) {
        InvalidValue(field->type_url(),
                     StrCat("Field '", name, "', ", status.error_message()));
      }
      Pop();
      return this;
    }

    // If we are rendering explicit null values and the backend proto field is
    // not of the google.protobuf.NullType type, we do nothing.
    if (data.type() == DataPiece::TYPE_NULL &&
        field->type_url() != kStructNullValueTypeUrl) {
      return this;
    }

    // Render the map value as a primitive type.
    ProtoWriter::RenderDataPiece("value", data);
    Pop();
    return this;
  }

  field = Lookup(name);
  if (field == NULL) return this;

  // Check if the field is of special type. Render it accordingly if so.
  const TypeRenderer* type_renderer = FindTypeRenderer(field->type_url());
  if (type_renderer != NULL) {
    Push(name, Item::MESSAGE, false, false);
    status = (*type_renderer)(this, data);
    if (!status.ok()) {
      InvalidValue(field->type_url(),
                   StrCat("Field '", name, "', ", status.error_message()));
    }
    Pop();
    return this;
  }

  // If we are rendering explicit null values and the backend proto field is
  // not of the google.protobuf.NullType type, we do nothing.
  if (data.type() == DataPiece::TYPE_NULL &&
      field->type_url() != kStructNullValueTypeUrl) {
    return this;
  }

  ProtoWriter::RenderDataPiece(name, data);
  return this;
}

// Map of functions that are responsible for rendering well known type
// represented by the key.
hash_map<string, ProtoStreamObjectWriter::TypeRenderer>*
    ProtoStreamObjectWriter::renderers_ = NULL;
GOOGLE_PROTOBUF_DECLARE_ONCE(writer_renderers_init_);

void ProtoStreamObjectWriter::InitRendererMap() {
  renderers_ = new hash_map<string, ProtoStreamObjectWriter::TypeRenderer>();
  (*renderers_)["type.googleapis.com/google.protobuf.Timestamp"] =
      &ProtoStreamObjectWriter::RenderTimestamp;
  (*renderers_)["type.googleapis.com/google.protobuf.Duration"] =
      &ProtoStreamObjectWriter::RenderDuration;
  (*renderers_)["type.googleapis.com/google.protobuf.FieldMask"] =
      &ProtoStreamObjectWriter::RenderFieldMask;
  (*renderers_)["type.googleapis.com/google.protobuf.Double"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Float"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Int64"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.UInt64"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Int32"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.UInt32"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Bool"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.String"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Bytes"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.DoubleValue"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.FloatValue"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Int64Value"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.UInt64Value"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Int32Value"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.UInt32Value"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.BoolValue"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.StringValue"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.BytesValue"] =
      &ProtoStreamObjectWriter::RenderWrapperType;
  (*renderers_)["type.googleapis.com/google.protobuf.Value"] =
      &ProtoStreamObjectWriter::RenderStructValue;
  ::google::protobuf::internal::OnShutdown(&DeleteRendererMap);
}

void ProtoStreamObjectWriter::DeleteRendererMap() {
  delete ProtoStreamObjectWriter::renderers_;
  renderers_ = NULL;
}

ProtoStreamObjectWriter::TypeRenderer*
ProtoStreamObjectWriter::FindTypeRenderer(const string& type_url) {
  ::google::protobuf::GoogleOnceInit(&writer_renderers_init_, &InitRendererMap);
  return FindOrNull(*renderers_, type_url);
}

bool ProtoStreamObjectWriter::ValidMapKey(StringPiece unnormalized_name) {
  if (current_ == NULL) return true;

  if (!current_->InsertMapKeyIfNotPresent(unnormalized_name)) {
    listener()->InvalidName(
        location(), unnormalized_name,
        StrCat("Repeated map key: '", unnormalized_name, "' is already set."));
    return false;
  }

  return true;
}

void ProtoStreamObjectWriter::Push(StringPiece name, Item::ItemType item_type,
                                   bool is_placeholder, bool is_list) {
  is_list ? ProtoWriter::StartList(name) : ProtoWriter::StartObject(name);

  // invalid_depth == 0 means it is a successful StartObject or StartList.
  if (invalid_depth() == 0)
    current_.reset(
        new Item(current_.release(), item_type, is_placeholder, is_list));
}

void ProtoStreamObjectWriter::Pop() {
  // Pop all placeholder items sending StartObject or StartList events to
  // ProtoWriter according to is_list value.
  while (current_ != NULL && current_->is_placeholder()) {
    PopOneElement();
  }
  if (current_ != NULL) {
    PopOneElement();
  }
}

void ProtoStreamObjectWriter::PopOneElement() {
  current_->is_list() ? ProtoWriter::EndList() : ProtoWriter::EndObject();
  current_.reset(current_->pop<Item>());
}

bool ProtoStreamObjectWriter::IsMap(const google::protobuf::Field& field) {
  if (field.type_url().empty() ||
      field.kind() != google::protobuf::Field_Kind_TYPE_MESSAGE ||
      field.cardinality() !=
          google::protobuf::Field_Cardinality_CARDINALITY_REPEATED) {
    return false;
  }
  const google::protobuf::Type* field_type =
      typeinfo()->GetTypeByTypeUrl(field.type_url());

  // TODO(xiaofeng): Unify option names.
  return GetBoolOptionOrDefault(field_type->options(),
                                "google.protobuf.MessageOptions.map_entry", false) ||
         GetBoolOptionOrDefault(field_type->options(), "map_entry", false);
}

bool ProtoStreamObjectWriter::IsAny(const google::protobuf::Field& field) {
  return GetTypeWithoutUrl(field.type_url()) == kAnyType;
}

bool ProtoStreamObjectWriter::IsStruct(const google::protobuf::Field& field) {
  return GetTypeWithoutUrl(field.type_url()) == kStructType;
}

bool ProtoStreamObjectWriter::IsStructValue(
    const google::protobuf::Field& field) {
  return GetTypeWithoutUrl(field.type_url()) == kStructValueType;
}

bool ProtoStreamObjectWriter::IsStructListValue(
    const google::protobuf::Field& field) {
  return GetTypeWithoutUrl(field.type_url()) == kStructListValueType;
}

}  // namespace converter
}  // namespace util
}  // namespace protobuf
}  // namespace google
