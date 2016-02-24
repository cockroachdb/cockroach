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

#ifndef GOOGLE_PROTOBUF_UTIL_CONVERTER_PROTO_WRITER_H__
#define GOOGLE_PROTOBUF_UTIL_CONVERTER_PROTO_WRITER_H__

#include <deque>
#include <google/protobuf/stubs/hash.h>
#include <string>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/util/internal/type_info.h>
#include <google/protobuf/util/internal/datapiece.h>
#include <google/protobuf/util/internal/error_listener.h>
#include <google/protobuf/util/internal/structured_objectwriter.h>
#include <google/protobuf/util/type_resolver.h>
#include <google/protobuf/stubs/bytestream.h>

namespace google {
namespace protobuf {
namespace io {
class CodedOutputStream;
}  // namespace io
}  // namespace protobuf


namespace protobuf {
class Type;
class Field;
}  // namespace protobuf


namespace protobuf {
namespace util {
namespace converter {

class ObjectLocationTracker;

// An ObjectWriter that can write protobuf bytes directly from writer events.
// This class does not support special types like Struct or Map. However, since
// this class supports raw protobuf, it can be used to provide support for
// special types by inheriting from it or by wrapping it.
//
// It also supports streaming.
class LIBPROTOBUF_EXPORT ProtoWriter : public StructuredObjectWriter {
 public:
// Constructor. Does not take ownership of any parameter passed in.
  ProtoWriter(TypeResolver* type_resolver, const google::protobuf::Type& type,
              strings::ByteSink* output, ErrorListener* listener);
  virtual ~ProtoWriter();

  // ObjectWriter methods.
  virtual ProtoWriter* StartObject(StringPiece name);
  virtual ProtoWriter* EndObject();
  virtual ProtoWriter* StartList(StringPiece name);
  virtual ProtoWriter* EndList();
  virtual ProtoWriter* RenderBool(StringPiece name, bool value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderInt32(StringPiece name, int32 value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderUint32(StringPiece name, uint32 value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderInt64(StringPiece name, int64 value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderUint64(StringPiece name, uint64 value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderDouble(StringPiece name, double value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderFloat(StringPiece name, float value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderString(StringPiece name, StringPiece value) {
    return RenderDataPiece(name, DataPiece(value));
  }
  virtual ProtoWriter* RenderBytes(StringPiece name, StringPiece value) {
    return RenderDataPiece(name, DataPiece(value, false));
  }
  virtual ProtoWriter* RenderNull(StringPiece name) {
    return RenderDataPiece(name, DataPiece::NullData());
  }

  // Renders a DataPiece 'value' into a field whose wire type is determined
  // from the given field 'name'.
  virtual ProtoWriter* RenderDataPiece(StringPiece name,
                                       const DataPiece& value);

  // Returns the location tracker to use for tracking locations for errors.
  const LocationTrackerInterface& location() {
    return element_ != NULL ? *element_ : *tracker_;
  }

  // When true, we finished writing to output a complete message.
  bool done() const { return done_; }

  // Returns the proto stream object.
  google::protobuf::io::CodedOutputStream* stream() { return stream_.get(); }

  // Getters and mutators of invalid_depth_.
  void IncrementInvalidDepth() { ++invalid_depth_; }
  void DecrementInvalidDepth() { --invalid_depth_; }
  int invalid_depth() { return invalid_depth_; }

  ErrorListener* listener() { return listener_; }

  const TypeInfo* typeinfo() { return typeinfo_; }

 protected:
  class LIBPROTOBUF_EXPORT ProtoElement : public BaseElement, public LocationTrackerInterface {
   public:
    // Constructor for the root element. No parent nor field.
    ProtoElement(const TypeInfo* typeinfo, const google::protobuf::Type& type,
                 ProtoWriter* enclosing);

    // Constructor for a field of an element.
    ProtoElement(ProtoElement* parent, const google::protobuf::Field* field,
                 const google::protobuf::Type& type, bool is_list);

    virtual ~ProtoElement() {}

    // Called just before the destructor for clean up:
    //   - reports any missing required fields
    //   - computes the space needed by the size field, and augment the
    //     length of all parent messages by this additional space.
    //   - releases and returns the parent pointer.
    ProtoElement* pop();

    // Accessors
    // parent_field() may be NULL if we are at root.
    const google::protobuf::Field* parent_field() const {
      return parent_field_;
    }
    const google::protobuf::Type& type() const { return type_; }

    // Registers field for accounting required fields.
    void RegisterField(const google::protobuf::Field* field);

    // To report location on error messages.
    virtual string ToString() const;

    virtual ProtoElement* parent() const {
      return static_cast<ProtoElement*>(BaseElement::parent());
    }

    // Returns true if the index is already taken by a preceeding oneof input.
    bool IsOneofIndexTaken(int32 index);

    // Marks the oneof 'index' as taken. Future inputs to this oneof will
    // generate an error.
    void TakeOneofIndex(int32 index);

   private:
    // Used for access to variables of the enclosing instance of
    // ProtoWriter.
    ProtoWriter* ow_;

    // Describes the element as a field in the parent message.
    // parent_field_ is NULL if and only if this element is the root element.
    const google::protobuf::Field* parent_field_;

    // TypeInfo to lookup types.
    const TypeInfo* typeinfo_;

    // Additional variables if this element is a message:
    // (Root element is always a message).
    // type_             : the type of this element.
    // required_fields_  : set of required fields.
    // size_index_       : index into ProtoWriter::size_insert_
    //                     for later insertion of serialized message length.
    const google::protobuf::Type& type_;
    std::set<const google::protobuf::Field*> required_fields_;
    const int size_index_;

    // Tracks position in repeated fields, needed for LocationTrackerInterface.
    int array_index_;

    // Set of oneof indices already seen for the type_. Used to validate
    // incoming messages so no more than one oneof is set.
    hash_set<int32> oneof_indices_;

    GOOGLE_DISALLOW_IMPLICIT_CONSTRUCTORS(ProtoElement);
  };

  // Container for inserting 'size' information at the 'pos' position.
  struct SizeInfo {
    const int pos;
    int size;
  };

  ProtoWriter(const TypeInfo* typeinfo, const google::protobuf::Type& type,
              strings::ByteSink* output, ErrorListener* listener);

  virtual ProtoElement* element() { return element_.get(); }

  // Helper methods for calling ErrorListener. See error_listener.h.
  void InvalidName(StringPiece unknown_name, StringPiece message);
  void InvalidValue(StringPiece type_name, StringPiece value);
  void MissingField(StringPiece missing_name);

  // Common code for BeginObject() and BeginList() that does invalid_depth_
  // bookkeeping associated with name lookup.
  const google::protobuf::Field* BeginNamed(StringPiece name, bool is_list);

  // Lookup the field in the current element. Looks in the base descriptor
  // and in any extension. This will report an error if the field cannot be
  // found or if multiple matching extensions are found.
  const google::protobuf::Field* Lookup(StringPiece name);

  // Lookup the field type in the type descriptor. Returns NULL if the type
  // is not known.
  const google::protobuf::Type* LookupType(
      const google::protobuf::Field* field);

  // Write serialized output to the final output ByteSink, inserting all
  // the size information for nested messages that are missing from the
  // intermediate Cord buffer.
  void WriteRootMessage();

  // Helper method to write proto tags based on the given field.
  void WriteTag(const google::protobuf::Field& field);


  // Returns true if the field for type_ can be set as a oneof. If field is not
  // a oneof type, this function does nothing and returns true.
  // If another field for this oneof is already set, this function returns
  // false. It also calls the appropriate error callback.
  // unnormalized_name is used for error string.
  bool ValidOneof(const google::protobuf::Field& field,
                  StringPiece unnormalized_name);

  // Returns true if the field is repeated.
  bool IsRepeated(const google::protobuf::Field& field);

 private:
  // Variables for describing the structure of the input tree:
  // master_type_: descriptor for the whole protobuf message.
  // typeinfo_ : the TypeInfo object to lookup types.
  const google::protobuf::Type& master_type_;
  const TypeInfo* typeinfo_;
  // Whether we own the typeinfo_ object.
  bool own_typeinfo_;

  // Indicates whether we finished writing root message completely.
  bool done_;

  // Variable for internal state processing:
  // element_    : the current element.
  // size_insert_: sizes of nested messages.
  //               pos  - position to insert the size field.
  //               size - size value to be inserted.
  google::protobuf::scoped_ptr<ProtoElement> element_;
  std::deque<SizeInfo> size_insert_;

  // Variables for output generation:
  // output_  : pointer to an external ByteSink for final user-visible output.
  // buffer_  : buffer holding partial message before being ready for output_.
  // adapter_ : internal adapter between CodedOutputStream and buffer_.
  // stream_  : wrapper for writing tags and other encodings in wire format.
  strings::ByteSink* output_;
  string buffer_;
  google::protobuf::io::StringOutputStream adapter_;
  google::protobuf::scoped_ptr<google::protobuf::io::CodedOutputStream> stream_;

  // Variables for error tracking and reporting:
  // listener_     : a place to report any errors found.
  // invalid_depth_: number of enclosing invalid nested messages.
  // tracker_      : the root location tracker interface.
  ErrorListener* listener_;
  int invalid_depth_;
  google::protobuf::scoped_ptr<LocationTrackerInterface> tracker_;

  GOOGLE_DISALLOW_IMPLICIT_CONSTRUCTORS(ProtoWriter);
};

}  // namespace converter
}  // namespace util
}  // namespace protobuf

}  // namespace google
#endif  // GOOGLE_PROTOBUF_UTIL_CONVERTER_PROTO_WRITER_H__
