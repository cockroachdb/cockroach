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

#ifndef GOOGLE_PROTOBUF_COMPILER_CPP_FIELD_H__
#define GOOGLE_PROTOBUF_COMPILER_CPP_FIELD_H__

#include <map>
#include <memory>
#ifndef _SHARED_PTR_H
#include <google/protobuf/stubs/shared_ptr.h>
#endif
#include <string>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/compiler/cpp/cpp_options.h>

namespace google {
namespace protobuf {
  namespace io {
    class Printer;             // printer.h
  }
}

namespace protobuf {
namespace compiler {
namespace cpp {

// Helper function: set variables in the map that are the same for all
// field code generators.
// ['name', 'index', 'number', 'classname', 'declared_type', 'tag_size',
// 'deprecation'].
void SetCommonFieldVariables(const FieldDescriptor* descriptor,
                             map<string, string>* variables,
                             const Options& options);

void SetCommonOneofFieldVariables(const FieldDescriptor* descriptor,
                                  map<string, string>* variables);

class FieldGenerator {
 public:
  FieldGenerator() {}
  virtual ~FieldGenerator();

  // Generate lines of code declaring members fields of the message class
  // needed to represent this field.  These are placed inside the message
  // class.
  virtual void GeneratePrivateMembers(io::Printer* printer) const = 0;

  // Generate static default variable for this field. These are placed inside
  // the message class. Most field types don't need this, so the default
  // implementation is empty.
  virtual void GenerateStaticMembers(io::Printer* /*printer*/) const {}

  // Generate prototypes for accessors that will manipulate imported
  // messages inline.  These are for .proto.h headers.
  //
  // In .proto.h mode, the headers of imports are not #included. However,
  // functions that manipulate the imported message types need access to
  // the class definition of the imported message, meaning that the headers
  // must be #included. To get around this, functions that manipulate
  // imported message objects are defined as dependent functions in a base
  // template class. By making them dependent template functions, the
  // function templates will not be instantiated until they are called, so
  // we can defer to those translation units to #include the necessary
  // generated headers.
  //
  // See:
  // http://en.cppreference.com/w/cpp/language/class_template#Implicit_instantiation
  //
  // Most field types don't need this, so the default implementation is empty.
  virtual void GenerateDependentAccessorDeclarations(
      io::Printer* printer) const {}

  // Generate prototypes for all of the accessor functions related to this
  // field.  These are placed inside the class definition.
  virtual void GenerateAccessorDeclarations(io::Printer* printer) const = 0;

  // Generate inline definitions of depenent accessor functions for this field.
  // These are placed inside the header after all class definitions.
  virtual void GenerateDependentInlineAccessorDefinitions(
    io::Printer* printer) const {}

  // Generate inline definitions of accessor functions for this field.
  // These are placed inside the header after all class definitions.
  // In non-.proto.h mode, this generates dependent accessor functions as well.
  virtual void GenerateInlineAccessorDefinitions(
    io::Printer* printer, bool is_inline) const = 0;

  // Generate definitions of accessors that aren't inlined.  These are
  // placed somewhere in the .cc file.
  // Most field types don't need this, so the default implementation is empty.
  virtual void GenerateNonInlineAccessorDefinitions(
    io::Printer* /*printer*/) const {}

  // Generate lines of code (statements, not declarations) which clear the
  // field.  This is used to define the clear_$name$() method as well as
  // the Clear() method for the whole message.
  virtual void GenerateClearingCode(io::Printer* printer) const = 0;

  // Generate lines of code (statements, not declarations) which merges the
  // contents of the field from the current message to the target message,
  // which is stored in the generated code variable "from".
  // This is used to fill in the MergeFrom method for the whole message.
  // Details of this usage can be found in message.cc under the
  // GenerateMergeFrom method.
  virtual void GenerateMergingCode(io::Printer* printer) const = 0;

  // Generate lines of code (statements, not declarations) which swaps
  // this field and the corresponding field of another message, which
  // is stored in the generated code variable "other". This is used to
  // define the Swap method. Details of usage can be found in
  // message.cc under the GenerateSwap method.
  virtual void GenerateSwappingCode(io::Printer* printer) const = 0;

  // Generate initialization code for private members declared by
  // GeneratePrivateMembers(). These go into the message class's SharedCtor()
  // method, invoked by each of the generated constructors.
  virtual void GenerateConstructorCode(io::Printer* printer) const = 0;

  // Generate any code that needs to go in the class's SharedDtor() method,
  // invoked by the destructor.
  // Most field types don't need this, so the default implementation is empty.
  virtual void GenerateDestructorCode(io::Printer* /*printer*/) const {}

  // Generate a manual destructor invocation for use when the message is on an
  // arena. The code that this method generates will be executed inside a
  // shared-for-the-whole-message-class method registered with OwnDestructor().
  // The method should return |true| if it generated any code that requires a
  // call; this allows the message generator to eliminate the OwnDestructor()
  // registration if no fields require it.
  virtual bool GenerateArenaDestructorCode(io::Printer* printer) const {
    return false;
  }

  // Generate code that allocates the fields's default instance.
  virtual void GenerateDefaultInstanceAllocator(io::Printer* /*printer*/)
      const {}

  // Generate code that should be run when ShutdownProtobufLibrary() is called,
  // to delete all dynamically-allocated objects.
  virtual void GenerateShutdownCode(io::Printer* /*printer*/) const {}

  // Generate lines to decode this field, which will be placed inside the
  // message's MergeFromCodedStream() method.
  virtual void GenerateMergeFromCodedStream(io::Printer* printer) const = 0;

  // Generate lines to decode this field from a packed value, which will be
  // placed inside the message's MergeFromCodedStream() method.
  virtual void GenerateMergeFromCodedStreamWithPacking(io::Printer* printer)
      const;

  // Generate lines to serialize this field, which are placed within the
  // message's SerializeWithCachedSizes() method.
  virtual void GenerateSerializeWithCachedSizes(io::Printer* printer) const = 0;

  // Generate lines to serialize this field directly to the array "target",
  // which are placed within the message's SerializeWithCachedSizesToArray()
  // method. This must also advance "target" past the written bytes.
  virtual void GenerateSerializeWithCachedSizesToArray(
      io::Printer* printer) const = 0;

  // Generate lines to compute the serialized size of this field, which
  // are placed in the message's ByteSize() method.
  virtual void GenerateByteSize(io::Printer* printer) const = 0;

 private:
  GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(FieldGenerator);
};

// Convenience class which constructs FieldGenerators for a Descriptor.
class FieldGeneratorMap {
 public:
  explicit FieldGeneratorMap(const Descriptor* descriptor, const Options& options);
  ~FieldGeneratorMap();

  const FieldGenerator& get(const FieldDescriptor* field) const;

 private:
  const Descriptor* descriptor_;
  google::protobuf::scoped_array<google::protobuf::scoped_ptr<FieldGenerator> > field_generators_;

  static FieldGenerator* MakeGenerator(const FieldDescriptor* field,
                                       const Options& options);

  GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(FieldGeneratorMap);
};


}  // namespace cpp
}  // namespace compiler
}  // namespace protobuf

}  // namespace google
#endif  // GOOGLE_PROTOBUF_COMPILER_CPP_FIELD_H__
