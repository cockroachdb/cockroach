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

#include <google/protobuf/compiler/command_line_interface.h>
#include <google/protobuf/compiler/cpp/cpp_generator.h>
#include <google/protobuf/compiler/python/python_generator.h>
#include <google/protobuf/compiler/java/java_generator.h>
#include <google/protobuf/compiler/javanano/javanano_generator.h>
#include <google/protobuf/compiler/ruby/ruby_generator.h>
#include <google/protobuf/compiler/csharp/csharp_generator.h>
#include <google/protobuf/compiler/objectivec/objectivec_generator.h>
#include <google/protobuf/compiler/js/js_generator.h>

int main(int argc, char* argv[]) {

  google::protobuf::compiler::CommandLineInterface cli;
  cli.AllowPlugins("protoc-");

  // Proto2 C++
  google::protobuf::compiler::cpp::CppGenerator cpp_generator;
  cli.RegisterGenerator("--cpp_out", "--cpp_opt", &cpp_generator,
                        "Generate C++ header and source.");

  // Proto2 Java
  google::protobuf::compiler::java::JavaGenerator java_generator;
  cli.RegisterGenerator("--java_out", &java_generator,
                        "Generate Java source file.");


  // Proto2 Python
  google::protobuf::compiler::python::Generator py_generator;
  cli.RegisterGenerator("--python_out", &py_generator,
                        "Generate Python source file.");

  // Java Nano
  google::protobuf::compiler::javanano::JavaNanoGenerator javanano_generator;
  cli.RegisterGenerator("--javanano_out", &javanano_generator,
                        "Generate Java Nano source file.");

  // Ruby
  google::protobuf::compiler::ruby::Generator rb_generator;
  cli.RegisterGenerator("--ruby_out", &rb_generator,
                        "Generate Ruby source file.");

  // CSharp
  google::protobuf::compiler::csharp::Generator csharp_generator;
  cli.RegisterGenerator("--csharp_out", "--csharp_opt", &csharp_generator,
                        "Generate C# source file.");

  // Objective C
  google::protobuf::compiler::objectivec::ObjectiveCGenerator objc_generator;
  cli.RegisterGenerator("--objc_out", &objc_generator,
                        "Generate Objective C header and source.");

  // JavaScript
  google::protobuf::compiler::js::Generator js_generator;
  cli.RegisterGenerator("--js_out", &js_generator,
                        "Generate JavaScript source.");

  return cli.Run(argc, argv);
}
