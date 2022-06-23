// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build windows
// +build windows

package protoc

// https://github.com/protocolbuffers/protobuf/blob/336ed1820a4f2649c9aa3953d5059b03b7a77100/src/google/protobuf/compiler/command_line_interface.cc#L892-L896
//
// This will be ":" for all unix-like platforms including darwin.
// This will be ";" for windows.
const includeDirPathSeparator = ";"
