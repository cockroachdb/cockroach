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

#ifndef GOOGLE_PROTOBUF_UTIL_CONVERTER_CONSTANTS_H__
#define GOOGLE_PROTOBUF_UTIL_CONVERTER_CONSTANTS_H__

#include <google/protobuf/stubs/common.h>

// This file contains constants used by //net/proto2/util/converter.

namespace google {
namespace protobuf {
namespace util {
namespace converter {
// Prefix for type URLs.
const char kTypeServiceBaseUrl[] = "type.googleapis.com";

// Format string for RFC3339 timestamp formatting.
const char kRfc3339TimeFormat[] = "%Y-%m-%dT%H:%M:%S";

// Minimum seconds allowed in a google.protobuf.TimeStamp or Duration value.
const int64 kMinSeconds = -315576000000;

// Maximum seconds allowed in a google.protobuf.TimeStamp or Duration value.
const int64 kMaxSeconds = 315576000000;

// Nano seconds in a second.
const int32 kNanosPerSecond = 1000000000;

// Type url representing NULL values in google.protobuf.Struct type.
const char kStructNullValueTypeUrl[] =
    "type.googleapis.com/google.protobuf.NullValue";

// Type string for google.protobuf.Struct
const char kStructType[] = "google.protobuf.Struct";

// Type string for struct.proto's google.protobuf.Value value type.
const char kStructValueType[] = "google.protobuf.Value";

// Type string for struct.proto's google.protobuf.ListValue value type.
const char kStructListValueType[] = "google.protobuf.ListValue";

// Type string for google.protobuf.Timestamp
const char kTimestampType[] = "google.protobuf.Timestamp";

// Type string for google.protobuf.Duration
const char kDurationType[] = "google.protobuf.Duration";

// Type URL for struct value type google.protobuf.Value
const char kStructValueTypeUrl[] = "type.googleapis.com/google.protobuf.Value";

// Type URL for struct value type google.protobuf.Value
const char kStructTypeUrl[] = "type.googleapis.com/google.protobuf.Struct";

// Type string for google.protobuf.Any
const char kAnyType[] = "google.protobuf.Any";

// The type URL of google.protobuf.FieldMask;
const char kFieldMaskTypeUrl[] =
    "type.googleapis.com/google.protobuf.FieldMask";

}  // namespace converter
}  // namespace util
}  // namespace protobuf
}  // namespace google
#endif  // GOOGLE_PROTOBUF_UTIL_CONVERTER_CONSTANTS_H__
