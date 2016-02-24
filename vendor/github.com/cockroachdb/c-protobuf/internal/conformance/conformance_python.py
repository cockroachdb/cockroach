#!/usr/bin/env python
#
# Protocol Buffers - Google's data interchange format
# Copyright 2008 Google Inc.  All rights reserved.
# https://developers.google.com/protocol-buffers/
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""A conformance test implementation for the Python protobuf library.

See conformance.proto for more information.
"""

import struct
import sys
import os
from google.protobuf import message
from google.protobuf import json_format
import conformance_pb2

sys.stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)
sys.stdin = os.fdopen(sys.stdin.fileno(), 'rb', 0)

test_count = 0
verbose = False

class ProtocolError(Exception):
  pass

def do_test(request):
  test_message = conformance_pb2.TestAllTypes()
  response = conformance_pb2.ConformanceResponse()
  test_message = conformance_pb2.TestAllTypes()

  try:
    if request.WhichOneof('payload') == 'protobuf_payload':
      try:
        test_message.ParseFromString(request.protobuf_payload)
      except message.DecodeError as e:
        response.parse_error = str(e)
        return response

    elif request.WhichOneof('payload') == 'json_payload':
      try:
        json_format.Parse(request.json_payload, test_message)
      except json_format.ParseError as e:
        response.parse_error = str(e)
        return response

    else:
      raise ProtocolError("Request didn't have payload.")

    if request.requested_output_format == conformance_pb2.UNSPECIFIED:
      raise ProtocolError("Unspecified output format")

    elif request.requested_output_format == conformance_pb2.PROTOBUF:
      response.protobuf_payload = test_message.SerializeToString()

    elif request.requested_output_format == conformance_pb2.JSON:
      response.json_payload = json_format.MessageToJson(test_message)

  except Exception as e:
    response.runtime_error = str(e)

  return response

def do_test_io():
  length_bytes = sys.stdin.read(4)
  if len(length_bytes) == 0:
    return False   # EOF
  elif len(length_bytes) != 4:
    raise IOError("I/O error")

  # "I" is "unsigned int", so this depends on running on a platform with
  # 32-bit "unsigned int" type.  The Python struct module unfortunately
  # has no format specifier for uint32_t.
  length = struct.unpack("<I", length_bytes)[0]
  serialized_request = sys.stdin.read(length)
  if len(serialized_request) != length:
    raise IOError("I/O error")

  request = conformance_pb2.ConformanceRequest()
  request.ParseFromString(serialized_request)

  response = do_test(request)

  serialized_response = response.SerializeToString()
  sys.stdout.write(struct.pack("<I", len(serialized_response)))
  sys.stdout.write(serialized_response)
  sys.stdout.flush()

  if verbose:
    sys.stderr.write("conformance_python: request=%s, response=%s\n" % (
                       request.ShortDebugString().c_str(),
                       response.ShortDebugString().c_str()))

  global test_count
  test_count += 1

  return True

while True:
  if not do_test_io():
    sys.stderr.write("conformance_python: received EOF from test runner " +
                     "after %s tests, exiting\n" % (test_count))
    sys.exit(0)
