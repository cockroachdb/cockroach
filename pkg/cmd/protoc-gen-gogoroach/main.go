// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/gogo/protobuf/vanity"
	"github.com/gogo/protobuf/vanity/command"
)

func main() {
	req := command.Read()
	files := req.GetProtoFile()
	files = vanity.FilterFiles(files, vanity.NotGoogleProtobufDescriptorProto)

	for _, opt := range []func(*descriptor.FileDescriptorProto){
		vanity.TurnOffGoGettersAll,

		// Currently harms readability.
		// vanity.TurnOffGoEnumPrefixAll,

		// `String() string` is part of gogoproto.Message, so we need this.
		// vanity.TurnOffGoStringerAll,

		// Maybe useful for tests? Not using currently.
		// vanity.TurnOnVerboseEqualAll,

		// Incompatible with oneof, and also not sure what the value is.
		// vanity.TurnOnFaceAll,

		// Requires that all child messages are generated with this, which
		// is not the case for Raft messages which wrap raftpb (which
		// doesn't use this).
		// vanity.TurnOnGoStringAll,

		// Not useful for us.
		// vanity.TurnOnPopulateAll,

		// Conflicts with `GoStringerAll`, which is enabled.
		// vanity.TurnOnStringerAll,

		// This generates a method that takes `interface{}`, which sucks.
		// vanity.TurnOnEqualAll,

		// Not useful for us.
		// vanity.TurnOnDescriptionAll,
		// vanity.TurnOnTestGenAll,
		// vanity.TurnOnBenchGenAll,

		vanity.TurnOnMarshalerAll,
		vanity.TurnOnUnmarshalerAll,
		vanity.TurnOnSizerAll,

		// We want marshaled protobufs to be deterministic so that they can be
		// compared byte-for-byte. At the time of writing, this is depended upon by
		// the consistency checker.
		vanity.TurnOnStable_MarshalerAll,

		// Enabling these causes `String() string` on Enums to be inlined.
		// Not worth it.
		// vanity.TurnOffGoEnumStringerAll,
		// vanity.TurnOnEnumStringerAll,

		// Not clear that this is worthwhile.
		// vanity.TurnOnUnsafeUnmarshalerAll,
		// vanity.TurnOnUnsafeMarshalerAll,

		// Something something extensions; we don't use 'em currently.
		// vanity.TurnOffGoExtensionsMapAll,

		vanity.TurnOffGoUnrecognizedAll,

		// Adds unnecessary dependency on golang/protobuf.
		// vanity.TurnOffGogoImport,
	} {
		vanity.ForEachFile(files, opt)
	}

	resp := command.Generate(req)
	command.Write(resp)
}
