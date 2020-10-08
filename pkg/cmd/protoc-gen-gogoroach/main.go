// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

		// Disable generation of the following fields, which aren't worth
		// their associated runtime cost:
		// - XXX_unrecognized
		// - XXX_NoUnkeyedLiteral
		// - XXX_sizecache
		vanity.TurnOffGoUnrecognizedAll,
		vanity.TurnOffGoUnkeyedAll,
		vanity.TurnOffGoSizecacheAll,

		// Adds unnecessary dependency on golang/protobuf.
		// vanity.TurnOffGogoImport,
	} {
		vanity.ForEachFile(files, opt)
	}

	resp := command.Generate(req)
	command.Write(resp)
}
