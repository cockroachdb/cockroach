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
	"regexp"
	"strings"

	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/gogo/protobuf/vanity"
	"github.com/gogo/protobuf/vanity/command"
)

// As we invoke it, the generator will sometimes prepend the cockroachdb github
// URL to what should be unqualified standard library imports. This regexp
// allows us to identify and fix those bad imports.
var builtinRegex *regexp.Regexp = regexp.MustCompile(`github.com/cockroachdb/cockroach/pkg/(?P<capture>(bytes|context|encoding/binary|errors|fmt|io|math|github\.com|(google\.)?golang\.org)([^a-z]|$$))`)

func fixImports(s string) string {
	lines := strings.Split(s, "\n")
	var builder strings.Builder
	for _, line := range lines {
		if strings.Contains(line, "import _ ") ||
			strings.Contains(line, "import fmt \"github.com/cockroachdb/cockroach/pkg/fmt\"") ||
			strings.Contains(line, "import math \"github.com/cockroachdb/cockroach/pkg/math\"") {
			continue
		}

		line = strings.ReplaceAll(line, "github.com/cockroachdb/cockroach/pkg/etcd", "go.etcd.io/etcd")
		line = strings.ReplaceAll(line, "github.com/cockroachdb/cockroach/pkg/errorspb", "github.com/cockroachdb/errors/errorspb")
		line = strings.ReplaceAll(line, "golang.org/x/net/context", "context")
		if builtinRegex.MatchString(line) {
			line = builtinRegex.ReplaceAllString(line, "$1")
		}
		builder.WriteString(line)
		builder.WriteByte('\n')
	}
	return builder.String()
}

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
	for i := 0; i < len(resp.File); i++ {
		*resp.File[i].Content = fixImports(*resp.File[i].Content)
	}
	command.Write(resp)
}
