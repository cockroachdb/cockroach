// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"io"
	"strings"
)

const (
	// aggKindTmplVar specifies the template "variable" that describes the kind
	// of aggregator using an aggregate function. It is replaced with either
	// "Hash" or "Ordered" before executing the template.
	aggKindTmplVar = "_AGGKIND"
	hashAggKind    = "Hash"
	orderedAggKind = "Ordered"
)

func registerAggGenerator(aggGen generator, filenameSuffix, dep string) {
	aggGeneratorAdapter := func(aggKind string) generator {
		return func(inputFileContents string, wr io.Writer) error {
			inputFileContents = strings.ReplaceAll(inputFileContents, aggKindTmplVar, aggKind)
			return aggGen(inputFileContents, wr)
		}
	}
	for _, aggKind := range []string{hashAggKind, orderedAggKind} {
		registerGenerator(
			aggGeneratorAdapter(aggKind),
			fmt.Sprintf("%s_%s", strings.ToLower(aggKind), filenameSuffix),
			dep,
		)
	}
}
