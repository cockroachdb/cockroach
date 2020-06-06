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
	aggKindTmplVar = "_AGG_KIND"
	hashAggKind    = "Hash"
	orderedAggKind = "Ordered"
)

func registerAggGenerator(aggGen generator, filenameWithSingleFormatDirective, dep string) {
	aggGeneratorAdapter := func(aggKind string) generator {
		return func(inputFileContents string, wr io.Writer) error {
			r := strings.NewReplacer(
				"_IF_HAS_ONLY_ONE_GROUP", fmt.Sprintf(`{{if eq "%s" "%s"}}`, aggKindTmplVar, hashAggKind),
				"_IF_CAN_HAVE_MULTIPLE_GROUPS", fmt.Sprintf(`{{if eq "%s" "%s"}}`, aggKindTmplVar, orderedAggKind),
			)
			inputFileContents = r.Replace(inputFileContents)
			// We cannot include this replacement into the replacer above because
			// the replacements depend on each other which the replacer doesn't
			// support.
			inputFileContents = strings.ReplaceAll(inputFileContents, aggKindTmplVar, aggKind)
			return aggGen(inputFileContents, wr)
		}
	}
	for _, aggKind := range []string{hashAggKind, orderedAggKind} {
		registerGenerator(
			aggGeneratorAdapter(aggKind),
			fmt.Sprintf(filenameWithSingleFormatDirective, strings.ToLower(aggKind)),
			dep,
		)
	}
}
