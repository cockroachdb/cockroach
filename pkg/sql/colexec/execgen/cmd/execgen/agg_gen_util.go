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

	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

// aggTmplInfoBase is a helper struct used in generating the code of many
// aggregates serving as a base for calling methods on (whenever
// lastArgWidthOverload isn't available).
type aggTmplInfoBase struct {
	// canonicalTypeFamily is the canonical type family of the current aggregate
	// object used by the aggregate function.
	canonicalTypeFamily types.Family
}

// SetVariableSize is a function that should only be used in templates. See the
// comment on setVariableSize for more details.
func (b aggTmplInfoBase) SetVariableSize(target, value string) string {
	return setVariableSize(b.canonicalTypeFamily, target, value)
}

// Remove unused warning.
var (
	a aggTmplInfoBase
	_ = a.SetVariableSize
)
