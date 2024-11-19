// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const (
	// aggKindTmplVar specifies the template "variable" that describes the kind
	// of aggregator using an aggregate function. It is replaced with "Hash",
	// "Ordered", or "Window" before executing the template.
	aggKindTmplVar = "_AGGKIND"
	aggNameTmplVar = "_AGGNAME"
	hashAggKind    = "Hash"
	orderedAggKind = "Ordered"
	windowAggKind  = "Window"
)

func registerAggGenerator(
	aggGen generator, filenameSuffix, dep, aggName string, genWindowVariant bool,
) {
	aggGeneratorAdapter := func(aggKind string) generator {
		return func(inputFileContents string, wr io.Writer) error {
			inputFileContents = strings.ReplaceAll(inputFileContents, "var _ = _ALLOC_CODE", `
// {{if eq "_AGGKIND" "Ordered"}}

func init() {
	// Sanity check the hard-coded number of overloads.
	var numOverloads int
	// {{range .}}
	// {{range .WidthOverloads}}
	numOverloads++
	// {{end}}
	// {{end}}
	if numOverloads != _AGGNAMENumOverloads {
		colexecerror.InternalError(errors.AssertionFailedf(
			"_AGGNAMENumOverloads should be updated: expected %d, found %d", numOverloads, _AGGNAMENumOverloads,
		))
	}
}

// _AGGNAMEOverloadOffset returns the offset for this particular type overload
// within contiguous slice of allocators for this aggregate function. 
func _AGGNAMEOverloadOffset(t *types.T) int {
	var offset int
	canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(t.Family())
	// {{range .}}
	if canonicalTypeFamily == _CANONICAL_TYPE_FAMILY {
		// {{range .WidthOverloads}}
		// {{if eq .Width -1}}
		return offset
		// {{else}}
		if t.Width() == {{.Width}} {
			return offset
		}
		offset++
		// {{end}}
		// {{end}}
	}
	offset += {{len .WidthOverloads}}
	// {{end}}
	colexecerror.InternalError(errors.AssertionFailedf("didn't find overload offset for %s", t.SQLStringForError()))
	return 0
}

// {{end}}
`)
			inputFileContents = strings.ReplaceAll(inputFileContents, aggKindTmplVar, aggKind)
			inputFileContents = strings.ReplaceAll(inputFileContents, aggNameTmplVar, aggName)
			return aggGen(inputFileContents, wr)
		}
	}
	aggKinds := []string{hashAggKind, orderedAggKind}
	if genWindowVariant {
		aggKinds = append(aggKinds, windowAggKind)
	}
	for _, aggKind := range aggKinds {
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

// CopyVal is a function that should only be used in templates.
func (b *aggTmplInfoBase) CopyVal(dest, src string) string {
	return copyVal(b.canonicalTypeFamily, dest, src)
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
	_ = a.CopyVal
)
