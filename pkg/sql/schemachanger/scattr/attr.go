// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scattr

import (
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

// Attr are keys used for finger prints of objects
// for comparing uniqueness
type Attr int

//go:generate stringer -type=Attr
const (

	// Type type id of the element.
	Type Attr = iota
	// DescID is the descriptor ID to which this element belongs.
	DescID
	// ReferencedDescID is the descriptor ID to which this element refers.
	ReferencedDescID
	//ColumnID is the column ID to which this element corresponds.
	ColumnID
	// Name is the name of the element.
	Name
	// IndexID is the index ID to which this element corresponds.
	IndexID

	numAttributes int = iota
)

var attributeOrder = [numAttributes]Attr{
	0: Type,
	1: DescID,
	2: ReferencedDescID,
	3: ColumnID,
	4: Name,
	5: IndexID,
}

// Compare compares two elements by their attributes.
func Compare(a, b scpb.Element) (less, eq bool) {
	for i := 0; i < numAttributes; i++ {
		less, eq := CompareOn(attributeOrder[i], a, b)
		if !eq {
			return less, false
		}
	}
	return false, true
}

// Equal returns true if the two elements have identical attributes.
func Equal(a, b scpb.Element) bool {
	_, eq := Compare(a, b)
	return eq
}

// CompareOn compares two elements on a given attribute.
func CompareOn(attr Attr, a, b scpb.Element) (less, eq bool) {
	av, bv := Get(attr, a), Get(attr, b)
	switch {
	case av == nil && bv == nil:
		return false, true
	case av == nil:
		return false, false
	case bv == nil:
		return true, false
	default:
		var ok bool
		ok, less, eq = av.compare(bv)
		if !ok {
			// See TestElementAttributeValueTypesMatch for why this is safe.
			panic(errors.AssertionFailedf(
				"type mismatch (%T, %T) for attribute %s", av, bv, attr))
		}
		return less, eq
	}
}

// ToString renders an element's attributes to a string.
func ToString(e scpb.Element) string {
	var buf strings.Builder
	Format(e, &buf)
	return buf.String()
}

// Format serializes attribute into a writer.
func Format(e scpb.Element, w io.Writer) {
	fmt.Fprintf(w, "%s:{", Get(Type, e))
	var written int
	for i := 0; i < numAttributes; i++ {
		attr := attributeOrder[i]
		if attr == Type {
			continue
		}
		av := Get(attr, e)
		if av == nil {
			continue
		}
		if written > 0 {
			_, _ = io.WriteString(w, ", ")
		}
		written++
		fmt.Fprintf(w, "%s: %s", attr, av)
	}
	_, _ = io.WriteString(w, "}")
}
