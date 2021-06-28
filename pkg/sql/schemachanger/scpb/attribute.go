// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
)

// Attribute are keys used for finger prints of objects
// for comparing uniqueness
type Attribute int

//go:generate stringer -type=Attribute  -trimprefix=Attribute
const (

	// AttributeType type id of the element.
	AttributeType Attribute = iota
	// AttributeDescID is the descriptor ID to which this element belongs.
	AttributeDescID
	// AttributeReferencedDescID is the descriptor ID to which this element refers.
	AttributeReferencedDescID
	//AttributeColumnID is the column ID to which this element corresponds.
	AttributeColumnID
	// AttributeElementName is the name of the element.
	AttributeElementName
	// AttributeIndexID is the index ID to which this element corresponds.
	AttributeIndexID

	numAttributes int = iota
)

var attributeOrder = [numAttributes]Attribute{
	0: AttributeType,
	1: AttributeDescID,
	2: AttributeReferencedDescID,
	3: AttributeColumnID,
	4: AttributeElementName,
	5: AttributeIndexID,
}

// CompareElements compares two elements by their attributes.
func CompareElements(a, b Element) (less, eq bool) {
	for i := 0; i < numAttributes; i++ {
		less, eq := CompareAttribute(attributeOrder[i], a, b)
		if !eq {
			return less, false
		}
	}
	return false, true
}

// EqualElements returns true if the two elements have identical attributes.
func EqualElements(a, b Element) bool {
	_, eq := CompareElements(a, b)
	return eq
}

// CompareAttribute compares two elements on a given attribute.
func CompareAttribute(attr Attribute, a, b Element) (less, eq bool) {
	av, bv := a.getAttribute(attr), b.getAttribute(attr)
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

// AttributesString renders an element's attributes to a string.
func AttributesString(e Element) string {
	var buf strings.Builder
	FormatAttributes(e, &buf)
	return buf.String()
}

// FormatAttributes serializes attribute into a writer.
func FormatAttributes(e Element, w io.Writer) {
	fmt.Fprintf(w, "%s:{", e.getAttribute(AttributeType))
	var written int
	for i := 0; i < numAttributes; i++ {
		attr := attributeOrder[i]
		if attr == AttributeType {
			continue
		}
		av := e.getAttribute(attr)
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
