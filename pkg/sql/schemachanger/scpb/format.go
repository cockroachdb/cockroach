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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/errors"
)

// ToString renders an element's attributes to a string.
func ToString(e Entity) string {
	var buf strings.Builder
	Format(e, &buf)
	return buf.String()
}

// Format formats an attribute value.
func (a Attr) Format(val eav.Value, w io.Writer) (err error) {
	switch a {
	case AttrDirection:
		_, err = fmt.Fprintf(w, "%s", (*Target_Direction)(val.(*eav.Int32)))
	case AttrStatus:
		_, err = fmt.Fprintf(w, "%s", (*Status)(val.(*eav.Int32)))
	case AttrElementType:
		tid := *(*typeID)(val.(*eav.Int32))
		s, ok := elementNames[tid]
		if !ok {
			_, err = fmt.Fprintf(w, "%d", tid)
		} else {
			_, err = fmt.Fprintf(w, "%s", s)
		}
	default:
		_, err = fmt.Fprintf(w, "%s", val)
	}
	return err
}

// FormatAttr is a shorthand for a.Format(e.Get(a), w).
func FormatAttr(e Entity, a Attr, w io.Writer) error {
	return a.Format(e.Get(a), w)
}

// Format serializes attribute into a writer.
func Format(e Entity, w io.Writer) (err error) {
	panicIf := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	panicIfN := func(_ int, err error) { panicIf(err) }
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok && err == nil {
				err = errors.WithStack(rErr)
				return
			}
			panic(r)
		}
	}()
	var isContainer bool
	switch e.(type) {
	case *Node, *Target:
		isContainer = true
	}

	if isContainer {
		panicIfN(io.WriteString(w, "["))
	}
	elem := e.GetElement()
	panicIf(FormatAttr(e, AttrElementType, w))
	panicIfN(io.WriteString(w, ": {"))
	var written int
	elem.Attributes().Remove(AttrElementType.Ordinal()).ForEach(attrSet, func(
		a eav.Attribute,
	) (wantMore bool) {
		if written > 0 {
			panicIfN(io.WriteString(w, ", "))
		}
		written++
		panicIfN(fmt.Fprintf(w, "%s: ", a))
		panicIf(FormatAttr(elem, a.(Attr), w))
		return true
	})
	panicIfN(io.WriteString(w, "}"))
	if isContainer {
		panicIfN(io.WriteString(w, ", "))
		if status := e.Get(AttrStatus); status != nil {
			panicIf(FormatAttr(e, AttrStatus, w))
			panicIfN(io.WriteString(w, ", "))
		}
		panicIf(FormatAttr(e, AttrDirection, w))
		panicIfN(io.WriteString(w, "]"))
	}
	return nil
}
