// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// NodeString formats a node as a string by invoking FormatNode.
func NodeString(n *Node) string {
	var v redact.StringBuilder
	if err := FormatNode(&v, n); err != nil {
		return fmt.Sprintf("failed for format node: %v", err)
	}
	return v.String()
}

// FormatNode formats the node into the SafeWriter.
func FormatNode(w redact.SafeWriter, e *Node) (err error) {
	w.SafeString("[[")
	if err := FormatElement(w, e.Element()); err != nil {
		return err
	}
	w.SafeString(", ")
	w.SafeString(redact.SafeString(e.Target.TargetStatus.String()))
	w.SafeString("], ")
	w.SafeString(redact.SafeString(e.CurrentStatus.String()))
	w.SafeString("]")
	return nil
}

// ElementString formats an element as a string by invoking FormatElement.
func ElementString(e scpb.Element) string {
	var v redact.StringBuilder
	if err := FormatElement(&v, e); err != nil {
		return fmt.Sprintf("failed for format element %T: %v", e, err)
	}
	return v.String()
}

// FormatElement formats the element into the SafeWriter.
func FormatElement(w redact.SafeWriter, e scpb.Element) (err error) {
	if e == nil {
		return errors.Errorf("nil element")
	}
	w.SafeString(redact.SafeString(reflect.TypeOf(e).Elem().Name()))
	w.SafeString(":{")
	var written int
	if err := Schema.IterateAttributes(e, func(attr rel.Attr, value interface{}) error {
		switch attr {
		case TemporaryIndexID, SourceIndexID:
			if value == descpb.IndexID(0) {
				return nil
			}
		}
		if written > 0 {
			w.SafeString(", ")
		}
		written++
		// Change the type of strings so that they get quoted appropriately.
		if str, isStr := value.(string); isStr {
			value = tree.Name(str)
		}
		w.SafeString(redact.SafeString(attr.String()))
		w.SafeString(": ")
		w.Printf("%v", value)
		return nil
	}); err != nil {
		return err
	}
	w.SafeRune('}')
	return nil
}

// FormatTargetElement formats the target element into the SafeWriter.
// This differs from FormatElement in that an attempt is made to decorate
// the IDs with names.
func FormatTargetElement(w redact.SafeWriter, ts scpb.TargetState, e scpb.Element) (err error) {
	w.SafeString(redact.SafeString(reflect.TypeOf(e).Elem().Name()))
	w.SafeString(":{")
	maybePrintName := func(name string) {
		if !strings.HasSuffix(name, "]") {
			w.UnsafeString(" (")
			w.UnsafeString(name)
			w.UnsafeString(")")
		}
	}
	var written int
	descID := GetDescID(e)
	if err := Schema.IterateAttributes(e, func(attr rel.Attr, value interface{}) error {
		switch attr {
		case ReferencedDescID:
			if value == descpb.ID(0) {
				return nil
			}
		case TemporaryIndexID, SourceIndexID:
			if value == descpb.IndexID(0) {
				return nil
			}
		case ColumnID:
			if value == descpb.ColumnID(0) {
				return nil
			}
		case ConstraintID:
			if value == descpb.ConstraintID(0) {
				return nil
			}
		}
		if written > 0 {
			w.SafeString(", ")
		}
		written++
		w.SafeString(redact.SafeString(attr.String()))
		w.SafeString(": ")
		switch attr {
		case DescID, ReferencedDescID:
			id, ok := value.(descpb.ID)
			if !ok {
				return errors.AssertionFailedf("unexpected type %T for %s value in %T", value, attr, e)
			}
			w.SafeUint(redact.SafeUint(id))
			maybePrintName(ts.Name(id))
		case ReferencedTypeIDs, ReferencedSequenceIDs, ReferencedFunctionIDs:
			ids, ok := value.([]descpb.ID)
			if !ok {
				return errors.AssertionFailedf("unexpected type %T for %s value in %T", value, attr, e)
			}
			w.SafeString("[")
			for i, id := range ids {
				if i > 0 {
					w.SafeString(", ")
				}
				w.Printf("%d", id)
				maybePrintName(ts.Name(id))
			}
			w.SafeString("]")
		case IndexID, TemporaryIndexID, SourceIndexID:
			indexID, ok := value.(descpb.IndexID)
			if !ok {
				return errors.AssertionFailedf("unexpected type %T for %s value in %T", value, attr, e)
			}
			w.Printf("%d", indexID)
			maybePrintName(ts.IndexName(descID, indexID))
		case ColumnFamilyID:
			familyID, ok := value.(descpb.FamilyID)
			if !ok {
				return errors.AssertionFailedf("unexpected type %T for %s value in %T", value, attr, e)
			}
			w.Printf("%d", familyID)
			maybePrintName(ts.FamilyName(descID, familyID))
		case ColumnID:
			columnID, ok := value.(descpb.ColumnID)
			if !ok {
				return errors.AssertionFailedf("unexpected type %T for %s value in %T", value, attr, e)
			}
			w.Printf("%d", columnID)
			maybePrintName(ts.ColumnName(descID, columnID))
		case ConstraintID:
			constraintID, ok := value.(descpb.ConstraintID)
			if !ok {
				return errors.AssertionFailedf("unexpected type %T for %s value in %T", value, attr, e)
			}
			w.Printf("%d", constraintID)
			maybePrintName(ts.ConstraintName(descID, constraintID))
		default:
			if str, isStr := value.(string); isStr {
				w.Printf("%q", str)
			} else {
				w.Printf("%v", value)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	w.SafeRune('}')
	return nil
}
