// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package typedesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/redact"
)

// SafeMessage makes immutable a SafeMessager.
func (desc *immutable) SafeMessage() string {
	return formatSafeType("typedesc.immutable", desc)
}

// SafeMessage makes Mutable a SafeMessager.
func (desc *Mutable) SafeMessage() string {
	return formatSafeType("typedesc.Mutable", desc)
}

func formatSafeType(typeName string, desc catalog.TypeDescriptor) string {
	var buf redact.StringBuilder
	buf.Printf(typeName + ": {")
	formatSafeTypeProperties(&buf, desc)
	buf.Printf("}")
	return buf.String()
}

func formatSafeTypeProperties(w *redact.StringBuilder, desc catalog.TypeDescriptor) {
	catalog.FormatSafeDescriptorProperties(w, desc)
	td := desc.TypeDesc()
	w.Printf(", Kind: %s", td.Kind)
	if len(td.EnumMembers) > 0 {
		w.Printf(", NumEnumMembers: %d", len(td.EnumMembers))
	}
	if td.Alias != nil {
		w.Printf(", Alias: %d", td.Alias.Oid())
	}
	if td.ArrayTypeID != 0 {
		w.Printf(", ArrayTypeID: %d", td.ArrayTypeID)
	}
	for i := range td.ReferencingDescriptorIDs {
		w.Printf(", ")
		if i == 0 {
			w.Printf("ReferencingDescriptorIDs: [")
		}
		w.Printf("%d", td.ReferencingDescriptorIDs[i])
	}
	if len(td.ReferencingDescriptorIDs) > 0 {
		w.Printf("]")
	}
}
