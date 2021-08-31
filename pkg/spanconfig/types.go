// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// Update captures a span and the corresponding config change. It's the unit of
// what can be applied to a StoreWriter.
type Update struct {
	// Span captures the key span being updated.
	Span roachpb.Span

	// Config captures the span config the key span was updated to. An empty
	// config indicates the span config being deleted.
	Config roachpb.SpanConfig
}

// Deletion returns true if the update corresponds to a span config being
// deleted.
func (u Update) Deletion() bool {
	return u.Config.IsEmpty()
}

// Addition returns true if the update corresponds to a span config being
// added.
func (u Update) Addition() bool {
	return !u.Deletion()
}

// DescriptorUpdate captures the ID and type of a descriptor or zone that the
// SQLWatcher has observed change.
type DescriptorUpdate struct {
	// ID of the descriptor/zone that has changed.
	ID descpb.ID

	// DescriptorType of the descriptor/zone that has changed. Could be either the
	// specific type or Any if no information is available.
	DescriptorType DescriptorType
}

// DescriptorType represents a {Table,Database,Schema,Type,Any} Descriptor type.
// {Table,Database,Schema,Type} descriptor types are concrete descriptor types
// for the purposes of the Combine semantics described below.
type DescriptorType uint32

const (
	// Table represents a TableDescriptor type.
	Table DescriptorType = 1
	// Database represents a DatabaseDescriptor type.
	Database DescriptorType = 2
	// Schema represents a SchemaDescriptor type.
	Schema DescriptorType = 3
	// Type represents a TypeDescriptor type.
	Type DescriptorType = 4
	// Any represents any descriptor type.
	Any DescriptorType = 5
)

// descriptorTypeNames maps DescriptorTypes to their string representions.
var descriptorTypeNames = map[DescriptorType]string{
	Table:    "table",
	Database: "database",
	Schema:   "schema",
	Type:     "type",
	Any:      "any",
}

// Combine takes two DescriptorTypes and combines them according to the
// following semantics:
// - Any can combine with any concrete descriptor type (including itself).
// - Concrete descriptor types can combine with themselves.
// - A concrete descriptor type cannot combine with another concrete descriptor
// type.
func Combine(d1, d2 DescriptorType) (DescriptorType, error) {
	if d1 == d2 {
		return d1, nil
	}
	if d1 == Any {
		return d2, nil
	}
	if d2 == Any {
		return d1, nil
	}
	return Any, errors.AssertionFailedf(
		"cannot combine %s and %s", descriptorTypeNames[d1], descriptorTypeNames[d2],
	)
}
