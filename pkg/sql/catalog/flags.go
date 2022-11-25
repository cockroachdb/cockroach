// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CommonLookupFlags is the common set of flags for the various accessor interfaces.
type CommonLookupFlags struct {
	// if required is set, lookup will return an error if the item is not found.
	Required bool
	// RequireMutable specifies whether to return a mutable descriptor.
	RequireMutable bool
	// AvoidLeased, if set, avoid the leased (possibly stale) version of the
	// descriptor. It must be set when callers want consistent reads.
	AvoidLeased bool
	// AvoidCommittedAdding specifies if committed descriptors in the adding state
	// will be ignored.
	AvoidCommittedAdding bool
	// IncludeOffline specifies if offline descriptors should be visible.
	IncludeOffline bool
	// IncludeOffline specifies if dropped descriptors should be visible.
	IncludeDropped bool
	// AvoidSynthetic specifies if the synthetic descriptors will be ignored.
	AvoidSynthetic bool
	// AvoidStorage specifies if the descriptors in storage will be ignored.
	AvoidStorage bool
	// ParentID enforces that the resolved descriptor exist with this parent
	// ID if non-zero.
	ParentID catid.DescID
}

// SchemaLookupFlags is the flag struct suitable for GetSchemaByName().
type SchemaLookupFlags = CommonLookupFlags

// DatabaseLookupFlags is the flag struct suitable for GetImmutableDatabaseByName().
type DatabaseLookupFlags = CommonLookupFlags

// DatabaseListFlags is the flag struct suitable for GetObjectNamesAndIDs().
type DatabaseListFlags struct {
	CommonLookupFlags
	// ExplicitPrefix, when set, will cause the returned table names to
	// have an explicit schema and catalog part.
	ExplicitPrefix bool
}

// DesiredObjectKind represents what kind of object is desired in a name
// resolution attempt.
type DesiredObjectKind byte

const (
	// TableObject is used when a table-like object is desired from resolution.
	TableObject DesiredObjectKind = iota
	// TypeObject is used when a type-like object is desired from resolution.
	TypeObject
)

// ObjectLookupFlags is the flag struct suitable for GetObjectByName().
type ObjectLookupFlags struct {
	CommonLookupFlags
	AllowWithoutPrimaryKey bool
	// Control what type of object is being requested.
	DesiredObjectKind DesiredObjectKind
	// Control what kind of table object is being requested. This field is
	// only respected when DesiredObjectKind is TableObject.
	DesiredTableDescKind tree.RequiredTableKind
}

// ObjectLookupFlagsWithRequired returns a default ObjectLookupFlags object
// with just the Required flag true. This is a common configuration of the
// flags.
func ObjectLookupFlagsWithRequired() ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: CommonLookupFlags{Required: true},
	}
}

// ObjectLookupFlagsWithRequiredTableKind returns an ObjectLookupFlags with
// Required set to true, and the DesiredTableDescKind set to the input kind.
func ObjectLookupFlagsWithRequiredTableKind(kind tree.RequiredTableKind) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags:    CommonLookupFlags{Required: true},
		DesiredObjectKind:    TableObject,
		DesiredTableDescKind: kind,
	}
}
