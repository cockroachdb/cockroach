// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemadesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// GetVirtualSchemaByID returns a virtual schema with a given ID if it exists.
//
// The returned descriptor carries only a basic functionality, requiring the
// caller to check the SchemaKind to determine how to use the descriptor. The
// returned descriptor is not mapped to a database; every database has all of
// the same virtual schemas and the ParentID on the returned descriptor will be
// descpb.InvalidID.
func GetVirtualSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, bool) {
	sc, ok := virtualSchemasByID[id]
	return sc, ok
}

var virtualSchemasByID = func() map[descpb.ID]catalog.SchemaDescriptor {
	m := make(map[descpb.ID]catalog.SchemaDescriptor, len(catconstants.StaticSchemaIDMap))
	for id, name := range catconstants.StaticSchemaIDMap {
		id := descpb.ID(id)
		sc := virtual{
			synthetic: synthetic{virtualBase{}},
			id:        id,
			name:      name,
		}
		m[id] = sc
	}
	return m
}()

// virtual represents the virtual schemas which are part of every database.
// See the commentary on GetVirtualSchemaByID.
type virtual struct {
	synthetic
	id   descpb.ID
	name string
}

var _ catalog.SchemaDescriptor = virtual{}

func (p virtual) GetID() descpb.ID       { return p.id }
func (p virtual) GetName() string        { return p.name }
func (p virtual) GetParentID() descpb.ID { return descpb.InvalidID }
func (p virtual) GetPrivileges() *descpb.PrivilegeDescriptor {
	return descpb.NewPublicSelectPrivilegeDescriptor()
}

type virtualBase struct{}

var _ syntheticBase = virtualBase{}

func (v virtualBase) kindName() string                 { return "virtual" }
func (v virtualBase) kind() catalog.ResolvedSchemaKind { return catalog.SchemaVirtual }
