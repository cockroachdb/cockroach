// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemadesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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
var _ privilege.Object = virtual{}

func (p virtual) GetID() descpb.ID                                  { return p.id }
func (p virtual) GetName() string                                   { return p.name }
func (p virtual) GetParentID() descpb.ID                            { return descpb.InvalidID }
func (p virtual) SchemaDesc() *descpb.SchemaDescriptor              { return makeSyntheticSchemaDesc(p) }
func (p virtual) DescriptorProto() *descpb.Descriptor               { return makeSyntheticDesc(p) }
func (p virtual) GetReplicatedPCRVersion() descpb.DescriptorVersion { return 0 }

type virtualBase struct{}

var _ syntheticBase = virtualBase{}

func (virtualBase) kindName() string                 { return "virtual" }
func (virtualBase) kind() catalog.ResolvedSchemaKind { return catalog.SchemaVirtual }
func (virtualBase) GetPrivileges() *catpb.PrivilegeDescriptor {
	return catpb.NewVirtualSchemaPrivilegeDescriptor()
}
