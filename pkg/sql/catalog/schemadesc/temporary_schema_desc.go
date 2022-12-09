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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// NewTemporarySchema returns a temporary schema with a given name, id, and
// parent. Temporary schemas do not have a durable descriptor in the store;
// they only have a namespace entry to indicate their existence. Given that,
// a different kind of "synthetic" descriptor is used to indicate temporary
// schemas.
//
// The returned descriptor carries only a basic functionality, requiring the
// caller to check the SchemaKind to determine how to use the descriptor.
func NewTemporarySchema(name string, id descpb.ID, parentDB descpb.ID) catalog.SchemaDescriptor {
	return &temporary{
		synthetic: synthetic{temporaryBase{}},
		id:        id,
		name:      name,
		parentID:  parentDB,
	}
}

// temporary represents the synthetic temporary schema.
type temporary struct {
	synthetic
	id       descpb.ID
	name     string
	parentID descpb.ID
}

var _ catalog.SchemaDescriptor = temporary{}
var _ privilege.Object = temporary{}

func (p temporary) GetID() descpb.ID                     { return p.id }
func (p temporary) GetName() string                      { return p.name }
func (p temporary) GetParentID() descpb.ID               { return p.parentID }
func (p temporary) SchemaDesc() *descpb.SchemaDescriptor { return makeSyntheticSchemaDesc(p) }
func (p temporary) DescriptorProto() *descpb.Descriptor  { return makeSyntheticDesc(p) }

type temporaryBase struct{}

var _ syntheticBase = temporaryBase{}

func (temporaryBase) kindName() string                 { return "temporary" }
func (temporaryBase) kind() catalog.ResolvedSchemaKind { return catalog.SchemaTemporary }
func (temporaryBase) GetPrivileges() *catpb.PrivilegeDescriptor {
	return catpb.NewTemporarySchemaPrivilegeDescriptor()
}
