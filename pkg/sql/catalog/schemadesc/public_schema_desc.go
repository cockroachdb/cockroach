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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetPublicSchema returns a synthetic public schema which is
// part of every database. The public schema's implementation is a vestige
// of a time when there were no user-defined schemas. The public schema is
// interchangeable with the database itself in terms of privileges.
//
// The returned descriptor carries only a basic functionality, requiring the
// caller to check the SchemaKind to determine how to use the descriptor. The
// returned descriptor is not mapped to a database; every database has all of
// the same virtual schemas and the ParentID on the returned descriptor will be
// descpb.InvalidID.
func GetPublicSchema() catalog.SchemaDescriptor {
	return publicDesc
}

type public struct {
	synthetic
}

var _ catalog.SchemaDescriptor = public{}

func (p public) GetParentID() descpb.ID { return descpb.InvalidID }
func (p public) GetID() descpb.ID       { return keys.PublicSchemaID }
func (p public) GetName() string        { return tree.PublicSchema }

type publicBase struct{}

func (p publicBase) kindName() string                 { return "public" }
func (p publicBase) kind() catalog.ResolvedSchemaKind { return catalog.SchemaPublic }

// publicDesc is a singleton returned by GetPublicSchema.
var publicDesc catalog.SchemaDescriptor = public{
	synthetic{publicBase{}},
}
