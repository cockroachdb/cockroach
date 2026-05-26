// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemadesc

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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
// This is deprecated and should not be used except for certain edge cases.
// This will be removed in 22.2 completely.
func GetPublicSchema() catalog.SchemaDescriptor {
	return publicDesc
}

type public struct {
	synthetic
}

var _ catalog.SchemaDescriptor = public{}
var _ privilege.Object = public{}

func (p public) GetID() descpb.ID                                  { return keys.PublicSchemaID }
func (p public) GetParentID() descpb.ID                            { return descpb.InvalidID }
func (p public) GetName() string                                   { return catconstants.PublicSchemaName }
func (p public) SchemaDesc() *descpb.SchemaDescriptor              { return makeSyntheticSchemaDesc(p) }
func (p public) DescriptorProto() *descpb.Descriptor               { return makeSyntheticDesc(p) }
func (p public) GetReplicatedPCRVersion() descpb.DescriptorVersion { return 0 }

type publicBase struct{}

var _ syntheticBase = publicBase{}

func (publicBase) kindName() string                 { return "public" }
func (publicBase) kind() catalog.ResolvedSchemaKind { return catalog.SchemaPublic }
func (publicBase) GetPrivileges() *catpb.PrivilegeDescriptor {
	// As of this writing (May 2024), it has been the case for many releases that
	// the only usage of this synthetic public schema is for the system database,
	// so it is owned by node.
	return catpb.NewPublicSchemaPrivilegeDescriptor(username.NodeUserName(), true /*includeCreatePriv*/)
}

// publicDesc is a singleton returned by GetPublicSchema.
var publicDesc catalog.SchemaDescriptor = public{synthetic{publicBase{}}}
