// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package oidext contains oids that are not in `github.com/lib/pq/oid`
// as they are not shipped by default with postgres.
// As CRDB does not support extensions, we'll need to automatically assign
// a few OIDs of our own.
package oidext

import "github.com/lib/pq/oid"

// CockroachPredefinedOIDMax defines the maximum OID allowed for use by non user
// defined types/functions. OIDs for user defined types/functions will start at
// CockroachPrefixedOIDMax and increase as new types are created. User defined
// type/function descriptors have a cluster-wide unique stable ID.
// CockroachPredefinedOIDMax defines the mapping from this stable ID to a
// type/function OID. In particular, stable ID + CockroachPredefinedOIDMax =
// type OID. types.StableTypeIDToOID and types.UserDefinedTypeOIDToID should be
// used when converting between stable ID's and type/function OIDs.
const CockroachPredefinedOIDMax = 100000

// OIDs in this block are extensions of postgres, thus having no official OID.
const (
	T_geometry   = oid.Oid(90000)
	T__geometry  = oid.Oid(90001)
	T_geography  = oid.Oid(90002)
	T__geography = oid.Oid(90003)
	T_box2d      = oid.Oid(90004)
	T__box2d     = oid.Oid(90005)
	T_pgvector   = oid.Oid(90006)
	T__pgvector  = oid.Oid(90007)
)

// OIDs in this block are not extensions of postgres, but are not supported in
// github.com/lib/pq/oid. See postgres/src/include/catalog/pg_type.dat for oids.
const (
	T_jsonpath  = oid.Oid(4072)
	T__jsonpath = oid.Oid(4073)
)

// ExtensionTypeName returns a mapping from extension oids
// to their type name.
var ExtensionTypeName = map[oid.Oid]string{
	T_geometry:   "GEOMETRY",
	T__geometry:  "_GEOMETRY",
	T_geography:  "GEOGRAPHY",
	T__geography: "_GEOGRAPHY",
	T_box2d:      "BOX2D",
	T__box2d:     "_BOX2D",
	T_pgvector:   "VECTOR",
	T__pgvector:  "_VECTOR",
	T_jsonpath:   "JSONPATH",
	T__jsonpath:  "_JSONPATH",
}

// TypeName checks the name for a given type by first looking up oid.TypeName
// before falling back to looking at the oid extension ExtensionTypeName.
func TypeName(o oid.Oid) (string, bool) {
	name, ok := oid.TypeName[o]
	if ok {
		return name, ok
	}
	name, ok = ExtensionTypeName[o]
	return name, ok
}
