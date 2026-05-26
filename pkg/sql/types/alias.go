// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

// PublicSchemaAliases contain a mapping from type name to builtin types
// which are on the public schema on PostgreSQL as they are available
// as an extension.
var PublicSchemaAliases = map[string]*T{
	"box2d":     Box2D,
	"geometry":  Geometry,
	"geography": Geography,
}
