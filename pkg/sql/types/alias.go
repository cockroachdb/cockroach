// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

// PublicSchemaAliases contain a mapping from type name to builtin types
// which are on the public schema on PostgreSQL as they are available
// as an extension.
var PublicSchemaAliases = map[string]*T{
	"box2d":     Box2D,
	"geometry":  Geometry,
	"geography": Geography,
}
