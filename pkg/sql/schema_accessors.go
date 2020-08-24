// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// This file provides high-level interfaces to abstract access to the
// SQL schema (the descriptors).
//
// The following interfaces are defined:
// - DatabaseAccessor, which provides access to database descriptors.
// - DatabaseLister, which provides a service to list the contents of a database.
// - ObjectAccessor, which provides access to individual object descriptors.
//
// A common interface Accessor is provided for convenience.
//
// See physical_schema_accessors.go and logical_schema_accessors.go for
// reference implementations of these interfaces.
//
// TODO(ajwerner): Do something about moving these symbols. Doing it in the
// initial pass makes a mess for not a ton of win.

type (
	// TableName is provided for convenience and to make the interface
	// definitions below more intuitive.
	TableName = tree.TableName
	// DatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	DatabaseDescriptor = descpb.DatabaseDescriptor
	// MutableDatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	MutableDatabaseDescriptor = dbdesc.MutableDatabaseDescriptor
	// ImmutableDatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ImmutableDatabaseDescriptor = dbdesc.ImmutableDatabaseDescriptor
	// MutableTableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	MutableTableDescriptor = tabledesc.Mutable
	// ImmutableTableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ImmutableTableDescriptor = tabledesc.Immutable
	// TableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	TableDescriptor = descpb.TableDescriptor
	// TypeDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	TypeDescriptor = descpb.TypeDescriptor
	// MutableTypeDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	MutableTypeDescriptor = typedesc.Mutable
	// ViewDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ViewDescriptor = descpb.TableDescriptor
	// SequenceDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	SequenceDescriptor = descpb.TableDescriptor
	// TableNames is provided for convenience and to make the interface
	// definitions below more intuitive.
	TableNames = tree.TableNames
	// MutableSchemaDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	MutableSchemaDescriptor = schemadesc.Mutable
)
