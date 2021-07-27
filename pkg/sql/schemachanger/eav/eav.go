// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package eav provides a toolkit to interact with and store data generically.
//
// The package is useful for indexing heterogeneous data structures over
// multiple dimensions. It provides an in-memory implementation of a database
// for performing such queries. The eavquery package utilizes this data
// structure to perform index-accelerated, fixed-depth graph queries over
// this graph.
package eav

import "fmt"

// Entity is any object which provides values for attributes.
// An entity's supported Attributes should be part of a Schema.
// Entities can be stored in a database.
//
// Note that an entity in this formulation does not have a unique ID;
// two entities are deemed to be the same if they have exactly the same
// attribute values. If a unique ID is desired, add an Attribute to represent
// that ID.
type Entity interface {

	// Attributes returns the set of ordinals for which this entity advertises
	// values. The caller must use the Entity's schema to determine the actual
	// attributes.
	Attributes() OrdinalSet

	// Get is used to retrieve a value for an attribute.
	Get(Attribute) Value
}

// Attribute is an interface representing a member of a Schema.
type Attribute interface {
	fmt.Stringer
	Ordinal() Ordinal
	Type() Type
}

// Value is a value corresponding to a given attribute of an element.
type Value interface {
	fmt.Stringer

	// Type returns the type of the value.
	Type() Type

	// Compare compares this to another Value. If the types
	// are not the same, the call may panic.
	Compare(other Value) (ok, less, eq bool)
}

// Ordinal is used to correlate attributes in a schema.
// It enables use of the OrdinalSet.
type Ordinal = int8

// Schema describes a set of attributes.
type Schema interface {
	// Attributes returns the set of ordinals which correspond to values.
	Attributes() OrdinalSet

	// At returns the Attribute with the given ordinal.
	At(ordinal Ordinal) Attribute
}

// Database stores entities and allows iteration with filtering
// based on value equality for attributes.
type Database interface {

	// Schema describes the set of attributes and their types for the
	// entities to be stored.
	Schema() Schema

	// Iterate iterates the database for all entities which have the
	// value settings specified by where. Use a nil where to iterate
	// all entities.
	Iterate(where Values, iterator Iterator) error
}

// DatabaseWriter is used to a database.
type DatabaseWriter interface {
	Database

	// Insert will insert the Entity into the database. If another entity with
	// all of the same attribute values exists in the database, it will be
	// overwritten and returned.
	Insert(entity Entity) (replaced Entity)

	// TODO(ajwerner): Consider adding Delete and DeleteWhere.
}

// Iterator is used to iterate Entities.
type Iterator interface {
	// Visit visits an Entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	Visit(Entity) error
}

// IteratorFunc implements Iterator.
type IteratorFunc func(Entity) error

// Visit is part of the Iterator interface.
func (f IteratorFunc) Visit(e Entity) error { return f(e) }
