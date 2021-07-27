// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"

func makeElementAttrs(extra ...Attr) eav.OrdinalSet {
	attributes := []eav.Attribute{
		AttrElementType,
		AttrDescID,
	}
	for _, a := range extra {
		attributes = append(attributes, a)
	}
	return eav.MakeOrdinalSetWithAttributes(attributes)
}

var (
	columnAttributes               = makeElementAttrs(AttrColumnID, AttrName)
	primaryIndexAttributes         = makeElementAttrs(AttrIndexID, AttrName)
	secondaryIndexAttributes       = makeElementAttrs(AttrIndexID, AttrName)
	sequenceDependencyAttributes   = makeElementAttrs(AttrColumnID, AttrReferencedDescID)
	uniqueConstraintAttributes     = makeElementAttrs(AttrIndexID)
	checkConstraintAttributes      = makeElementAttrs(AttrName)
	sequenceAttributes             = makeElementAttrs()
	defaultExpressionAttributes    = makeElementAttrs(AttrColumnID)
	viewAttributes                 = makeElementAttrs()
	typeReferenceAttributes        = makeElementAttrs(AttrReferencedDescID)
	tableAttributes                = makeElementAttrs()
	inboundForeinKeyAttributes     = makeElementAttrs(AttrReferencedDescID, AttrName)
	outboundForeinKeyAttributes    = makeElementAttrs(AttrReferencedDescID, AttrName)
	relationDependedOnByAttributes = makeElementAttrs(AttrReferencedDescID)
	sequenceOwnedByAttributes      = makeElementAttrs(AttrReferencedDescID)
	typeAttributes                 = makeElementAttrs()
	schemaAttributes               = makeElementAttrs()
	databaseAttributes             = makeElementAttrs()
)

// Attributes is part of the eav.Entity interface.
func (m *Column) Attributes() eav.OrdinalSet { return columnAttributes }

// Attributes is part of the eav.Entity interface.
func (m *PrimaryIndex) Attributes() eav.OrdinalSet { return primaryIndexAttributes }

// Attributes is part of the eav.Entity interface.
func (m *SecondaryIndex) Attributes() eav.OrdinalSet { return secondaryIndexAttributes }

// Attributes is part of the eav.Entity interface.
func (m *SequenceDependency) Attributes() eav.OrdinalSet { return sequenceDependencyAttributes }

// Attributes is part of the eav.Entity interface.
func (m *UniqueConstraint) Attributes() eav.OrdinalSet { return uniqueConstraintAttributes }

// Attributes is part of the eav.Entity interface.
func (m *CheckConstraint) Attributes() eav.OrdinalSet { return checkConstraintAttributes }

// Attributes is part of the eav.Entity interface.
func (m *Sequence) Attributes() eav.OrdinalSet { return sequenceAttributes }

// Attributes is part of the eav.Entity interface.
func (m *DefaultExpression) Attributes() eav.OrdinalSet { return defaultExpressionAttributes }

// Attributes is part of the eav.Entity interface.
func (m *View) Attributes() eav.OrdinalSet { return viewAttributes }

// Attributes is part of the eav.Entity interface.
func (m *TypeReference) Attributes() eav.OrdinalSet { return typeReferenceAttributes }

// Attributes is part of the eav.Entity interface.
func (m *Table) Attributes() eav.OrdinalSet { return tableAttributes }

// Attributes is part of the eav.Entity interface.
func (m *InboundForeignKey) Attributes() eav.OrdinalSet { return inboundForeinKeyAttributes }

// Attributes is part of the eav.Entity interface.
func (m *OutboundForeignKey) Attributes() eav.OrdinalSet { return outboundForeinKeyAttributes }

// Attributes is part of the eav.Entity interface.
func (m *RelationDependedOnBy) Attributes() eav.OrdinalSet { return relationDependedOnByAttributes }

// Attributes is part of the eav.Entity interface.
func (m *SequenceOwnedBy) Attributes() eav.OrdinalSet { return sequenceOwnedByAttributes }

// Attributes is part of the eav.Entity interface.
func (m *Type) Attributes() eav.OrdinalSet { return typeAttributes }

// Attributes is part of the eav.Entity interface.
func (m *Schema) Attributes() eav.OrdinalSet { return schemaAttributes }

// Attributes is part of the eav.Entity interface.
func (m *Database) Attributes() eav.OrdinalSet { return databaseAttributes }

// Get is part of the eav.Entity interface.
func (m *Column) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrColumnID:
		return (*eav.Uint32)(&m.Column.ID)
	case AttrName:
		return (*eav.String)(&m.Column.Name)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *PrimaryIndex) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrIndexID:
		return (*eav.Uint32)(&m.Index.ID)
	case AttrName:
		return (*eav.String)(&m.Index.Name)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *SecondaryIndex) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrIndexID:
		return (*eav.Uint32)(&m.Index.ID)
	case AttrName:
		return (*eav.String)(&m.Index.Name)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *SequenceDependency) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.SequenceID)
	case AttrReferencedDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrColumnID:
		return (*eav.Uint32)(&m.ColumnID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *UniqueConstraint) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrIndexID:
		return (*eav.Uint32)(&m.IndexID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *CheckConstraint) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrName:
		return (*eav.String)(&m.Name)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *Sequence) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.SequenceID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *DefaultExpression) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrColumnID:
		return (*eav.Uint32)(&m.ColumnID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *View) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *TypeReference) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.DescID)
	case AttrReferencedDescID:
		return (*eav.Uint32)(&m.TypeID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *Table) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *InboundForeignKey) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.OriginID)
	case AttrReferencedDescID:
		return (*eav.Uint32)(&m.ReferenceID)
	case AttrName:
		return (*eav.String)(&m.Name)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *OutboundForeignKey) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.OriginID)
	case AttrReferencedDescID:
		return (*eav.Uint32)(&m.ReferenceID)
	case AttrName:
		return (*eav.String)(&m.Name)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *RelationDependedOnBy) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TableID)
	case AttrReferencedDescID:
		return (*eav.Uint32)(&m.DependedOnBy)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *SequenceOwnedBy) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.SequenceID)
	case AttrReferencedDescID:
		return (*eav.Uint32)(&m.OwnerTableID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *Type) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.TypeID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *Schema) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.SchemaID)
	default:
		return nil
	}
}

// Get is part of the eav.Entity interface.
func (m *Database) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrElementType:
		return getElementType(m)
	case AttrDescID:
		return (*eav.Uint32)(&m.DatabaseID)
	default:
		return nil
	}
}
