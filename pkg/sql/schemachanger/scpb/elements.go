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

// GetElement implements the Entity interface.
func (m *Column) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *PrimaryIndex) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *SecondaryIndex) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *SequenceDependency) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *UniqueConstraint) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *CheckConstraint) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *Sequence) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *DefaultExpression) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *View) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *TypeReference) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *Table) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *InboundForeignKey) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *OutboundForeignKey) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *RelationDependedOnBy) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *SequenceOwnedBy) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *Type) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *Schema) GetElement() Element { return m }

// GetElement implements the Entity interface.
func (m *Database) GetElement() Element { return m }

func (m *Column) element()               {}
func (m *PrimaryIndex) element()         {}
func (m *SecondaryIndex) element()       {}
func (m *SequenceDependency) element()   {}
func (m *UniqueConstraint) element()     {}
func (m *CheckConstraint) element()      {}
func (m *Sequence) element()             {}
func (m *DefaultExpression) element()    {}
func (m *View) element()                 {}
func (m *TypeReference) element()        {}
func (m *Table) element()                {}
func (m *InboundForeignKey) element()    {}
func (m *OutboundForeignKey) element()   {}
func (m *RelationDependedOnBy) element() {}
func (m *SequenceOwnedBy) element()      {}
func (m *Type) element()                 {}
func (m *Schema) element()               {}
func (m *Database) element()             {}
