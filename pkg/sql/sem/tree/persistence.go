// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Persistence defines the persistence strategy for a given table.
type Persistence int

const (
	// PersistencePermanent indicates a permanent table.
	PersistencePermanent Persistence = iota
	// PersistenceTemporary indicates a temporary table.
	PersistenceTemporary
	// PersistenceUnlogged indicates an unlogged table.
	// Note this state is not persisted on disk and is used at parse time only.
	PersistenceUnlogged
)

// IsTemporary returns whether the Persistence value is Temporary.
func (p Persistence) IsTemporary() bool {
	return p == PersistenceTemporary
}

// IsUnlogged returns whether the Persistence value is Unlogged.
func (p Persistence) IsUnlogged() bool {
	return p == PersistenceUnlogged
}
