// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
