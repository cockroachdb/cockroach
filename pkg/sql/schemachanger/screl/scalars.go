// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

// GetDescID retrieves the descriptor ID from the element.
func GetDescID(e scpb.Element) descpb.ID {
	id, err := Schema.GetAttribute(DescID, e)
	if err != nil {
		// Note that this is safe because we have a unit test that ensures that
		// all elements don't panic on this.
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to retrieve descriptor ID for %T", e,
		))
	}
	return id.(descpb.ID)
}

// GetDescIDs returns the descriptor IDs referenced in the state's elements.
func GetDescIDs(s scpb.TargetState) descpb.IDs {
	descIDSet := catalog.MakeDescriptorIDSet()
	for i := range s.Targets {
		// Depending on the element type either a single descriptor ID
		// will exist or multiple (i.e. foreign keys).
		if id := GetDescID(s.Targets[i].Element()); id != descpb.InvalidID {
			descIDSet.Add(id)
		}
	}
	return descIDSet.Ordered()
}

// GetConstraintId retrieves the constraint ID from the element.
func GetConstraintId(e scpb.Element) uint32 {
	id, err := Schema.GetAttribute(ConstraintId, e)
	if err != nil {
		// Note that this is safe because we have a unit test that ensures that
		// all elements don't panic on this.
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to retrieve constraint ID for %T", e,
		))
	}
	return id.(uint32)
}
