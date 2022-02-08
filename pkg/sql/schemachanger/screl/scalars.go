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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

// GetDescID retrieves the descriptor ID from the element.
func GetDescID(e scpb.Element) catid.DescID {
	id, err := Schema.GetAttribute(DescID, e)
	if err != nil {
		// Note that this is safe because we have a unit test that ensures that
		// all elements don't panic on this.
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to retrieve descriptor ID for %T", e,
		))
	}
	return id.(catid.DescID)
}

// AllTargetDescIDs returns all the descriptor IDs referenced in the
// target state's elements. This is a superset of the IDs of the descriptors
// affected by the schema change.
func AllTargetDescIDs(s scpb.TargetState) (ids catalog.DescriptorIDSet) {
	for i := range s.Targets {
		e := s.Targets[i].Element()
		// Handle special cases to tighten this superset a bit.
		switch te := e.(type) {
		case *scpb.Namespace:
			// Ignore the parent database and schema in the namespace element:
			// - the parent schema of an object has no back-references to it,
			// - the parent database has back-references to a schema, but these
			//   will be captured by the scpb.SchemaParent target.
			ids.Add(te.DescriptorID)
		case *scpb.ObjectParent:
			// Ignore the parent schema, it won't have back-references.
			ids.Add(te.ObjectID)
		default:
			AllDescIDs(e).ForEach(ids.Add)
		}
	}
	return ids
}

// AllDescIDs returns all the IDs referenced by an element.
func AllDescIDs(e scpb.Element) (ids catalog.DescriptorIDSet) {
	if e == nil {
		return ids
	}
	_ = WalkDescIDs(e, func(id *catid.DescID) error {
		ids.Add(*id)
		return nil
	})
	return ids
}
