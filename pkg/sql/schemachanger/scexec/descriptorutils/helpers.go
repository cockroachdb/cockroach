// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descriptorutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// MutationSelector defines a predicate on a catalog.Mutation with no
// side-effects.
type MutationSelector func(mutation catalog.Mutation) (matches bool)

// FindMutation returns the first mutation in table for which the selector
// returns true.
// Such a mutation is expected to exist, if none are found, an internal error
// is returned.
func FindMutation(
	table catalog.TableDescriptor, selector MutationSelector,
) (catalog.Mutation, error) {
	for _, mut := range table.AllMutations() {
		if selector(mut) {
			return mut, nil
		}
	}
	return nil, errors.AssertionFailedf("matching mutation not found in table %d", table.GetID())
}

// MakeIndexIDMutationSelector returns a MutationSelector which matches an
// index mutation with the correct ID.
func MakeIndexIDMutationSelector(indexID descpb.IndexID) MutationSelector {
	return func(mut catalog.Mutation) bool {
		if mut.AsIndex() == nil {
			return false
		}
		return mut.AsIndex().GetID() == indexID
	}
}

// MakeColumnIDMutationSelector returns a MutationSelector which matches a
// column mutation with the correct ID.
func MakeColumnIDMutationSelector(columnID descpb.ColumnID) MutationSelector {
	return func(mut catalog.Mutation) bool {
		if mut.AsColumn() == nil {
			return false
		}
		return mut.AsColumn().GetID() == columnID
	}
}
