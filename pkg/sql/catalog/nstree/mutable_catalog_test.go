// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestMutableCatalogAddAll validates the functionality
// for the add all operation.
func TestMutableCatalogAddAll(t *testing.T) {
	firstMc := nstree.MutableCatalog{}
	secondMc := nstree.MutableCatalog{}

	countNamespace := func(c nstree.Catalog) int {
		count := 0
		require.NoError(t, c.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			count += 1
			return nil
		}))
		return count
	}
	countComments := func(c nstree.Catalog) int {
		count := 0
		require.NoError(t, c.ForEachComment(func(key catalogkeys.CommentKey, cmt string) error {
			count += 1
			return nil
		}))
		return count
	}

	for _, descs := range []catalog.Descriptor{systemschema.CommentsTable,
		systemschema.LeaseTable(),
		systemschema.DescriptorTable} {
		firstMc.UpsertNamespaceEntry(descs, descs.GetID(), hlc.Timestamp{})
		firstMc.UpsertDescriptor(descs)
		require.NoError(t, firstMc.UpsertComment(catalogkeys.MakeCommentKey(uint32(descs.GetID()), 0, catalogkeys.TableCommentType),
			"just fake data."))
	}

	for _, descs := range []catalog.Descriptor{systemschema.NamespaceTable,
		systemschema.UsersTable,
		systemschema.JobsTable} {
		secondMc.UpsertNamespaceEntry(descs, descs.GetID(), hlc.Timestamp{})
		secondMc.UpsertDescriptor(descs)
		require.NoError(t, secondMc.UpsertComment(catalogkeys.MakeCommentKey(uint32(descs.GetID()), 0, catalogkeys.TableCommentType),
			"just fake data."))
	}

	// Validate the counts match for the new catalogs.
	require.Equal(t, 3, len(firstMc.OrderedDescriptorIDs()))
	require.Equal(t, 3, countComments(firstMc.Catalog))
	require.Equal(t, 3, countNamespace(firstMc.Catalog))

	require.Equal(t, 3, len(secondMc.OrderedDescriptorIDs()))
	require.Equal(t, 3, countComments(secondMc.Catalog))
	require.Equal(t, 3, countNamespace(secondMc.Catalog))

	// Start with a new catalog, and merge firstMc in.
	mergedMc := nstree.MutableCatalog{}
	mergedMc.AddAll(firstMc.Catalog)
	oldSize := mergedMc.ByteSize()
	require.Equal(t, 3, len(mergedMc.OrderedDescriptorIDs()))
	require.Equal(t, 3, countComments(mergedMc.Catalog))
	require.Equal(t, 3, countNamespace(mergedMc.Catalog))
	// Adding the same items again is a no-op.
	mergedMc.AddAll(firstMc.Catalog)
	require.Equal(t, 3, len(mergedMc.OrderedDescriptorIDs()))
	require.Equal(t, 3, countComments(mergedMc.Catalog))
	require.Equal(t, 3, countNamespace(mergedMc.Catalog))
	newSize := mergedMc.ByteSize()
	// Size should be the same.
	require.Equal(t, oldSize, newSize)
	// Next merge in the second mutable catalog.
	mergedMc.AddAll(secondMc.Catalog)
	// Sizes should no longer match.
	require.Greater(t, mergedMc.ByteSize(), newSize)
	// We expect the counts to double.
	require.Equal(t, 6, len(mergedMc.OrderedDescriptorIDs()))
	require.Equal(t, 6, countComments(mergedMc.Catalog))
	require.Equal(t, 6, countNamespace(mergedMc.Catalog))
	// Finally confirm the descriptors contained in each one.
	mergedSet := catalog.MakeDescriptorIDSet(mergedMc.OrderedDescriptorIDs()...)
	firstSet := catalog.MakeDescriptorIDSet(firstMc.OrderedDescriptorIDs()...)
	secondSet := catalog.MakeDescriptorIDSet(secondMc.OrderedDescriptorIDs()...)
	require.Truef(t, mergedSet.Difference(firstSet).Difference(secondSet).Empty(),
		"merge descriptor set doesn't have everything %v %v %v",
		mergedSet.Ordered(),
		firstSet.Ordered(),
		secondSet.Ordered())
}
