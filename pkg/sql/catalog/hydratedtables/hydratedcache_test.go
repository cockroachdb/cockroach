// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hydratedtables

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHydratedCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("basic caching", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)
		hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 1)

		// Observe that the cache's lookup functionality only gets each type
		// one time. The table in question uses one type two times.
		require.Equal(t, res.calls, 2)

		// Show that the cache returned a new pointer and hydrated the UDT
		// (user-defined type).
		require.NotEqual(t, tableDescUDT, hydrated)
		require.EqualValues(t, hydrated.PublicColumns()[0].GetType(), typ1T)

		// Try again and ensure we get pointer-for-pointer the same descriptor.
		res.calls = 0
		cached, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		require.Equal(t, hydrated, cached)
		assertMetrics(t, m, 1, 1)

		// Observe that the cache's cache checking functionality only gets each
		// type one time.
		require.Equal(t, res.calls, 2)
	})
	t.Run("no UDT, no metrics", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		td := tableDescNoUDT.ImmutableCopy().(catalog.TableDescriptor)
		_, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 0)
	})
	t.Run("name change causes eviction", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)
		hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 1)

		// Change the database name.
		dbDesc := dbdesc.NewBuilder(dg.Descriptors[dbID].(catalog.DatabaseDescriptor).DatabaseDesc()).BuildExistingMutableDatabase()
		dbDesc.SetName("new_name")
		dbDesc.Version++
		dg.Descriptors[dbID] = dbDesc.ImmutableCopy()

		// Ensure that we observe a new descriptor get created due to
		// the name change.
		retrieved, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 2)

		require.NotEqual(t, hydrated, retrieved)
	})
	t.Run("unqualified resolution after qualified does not cause eviction", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)
		hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 1)

		// Attempt to retrieve retrieve the same hydrated descriptor
		// using a resolver that does not create a qualified name and
		// see that the same descriptor with the qualified name gets
		// returned.
		res.unqualifiedName = true
		retrieved, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 1, 1)

		require.Equal(t, hydrated, retrieved)
	})
	t.Run("qualified resolution after unqualified causes eviction", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		res.unqualifiedName = true
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)
		hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 1)

		// Attempt to retrieve retrieve the same hydrated descriptor
		// using a resolver that does create a qualified name and
		// see that the old descriptor with an unqualified name gets
		// evicted.
		res.unqualifiedName = false
		retrieved, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 2)

		require.NotEqual(t, hydrated, retrieved)
	})
	t.Run("version change causes eviction", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		res.unqualifiedName = true
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)
		hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 1)

		// Change the type descriptor.
		typDesc := typedesc.NewBuilder(dg.Descriptors[typ1ID].(catalog.TypeDescriptor).TypeDesc()).BuildExistingMutableType()
		typDesc.Version++
		dg.Descriptors[typ1ID] = typedesc.NewBuilder(typDesc.TypeDesc()).BuildImmutable()

		// Ensure that a new descriptor is returned.
		retrieved, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		assertMetrics(t, m, 0, 2)

		require.NotEqual(t, hydrated, retrieved)
	})
	// If one cache retrieval hits an error during a lookup, it should not
	// propagate back to a concurrent retrieval as it could be due to something
	// like cancellation.
	t.Run("errors do not propagate to concurrent calls", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		calledCh := make(chan chan error, 1)
		res.called = func(ctx context.Context, id descpb.ID) error {
			errCh := make(chan error, 1)
			calledCh <- errCh
			return <-errCh
		}
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)

		callOneErrCh := make(chan error, 1)
		go func() {
			_, err := c.GetHydratedTableDescriptor(ctx, td, res)
			callOneErrCh <- err
		}()

		call2Res := &descGetterTypeDescriptorResolver{dg: &dg}
		unblockCallOne := <-calledCh
		callTwoErrCh := make(chan error, 1)
		go func() {
			_, err := c.GetHydratedTableDescriptor(ctx, td, call2Res)
			callTwoErrCh <- err
		}()
		unblockCallOne <- context.Canceled
		require.Equal(t, context.Canceled, <-callOneErrCh)
		require.NoError(t, <-callTwoErrCh)
		assertMetrics(t, c.Metrics(), 0, 1)
	})
	t.Run("modified table gets rejected", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}
		mut := tabledesc.NewBuilder(dg.Descriptors[tableUDTID].(catalog.TableDescriptor).TableDesc()).BuildExistingMutable()
		mut.MaybeIncrementVersion()
		td := mut.ImmutableCopy().(catalog.TableDescriptor)
		hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
		require.NoError(t, err)
		require.Nil(t, hydrated)
	})
	t.Run("modified type does not get cached", func(t *testing.T) {
		c := NewCache(cluster.MakeTestingClusterSettings())
		m := c.Metrics()

		dg := mkDescGetter(descs...)
		res := &descGetterTypeDescriptorResolver{dg: &dg}

		mut := typedesc.NewBuilder(dg.Descriptors[typ1ID].(catalog.TypeDescriptor).TypeDesc()).BuildExistingMutable()
		mut.MaybeIncrementVersion()
		dgWithMut := mkDescGetter(append(descs, mut)...)
		resWithMut := &descGetterTypeDescriptorResolver{dg: &dgWithMut}

		// Given that there is no cached value for this version, we will not find
		// check for a cached type and will construct the hydrated type underneath
		// the cache. We can use this descriptor, however, it will not be stored.
		//
		// This behavior is a bit bizarre but exists to not waste the work of
		// hydrating the descriptor if we've already started to do it.
		// This case should not meaningfully arise in practice.
		td := tableDescUDT.ImmutableCopy().(catalog.TableDescriptor)
		{
			hydrated, err := c.GetHydratedTableDescriptor(ctx, td, resWithMut)
			require.NoError(t, err)
			require.NotNil(t, hydrated)
			assertMetrics(t, m, 0, 1)
		}
		{
			hydrated, err := c.GetHydratedTableDescriptor(ctx, td, resWithMut)
			require.NoError(t, err)
			require.NotNil(t, hydrated)
			assertMetrics(t, m, 0, 2)
		}

		// Now cache the old version.
		{
			hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
			require.NoError(t, err)
			require.NotNil(t, hydrated)
			assertMetrics(t, m, 0, 3)
		}
		{
			hydrated, err := c.GetHydratedTableDescriptor(ctx, td, res)
			require.NoError(t, err)
			require.NotNil(t, hydrated)
			assertMetrics(t, m, 1, 3)
		}

		// Show that now we won't use the cache for the mutated type.
		{
			hydrated, err := c.GetHydratedTableDescriptor(ctx, td, resWithMut)
			require.NoError(t, err)
			require.Nil(t, hydrated)
			assertMetrics(t, m, 1, 3)
		}

	})
}

func mkTypeT(desc catalog.TypeDescriptor, name *tree.TypeName) *types.T {
	typT, err := desc.MakeTypesT(context.Background(), name, nil)
	if err != nil {
		panic(err)
	}
	return typT
}

const (
	dbID         = 1
	scID         = 2
	typ1ID       = 3
	typ2ID       = 4
	tableUDTID   = 5
	tableNoUDTID = 6
)

// This block contains definitions for a mocked schema.
//
// TODO(ajwerner): This is horrible to both read and write. Build tools to make
// constructing descriptors for testing less terrible without running a whole
// server.
var (
	dbDesc     = dbdesc.NewInitial(dbID, "db", security.RootUserName())
	schemaDesc = schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		Name:     "schema",
		ID:       scID,
		ParentID: dbID,
	}).BuildCreatedMutable()
	enumMembers = []descpb.TypeDescriptor_EnumMember{
		{
			LogicalRepresentation:  "hello",
			PhysicalRepresentation: []byte{128},
		},
		{
			LogicalRepresentation:  "hi",
			PhysicalRepresentation: []byte{200},
		},
	}

	typ1Desc = typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:                     "enum",
		ID:                       typ1ID,
		Version:                  1,
		ParentID:                 dbID,
		ParentSchemaID:           scID,
		State:                    descpb.DescriptorState_PUBLIC,
		Kind:                     descpb.TypeDescriptor_ENUM,
		ReferencingDescriptorIDs: []descpb.ID{tableUDTID},
		EnumMembers:              enumMembers,
	}).BuildExistingMutableType()
	typ1Name        = tree.MakeQualifiedTypeName(dbDesc.Name, schemaDesc.GetName(), typ1Desc.Name)
	typ1T           = mkTypeT(typ1Desc, &typ1Name)
	typ1TSerialized = &types.T{InternalType: typ1T.InternalType}

	typ2Desc = typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:                     "other_enum",
		ID:                       typ2ID,
		Version:                  1,
		ParentID:                 dbID,
		ParentSchemaID:           scID,
		State:                    descpb.DescriptorState_PUBLIC,
		Kind:                     descpb.TypeDescriptor_ENUM,
		ReferencingDescriptorIDs: []descpb.ID{tableUDTID},
		EnumMembers:              enumMembers,
	}).BuildExistingMutableType()
	typ2Name        = tree.MakeQualifiedTypeName(dbDesc.Name, schemaDesc.GetName(), typ2Desc.Name)
	typ2T           = mkTypeT(typ2Desc, &typ2Name)
	typ2TSerialized = &types.T{InternalType: typ2T.InternalType}
	tableDescUDT    = tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name:                    "foo",
		ID:                      tableUDTID,
		Version:                 1,
		ParentID:                dbID,
		UnexposedParentSchemaID: scID,
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", ID: 1, Type: typ1TSerialized},
			{Name: "b", ID: 1, Type: typ2TSerialized},
			{Name: "c", ID: 1, Type: typ1TSerialized},
		},
	}).BuildExistingMutableTable()
	tableDescNoUDT = tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name:                    "bar",
		ID:                      tableNoUDTID,
		Version:                 1,
		ParentID:                dbID,
		UnexposedParentSchemaID: scID,
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", ID: 1, Type: types.Int},
		},
	}).BuildExistingMutableTable()
	descs = []catalog.MutableDescriptor{
		dbDesc, schemaDesc, typ1Desc, typ2Desc, tableDescUDT, tableDescNoUDT,
	}
)

func mkDescGetter(descs ...catalog.MutableDescriptor) catalog.MapDescGetter {
	ret := catalog.MapDescGetter{
		Descriptors: make(map[descpb.ID]catalog.Descriptor, len(descs)),
	}
	for _, desc := range descs {
		ret.Descriptors[desc.GetID()] = desc.ImmutableCopy()
	}
	return ret
}

type descGetterTypeDescriptorResolver struct {
	dg              catalog.DescGetter
	called          func(ctx context.Context, id descpb.ID) error
	unqualifiedName bool
	calls           int
}

func (d *descGetterTypeDescriptorResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	d.calls++
	if d.called != nil {
		if err := d.called(ctx, id); err != nil {
			return tree.TypeName{}, nil, err
		}
	}
	desc, err := d.dg.GetDesc(ctx, id)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	if d.unqualifiedName {
		return tree.MakeUnqualifiedTypeName(desc.GetName()),
			desc.(catalog.TypeDescriptor), nil
	}
	dbDesc, err := d.dg.GetDesc(ctx, desc.GetParentID())
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	// Assume we've got a user-defined schema.
	// TODO(ajwerner): Unify this with some other resolution logic.
	scDesc, err := d.dg.GetDesc(ctx, desc.GetParentSchemaID())
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeQualifiedTypeName(dbDesc.GetName(), scDesc.GetName(), desc.GetName())
	return name, desc.(catalog.TypeDescriptor), nil
}

func assertMetrics(t *testing.T, m *Metrics, hits, misses int64) {
	t.Helper()
	require.Equal(t, hits, m.Hits.Count())
	require.Equal(t, misses, m.Misses.Count())
}

var _ catalog.TypeDescriptorResolver = (*descGetterTypeDescriptorResolver)(nil)
