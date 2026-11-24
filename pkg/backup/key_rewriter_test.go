// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestPrefixRewriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	kr := prefixRewriter{rewrites: []prefixRewrite{
		{
			OldPrefix: []byte{1, 2, 3},
			NewPrefix: []byte{4, 5, 6},
		},
		{
			OldPrefix: []byte{7, 8, 9},
			NewPrefix: []byte{10},
		},
	}}
	t.Run("match", func(t *testing.T) {
		key := []byte{1, 2, 3, 4}
		newKey, ok := kr.rewriteKey(key)
		if !ok {
			t.Fatalf("expected to match %s but didn't", key)
		}
		if expected := []byte{4, 5, 6, 4}; !bytes.Equal(newKey, expected) {
			t.Fatalf("got %x expected %x", newKey, expected)
		}
	})

	t.Run("no match", func(t *testing.T) {
		key := []byte{4, 5, 6}
		_, ok := kr.rewriteKey(key)
		if ok {
			t.Fatalf("expected to not match %s but did", key)
		}
	})
}

func TestKeyRewriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := tabledesc.NewBuilder(systemschema.NamespaceTable.TableDesc()).BuildCreatedMutableTable()
	oldID := desc.ID
	newID := desc.ID + 1
	desc.ID = newID
	rekeys := []execinfrapb.TableRekey{
		{
			OldID:   uint32(oldID),
			NewDesc: mustMarshalDesc(t, desc.TableDesc()),
		},
	}

	kr, err := MakeKeyRewriterFromRekeys(keys.SystemSQLCodec, rekeys,
		nil /* tenantRekeys */, false /* restoreTenantFromStream */)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("normal", func(t *testing.T) {
		key := rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec,
			systemschema.NamespaceTable.GetID(), desc.GetPrimaryIndexID())
		newKey, ok, err := kr.RewriteKey(key, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected rewrite")
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if descpb.ID(id) != newID {
			t.Fatalf("got %d expected %d", id, newID)
		}
	})

	t.Run("prefix end", func(t *testing.T) {
		key := roachpb.Key(rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec,
			systemschema.NamespaceTable.GetID(), desc.GetPrimaryIndexID())).PrefixEnd()
		newKey, ok, err := kr.RewriteKey(key, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected rewrite")
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if descpb.ID(id) != newID {
			t.Fatalf("got %d expected %d", id, newID)
		}
	})

	t.Run("multi", func(t *testing.T) {
		desc.ID = oldID + 10
		desc2 := tabledesc.NewBuilder(&desc.TableDescriptor).BuildCreatedMutableTable()
		desc2.ID += 10
		newKr, err := MakeKeyRewriterFromRekeys(keys.SystemSQLCodec, []execinfrapb.TableRekey{
			{OldID: uint32(oldID), NewDesc: mustMarshalDesc(t, desc.TableDesc())},
			{OldID: uint32(desc.ID), NewDesc: mustMarshalDesc(t, desc2.TableDesc())},
		}, nil /* tenantRekeys */, false /* restoreTenantFromStream */)
		if err != nil {
			t.Fatal(err)
		}

		key := rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, systemschema.NamespaceTable.GetID(), desc.GetPrimaryIndexID())
		newKey, ok, err := newKr.RewriteKey(key, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected rewrite")
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if descpb.ID(id) != oldID+10 {
			t.Fatalf("got %d expected %d", id, oldID+10)
		}
	})

	t.Run("importing", func(t *testing.T) {

		// Create a new key rewriter with a table descriptor undergoing an in-progress import.
		desc.ID = oldID + 20
		desc.ImportStartWallTime = 2
		newKr, err := MakeKeyRewriterFromRekeys(keys.SystemSQLCodec, []execinfrapb.TableRekey{
			{OldID: uint32(oldID), NewDesc: mustMarshalDesc(t, desc.TableDesc())},
		}, nil /* tenantRekeys */, false /* restoreTenantFromStream */)
		require.NoError(t, err)

		key := rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec,
			systemschema.NamespaceTable.GetID(), desc.GetPrimaryIndexID())

		// If the passed in walltime is at or above the ImportStartWalltime,
		// rewriting should not have occurred.
		_, ok, err := newKr.RewriteKey(key, 2)
		require.NoError(t, err)
		require.False(t, ok)

		// Else, the key should get encoded normally.
		newKey, ok, err := newKr.RewriteKey(key, 1)

		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected rewrite")
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if descpb.ID(id) != oldID+20 {
			t.Fatalf("got %d expected %d", id, newID)
		}
	})
	systemTenant := roachpb.SystemTenantID
	tenant3 := roachpb.MustMakeTenantID(3)
	tenant4 := roachpb.MustMakeTenantID(4)

	// Restoring a tenant's data from a backup as another tenant.
	testTableRekeyAsDiffTenant := func(srcTenant, destTenant roachpb.TenantID) {
		desc.ID = oldID + 10
		srcCodec := keys.MakeSQLCodec(srcTenant)
		destCodec := keys.MakeSQLCodec(destTenant)
		newKr, err := MakeKeyRewriterFromRekeys(destCodec, []execinfrapb.TableRekey{
			{OldID: uint32(oldID), NewDesc: mustMarshalDesc(t, desc.TableDesc())},
		}, nil /* tenantRekeys */, false /* restoreTenantFromStream */)
		require.NoError(t, err)

		key := rowenc.MakeIndexKeyPrefix(srcCodec, systemschema.NamespaceTable.GetID(), desc.GetPrimaryIndexID())
		newKey, ok, err := newKr.RewriteKey(key, 0)
		require.NoError(t, err)
		if !ok {
			t.Fatalf("expected rewrite")
		}
		noTenantKey, tenantID, err := keys.DecodeTenantPrefix(newKey)
		require.NoError(t, err)
		require.Equal(t, destTenant, tenantID)
		_, id, err := encoding.DecodeUvarintAscending(noTenantKey)
		require.NoError(t, err)
		require.Equal(t, oldID+10, descpb.ID(id))
	}

	tcs := []struct {
		from, to roachpb.TenantID
	}{
		{from: systemTenant, to: tenant3},
		{from: tenant3, to: tenant3},
		{from: tenant3, to: tenant4},
		// TODO: Restoring to the system tenant is currently special cased.
		// {from: tenant3, to: systemTenant},
	}
	for _, tc := range tcs {
		t.Run(fmt.Sprintf("from-tenant-%v-to-%v", tc.from, tc.to), func(t *testing.T) {
			testTableRekeyAsDiffTenant(tc.from, tc.to)
		})
	}

	// Restoring a tenant from replication stream to another tenant.
	testTenantRekey := func(srcTenant, destTenant roachpb.TenantID) {
		desc.ID = oldID + 10
		srcCodec := keys.MakeSQLCodec(srcTenant)
		destCodec := keys.MakeSQLCodec(destTenant)
		newKr, err := MakeKeyRewriterFromRekeys(destCodec, nil,
			[]execinfrapb.TenantRekey{
				{
					OldID: srcTenant,
					NewID: destTenant,
				},
			}, true /* restoreTenantFromStream */)
		require.NoError(t, err)

		key := rowenc.MakeIndexKeyPrefix(srcCodec, systemschema.NamespaceTable.GetID(), desc.GetPrimaryIndexID())
		oldNoTenantKey, oldTenantID, err := keys.DecodeTenantPrefix(key)
		require.NoError(t, err)
		newKey, ok, err := newKr.RewriteKey(key, 0)
		require.NoError(t, err)
		if !ok {
			t.Fatalf("expected rewrite")
		}

		require.NoError(t, err)
		newNoTenantKey, newTenantID, err := keys.DecodeTenantPrefix(newKey)
		require.NoError(t, err)
		require.Equal(t, srcTenant, oldTenantID)
		require.Equal(t, destTenant, newTenantID)
		require.Equal(t, oldNoTenantKey, newNoTenantKey)
	}

	tcs = []struct {
		from, to roachpb.TenantID
	}{
		// Rewriting a system tenant is not supported
		{from: tenant3, to: tenant3},
		{from: tenant3, to: tenant4},
	}
	for _, tc := range tcs {
		t.Run(fmt.Sprintf("from-tenant-%v-to-%v", tc.from, tc.to), func(t *testing.T) {
			testTenantRekey(tc.from, tc.to)
		})
	}
}

// mustMarshalDesc marshals the provided TableDescriptor.
func mustMarshalDesc(t *testing.T, tableDesc *descpb.TableDescriptor) []byte {
	pb := tabledesc.NewBuilder(tableDesc).BuildCreatedMutable().DescriptorProto()
	// Set the timestamp to a non-zero value.
	mut, err := descbuilder.BuildMutable(nil /* original */, pb, hlc.Timestamp{WallTime: 1})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := protoutil.Marshal(mut.DescriptorProto())
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func BenchmarkPrefixRewriter(b *testing.B) {
	kr := prefixRewriter{rewrites: []prefixRewrite{
		{
			OldPrefix: []byte{1, 2, 3},
			NewPrefix: []byte{4, 5, 6},
		},
		{
			OldPrefix: []byte{7, 8, 9},
			NewPrefix: []byte{10},
		},
	}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte{1, 2, 3, 4}
		_, _ = kr.rewriteKey(key)
	}
}
