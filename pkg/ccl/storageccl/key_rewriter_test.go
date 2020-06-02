// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func TestPrefixRewriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	kr := prefixRewriter{
		{
			OldPrefix: []byte{1, 2, 3},
			NewPrefix: []byte{4, 5, 6},
		},
		{
			OldPrefix: []byte{7, 8, 9},
			NewPrefix: []byte{10},
		},
	}

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

	desc := sqlbase.NewMutableCreatedTableDescriptor(sqlbase.NamespaceTable.TableDescriptor)
	oldID := desc.ID
	newID := desc.ID + 1
	desc.ID = newID
	rekeys := []roachpb.ImportRequest_TableRekey{
		{
			OldID:   uint32(oldID),
			NewDesc: mustMarshalDesc(t, desc.TableDesc()),
		},
	}

	const notSpan = false

	kr, err := MakeKeyRewriterFromRekeys(rekeys)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("normal", func(t *testing.T) {
		key := sqlbase.MakeIndexKeyPrefix(keys.SystemSQLCodec,
			sqlbase.NamespaceTable.TableDesc(), desc.PrimaryIndex.ID)
		newKey, ok, err := kr.RewriteKey(key, notSpan)
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
		if sqlbase.ID(id) != newID {
			t.Fatalf("got %d expected %d", id, newID)
		}
	})

	t.Run("prefix end", func(t *testing.T) {
		key := roachpb.Key(sqlbase.MakeIndexKeyPrefix(keys.SystemSQLCodec,
			sqlbase.NamespaceTable.TableDesc(), desc.PrimaryIndex.ID)).PrefixEnd()
		newKey, ok, err := kr.RewriteKey(key, notSpan)
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
		if sqlbase.ID(id) != newID {
			t.Fatalf("got %d expected %d", id, newID)
		}
	})

	t.Run("multi", func(t *testing.T) {
		desc.ID = oldID + 10
		desc2 := sqlbase.NewMutableCreatedTableDescriptor(sqlbase.DescriptorTable.TableDescriptor)
		desc2.ID += 10
		newKr, err := MakeKeyRewriterFromRekeys([]roachpb.ImportRequest_TableRekey{
			{OldID: uint32(oldID), NewDesc: mustMarshalDesc(t, desc.TableDesc())},
			{OldID: uint32(sqlbase.DescriptorTable.ID), NewDesc: mustMarshalDesc(t, desc2.TableDesc())},
		})
		if err != nil {
			t.Fatal(err)
		}

		key := sqlbase.MakeIndexKeyPrefix(keys.SystemSQLCodec,
			sqlbase.NamespaceTable.TableDesc(), desc.PrimaryIndex.ID)
		newKey, ok, err := newKr.RewriteKey(key, notSpan)
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
		if sqlbase.ID(id) != oldID+10 {
			t.Fatalf("got %d expected %d", id, desc.ID+1)
		}
	})
}

func mustMarshalDesc(t *testing.T, tableDesc *sqlbase.TableDescriptor) []byte {
	desc := sqlbase.NewImmutableTableDescriptor(*tableDesc).DescriptorProto()
	// Set the timestamp to a non-zero value.
	desc.Table(hlc.Timestamp{WallTime: 1})
	bytes, err := protoutil.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func BenchmarkPrefixRewriter(b *testing.B) {
	kr := prefixRewriter{
		{
			OldPrefix: []byte{1, 2, 3},
			NewPrefix: []byte{4, 5, 6},
		},
		{
			OldPrefix: []byte{7, 8, 9},
			NewPrefix: []byte{10},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte{1, 2, 3, 4}
		_, _ = kr.rewriteKey(key)
	}
}
