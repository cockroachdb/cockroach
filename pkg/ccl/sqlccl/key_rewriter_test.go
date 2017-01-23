// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func TestKeyRewriter(t *testing.T) {
	desc := sqlbase.NamespaceTable
	kr := MakeKeyRewriterForNewTableID(&desc, desc.ID+1)

	t.Run("normal", func(t *testing.T) {
		key := keys.MakeRowSentinelKey(sqlbase.MakeIndexKeyPrefix(&desc, desc.PrimaryIndex.ID))
		newKey, ok := kr.RewriteKey(key)
		if !ok {
			t.Fatalf("expected to match %s but didn't", key)
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if sqlbase.ID(id) != desc.ID+1 {
			t.Fatalf("got %d expected %d", id, desc.ID+1)
		}
	})

	t.Run("prefix end", func(t *testing.T) {
		key := roachpb.Key(sqlbase.MakeIndexKeyPrefix(&desc, desc.PrimaryIndex.ID)).PrefixEnd()
		newKey, ok := kr.RewriteKey(key)
		if !ok {
			t.Fatalf("expected to match %s but didn't", key)
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if sqlbase.ID(id) != desc.ID+1 {
			t.Fatalf("got %d expected %d", id, desc.ID+1)
		}
	})

	t.Run("multi", func(t *testing.T) {
		kr, err := MakeKeyRewriterForNewTableIDs(
			[]*sqlbase.TableDescriptor{&desc, &sqlbase.DescriptorTable},
			map[sqlbase.ID]sqlbase.ID{
				desc.ID:                    desc.ID + 10,
				sqlbase.DescriptorTable.ID: sqlbase.DescriptorTable.ID + 10,
			},
		)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		key := keys.MakeRowSentinelKey(sqlbase.MakeIndexKeyPrefix(&desc, desc.PrimaryIndex.ID))
		newKey, ok := kr.RewriteKey(key)
		if !ok {
			t.Fatalf("expected to match %s but didn't", key)
		}
		_, id, err := encoding.DecodeUvarintAscending(newKey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if sqlbase.ID(id) != desc.ID+10 {
			t.Fatalf("got %d expected %d", id, desc.ID+1)
		}
	})
}
