// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func testingGetDescriptor(
	ctx context.Context, kvDB *kv.DB, codec keys.SQLCodec, database string, object string,
) (hlc.Timestamp, *descpb.Descriptor) {
	dKey := NewDatabaseKey(database)
	gr, err := kvDB.Get(ctx, dKey.Key(codec))
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("database missing")
	}
	dbDescID := descpb.ID(gr.ValueInt())

	tKey := NewPublicTableKey(dbDescID, object)
	gr, err = kvDB.Get(ctx, tKey.Key(codec))
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("object missing")
	}

	descKey := MakeDescMetadataKey(codec, descpb.ID(gr.ValueInt()))
	desc := &descpb.Descriptor{}
	ts, err := kvDB.GetProtoTs(ctx, descKey, desc)
	if err != nil || desc.Equal(descpb.Descriptor{}) {
		log.Fatalf(ctx, "proto with id %d missing. err: %v", gr.ValueInt(), err)
	}
	return ts, desc
}

// TestingGetTableDescriptor retrieves a table descriptor directly from the KV
// layer.
func TestingGetTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *descpb.TableDescriptor {
	ctx := context.Background()
	ts, desc := testingGetDescriptor(ctx, kvDB, codec, database, table)
	tableDesc := TableFromDescriptor(desc, ts)
	if tableDesc == nil {
		return nil
	}
	_, err := maybeFillInDescriptor(ctx, kvDB, codec, tableDesc, false)
	if err != nil {
		log.Fatalf(ctx, "failure to fill in descriptor. err: %v", err)
	}
	return tableDesc
}

// TestingGetImmutableTableDescriptor retrieves an immutable table descriptor
// directly from the KV layer.
func TestingGetImmutableTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *ImmutableTableDescriptor {
	return NewImmutableTableDescriptor(*TestingGetTableDescriptor(kvDB, codec, database, table))
}
