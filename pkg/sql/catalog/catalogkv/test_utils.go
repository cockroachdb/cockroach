// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// TestingGetTableDescriptor retrieves a table descriptor directly from the KV
// layer.
//
// TODO(ajwerner): Move this to catalogkv and/or question the very existence of
// this function. Consider renaming to TestingGetTableDescriptorByName or
// removing it altogether.
func TestingGetTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *sqlbase.ImmutableTableDescriptor {
	return TestingGetImmutableTableDescriptor(kvDB, codec, database, table)
}

// TestingGetImmutableTableDescriptor retrieves an immutable table descriptor
// directly from the KV layer.
func TestingGetImmutableTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *sqlbase.ImmutableTableDescriptor {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TableObject, false /* mutable */, database, table,
	).(*sqlbase.ImmutableTableDescriptor)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetMutableExistingTableDescriptor retrieves a MutableTableDescriptor
// directly from the KV layer.
func TestingGetMutableExistingTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *sqlbase.MutableTableDescriptor {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TableObject, true /* mutable */, database, table,
	).(*sqlbase.MutableTableDescriptor)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetTypeDescriptor retrieves a type descriptor directly from the kv layer.
//
// This function should be moved wherever TestingGetTableDescriptor is moved.
func TestingGetTypeDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, object string,
) *typedesc.ImmutableTypeDescriptor {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TypeObject, false /* mutable */, database, object,
	).(*typedesc.ImmutableTypeDescriptor)
	if !ok {
		return nil
	}
	return desc
}

func testingGetObjectDescriptor(
	kvDB *kv.DB,
	codec keys.SQLCodec,
	kind tree.DesiredObjectKind,
	mutable bool,
	database string,
	object string,
) (desc catalog.Descriptor) {
	ctx := context.TODO()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		lookupFlags := tree.ObjectLookupFlagsWithRequired()
		lookupFlags.IncludeOffline = true
		lookupFlags.IncludeDropped = true
		lookupFlags.DesiredObjectKind = kind
		lookupFlags.RequireMutable = mutable
		desc, err = UncachedPhysicalAccessor{}.GetObjectDesc(ctx,
			txn, cluster.MakeTestingClusterSettings(), codec,
			database, "public", object, lookupFlags)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return desc
}
