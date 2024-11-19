// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalogkeys

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestKeyAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenSysCodec := keys.SystemSQLCodec
	ten5Codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	testCases := []struct {
		key roachpb.Key
	}{
		{MakeDescMetadataKey(tenSysCodec, 123)},
		{MakeDescMetadataKey(tenSysCodec, 124)},
		{EncodeNameKey(tenSysCodec, &descpb.NameInfo{ParentID: 0, ParentSchemaID: 29, Name: "BAR"})},
		{EncodeNameKey(tenSysCodec, &descpb.NameInfo{ParentID: 1, ParentSchemaID: 29, Name: "BAR"})},
		{EncodeNameKey(tenSysCodec, &descpb.NameInfo{ParentID: 1, ParentSchemaID: 29, Name: "foo"})},
		{EncodeNameKey(tenSysCodec, &descpb.NameInfo{ParentID: 2, ParentSchemaID: 29, Name: "foo"})},
		{MakeDescMetadataKey(ten5Codec, 123)},
		{MakeDescMetadataKey(ten5Codec, 124)},
		{EncodeNameKey(ten5Codec, &descpb.NameInfo{ParentID: 0, ParentSchemaID: 29, Name: "BAR"})},
		{EncodeNameKey(ten5Codec, &descpb.NameInfo{ParentID: 1, ParentSchemaID: 29, Name: "BAR"})},
		{EncodeNameKey(ten5Codec, &descpb.NameInfo{ParentID: 1, ParentSchemaID: 29, Name: "foo"})},
		{EncodeNameKey(ten5Codec, &descpb.NameInfo{ParentID: 2, ParentSchemaID: 29, Name: "foo"})},
	}
	var lastKey roachpb.Key
	for i, test := range testCases {
		resultAddr, err := keys.Addr(test.key)
		if err != nil {
			t.Fatal(err)
		}
		result := resultAddr.AsRawKey()
		if result.Compare(lastKey) <= 0 {
			t.Errorf("%d: key address %q is <= %q", i, result, lastKey)
		}
		lastKey = result
	}
}
