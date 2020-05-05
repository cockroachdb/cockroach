// Copyright 2015 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestKeyAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenSysCodec := keys.SystemSQLCodec
	ten5Codec := keys.MakeSQLCodec(roachpb.MakeTenantID(5))
	testCases := []struct {
		key roachpb.Key
	}{
		{MakeDescMetadataKey(tenSysCodec, 123)},
		{MakeDescMetadataKey(tenSysCodec, 124)},
		{NewPublicTableKey(0, "BAR").Key(tenSysCodec)},
		{NewPublicTableKey(1, "BAR").Key(tenSysCodec)},
		{NewPublicTableKey(1, "foo").Key(tenSysCodec)},
		{NewPublicTableKey(2, "foo").Key(tenSysCodec)},
		{MakeDescMetadataKey(ten5Codec, 123)},
		{MakeDescMetadataKey(ten5Codec, 124)},
		{NewPublicTableKey(0, "BAR").Key(ten5Codec)},
		{NewPublicTableKey(1, "BAR").Key(ten5Codec)},
		{NewPublicTableKey(1, "foo").Key(ten5Codec)},
		{NewPublicTableKey(2, "foo").Key(ten5Codec)},
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
