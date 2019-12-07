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
	testCases := []struct {
		key roachpb.Key
	}{
		{MakeDescMetadataKey(123)},
		{MakeDescMetadataKey(124)},
		{NewPublicTableKey(0, "BAR").Key()},
		{NewPublicTableKey(1, "BAR").Key()},
		{NewPublicTableKey(1, "foo").Key()},
		{NewPublicTableKey(2, "foo").Key()},
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
