// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAllTypesCastableToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, typ := range types.Scalar {
		if ok, _ := isCastDeepValid(typ, types.String); !ok {
			t.Errorf("%s is not castable to STRING, all types should be", typ)
		}
	}
}

func TestAllTypesCastableFromString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, typ := range types.Scalar {
		if ok, _ := isCastDeepValid(types.String, typ); !ok {
			t.Errorf("%s is not castable from STRING, all types should be", typ)
		}
	}
}
