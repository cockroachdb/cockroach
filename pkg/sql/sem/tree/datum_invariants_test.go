// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAllTypesCastableToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, typ := range types.Scalar {
		if err := resolveCast("", typ, types.String, true); err != nil {
			t.Errorf("%s is not castable to STRING, all types should be", typ)
		}
	}
}

func TestAllTypesCastableFromString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, typ := range types.Scalar {
		if err := resolveCast("", types.String, typ, true); err != nil {
			t.Errorf("%s is not castable from STRING, all types should be", typ)
		}
	}
}
