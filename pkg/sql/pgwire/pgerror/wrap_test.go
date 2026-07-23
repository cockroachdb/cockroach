// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgerror_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

func TestWrap(t *testing.T) {
	testData := []struct {
		err error
	}{
		{errors.New("woo")},
		{&kvpb.TransactionRetryWithProtoRefreshError{}},
		{&kvpb.AmbiguousResultError{}},
	}

	for i, test := range testData {
		werr := pgerror.Wrap(test.err, pgcode.Syntax, "woo")

		if !errors.Is(werr, test.err) {
			t.Errorf("%d: original error not preserved; expected %+v, got %+v", i, test.err, werr)
		}
	}
}
