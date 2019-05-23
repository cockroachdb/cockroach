// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgerror_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func TestWrap(t *testing.T) {
	testData := []struct {
		err error
	}{
		{errors.New("woo")},
		{&roachpb.UnhandledRetryableError{}},
		{&roachpb.TransactionRetryWithProtoRefreshError{}},
		{&roachpb.AmbiguousResultError{}},
	}

	for i, test := range testData {
		werr := pgerror.Wrap(test.err, pgerror.CodeSyntaxError, "woo")

		if !errors.Is(werr, test.err) {
			t.Errorf("%d: original error not preserved; expected %+v, got %+v", i, test.err, werr)
		}
	}
}
