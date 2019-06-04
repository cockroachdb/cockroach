// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package causer

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

type fooErr struct {
	error
}

func isFoo(err error) bool {
	return Visit(err, func(err error) bool {
		_, ok := err.(*fooErr)
		return ok
	})
}

func TestCauserVisit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	loudErr := errors.Wrap(errors.New("root cause"), "this happened")
	quietErr := errors.Wrap(errors.Wrap(&fooErr{loudErr}, "foo"), "bar")

	if isFoo(loudErr) {
		t.Fatal("non-benign error marked as benign")
	}
	if !isFoo(&fooErr{errors.New("foo")}) {
		t.Fatal("foo error not recognized as such")
	}
	if !isFoo(quietErr) {
		t.Fatal("wrapped foo error not recognized as such")
	}
	if isFoo(nil) {
		t.Fatal("nil error should not be foo")
	}
}
