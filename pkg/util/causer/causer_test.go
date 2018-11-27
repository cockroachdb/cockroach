// Copyright 2018 The Cockroach Authors.
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
