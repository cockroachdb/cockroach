// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func TestPGError(t *testing.T) {
	const msg = "err"
	const code = "abc"

	checkErr := func(pErr *pgerror.Error, errMsg string) {
		if pErr.Code != code {
			t.Fatalf("got: %q\nwant: %q", pErr.Code, code)
		}
		if pErr.Message != errMsg {
			t.Fatalf("got: %q\nwant: %q", pErr.Message, errMsg)
		}
		const want = `errors_test.go`
		match, err := regexp.MatchString(want, pErr.Source.File)
		if err != nil {
			t.Fatal(err)
		}
		if !match {
			t.Fatalf("got: %q\nwant: %q", pErr.Source.File, want)
		}
	}

	// Test NewError.
	pErr := pgerror.Flatten(pgerror.New(code, msg))
	checkErr(pErr, msg)

	pErr = pgerror.Flatten(pgerror.New(code, "bad%format"))
	checkErr(pErr, "bad%format")

	// Test NewErrorf.
	const prefix = "prefix"
	pErr = pgerror.Flatten(pgerror.Newf(code, "%s: %s", prefix, msg))
	expected := fmt.Sprintf("%s: %s", prefix, msg)
	checkErr(pErr, expected)
}

func TestIsSQLRetryableError(t *testing.T) {
	errAmbiguous := &roachpb.AmbiguousResultError{}
	if !pgerror.IsSQLRetryableError(roachpb.NewError(errAmbiguous).GoError()) {
		t.Fatalf("%s should be a SQLRetryableError", errAmbiguous)
	}
}
