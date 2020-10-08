// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror_test

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func TestPGError(t *testing.T) {
	const msg = "err"
	var code = pgcode.MakeCode("abc")

	checkErr := func(pErr *pgerror.Error, errMsg string) {
		if pgcode.MakeCode(pErr.Code) != code {
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
