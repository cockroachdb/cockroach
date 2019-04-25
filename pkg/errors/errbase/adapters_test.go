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

package errbase_test

import (
	goErr "errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/kr/pretty"
	pkgErr "github.com/pkg/errors"
)

func network(t *testing.T, err error) error {
	t.Helper()
	enc := errbase.EncodeError(err)
	t.Logf("encoded: %# v", pretty.Formatter(enc))
	newErr := errbase.DecodeError(enc)
	t.Logf("decoded: %# v", pretty.Formatter(newErr))
	return newErr
}

func TestAdaptBaseGoErr(t *testing.T) {
	// Base Go errors are preserved completely.
	origErr := goErr.New("world")
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	newErr := network(t, origErr)

	tt := testutils.T{T: t}
	// The library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// It actually preserves the full structure of the message,
	// including its Go type.
	tt.CheckDeepEqual(newErr, origErr)
}

func TestAdaptPkgWithMessage(t *testing.T) {
	// Simple message wrappers from github.com/pkg/errors are preserved
	// completely.
	origErr := pkgErr.WithMessage(goErr.New("world"), "hello")
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	newErr := network(t, origErr)

	tt := testutils.T{T: t}
	// The library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// It actually preserves the full structure of the message,
	// including its Go type.
	tt.CheckDeepEqual(newErr, origErr)
}

func TestAdaptPkgFundamental(t *testing.T) {
	// The "simple error" from github.com/pkg/errors is not
	// that simple because it contains a stack trace. However,
	// we are happy to preserve this stack trace.
	origErr := pkgErr.New("hello")
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	tt := testutils.T{T: t}

	// Show that there is indeed a stack trace.
	theStack := fmt.Sprintf("%+v", errbase.GetSafeDetails(origErr))
	tt.Check(strings.Contains(theStack, "adapters_test.go"))

	newErr := network(t, origErr)

	// In any case, the library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// The decoded error does not compare equal, since
	// we had to change the type to preserve the stack trace.
	tt.Check(!reflect.DeepEqual(origErr, newErr))

	// However, it remembers what type the error is coming from.
	errV := fmt.Sprintf("%+v", newErr)
	tt.Check(strings.Contains(errV, "github.com/pkg/errors/*errors.fundamental"))

	// Also, the decoded error does include the stack trace.
	details := errbase.GetSafeDetails(newErr).SafeDetails
	tt.Check(len(details) > 0 && strings.Contains(details[0], "adapters_test.go"))

	// Moreover, if we re-encode and re-decode, that will be preserved exactly!
	newErr2 := network(t, newErr)
	tt.CheckDeepEqual(newErr2, newErr)
}

func TestAdaptPkgWithStack(t *testing.T) {
	// The "with stack" wrapper from github.com/pkg/errors cannot be
	// serialized exactly, however we are happy to preserve this stack
	// trace.
	origErr := pkgErr.WithStack(goErr.New("hello"))
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	tt := testutils.T{T: t}
	// Show that there is indeed a stack trace.
	theStack := fmt.Sprintf("%+v", errbase.GetSafeDetails(origErr))
	tt.Check(strings.Contains(theStack, "adapters_test.go"))

	newErr := network(t, origErr)

	// In any case, the library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// The decoded error does not compare equal, since
	// we had to change the type to preserve the stack trace.
	tt.Check(!reflect.DeepEqual(newErr, origErr))

	// However, it does include the stack trace.
	details := errbase.GetSafeDetails(newErr).SafeDetails
	tt.Check(len(details) > 0 && strings.Contains(details[0], "adapters_test.go"))

	// Moreover, if we re-encode and re-decode, that will be preserved exactly!
	newErr2 := network(t, newErr)
	tt.CheckDeepEqual(newErr2, newErr)
}

func TestAdaptProtoErrors(t *testing.T) {
	// If an error type has a proto representation already,
	// it will be preserved exactly.
	origErr := &roachpb.NotLeaseHolderError{}
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	newErr := network(t, origErr)

	tt := testutils.T{T: t}

	// In any case, the library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// Moreover, it preserves the entire structure.
	tt.CheckDeepEqual(newErr, origErr)
}

func TestAdaptProtoErrorsWithWrapper(t *testing.T) {
	// proto-native error types are preserved exactly
	// together with their wrappers.
	rErr := &roachpb.NotLeaseHolderError{}
	origErr := pkgErr.WithMessage(rErr, "hello roachpb")
	t.Logf("start err: %# v", pretty.Formatter(origErr))

	newErr := network(t, origErr)

	tt := testutils.T{T: t}

	// In any case, the library preserves the error message.
	tt.CheckEqual(newErr.Error(), origErr.Error())

	// Moreover, it preserves the entire structure.
	tt.CheckDeepEqual(newErr, origErr)
}
