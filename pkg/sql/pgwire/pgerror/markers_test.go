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

package pgerror

import (
	"context"
	e1 "errors"
	"io"
	"regexp"
	"testing"

	e2 "github.com/pkg/errors"
)

func TestMarkerEquivalence(t *testing.T) {
	testData := []struct {
		expEq      bool
		err1       error
		err2       error
		expectedRe *regexp.Regexp
	}{
		{true, NewError(CodeSyntaxError, "woo"), NewError(CodeSyntaxError, "woo"), regexp.MustCompile(`woo\n.*pgerror.Error`)},
		{false, NewError(CodeSyntaxError, "woo"), NewError(CodeSystemError, "woo"), nil},
		{true, Wrapf(NewError(CodeSyntaxError, "woo"), CodeSystemError, "waa"), NewError(CodeSyntaxError, "woo"), nil},
		{true, e2.Wrap(NewError(CodeSyntaxError, "woo"), "waa"), NewError(CodeSyntaxError, "woo"), nil},
		{true, e2.WithMessage(e2.New("woo"), "waa"), e2.New("woo"), nil},
		{false, e1.New("woo"), e2.New("woo"), nil},

		{true, io.EOF, io.EOF, regexp.MustCompile(`EOF\n.*errorString`)},
		{true, io.EOF, IOEOFMarkerError, nil},
		{false, io.EOF, WithMarker(io.EOF), nil},
		{true, WithMarker(io.EOF), WithMarker(io.EOF), regexp.MustCompile(`EOF\n.*\n.*\n.*/markers_test.go`)},

		{true, context.Canceled, context.Canceled, regexp.MustCompile(`context canceled\n.*errorString`)},
		{true, context.Canceled, ContextCanceledMarkerError, nil},
		{false, context.Canceled, WithMarker(context.Canceled), nil},

		{true, WithSameMarker(context.Canceled, ContextCanceledMarkerError), ContextCanceledMarkerError, nil},
		{true, WithSameMarker(context.Canceled, WithMarker(context.Canceled)), WithMarker(context.Canceled), nil},
		{true, WithSameMarker(e2.New("woo"), context.Canceled), context.Canceled, nil},
		{false, WithSameMarker(e2.New("woo"), context.Canceled), WithMarker(context.Canceled), nil},
		{false, WithSameMarker(e2.New("woo"), context.Canceled), e2.New("woo"), nil},
	}

	for i, test := range testData {
		m1 := getMarker(test.err1)
		m2 := getMarker(test.err2)
		if test.expEq && m1 != m2 {
			t.Errorf("%d: unexpected marker difference:\n%s\n-- vs. --\n%s", i, m1, m2)
		} else if !test.expEq && m1 == m2 {
			t.Errorf("%d: unexpected marker equivalence:\n%s", i, m1)
		}

		if test.expectedRe != nil && !test.expectedRe.MatchString(m1) {
			t.Errorf("%d: marker does not match %q:\n%s", i, test.expectedRe, m1)
		}
	}
}
