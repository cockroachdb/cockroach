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

package hintdetail_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/assert"
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/hintdetail"
	"github.com/cockroachdb/cockroach/pkg/errors/issuelink"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/stdstrings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

func TestDetail(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("world")

	err := errors.Wrap(
		hintdetail.WithDetail(
			errors.WithStack(
				hintdetail.WithDetail(origErr, "foo"),
			),
			"bar",
		),
		"hello")

	theTest := func(tt testutils.T, err error) {
		tt.Check(markers.Is(err, origErr))
		tt.CheckEqual(err.Error(), "hello: world")

		details := hintdetail.GetAllDetails(err)
		tt.CheckDeepEqual(details, []string{"foo", "bar"})

		errV := fmt.Sprintf("%+v", err)
		tt.Check(strings.Contains(errV, "\nfoo"))
		tt.Check(strings.Contains(errV, "\nbar"))
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })
}

func TestHint(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("world")

	err := errors.Wrap(
		hintdetail.WithHint(
			hintdetail.WithHint(
				errors.WithStack(
					hintdetail.WithHint(origErr, "foo"),
				),
				"bar",
			),
			"foo",
		),
		"hello")

	theTest := func(tt testutils.T, err error) {
		tt.Check(markers.Is(err, origErr))
		tt.CheckEqual(err.Error(), "hello: world")

		hints := hintdetail.GetAllHints(err)
		tt.CheckDeepEqual(hints, []string{"foo", "bar"})

		errV := fmt.Sprintf("%+v", err)
		tt.Check(strings.Contains(errV, "\nfoo"))
		tt.Check(strings.Contains(errV, "\nbar"))
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })
}

func TestIssueLinkHint(t *testing.T) {
	tt := testutils.T{T: t}

	err := issuelink.WithIssueLink(
		issuelink.WithIssueLink(
			errors.New("hello world"),
			issuelink.IssueLink{IssueURL: "foo"},
		),
		issuelink.IssueLink{IssueURL: "bar"},
	)

	theTest := func(tt testutils.T, err error) {
		tt.CheckEqual(err.Error(), "hello world")

		hints := hintdetail.GetAllHints(err)
		tt.Assert(len(hints) == 2)

		tt.CheckEqual(hints[0], "See: foo")
		tt.CheckEqual(hints[1], "See: bar")
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })
}

func TestUnimplementedHint(t *testing.T) {
	tt := testutils.T{T: t}

	err := issuelink.UnimplementedError(issuelink.IssueLink{IssueURL: "woo"}, "hello world")

	theTest := func(tt testutils.T, err error) {
		tt.CheckEqual(err.Error(), "hello world")

		hints := hintdetail.GetAllHints(err)
		tt.Assert(len(hints) > 0)

		tt.CheckEqual(hints[0], issuelink.UnimplementedErrorHint+"\nSee: woo")
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })
}

func TestUnimplementedNoIssueHint(t *testing.T) {
	tt := testutils.T{T: t}

	err := issuelink.UnimplementedError(issuelink.IssueLink{}, "hello world")

	theTest := func(tt testutils.T, err error) {
		tt.CheckEqual(err.Error(), "hello world")

		hints := hintdetail.GetAllHints(err)
		tt.Assert(len(hints) > 0)

		tt.CheckEqual(hints[0], issuelink.UnimplementedErrorHint+stdstrings.IssueReferral)
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })
}

func TestAssertionHints(t *testing.T) {
	tt := testutils.T{T: t}

	err := assert.WithAssertionFailure(errors.New("hello world"))

	theTest := func(tt testutils.T, err error) {
		tt.CheckEqual(err.Error(), "hello world")

		hints := hintdetail.GetAllHints(err)
		tt.Assert(len(hints) > 0)

		tt.CheckEqual(hints[0], assert.AssertionErrorHint+stdstrings.IssueReferral)
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })
}

func TestMultiHintDetail(t *testing.T) {
	tt := testutils.T{T: t}

	err := errors.New("hello world")
	err = hintdetail.WithHint(err, "woo")
	err = hintdetail.WithHint(err, "waa")

	tt.CheckEqual(hintdetail.FlattenHints(err), "woo\n--\nwaa")

	err = hintdetail.WithDetail(err, "foo")
	err = hintdetail.WithDetail(err, "bar")
	tt.CheckEqual(hintdetail.FlattenDetails(err), "foo\n--\nbar")
}
