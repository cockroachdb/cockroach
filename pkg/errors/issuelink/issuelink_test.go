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

package issuelink_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/issuelink"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

func TestIssueLink(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("world")

	err := errors.Wrap(
		issuelink.WithIssueLink(
			errors.WithStack(
				issuelink.WithIssueLink(origErr, issuelink.IssueLink{IssueURL: "123", Detail: "foo"}),
			),
			issuelink.IssueLink{IssueURL: "456", Detail: "bar"},
		),
		"hello")

	theTest := func(tt testutils.T, err error) {
		tt.Check(markers.Is(err, origErr))
		tt.CheckEqual(err.Error(), "hello: world")

		tt.Check(issuelink.HasIssueLink(err))
		if _, ok := markers.If(err, func(err error) (interface{}, bool) { return nil, issuelink.IsIssueLink(err) }); !ok {
			t.Error("woops")
		}

		details := issuelink.GetAllIssueLinks(err)
		tt.CheckDeepEqual(details, []issuelink.IssueLink{
			{IssueURL: "456", Detail: "bar"},
			{IssueURL: "123", Detail: "foo"},
		})

		errV := fmt.Sprintf("%+v", err)
		tt.Check(strings.Contains(errV, "issue: 123"))
		tt.Check(strings.Contains(errV, "issue: 456"))
		tt.Check(strings.Contains(errV, "detail: foo"))
		tt.Check(strings.Contains(errV, "detail: bar"))
	}

	tt.Run("local", func(tt testutils.T) { theTest(tt, err) })

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })

}
