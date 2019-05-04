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

package errpgcode_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/assert"
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/errpgcode"
	"github.com/cockroachdb/cockroach/pkg/errors/issuelink"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/pgcode"
	"github.com/pkg/errors"
)

func TestPGCode(t *testing.T) {
	tt := testutils.T{T: t}

	testData := []struct {
		outerCode    string
		innerCode    string
		innerErr     error
		expectedCode string
	}{
		{"foo", "bar", errors.New("world"), "bar"},
		{"foo", pgcode.Uncategorized, errors.New("world"), "foo"},
		{pgcode.Uncategorized, "foo", errors.New("world"), "foo"},
		{"foo", "bar", assert.WithAssertionFailure(errors.New("world")), pgcode.Internal},
		{"foo", "bar", issuelink.UnimplementedError(issuelink.IssueLink{}, "world"), pgcode.FeatureNotSupported},
		{"foo", pgcode.Internal, errors.New("world"), pgcode.Internal},
		{pgcode.Internal, "foo", errors.New("world"), pgcode.Internal},
	}

	for _, t := range testData {
		tt.Run(fmt.Sprintf("%s/%s/%s", t.outerCode, t.innerCode, t.innerErr),
			func(tt testutils.T) {
				origErr := t.innerErr
				origErr = errpgcode.WithCandidateCode(origErr, t.innerCode)
				origErr = errpgcode.WithCandidateCode(origErr, t.outerCode)

				theTest := func(tt testutils.T, err error) {
					tt.Check(markers.Is(err, t.innerErr))
					tt.CheckEqual(err.Error(), t.innerErr.Error())

					tt.Check(errpgcode.HasCandidateCode(err))
					if _, ok := markers.If(err, func(err error) (interface{}, bool) { return nil, errpgcode.IsCandidateCode(err) }); !ok {
						tt.Error("woops")
					}

					code := errpgcode.GetPGCode(err, errpgcode.SimpleComputeDefaultCode, errpgcode.SimpleCombineCodes)
					tt.CheckEqual(code, t.expectedCode)

					errV := fmt.Sprintf("%+v", err)
					tt.Check(strings.Contains(errV, "code: "+t.innerCode))
					tt.Check(strings.Contains(errV, "code: "+t.outerCode))
				}

				tt.Run("local", func(tt testutils.T) { theTest(tt, origErr) })

				enc := errbase.EncodeError(origErr)
				newErr := errbase.DecodeError(enc)

				tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })

			})
	}

}
