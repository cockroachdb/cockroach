// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/testutils"
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
		{"foo", "bar", errors.WithAssertionFailure(errors.New("world")), pgcode.Internal},
		{"foo", "bar", errors.UnimplementedError(errors.IssueLink{}, "world"), pgcode.FeatureNotSupported},
		{"foo", pgcode.Internal, errors.New("world"), pgcode.Internal},
		{pgcode.Internal, "foo", errors.New("world"), pgcode.Internal},
	}

	for _, t := range testData {
		tt.Run(fmt.Sprintf("%s/%s/%s", t.outerCode, t.innerCode, t.innerErr),
			func(tt testutils.T) {
				origErr := t.innerErr
				origErr = pgerror.WithCandidateCode(origErr, t.innerCode)
				origErr = pgerror.WithCandidateCode(origErr, t.outerCode)

				theTest := func(tt testutils.T, err error) {
					tt.Check(errors.Is(err, t.innerErr))
					tt.CheckEqual(err.Error(), t.innerErr.Error())

					tt.Check(pgerror.HasCandidateCode(err))
					if _, ok := errors.If(err, func(err error) (interface{}, bool) { return nil, pgerror.IsCandidateCode(err) }); !ok {
						tt.Error("woops")
					}

					code := pgerror.GetPGCodeInternal(err, pgerror.ComputeDefaultCode)
					tt.CheckEqual(code, t.expectedCode)

					errV := fmt.Sprintf("%+v", err)
					tt.Check(strings.Contains(errV, "code: "+t.innerCode))
					tt.Check(strings.Contains(errV, "code: "+t.outerCode))
				}

				tt.Run("local", func(tt testutils.T) { theTest(tt, origErr) })

				enc := errors.EncodeError(context.TODO(), origErr)
				newErr := errors.DecodeError(context.TODO(), enc)

				tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })

			})
	}

}
