// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestConstraintName(t *testing.T) {
	testCases := []struct {
		err                error
		expectedConstraint string
	}{
		{WithConstraintName(fmt.Errorf("test"), "fk1"), "fk1"},
		{WithConstraintName(WithConstraintName(fmt.Errorf("test"), "fk1"), "fk2"), "fk2"},
		{WithConstraintName(WithCandidateCode(fmt.Errorf("test"), pgcode.FeatureNotSupported), "fk1"), "fk1"},
		{New(pgcode.Uncategorized, "i am an error"), ""},
		{WithCandidateCode(WithConstraintName(errors.Newf("test"), "fk1"), pgcode.System), "fk1"},
		{fmt.Errorf("something else"), ""},
		{WithConstraintName(fmt.Errorf("test"), "fk\"⌂"), "fk\"⌂"},
	}

	for _, tc := range testCases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			constraint := GetConstraintName(tc.err)
			require.Equal(t, tc.expectedConstraint, constraint)
			// Test that the constraint name survives an encode/decode cycle.
			enc := errors.EncodeError(context.Background(), tc.err)
			err2 := errors.DecodeError(context.Background(), enc)
			constraint = GetConstraintName(err2)
			require.Equal(t, tc.expectedConstraint, constraint)
		})
	}
}
