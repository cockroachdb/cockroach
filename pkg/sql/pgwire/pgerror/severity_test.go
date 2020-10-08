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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSeverity(t *testing.T) {
	testCases := []struct {
		err              error
		expectedSeverity string
	}{
		{WithSeverity(fmt.Errorf("notice me"), "NOTICE ME"), "NOTICE ME"},
		{WithSeverity(WithSeverity(fmt.Errorf("notice me"), "IGNORE ME"), "NOTICE ME"), "NOTICE ME"},
		{WithSeverity(WithCandidateCode(fmt.Errorf("notice me"), pgcode.FeatureNotSupported), "NOTICE ME"), "NOTICE ME"},
		{New(pgcode.Uncategorized, "i am an error"), "ERROR"},
		{WithCandidateCode(WithSeverity(errors.Newf("i am not an error"), "NOT AN ERROR"), pgcode.System), "NOT AN ERROR"},
		{fmt.Errorf("something else"), "ERROR"},
	}

	for _, tc := range testCases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			severity := GetSeverity(tc.err)
			require.Equal(t, tc.expectedSeverity, severity)
		})
	}
}
