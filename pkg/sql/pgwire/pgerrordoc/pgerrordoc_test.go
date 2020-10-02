// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerrordoc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWithDocsDetails(t *testing.T) {
	testCases := []struct {
		desc                 string
		err                  error
		expectedDetailsRegex string
	}{
		{
			desc:                 "generic error message",
			err:                  errors.Newf("hello"),
			expectedDetailsRegex: `^$`,
		},
		{
			desc: "proto retry error with no matches",
			err: roachpb.NewTransactionRetryWithProtoRefreshError(
				"test",
				uuid.MakeV4(),
				roachpb.Transaction{},
			),
			expectedDetailsRegex: `transaction-retry-error-reference\.html`,
		},
		{
			desc: "match uncertain interval",
			err: roachpb.NewTransactionRetryWithProtoRefreshError(
				roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, nil).Error(),
				uuid.MakeV4(),
				roachpb.Transaction{},
			),
			expectedDetailsRegex: `transaction-retry-error-reference\.html#readwithinuncertaintyinterval`,
		},
		{
			desc: "match uncertain interval, but with another details message",
			err: errors.WithDetail(
				roachpb.NewTransactionRetryWithProtoRefreshError(
					roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, nil).Error(),
					uuid.MakeV4(),
					roachpb.Transaction{},
				),
				"i am a dog",
			),
			expectedDetailsRegex: `i am a dog.*\n--\n.*transaction-retry-error-reference\.html#readwithinuncertaintyinterval`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			postErr := WithDocsDetails(tc.err)
			details := errors.FlattenDetails(postErr)
			require.Regexp(t, tc.expectedDetailsRegex, details)
		})
	}
}
