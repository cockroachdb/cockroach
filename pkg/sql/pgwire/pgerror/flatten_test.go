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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/testutils"
	"github.com/stretchr/testify/require"
)

func TestFlatten(t *testing.T) {
	baseErr := errors.New("woo")

	testData := []struct {
		err     error
		checker func(t testutils.T, pgErr *pgerror.Error)
	}{
		{
			baseErr,
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				// Errors without code flatten to Uncategorized.
				t.CheckEqual(pgcode.MakeCode(e.Code), pgcode.Uncategorized)
				t.CheckEqual(e.Severity, "ERROR")
			},
		},
		{
			pgerror.WithCandidateCode(baseErr, pgcode.Syntax),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(pgcode.MakeCode(e.Code), pgcode.Syntax)
			},
		},
		{
			pgerror.WithSeverity(baseErr, "DEBUG"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.Severity, "DEBUG")
			},
		},
		{
			errors.WithHint(baseErr, "My Hint"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.Hint, "My Hint")
			},
		},
		{
			errors.WithDetail(baseErr, "My Detail"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.Detail, "My Detail")
			},
		},
		{
			unimplemented.New("woo", "woo"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(pgcode.MakeCode(e.Code), pgcode.FeatureNotSupported)
				t.CheckRegexpEqual(e.Hint, "You have attempted to use a feature that is not yet implemented")
				t.CheckRegexpEqual(e.Hint, "support form")
			},
		},
		{
			errors.AssertionFailedf("woo"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(pgcode.MakeCode(e.Code), pgcode.Internal)
				t.CheckRegexpEqual(e.Hint, "You have encountered an unexpected error")
				t.CheckRegexpEqual(e.Hint, "support form")
			},
		},
		{
			errors.Wrap(&roachpb.TransactionRetryWithProtoRefreshError{Msg: "woo"}, ""),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckRegexpEqual(e.Message, "restart transaction: .* woo")
				t.CheckEqual(pgcode.MakeCode(e.Code), pgcode.SerializationFailure)
			},
		},
		{
			errors.Wrap(&roachpb.AmbiguousResultError{Message: "woo"}, ""),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckRegexpEqual(e.Message, "result is ambiguous.*woo")
				t.CheckEqual(pgcode.MakeCode(e.Code), pgcode.StatementCompletionUnknown)
			},
		},
		{
			errors.Wrap(
				roachpb.NewTransactionRetryWithProtoRefreshError(
					"test",
					uuid.MakeV4(),
					roachpb.Transaction{},
				),
				"",
			),
			func(t testutils.T, e *pgerror.Error) {
				require.Regexp(t, `transaction-retry-error-reference\.html`, e.Hint)
			},
		},
		{
			errors.Wrap(
				roachpb.NewTransactionRetryWithProtoRefreshError(
					roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, hlc.Timestamp{}, nil).Error(),
					uuid.MakeV4(),
					roachpb.Transaction{},
				),
				"",
			),
			func(t testutils.T, e *pgerror.Error) {
				require.Regexp(t, `transaction-retry-error-reference\.html#readwithinuncertaintyinterval`, e.Hint)
			},
		},
		{
			errors.Wrap(
				roachpb.NewTransactionRetryWithProtoRefreshError(
					roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "").Error(),
					uuid.MakeV4(),
					roachpb.Transaction{},
				),
				"",
			),
			func(t testutils.T, e *pgerror.Error) {
				require.Regexp(t, `transaction-retry-error-reference\.html#retry_serializable`, e.Hint)
			},
		},
		{
			errors.Wrap(
				roachpb.NewTransactionRetryWithProtoRefreshError(
					roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_PUSHER_ABORTED).Error(),
					uuid.MakeV4(),
					roachpb.Transaction{},
				),
				"",
			),
			func(t testutils.T, e *pgerror.Error) {
				require.Regexp(t, `transaction-retry-error-reference\.html#abort_reason_pusher_aborted`, e.Hint)
			},
		},
	}
	tt := testutils.T{T: t}

	for _, test := range testData {
		// tt.Logf("input error: %# v", pretty.Formatter(test.err))
		pgErr := pgerror.Flatten(test.err)
		// tt.Logf("pg error: %# v", pretty.Formatter(pgErr))

		// Common checks for all errors.
		tt.CheckEqual(pgErr.Source.File, "flatten_test.go")
		tt.CheckEqual(pgErr.Source.Function, "TestFlatten")

		// Per-test specific checks.
		test.checker(tt, pgErr)
	}
}
