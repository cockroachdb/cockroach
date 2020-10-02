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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// WithDocsDetails appends a doc URL that contains information for
// commonly seen error messages.
func WithDocsDetails(err error) error {
	if protoRefreshErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(err, &protoRefreshErr) {
		var pageAnchor string

		// TransactionRetryWithProtoRefreshError does not actually wrap the error message,
		// so we're forced to read the message string.
		for _, match := range []string{
			roachpb.RETRY_WRITE_TOO_OLD.String(),
			roachpb.RETRY_SERIALIZABLE.String(),
			roachpb.RETRY_ASYNC_WRITE_FAILURE.String(),
			roachpb.RETRY_COMMIT_DEADLINE_EXCEEDED.String(),
			roachpb.ABORT_REASON_ABORTED_RECORD_FOUND.String(),
			roachpb.ABORT_REASON_CLIENT_REJECT.String(),
			roachpb.ABORT_REASON_ABORT_SPAN.String(),
			roachpb.ABORT_REASON_NEW_LEASE_PREVENTS_TXN.String(),
			roachpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED.String(),
			"ReadWithinUncertaintyInterval",
		} {
			if strings.Contains(protoRefreshErr.Msg, match) {
				pageAnchor = strings.ToLower(match)
				break
			}
		}

		url := base.DocsURL("transaction-retry-error-reference.html")
		if pageAnchor != "" {
			url += "#" + pageAnchor
		}
		err = errors.WithDetailf(
			err,
			"For more information about this error, see %s",
			url,
		)
	}
	return err
}
