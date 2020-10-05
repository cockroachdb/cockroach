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

// transactionRetryWithProtoRefreshMessages returns a slice of known
// TransactionRetryWithProtoRefreshError error messages.
// TransactionRetryWithProtoRefreshError does not actually wrap the error message,
// so we're forced to read the message string.
func transactionRetryWithProtoRefreshMessages() []string {
	var ret []string
	ret = append(ret, "ReadWithinUncertaintyInterval")
	for k := range roachpb.TransactionRetryReason_value {
		ret = append(ret, k)
	}
	for k := range roachpb.TransactionAbortedReason_value {
		ret = append(ret, k)
	}
	return ret
}

// WithRelevantLinks appends a doc URL that contains information for
// commonly seen error messages.
func WithRelevantLinks(err error) error {
	if protoRefreshErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(err, &protoRefreshErr) {
		var pageAnchor string

		for _, match := range transactionRetryWithProtoRefreshMessages() {
			if strings.Contains(protoRefreshErr.Msg, match) {
				pageAnchor = strings.ToLower(match)
				break
			}
		}

		url := base.DocsURL("transaction-retry-error-reference.html")
		if pageAnchor != "" {
			url += "#" + pageAnchor
		}
		err = errors.WithIssueLink(err, errors.IssueLink{IssueURL: url})
	}
	return err
}
