// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package tests

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/pkg/errors"
)

// CheckEndTransactionTrigger verifies that an EndTransactionRequest
// that includes intents for the SystemDB keys sets the proper trigger.
func CheckEndTransactionTrigger(args storagebase.FilterArgs) *roachpb.Error {
	req, ok := args.Req.(*roachpb.EndTransactionRequest)
	if !ok {
		return nil
	}

	if !req.Commit {
		// This is a rollback: skip trigger verification.
		return nil
	}

	modifiedSpanTrigger := req.InternalCommitTrigger.GetModifiedSpanTrigger()
	modifiedSystemConfigSpan := modifiedSpanTrigger != nil && modifiedSpanTrigger.SystemConfigSpan

	var hasSystemKey bool
	for _, span := range req.IntentSpans {
		if bytes.Compare(span.Key, keys.SystemConfigSpan.Key) >= 0 &&
			bytes.Compare(span.Key, keys.SystemConfigSpan.EndKey) < 0 {
			hasSystemKey = true
			break
		}
	}
	// If the transaction in question has intents in the system span, then
	// modifiedSystemConfigSpan should always be true. However, it is possible
	// for modifiedSystemConfigSpan to be set, even though no system keys are
	// present. This can occur with certain conditional DDL statements (e.g.
	// "CREATE TABLE IF NOT EXISTS"), which set the SystemConfigTrigger
	// aggressively but may not actually end up changing the system DB depending
	// on the current state.
	// For more information, see the related comment at the beginning of
	// planner.makePlan().
	if hasSystemKey && !modifiedSystemConfigSpan {
		return roachpb.NewError(errors.Errorf("EndTransaction hasSystemKey=%t, but hasSystemConfigTrigger=%t",
			hasSystemKey, modifiedSystemConfigSpan))
	}

	return nil
}
