// Copyright 2017 The Cockroach Authors.
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
