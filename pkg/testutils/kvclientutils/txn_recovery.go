// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvclientutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// PushExpectation expresses an expectation for CheckPushResult about what the
// push did.
type PushExpectation int

const (
	// ExpectPusheeTxnRecovery means we're expecting transaction recovery to be
	// performed (after finding a STAGING txn record).
	ExpectPusheeTxnRecovery PushExpectation = iota
	// ExpectPusheeTxnRecordNotFound means we're expecting the push to not find the
	// pushee txn record.
	ExpectPusheeTxnRecordNotFound
	// DontExpectAnything means we're not going to check the state in which the
	// pusher found the pushee's txn record.
	DontExpectAnything
)

// ExpectedTxnResolution expresses an expectation for CheckPushResult about the
// outcome of the push.
type ExpectedTxnResolution int

const (
	// ExpectAborted means that the pushee is expected to have been aborted. Note
	// that a committed txn that has been cleaned up also results in an ABORTED
	// result for a pusher.
	ExpectAborted ExpectedTxnResolution = iota
	// ExpectCommitted means that the pushee is expected to have found the pushee
	// to be committed - or STAGING in which case the push will have performed
	// successful transaction recovery.
	ExpectCommitted
)

// CheckPushResult pushes the specified txn and checks that the pushee's
// resolution is the expected one.
func CheckPushResult(
	ctx context.Context,
	db *kv.DB,
	tr *tracing.Tracer,
	txn roachpb.Transaction,
	expResolution ExpectedTxnResolution,
	pushExpectation PushExpectation,
) error {
	pushReq := roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		PusheeTxn: txn.TxnMeta,
		PushTo:    hlc.Timestamp{},
		PushType:  roachpb.PUSH_ABORT,
		// We're going to Force the push in order to not wait for the pushee to
		// expire.
		Force: true,
	}
	ba := roachpb.BatchRequest{}
	ba.Add(&pushReq)

	recCtx, collectRec, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test trace")
	defer cancel()

	resp, pErr := db.NonTransactionalSender().Send(recCtx, ba)
	if pErr != nil {
		return pErr.GoError()
	}

	var statusErr error
	pusheeStatus := resp.Responses[0].GetPushTxn().PusheeTxn.Status
	switch pusheeStatus {
	case roachpb.ABORTED:
		if expResolution != ExpectAborted {
			statusErr = errors.Errorf("transaction unexpectedly aborted")
		}
	case roachpb.COMMITTED:
		if expResolution != ExpectCommitted {
			statusErr = errors.Errorf("transaction unexpectedly committed")
		}
	default:
		return errors.Errorf("unexpected txn status: %s", pusheeStatus)
	}

	// Verify that we're not fooling ourselves and that checking for the implicit
	// commit actually caused the txn recovery procedure to run.
	recording := collectRec()
	var resolutionErr error
	switch pushExpectation {
	case ExpectPusheeTxnRecovery:
		expMsg := fmt.Sprintf("recovered txn %s", txn.ID.Short())
		if _, ok := recording.FindLogMessage(expMsg); !ok {
			resolutionErr = errors.Errorf(
				"recovery didn't run as expected (missing \"%s\"). recording: %s",
				expMsg, recording)
		}
	case ExpectPusheeTxnRecordNotFound:
		expMsg := "pushee txn record not found"
		if _, ok := recording.FindLogMessage(expMsg); !ok {
			resolutionErr = errors.Errorf(
				"push didn't run as expected (missing \"%s\"). recording: %s",
				expMsg, recording)
		}
	case DontExpectAnything:
	}

	return errors.CombineErrors(statusErr, resolutionErr)
}
